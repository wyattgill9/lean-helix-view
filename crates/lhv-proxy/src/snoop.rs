//! The snoop: reads a *copy* of the client→server stream (never the forwarded
//! bytes) and tracks the session well enough to know *where* the cursor is and
//! *whether* a goal query is legal.
//!
//! - **Session state:** pre-init → initialized (from `initialized`), and the
//!   open-document set + versions (from `didOpen` / `didClose` / `didChange`).
//!   A focus is legal only when initialized **and** its document is open.
//! - **Focus:** `(uri, position)` from the most recent explicit position request
//!   in the configurable trigger set. `didChange` bumps the version and offers
//!   an edit-derived cursor as a *fallback* — last-event-wins means a later
//!   explicit request overrides it, and it's only used when none arrives.
//!
//! The trigger set is **data, not a hardcoded assumption** ([`default_triggers`]
//! is just the default): a cadence capture tells you which methods Helix
//! actually fires on idle, and you tune the set accordingly.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lhv_wire::{DocRef, Position};
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;

use crate::json::parse_position;
use crate::query::Querier;

/// The default explicit-position trigger set: the standard
/// `TextDocumentPositionParams`-carrying requests.
pub fn default_triggers() -> Vec<String> {
    [
        "textDocument/hover",
        "textDocument/completion",
        "textDocument/definition",
        "textDocument/references",
        "textDocument/documentHighlight",
        "textDocument/signatureHelp",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// Resolve a (possibly empty) user-supplied trigger list: empty → the default
/// set; otherwise normalize bare names (`hover` → `textDocument/hover`).
pub fn resolve_triggers(raw: Vec<String>) -> Vec<String> {
    if raw.is_empty() {
        return default_triggers();
    }
    raw.into_iter()
        .map(|m| {
            if m.contains('/') {
                m
            } else {
                format!("textDocument/{m}")
            }
        })
        .collect()
}

/// A target the querier should query goals for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Focus {
    pub doc: DocRef,
    pub position: Position,
}

/// What a single client message yielded — rich enough for both the cadence
/// capture and focus emission.
#[derive(Debug, Default, Clone)]
pub struct Observation {
    pub method: Option<String>,
    pub uri: Option<String>,
    pub position: Option<Position>,
    pub version: Option<i32>,
    /// In the active trigger set?
    pub is_trigger: bool,
    /// Set iff this message produced a (legal) focus to query.
    pub focus: Option<Focus>,
}

/// The snoop's session state machine.
pub struct Snoop {
    initialized: bool,
    /// Open documents → their latest known version.
    docs: HashMap<String, i32>,
    triggers: Vec<String>,
}

impl Snoop {
    pub fn new(triggers: Vec<String>) -> Self {
        Self {
            initialized: false,
            docs: HashMap::new(),
            triggers,
        }
    }

    /// Update session state from one client message and report what it carried.
    pub fn observe(&mut self, body: &[u8]) -> Observation {
        let mut obs = Observation::default();
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return obs;
        };
        let method = value.get("method").and_then(Value::as_str);
        obs.method = method.map(str::to_owned);
        obs.is_trigger = method.is_some_and(|m| self.is_trigger(m));
        let params = value.get("params");

        match method {
            Some("initialized") => self.initialized = true,
            Some("textDocument/didOpen") => {
                if let Some(td) = params.and_then(|p| p.get("textDocument")) {
                    if let Some(uri) = td.get("uri").and_then(Value::as_str) {
                        let version = td.get("version").and_then(Value::as_i64).unwrap_or(0) as i32;
                        self.docs.insert(uri.to_owned(), version);
                        obs.uri = Some(uri.to_owned());
                        obs.version = Some(version);
                    }
                }
            }
            Some("textDocument/didClose") => {
                if let Some(uri) = params
                    .and_then(|p| p.get("textDocument"))
                    .and_then(|t| t.get("uri"))
                    .and_then(Value::as_str)
                {
                    self.docs.remove(uri);
                    obs.uri = Some(uri.to_owned());
                }
            }
            Some("textDocument/didChange") => {
                if let Some(uri) = params
                    .and_then(|p| p.get("textDocument"))
                    .and_then(|t| t.get("uri"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                {
                    if let Some(v) = params
                        .and_then(|p| p.get("textDocument"))
                        .and_then(|t| t.get("version"))
                        .and_then(Value::as_i64)
                    {
                        self.docs.insert(uri.clone(), v as i32);
                        obs.version = Some(v as i32);
                    }
                    // Fallback cursor: the start of the last edit's range.
                    let cursor = params
                        .and_then(|p| p.get("contentChanges"))
                        .and_then(Value::as_array)
                        .and_then(|cs| cs.last())
                        .and_then(|c| c.get("range"))
                        .and_then(|r| r.get("start"))
                        .and_then(parse_position);
                    obs.uri = Some(uri.clone());
                    obs.position = cursor;
                    if let Some(pos) = cursor {
                        obs.focus = self.legal_focus(&uri, pos);
                    }
                }
            }
            Some(m) if self.is_trigger(m) => {
                if let Some(p) = params {
                    let uri = p
                        .get("textDocument")
                        .and_then(|t| t.get("uri"))
                        .and_then(Value::as_str);
                    let pos = p.get("position").and_then(parse_position);
                    if let (Some(uri), Some(pos)) = (uri, pos) {
                        obs.uri = Some(uri.to_owned());
                        obs.position = Some(pos);
                        obs.focus = self.legal_focus(uri, pos);
                    }
                }
            }
            _ => {}
        }
        obs
    }

    fn is_trigger(&self, method: &str) -> bool {
        self.triggers.iter().any(|t| t == method)
    }

    /// A focus is legal only when initialized and the document is open; the
    /// version is taken from the snoop's open-doc table.
    fn legal_focus(&self, uri: &str, position: Position) -> Option<Focus> {
        if !self.initialized {
            return None;
        }
        let version = *self.docs.get(uri)?;
        Some(Focus {
            doc: DocRef {
                uri: uri.to_owned(),
                version,
            },
            position,
        })
    }
}

/// One line of the cadence capture (milestone 3). JSON-lines.
#[derive(Serialize)]
struct CaptureRecord<'a> {
    ts_unix_ms: u128,
    method: Option<&'a str>,
    has_position: bool,
    uri: Option<&'a str>,
    line: Option<u32>,
    character: Option<u32>,
    version: Option<i32>,
    is_trigger: bool,
    would_focus: bool,
}

/// The snoop task: consumes copied client bodies off the hot path, maintains
/// session/focus state, optionally records a cadence capture, and drives the
/// querier on a debounce so bursts coalesce to the latest focus.
pub async fn run_snoop_task(
    mut rx: mpsc::Receiver<Vec<u8>>,
    querier: Querier,
    debounce: Duration,
    triggers: Vec<String>,
    capture_path: Option<PathBuf>,
) {
    let mut snoop = Snoop::new(triggers);
    let mut pending: Option<Focus> = None;
    let mut capture = open_capture(capture_path.as_ref()).await;

    let timer = tokio::time::sleep(debounce);
    tokio::pin!(timer);

    loop {
        tokio::select! {
            received = rx.recv() => {
                let Some(body) = received else { break };
                let obs = snoop.observe(&body);
                if let Some(writer) = capture.as_mut() {
                    write_capture(writer, &obs).await;
                }
                if let Some(focus) = obs.focus {
                    pending = Some(focus);
                    timer.as_mut().reset(tokio::time::Instant::now() + debounce);
                }
            }
            _ = &mut timer, if pending.is_some() => {
                if let Some(focus) = pending.take() {
                    tracing::debug!(uri = %focus.doc.uri, position = ?focus.position, "debounce fired; injecting goal queries");
                    querier.request(focus.doc, focus.position);
                }
            }
        }
    }

    if let Some(mut writer) = capture {
        let _ = writer.flush().await;
    }
}

type CaptureWriter = BufWriter<tokio::fs::File>;

async fn open_capture(path: Option<&PathBuf>) -> Option<CaptureWriter> {
    let path = path?;
    match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    {
        Ok(file) => {
            tracing::info!(path = %path.display(), "capture recording client→server cadence");
            Some(BufWriter::new(file))
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to open capture file; capture disabled");
            None
        }
    }
}

async fn write_capture(writer: &mut CaptureWriter, obs: &Observation) {
    let record = CaptureRecord {
        ts_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
        method: obs.method.as_deref(),
        has_position: obs.position.is_some(),
        uri: obs.uri.as_deref(),
        line: obs.position.map(|p| p.line),
        character: obs.position.map(|p| p.character),
        version: obs.version,
        is_trigger: obs.is_trigger,
        would_focus: obs.focus.is_some(),
    };
    if let Ok(mut line) = serde_json::to_vec(&record) {
        line.push(b'\n');
        // Best-effort, off the hot path: a capture write must never wedge proxying.
        let _ = writer.write_all(&line).await;
        let _ = writer.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(v: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&v).unwrap()
    }

    fn ready_snoop() -> Snoop {
        let mut s = Snoop::new(default_triggers());
        s.observe(&body(serde_json::json!({"method":"initialized","params":{}})));
        s.observe(&body(serde_json::json!({
            "method":"textDocument/didOpen",
            "params":{"textDocument":{"uri":"file:///A.lean","version":1}}
        })));
        s
    }

    #[test]
    fn no_focus_before_initialized() {
        let mut s = Snoop::new(default_triggers());
        s.observe(&body(serde_json::json!({
            "method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///A.lean","version":1}}
        })));
        let obs = s.observe(&body(serde_json::json!({
            "method":"textDocument/hover",
            "params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":2,"character":1}}
        })));
        assert!(obs.focus.is_none(), "not initialized → no focus");
        assert!(obs.is_trigger);
    }

    #[test]
    fn no_focus_when_document_not_open() {
        let mut s = Snoop::new(default_triggers());
        s.observe(&body(serde_json::json!({"method":"initialized"})));
        let obs = s.observe(&body(serde_json::json!({
            "method":"textDocument/hover",
            "params":{"textDocument":{"uri":"file:///Closed.lean"},"position":{"line":0,"character":0}}
        })));
        assert!(obs.focus.is_none(), "doc not open → no focus");
    }

    #[test]
    fn hover_yields_focus_with_version_from_open_doc() {
        let mut s = ready_snoop();
        let obs = s.observe(&body(serde_json::json!({
            "method":"textDocument/hover",
            "params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":4,"character":2}}
        })));
        let focus = obs.focus.unwrap();
        assert_eq!(focus.doc.uri, "file:///A.lean");
        assert_eq!(focus.doc.version, 1);
        assert_eq!(focus.position, Position { line: 4, character: 2 });
    }

    #[test]
    fn didchange_bumps_version_and_offers_fallback_focus() {
        let mut s = ready_snoop();
        let obs = s.observe(&body(serde_json::json!({
            "method":"textDocument/didChange",
            "params":{
                "textDocument":{"uri":"file:///A.lean","version":7},
                "contentChanges":[{"range":{"start":{"line":3,"character":1},"end":{"line":3,"character":1}},"text":"x"}]
            }
        })));
        assert_eq!(obs.version, Some(7));
        let focus = obs.focus.expect("edit-derived fallback focus");
        assert_eq!(focus.position, Position { line: 3, character: 1 });
        assert_eq!(focus.doc.version, 7, "focus uses the bumped version");

        // A subsequent hover overrides at the new version.
        let obs = s.observe(&body(serde_json::json!({
            "method":"textDocument/hover",
            "params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":9,"character":0}}
        })));
        assert_eq!(obs.focus.unwrap().doc.version, 7);
    }

    #[test]
    fn non_positional_methods_yield_no_focus() {
        let mut s = ready_snoop();
        let obs = s.observe(&body(serde_json::json!({"id":1,"method":"shutdown"})));
        assert!(obs.focus.is_none());
        assert!(!obs.is_trigger);
        assert_eq!(obs.method.as_deref(), Some("shutdown"));
    }

    #[test]
    fn resolve_triggers_defaults_and_normalizes() {
        assert_eq!(resolve_triggers(vec![]), default_triggers());
        assert_eq!(
            resolve_triggers(vec!["hover".into(), "$/lean/custom".into()]),
            vec!["textDocument/hover".to_string(), "$/lean/custom".to_string()]
        );
    }
}
