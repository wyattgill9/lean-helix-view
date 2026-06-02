//! The bidirectional forwarder: the sacred Helix↔Lean pipe, now under injection
//! load.
//!
//! Invariants (unchanged, and they must survive injection):
//! 1. forwardable bytes are never altered — frames are re-emitted verbatim, and
//!    an unparseable frame is still forwarded;
//! 2. two frames are never interleaved on one sink — each sink has exactly one
//!    writer task, and **injection reuses the Lean-stdin writer** (no second
//!    writer);
//! 3. snoop / sink / logging never stall the path — the snoop runs on its own
//!    task fed a best-effort (drop-on-full) copy channel.
//!
//! The headline invariant for this milestone: responses to *injected* ids are
//! consumed and never reach Helix; everything else stays byte-identical.

use std::path::PathBuf;
use std::time::Duration;

use lhv_lsp::{Envelope, Frame, Id, read_frame, write_frame};
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::query::{Outstanding, Querier, handle_injected_response};
use crate::snoop::{default_triggers, run_snoop_task};
use crate::state::StateHandle;
use crate::tee;

/// Frames buffered toward a sink before the producing pump backpressures
/// (legitimate flow control on the sacred path).
const SINK_CAPACITY: usize = 1024;
/// Copied client bodies buffered for the snoop. On overflow the copy is dropped
/// — best-effort, so the snoop can never stall Helix↔Lean.
const SNOOP_CAPACITY: usize = 256;

/// Snoop/querier tuning, passed into [`Forwarder::spawn`].
#[derive(Debug, Clone)]
pub struct SnoopConfig {
    pub debounce: Duration,
    pub triggers: Vec<String>,
    pub capture_path: Option<PathBuf>,
}

impl Default for SnoopConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(120),
            triggers: default_triggers(),
            capture_path: None,
        }
    }
}

/// Which direction closed first, selecting the shutdown choreography.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstClosed {
    Client,
    Server,
}

enum TeeKind {
    Diagnostics,
    Progress,
}

/// The single writer for one sink. When every producer drops (or a write
/// fails) it half-closes the sink so the peer sees EOF.
async fn writer<W>(mut sink: W, mut rx: mpsc::Receiver<Frame>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = rx.recv().await {
        if let Err(e) = write_frame(&mut sink, &frame).await {
            tracing::warn!(error = %e, "sink write failed; stopping writer");
            break;
        }
    }
    let _ = sink.shutdown().await;
}

/// Client→server pump. Forwards verbatim, then hands a copy of the body to the
/// snoop best-effort — the forward always happens first and never waits on the
/// snoop.
async fn pump_c2s<R>(mut client_in: R, forward_tx: mpsc::Sender<Frame>, snoop_tx: mpsc::Sender<Vec<u8>>)
where
    R: AsyncBufRead + Unpin,
{
    loop {
        match read_frame(&mut client_in).await {
            Ok(Some(frame)) => {
                let body = frame.body().to_vec();
                if forward_tx.send(frame).await.is_err() {
                    tracing::debug!("c2s: to-Lean sink closed");
                    return;
                }
                let _ = snoop_tx.try_send(body); // drop if the snoop is behind
            }
            Ok(None) => {
                tracing::debug!("c2s: client (Helix) closed stdin");
                return;
            }
            Err(e) => {
                tracing::debug!(error = %e, "c2s: read error");
                return;
            }
        }
    }
}

/// Server→client pump. Consumes responses to ids we injected (never forwarding
/// them); tees diagnostics + progress; forwards everything else verbatim.
async fn pump_s2c<R>(
    mut server_in: R,
    to_helix_tx: mpsc::Sender<Frame>,
    outstanding: Outstanding,
    state: StateHandle,
) where
    R: AsyncBufRead + Unpin,
{
    loop {
        match read_frame(&mut server_in).await {
            Ok(Some(frame)) => {
                let envelope = Envelope::parse(frame.body());

                // Consume responses to ids WE injected — exact match, never forwarded.
                if let Some(env) = &envelope {
                    if env.is_response() {
                        if let Some(Id::Str(id)) = &env.id {
                            if let Some(pending) = outstanding.take(id) {
                                if pending.generation == outstanding.latest() {
                                    handle_injected_response(&state, pending, frame.body());
                                } else {
                                    tracing::trace!(%id, "dropping superseded injected response");
                                }
                                continue; // consumed regardless of staleness
                            }
                        }
                    }
                }

                // Tee (don't intercept) diagnostics + progress. Copy before the
                // frame moves; forward verbatim FIRST; then fold the copy in.
                let tee_target = match envelope.as_ref().and_then(|e| e.method.as_deref()) {
                    Some("textDocument/publishDiagnostics") => {
                        Some((TeeKind::Diagnostics, frame.body().to_vec()))
                    }
                    Some("$/lean/fileProgress") => Some((TeeKind::Progress, frame.body().to_vec())),
                    _ => None,
                };

                if to_helix_tx.send(frame).await.is_err() {
                    tracing::debug!("s2c: to-Helix sink closed");
                    return;
                }

                match tee_target {
                    Some((TeeKind::Diagnostics, body)) => tee::apply_diagnostics(&state, &body),
                    Some((TeeKind::Progress, body)) => tee::apply_progress(&state, &body),
                    None => {}
                }
            }
            Ok(None) => {
                tracing::debug!("s2c: server (Lean) closed stdout");
                return;
            }
            Err(e) => {
                tracing::debug!(error = %e, "s2c: read error");
                return;
            }
        }
    }
}

/// A running forwarder: two pumps, the snoop task, and two sink writers, plus
/// the shared outstanding-injected-id registry.
pub struct Forwarder {
    c2s: JoinHandle<()>,
    s2c: JoinHandle<()>,
    snoop: JoinHandle<()>,
    lean_writer: JoinHandle<()>,
    helix_writer: JoinHandle<()>,
    outstanding: Outstanding,
}

impl Forwarder {
    pub fn spawn<HIn, HOut, LIn, LOut>(
        helix_in: HIn,
        helix_out: HOut,
        lean_in: LIn,
        lean_out: LOut,
        state: StateHandle,
        config: SnoopConfig,
    ) -> Forwarder
    where
        HIn: AsyncBufRead + Unpin + Send + 'static,
        HOut: AsyncWrite + Unpin + Send + 'static,
        LIn: AsyncWrite + Unpin + Send + 'static,
        LOut: AsyncBufRead + Unpin + Send + 'static,
    {
        let (to_lean_tx, to_lean_rx) = mpsc::channel::<Frame>(SINK_CAPACITY);
        let (to_helix_tx, to_helix_rx) = mpsc::channel::<Frame>(SINK_CAPACITY);
        let (snoop_tx, snoop_rx) = mpsc::channel::<Vec<u8>>(SNOOP_CAPACITY);
        let outstanding = Outstanding::new();

        let lean_writer = tokio::spawn(writer(lean_in, to_lean_rx));
        let helix_writer = tokio::spawn(writer(helix_out, to_helix_rx));

        // Injection reuses the SAME Lean-stdin writer (a clone of its sender).
        // The querier lives in the snoop task, so its sender drops exactly when
        // the client closes (snoop_tx is dropped by the C2S pump) — closing
        // Lean's stdin without any second writer or shutdown cycle.
        let querier = Querier::new(to_lean_tx.clone(), outstanding.clone());
        let snoop = tokio::spawn(run_snoop_task(
            snoop_rx,
            querier,
            config.debounce,
            config.triggers,
            config.capture_path,
        ));

        let c2s = tokio::spawn(pump_c2s(helix_in, to_lean_tx, snoop_tx));
        let s2c = tokio::spawn(pump_s2c(lean_out, to_helix_tx, outstanding.clone(), state));

        Forwarder {
            c2s,
            s2c,
            snoop,
            lean_writer,
            helix_writer,
            outstanding,
        }
    }

    /// The shared outstanding-id registry. Exposed for tests that pre-register
    /// an injected id to exercise the consume branch.
    pub fn outstanding(&self) -> Outstanding {
        self.outstanding.clone()
    }

    /// Resolve when either direction's pump finishes.
    pub async fn wait_first(&mut self) -> FirstClosed {
        tokio::select! {
            _ = &mut self.c2s => FirstClosed::Client,
            _ = &mut self.s2c => FirstClosed::Server,
        }
    }

    pub fn abort_c2s(&self) {
        self.c2s.abort();
    }

    pub async fn join_s2c(&mut self) {
        let _ = (&mut self.s2c).await;
    }

    /// Drain both writers so each sink gets a clean half-close. Aborting the
    /// snoop first guarantees its Lean-stdin sender clone is dropped.
    pub async fn drain_writers(self) {
        self.snoop.abort();
        let _ = self.lean_writer.await;
        let _ = self.helix_writer.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Pending, QueryKind};
    use lhv_wire::{DocRef, Position};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, BufReader, duplex};

    fn frame_json(v: Value) -> Frame {
        Frame::from_body(&serde_json::to_vec(&v).unwrap())
    }

    fn body(v: Value) -> Vec<u8> {
        serde_json::to_vec(&v).unwrap()
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// Strict byte-equality both directions, minus a single injected response
    /// that is consumed (pre-registered, no snoop needed).
    #[tokio::test]
    async fn transparent_passthrough_and_injected_consume() {
        let (mut helix_tx, proxy_from_helix) = duplex(64 * 1024);
        let (proxy_to_helix, mut helix_rx) = duplex(64 * 1024);
        let (proxy_to_lean, mut lean_rx) = duplex(64 * 1024);
        let (mut lean_tx, proxy_from_lean) = duplex(64 * 1024);

        let state = StateHandle::new();
        let fwd = Forwarder::spawn(
            BufReader::new(proxy_from_helix),
            proxy_to_helix,
            proxy_to_lean,
            BufReader::new(proxy_from_lean),
            state.clone(),
            SnoopConfig::default(),
        );

        // Pre-register an injected id at the latest generation.
        let outstanding = fwd.outstanding();
        let generation = outstanding.next_gen();
        outstanding.register(
            "lhv-qtestg".into(),
            Pending {
                kind: QueryKind::PlainGoal,
                doc: DocRef {
                    uri: "file:///A.lean".into(),
                    version: 1,
                },
                position: Position {
                    line: 0,
                    character: 0,
                },
                generation,
            },
        );

        // client -> server
        let mut client_bytes = Vec::new();
        client_bytes.extend_from_slice(
            frame_json(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}})).as_bytes(),
        );
        client_bytes.extend_from_slice(
            frame_json(json!({"jsonrpc":"2.0","method":"textDocument/didOpen","params":{}}))
                .as_bytes(),
        );
        helix_tx.write_all(&client_bytes).await.unwrap();
        helix_tx.shutdown().await.unwrap();
        drop(helix_tx);

        let mut got_on_lean = Vec::new();
        lean_rx.read_to_end(&mut got_on_lean).await.unwrap();
        assert_eq!(got_on_lean, client_bytes, "client->server verbatim");

        // server -> client, with the injected response interleaved
        let notif =
            frame_json(json!({"jsonrpc":"2.0","method":"textDocument/publishDiagnostics","params":{"uri":"file:///A.lean","diagnostics":[]}}));
        let response = frame_json(json!({"jsonrpc":"2.0","id":1,"result":{"ok":true}}));
        let injected =
            frame_json(json!({"jsonrpc":"2.0","id":"lhv-qtestg","result":{"goals":["⊢ True"],"rendered":"r"}}));

        let mut expected = Vec::new();
        expected.extend_from_slice(notif.as_bytes());
        expected.extend_from_slice(response.as_bytes());

        lean_tx.write_all(notif.as_bytes()).await.unwrap();
        lean_tx.write_all(injected.as_bytes()).await.unwrap();
        lean_tx.write_all(response.as_bytes()).await.unwrap();
        lean_tx.shutdown().await.unwrap();
        drop(lean_tx);

        let mut got_on_helix = Vec::new();
        helix_rx.read_to_end(&mut got_on_helix).await.unwrap();
        assert_eq!(got_on_helix, expected, "server->client verbatim, minus the consumed response");
        assert!(!contains(&got_on_helix, b"lhv-q"), "no injected id reaches Helix");

        assert_eq!(state.snapshot().goals, vec!["⊢ True".to_string()]);
        fwd.drain_writers().await;
    }

    /// Full activation against a mock Lean: no-leak, tee of diagnostics +
    /// progress (reach Helix *and* state), focus association, and classification
    /// of a server→client request.
    #[tokio::test]
    async fn no_leak_tee_focus_and_classification() {
        let (mut helix_tx, proxy_from_helix) = duplex(64 * 1024);
        let (proxy_to_helix, mut helix_rx) = duplex(64 * 1024);
        let (proxy_to_lean, lean_rx) = duplex(64 * 1024);
        let (lean_tx, proxy_from_lean) = duplex(64 * 1024);

        let state = StateHandle::new();
        let config = SnoopConfig {
            debounce: Duration::from_millis(15),
            triggers: default_triggers(),
            capture_path: None,
        };
        let fwd = Forwarder::spawn(
            BufReader::new(proxy_from_helix),
            proxy_to_helix,
            proxy_to_lean,
            BufReader::new(proxy_from_lean),
            state.clone(),
            config,
        );

        // Mock Lean: respond to injected queries, emit diagnostics/progress and a
        // server→client request on initialize, ignore other client traffic.
        let mock = tokio::spawn(async move {
            let mut reader = BufReader::new(lean_rx);
            let mut lean_tx = lean_tx;
            while let Ok(Some(frame)) = read_frame(&mut reader).await {
                let v: Value = match serde_json::from_slice(frame.body()) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let method = v.get("method").and_then(|m| m.as_str());
                match method {
                    Some("$/lean/plainGoal") => {
                        let line = v["params"]["position"]["line"].as_i64().unwrap_or(-1);
                        let resp = json!({"jsonrpc":"2.0","id":v["id"],"result":{"goals":[format!("goal at line {line}")],"rendered":"r"}});
                        write_frame(&mut lean_tx, &frame_json(resp)).await.unwrap();
                    }
                    Some("$/lean/plainTermGoal") => {
                        let resp = json!({"jsonrpc":"2.0","id":v["id"],"result":{"goal":"ExpectedType"}});
                        write_frame(&mut lean_tx, &frame_json(resp)).await.unwrap();
                    }
                    Some("initialize") => {
                        write_frame(&mut lean_tx, &frame_json(json!({"jsonrpc":"2.0","id":v["id"],"result":{"capabilities":{}}}))).await.unwrap();
                        write_frame(&mut lean_tx, &frame_json(json!({"jsonrpc":"2.0","method":"textDocument/publishDiagnostics","params":{"uri":"file:///A.lean","diagnostics":[{"range":{"start":{"line":1,"character":0},"end":{"line":1,"character":4}},"severity":1,"message":"boom"}]}}))).await.unwrap();
                        write_frame(&mut lean_tx, &frame_json(json!({"jsonrpc":"2.0","method":"$/lean/fileProgress","params":{"textDocument":{"uri":"file:///A.lean","version":1},"processing":[{"range":{"start":{"line":0,"character":0},"end":{"line":5,"character":0}}}]}}))).await.unwrap();
                        write_frame(&mut lean_tx, &frame_json(json!({"jsonrpc":"2.0","id":1000,"method":"workspace/configuration","params":{"items":[]}}))).await.unwrap();
                    }
                    _ => {}
                }
            }
        });

        // Drive a session: init, initialized, open A.lean v1, hover at line 3.
        for msg in [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
            json!({"jsonrpc":"2.0","method":"initialized","params":{}}),
            json!({"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///A.lean","languageId":"lean4","version":1,"text":"x"}}}),
            json!({"jsonrpc":"2.0","id":2,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":3,"character":5}}}),
        ] {
            helix_tx.write_all(frame_json(msg).as_bytes()).await.unwrap();
        }

        // Wait for the injected goal response to land in state.
        let mut rx = state.subscribe();
        let landed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                {
                    let s = rx.borrow_and_update();
                    if !s.goals.is_empty() && s.term_goal.is_some() {
                        break;
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
        assert!(landed.is_ok(), "goal state should populate");

        let snap = state.snapshot();
        assert_eq!(snap.goals, vec!["goal at line 3".to_string()], "focus association");
        assert_eq!(snap.term_goal.as_deref(), Some("ExpectedType"));
        assert_eq!(snap.doc.as_ref().unwrap().uri, "file:///A.lean");
        assert!(snap.diagnostics.contains_key("file:///A.lean"), "diagnostics teed to state");
        assert!(snap.elaborating && !snap.progress.is_empty(), "progress teed to state");

        // Shut down and inspect everything Helix received.
        helix_tx.shutdown().await.unwrap();
        drop(helix_tx);
        let mut got = Vec::new();
        helix_rx.read_to_end(&mut got).await.unwrap();
        let text = String::from_utf8_lossy(&got);

        assert!(!text.contains("lhv-q"), "no injected id may reach Helix");
        assert!(text.contains("publishDiagnostics"), "diagnostics also forwarded to Helix");
        assert!(text.contains("fileProgress"), "progress also forwarded to Helix");
        assert!(text.contains("workspace/configuration"), "server→client request forwarded");
        assert!(text.contains("capabilities"), "initialize response forwarded");
        assert!(!text.contains("goal at line 3"), "goal results never reach Helix");

        let _ = mock.await;
        fwd.drain_writers().await;
    }

    /// Bursts inside the debounce window coalesce to a single query of the
    /// latest focus.
    #[tokio::test]
    async fn debounce_coalesces_bursts_to_latest_focus() {
        let (to_lean_tx, mut to_lean_rx) = mpsc::channel(16);
        let outstanding = Outstanding::new();
        let querier = Querier::new(to_lean_tx, outstanding.clone());
        let (snoop_tx, snoop_rx) = mpsc::channel(64);
        let task = tokio::spawn(run_snoop_task(
            snoop_rx,
            querier,
            Duration::from_millis(40),
            default_triggers(),
            None,
        ));

        snoop_tx.send(body(json!({"method":"initialized"}))).await.unwrap();
        snoop_tx
            .send(body(json!({"method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///A.lean","version":1}}})))
            .await
            .unwrap();
        snoop_tx
            .send(body(json!({"id":1,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":1,"character":0}}})))
            .await
            .unwrap();
        snoop_tx
            .send(body(json!({"id":2,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":2,"character":0}}})))
            .await
            .unwrap();

        let f1 = to_lean_rx.recv().await.unwrap();
        let f2 = to_lean_rx.recv().await.unwrap();
        for f in [&f1, &f2] {
            let v: Value = serde_json::from_slice(f.body()).unwrap();
            assert_eq!(v["params"]["position"]["line"].as_i64(), Some(2), "coalesced to latest");
        }
        assert!(to_lean_rx.try_recv().is_err(), "exactly one request for the burst");

        drop(snoop_tx);
        let _ = task.await;
    }

    /// A response whose focus has been superseded is consumed but dropped.
    #[tokio::test]
    async fn supersession_drops_a_stale_response() {
        let (to_lean_tx, mut to_lean_rx) = mpsc::channel(16);
        let outstanding = Outstanding::new();
        let querier = Querier::new(to_lean_tx, outstanding.clone());
        let (snoop_tx, snoop_rx) = mpsc::channel(64);
        let state = StateHandle::new();
        let task = tokio::spawn(run_snoop_task(
            snoop_rx,
            querier,
            Duration::from_millis(10),
            default_triggers(),
            None,
        ));

        snoop_tx.send(body(json!({"method":"initialized"}))).await.unwrap();
        snoop_tx
            .send(body(json!({"method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///A.lean","version":1}}})))
            .await
            .unwrap();

        // focus A, then (after it injects) focus B
        snoop_tx
            .send(body(json!({"id":1,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":1,"character":0}}})))
            .await
            .unwrap();
        let a = [to_lean_rx.recv().await.unwrap(), to_lean_rx.recv().await.unwrap()];
        snoop_tx
            .send(body(json!({"id":2,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":9,"character":0}}})))
            .await
            .unwrap();
        let b = [to_lean_rx.recv().await.unwrap(), to_lean_rx.recv().await.unwrap()];

        let route = |frame: &Frame, goal_line: i64| {
            let v: Value = serde_json::from_slice(frame.body()).unwrap();
            let id = v["id"].as_str().unwrap().to_string();
            let resp = if v["method"] == "$/lean/plainGoal" {
                json!({"id":id,"result":{"goals":[format!("line {goal_line}")],"rendered":"r"}})
            } else {
                json!({"id":id,"result":{"goal":format!("term {goal_line}")}})
            };
            let bytes = serde_json::to_vec(&resp).unwrap();
            if let Some(p) = outstanding.take(&id) {
                if p.generation == outstanding.latest() {
                    handle_injected_response(&state, p, &bytes);
                }
            }
        };
        for f in &a {
            route(f, 1); // stale: A's generation < latest (B)
        }
        for f in &b {
            route(f, 9); // current
        }

        let snap = state.snapshot();
        assert_eq!(snap.goals, vec!["line 9".to_string()], "only the latest focus renders");
        assert_eq!(snap.term_goal.as_deref(), Some("term 9"));

        drop(snoop_tx);
        let _ = task.await;
    }
}
