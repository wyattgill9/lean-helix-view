//! The goal querier: injects `$/lean/plainGoal` and `$/lean/plainTermGoal`
//! into the Lean stream and tracks the ids it issues so their responses can be
//! intercepted (and never leak to Helix).
//!
//! Live in v1 — the inject channel works and `request` is real — but **no
//! producer calls it yet**. Milestone 4 attaches the snoop.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lhv_lsp::Frame;
use lhv_wire::{DocRef, Position};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::state::StateHandle;

/// Prefix on every injected id. The id is *also* tracked in [`InjectedIds`];
/// interception is by exact match against that map, so the prefix is a
/// convenience, not the security boundary — a stray client id sharing the
/// prefix still can't be misrouted.
pub const INJECT_PREFIX: &str = "lhv-q";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    PlainGoal,
    PlainTermGoal,
}

/// What an outstanding injected id is waiting for, so its response can be
/// interpreted (responses carry only `id` + `result`, never the method).
#[derive(Debug, Clone)]
pub struct Pending {
    pub kind: QueryKind,
    pub doc: DocRef,
    pub position: Position,
}

/// The outstanding-injected-id set, shared by the querier (inserts) and the
/// S2C pump (matches + removes).
pub type InjectedIds = Arc<Mutex<HashMap<String, Pending>>>;

/// Issues goal queries and registers their ids. Cloneable — every field is a
/// shared handle.
#[derive(Clone)]
pub struct Querier {
    to_lean: mpsc::Sender<Frame>,
    injected: InjectedIds,
    next: Arc<AtomicU64>,
}

impl Querier {
    pub fn new(to_lean: mpsc::Sender<Frame>, injected: InjectedIds) -> Self {
        Self {
            to_lean,
            injected,
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Inject both goal queries for a position, returning the ids registered.
    ///
    /// Best-effort: if the to-Lean channel is full we drop the query (a newer
    /// one will supersede it) rather than block — the Helix↔Lean path is never
    /// stalled by snooping.
    pub fn request(&self, doc: DocRef, position: Position) -> Vec<String> {
        let mut ids = Vec::with_capacity(2);
        for kind in [QueryKind::PlainGoal, QueryKind::PlainTermGoal] {
            let id = self.fresh_id();
            let method = match kind {
                QueryKind::PlainGoal => "$/lean/plainGoal",
                QueryKind::PlainTermGoal => "$/lean/plainTermGoal",
            };
            let body = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": {
                    "textDocument": { "uri": doc.uri },
                    "position": { "line": position.line, "character": position.character },
                },
            });

            self.injected.lock().unwrap().insert(
                id.clone(),
                Pending {
                    kind,
                    doc: doc.clone(),
                    position,
                },
            );

            let frame = Frame::from_body(&serde_json::to_vec(&body).expect("query serializes"));
            if self.to_lean.try_send(frame).is_err() {
                // Sink backed up: forget this id so a stale response isn't eaten.
                self.injected.lock().unwrap().remove(&id);
                continue;
            }
            ids.push(id);
        }
        ids
    }

    fn fresh_id(&self) -> String {
        format!("{INJECT_PREFIX}{}", self.next.fetch_add(1, Ordering::Relaxed))
    }
}

/// Fold a consumed injected response into the state store. Called from the S2C
/// consume branch; dormant in v1 (the id map is empty). Best-effort — an
/// unparseable or unexpected result is ignored.
pub fn handle_injected_response(state: &StateHandle, pending: Pending, body: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let result = value.get("result");
    let doc = pending.doc;

    match pending.kind {
        QueryKind::PlainGoal => {
            // A null result means the position is not in tactic mode at all.
            let in_tactic = result.is_some_and(|r| !r.is_null());
            let goals: Vec<String> = result
                .and_then(|r| r.get("goals"))
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let rendered = result
                .and_then(|r| r.get("rendered"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.update(move |s| {
                s.doc = Some(doc);
                s.in_tactic = in_tactic;
                s.goals = goals;
                s.rendered = rendered;
            });
        }
        QueryKind::PlainTermGoal => {
            let term = result
                .and_then(|r| r.get("goal"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.update(move |s| {
                s.doc = Some(doc);
                s.term_goal = term;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc() -> DocRef {
        DocRef {
            uri: "file:///A.lean".into(),
            version: 1,
        }
    }

    #[tokio::test]
    async fn request_registers_two_ids_and_enqueues_two_frames() {
        let (tx, mut rx) = mpsc::channel(8);
        let injected: InjectedIds = Arc::new(Mutex::new(HashMap::new()));
        let q = Querier::new(tx, injected.clone());

        let ids = q.request(doc(), Position { line: 2, character: 3 });
        assert_eq!(ids.len(), 2);
        assert!(ids.iter().all(|i| i.starts_with(INJECT_PREFIX)));
        assert_eq!(injected.lock().unwrap().len(), 2);

        let f1 = rx.recv().await.unwrap();
        let f2 = rx.recv().await.unwrap();
        let bodies = format!(
            "{}{}",
            String::from_utf8_lossy(f1.body()),
            String::from_utf8_lossy(f2.body())
        );
        assert!(bodies.contains("$/lean/plainGoal"));
        assert!(bodies.contains("$/lean/plainTermGoal"));
    }

    #[test]
    fn plaingoal_response_populates_state() {
        let state = StateHandle::new();
        handle_injected_response(
            &state,
            Pending {
                kind: QueryKind::PlainGoal,
                doc: doc(),
                position: Position {
                    line: 0,
                    character: 0,
                },
            },
            r#"{"jsonrpc":"2.0","id":"lhv-q0","result":{"goals":["⊢ True"],"rendered":"r"}}"#
                .as_bytes(),
        );
        let s = state.snapshot();
        assert_eq!(s.goals, vec!["⊢ True".to_string()]);
        assert!(s.in_tactic);
        assert_eq!(s.rendered.as_deref(), Some("r"));
    }

    #[test]
    fn null_plaingoal_is_not_in_tactic() {
        let state = StateHandle::new();
        handle_injected_response(
            &state,
            Pending {
                kind: QueryKind::PlainGoal,
                doc: doc(),
                position: Position {
                    line: 0,
                    character: 0,
                },
            },
            br#"{"jsonrpc":"2.0","id":"lhv-q0","result":null}"#,
        );
        let s = state.snapshot();
        assert!(!s.in_tactic);
        assert!(s.goals.is_empty());
    }
}
