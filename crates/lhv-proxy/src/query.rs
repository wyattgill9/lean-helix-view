//! The goal querier: injects `$/lean/plainGoal` and `$/lean/plainTermGoal`
//! into the Lean stream (through the *shared* Lean-stdin writer) and tracks the
//! ids it issues so their responses can be consumed and never leak to Helix.
//!
//! Supersession is a monotonic **generation**: each `request` bumps it and tags
//! both of its queries with it. A response whose generation is no longer the
//! latest is consumed but dropped, so only the most recent focus renders.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lhv_lsp::Frame;
use lhv_wire::{DocRef, Position};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::state::StateHandle;

/// Prefix on every injected id. Interception is by exact match against
/// [`Outstanding`], so the prefix is a convenience (and the no-leak assertion's
/// hook), not the security boundary — a stray client id can't be misrouted.
pub const INJECT_PREFIX: &str = "lhv-q";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    PlainGoal,
    PlainTermGoal,
}

impl QueryKind {
    fn method(self) -> &'static str {
        match self {
            QueryKind::PlainGoal => "$/lean/plainGoal",
            QueryKind::PlainTermGoal => "$/lean/plainTermGoal",
        }
    }

    fn tag(self) -> &'static str {
        match self {
            QueryKind::PlainGoal => "g",
            QueryKind::PlainTermGoal => "t",
        }
    }
}

/// What an outstanding injected id is waiting for. `generation` lets the router
/// drop a response whose focus has been superseded.
#[derive(Debug, Clone)]
pub struct Pending {
    pub kind: QueryKind,
    pub doc: DocRef,
    pub position: Position,
    pub generation: u64,
}

/// The outstanding-injected-id registry plus the focus generation counter.
/// Shared (cheaply cloneable) between the querier (registers) and the S2C pump
/// (matches, removes, checks staleness).
#[derive(Clone, Default)]
pub struct Outstanding {
    map: Arc<Mutex<HashMap<String, Pending>>>,
    generation: Arc<AtomicU64>,
}

impl Outstanding {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a new focus generation (monotonic). Its value is, by construction,
    /// the current "latest".
    pub fn next_gen(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The most recently opened generation.
    pub fn latest(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn register(&self, id: String, pending: Pending) {
        self.map.lock().unwrap().insert(id, pending);
    }

    pub fn take(&self, id: &str) -> Option<Pending> {
        self.map.lock().unwrap().remove(id)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

/// Issues goal queries through the shared Lean-stdin sender and registers their
/// ids. Cloneable; owned by the snoop task in the live system.
#[derive(Clone)]
pub struct Querier {
    to_lean: mpsc::Sender<Frame>,
    outstanding: Outstanding,
}

impl Querier {
    pub fn new(to_lean: mpsc::Sender<Frame>, outstanding: Outstanding) -> Self {
        Self { to_lean, outstanding }
    }

    /// Inject both goal queries for a focus, returning the generation issued.
    ///
    /// Best-effort: if the shared sink is full we drop (a newer focus will
    /// supersede), never blocking the Helix↔Lean path.
    pub fn request(&self, doc: DocRef, position: Position) -> u64 {
        let generation = self.outstanding.next_gen();
        for kind in [QueryKind::PlainGoal, QueryKind::PlainTermGoal] {
            let id = format!("{INJECT_PREFIX}{generation}{}", kind.tag());
            let body = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": kind.method(),
                "params": {
                    "textDocument": { "uri": doc.uri },
                    "position": { "line": position.line, "character": position.character },
                },
            });
            self.outstanding.register(
                id.clone(),
                Pending {
                    kind,
                    doc: doc.clone(),
                    position,
                    generation,
                },
            );
            let frame = Frame::from_body(&serde_json::to_vec(&body).expect("query serializes"));
            if self.to_lean.try_send(frame).is_err() {
                self.outstanding.take(&id); // unregister so no stale response is eaten
            }
        }
        generation
    }
}

/// Fold a consumed injected response into the state store (the staleness check
/// happens in the caller). Best-effort — an unexpected result is ignored.
pub fn handle_injected_response(state: &StateHandle, pending: Pending, body: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let result = value.get("result");
    let doc = pending.doc;
    let position = pending.position;

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
                s.position = Some(position);
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
                s.position = Some(position);
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
    async fn request_bumps_gen_registers_two_ids_and_sends_two_frames() {
        let (tx, mut rx) = mpsc::channel(8);
        let outstanding = Outstanding::new();
        let querier = Querier::new(tx, outstanding.clone());

        let generation = querier.request(doc(), Position { line: 2, character: 3 });
        assert_eq!(generation, 1);
        assert_eq!(outstanding.latest(), 1);
        assert_eq!(outstanding.len(), 2);

        let bodies = format!(
            "{}{}",
            String::from_utf8_lossy(rx.recv().await.unwrap().body()),
            String::from_utf8_lossy(rx.recv().await.unwrap().body())
        );
        assert!(bodies.contains("$/lean/plainGoal"));
        assert!(bodies.contains("$/lean/plainTermGoal"));
        assert!(bodies.contains("lhv-q1g") && bodies.contains("lhv-q1t"));
    }

    #[test]
    fn latest_generation_supersedes_earlier_ones() {
        let outstanding = Outstanding::new();
        let g1 = outstanding.next_gen();
        let g2 = outstanding.next_gen();
        assert_eq!((g1, g2), (1, 2));
        assert_eq!(outstanding.latest(), 2);
        assert!(g1 != outstanding.latest(), "gen 1 is stale once gen 2 opened");
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
                generation: 1,
            },
            r#"{"jsonrpc":"2.0","id":"lhv-q1g","result":{"goals":["⊢ True"],"rendered":"r"}}"#
                .as_bytes(),
        );
        let snap = state.snapshot();
        assert_eq!(snap.goals, vec!["⊢ True".to_string()]);
        assert!(snap.in_tactic);
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
                generation: 1,
            },
            br#"{"jsonrpc":"2.0","id":"lhv-q1g","result":null}"#,
        );
        let snap = state.snapshot();
        assert!(!snap.in_tactic);
        assert!(snap.goals.is_empty());
    }
}
