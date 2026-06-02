//! Best-effort extraction of a cursor position from the position-carrying
//! client→server requests Helix sends.
//!
//! Present and tested, but **not yet wired into the forward path**. Milestone 4
//! runs this on a *copy* of each client frame, off the hot path, to drive the
//! [`crate::query::Querier`]. `textDocument/didChange` is intentionally absent:
//! it bumps the document version / invalidates rather than yielding a query
//! position (edit-derived cursor is only a fallback).

use lhv_wire::Position;
use serde_json::Value;

/// Position-carrying methods whose `params` are `TextDocumentPositionParams`.
const POSITIONAL_METHODS: &[&str] = &[
    "textDocument/hover",
    "textDocument/completion",
    "textDocument/definition",
    "textDocument/references",
    "textDocument/documentHighlight",
    "textDocument/signatureHelp",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub uri: String,
    pub position: Position,
}

/// Stateless classifier over client message bodies.
#[derive(Debug, Default, Clone, Copy)]
pub struct Snoop;

impl Snoop {
    /// Extract the `(uri, position)` if `body` is one of the position-carrying
    /// requests. `None` for anything else (including unparseable bytes).
    pub fn observe(&self, body: &[u8]) -> Option<Observed> {
        let value: Value = serde_json::from_slice(body).ok()?;
        let method = value.get("method")?.as_str()?;
        if !POSITIONAL_METHODS.contains(&method) {
            return None;
        }
        let params = value.get("params")?;
        let uri = params.get("textDocument")?.get("uri")?.as_str()?.to_owned();
        let pos = params.get("position")?;
        let line = pos.get("line")?.as_u64()? as u32;
        let character = pos.get("character")?.as_u64()? as u32;
        Some(Observed {
            uri,
            position: Position { line, character },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_position_from_hover() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":4,"character":2}}}"#;
        let observed = Snoop.observe(body).unwrap();
        assert_eq!(observed.uri, "file:///A.lean");
        assert_eq!(
            observed.position,
            Position {
                line: 4,
                character: 2
            }
        );
    }

    #[test]
    fn ignores_non_positional_and_garbage() {
        assert!(Snoop.observe(br#"{"method":"textDocument/didChange","params":{}}"#).is_none());
        assert!(Snoop.observe(br#"{"method":"initialize"}"#).is_none());
        assert!(Snoop.observe(b"not json").is_none());
    }
}
