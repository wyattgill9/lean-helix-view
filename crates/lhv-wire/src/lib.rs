//! The proxy → viewer protocol: a single source of truth for both ends.
//!
//! The proxy holds one authoritative [`Snapshot`] and ships **full snapshots**
//! (never deltas) on every change. Snapshots are small and idempotent, so a
//! reconnecting viewer needs no bookkeeping — it just renders the latest one.
//!
//! Messages travel over the viewer socket framed by `lhv-lsp`'s codec, with
//! [`ServerMsg`] as the JSON body. Both ends ship in one binary, so versions
//! match by construction and there is no protocol negotiation.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A message from the proxy (socket server) to a viewer (socket client).
///
/// An enum from day one so additional message kinds (e.g. an explicit
/// server-going-away) can slot in without a flag-day protocol change.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerMsg {
    /// The current authoritative state. Sent on connect (replay) and on change.
    Snapshot(Snapshot),
}

impl ServerMsg {
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ServerMsg serializes")
    }

    pub fn from_json(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// The complete state the viewer renders. `Default` is the empty state shown
/// before any goal query has resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The document + version the goal fields describe, if known.
    pub doc: Option<DocRef>,
    /// `$/lean/plainGoal` goals. Empty vec = proof complete at this position;
    /// the distinction from "no tactic goals here" is carried by `in_tactic`.
    pub goals: Vec<String>,
    /// `$/lean/plainGoal.rendered`, the pre-rendered combined goal block.
    pub rendered: Option<String>,
    /// `$/lean/plainTermGoal.goal`: the expected type at the position.
    pub term_goal: Option<String>,
    /// Whether the position is inside tactic mode at all (plainGoal non-null).
    pub in_tactic: bool,
    /// True while `$/lean/fileProgress` reports the position's region as still
    /// elaborating; the viewer dims possibly-stale goals.
    pub elaborating: bool,
    /// `textDocument/publishDiagnostics`, keyed by document URI.
    pub diagnostics: BTreeMap<String, Vec<Diagnostic>>,
    /// `$/lean/fileProgress` ranges currently being elaborated.
    pub progress: Vec<Range>,
    /// Monotonic counter, bumped on every state change. Lets the viewer ignore
    /// anything older than what it has already shown.
    pub seq: u64,
}

/// A document URI paired with the LSP version the state was computed against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocRef {
    pub uri: String,
    pub version: i32,
}

/// A zero-based position (LSP semantics: UTF-16 code units for `character`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub line: u32,
    pub character: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

/// A diagnostic, trimmed to the fields the viewer renders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub range: Range,
    pub severity: Severity,
    pub message: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Information,
    Hint,
}

impl Severity {
    /// Map an LSP `DiagnosticSeverity` integer (1..=4) to our enum, defaulting
    /// to `Error` for the absent/unknown case (LSP treats missing as error).
    pub fn from_lsp(n: Option<i64>) -> Severity {
        match n {
            Some(2) => Severity::Warning,
            Some(3) => Severity::Information,
            Some(4) => Severity::Hint,
            _ => Severity::Error,
        }
    }
}

/// The default viewer socket path, shared by the proxy (server) and the viewer
/// (client) so they meet at the same place by construction.
///
/// Workspace-root-keyed discovery is a later milestone; for v1 this is a fixed
/// per-user path under the XDG runtime dir, falling back to the temp dir.
pub fn default_socket_path() -> std::path::PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join("lean-helix-view.sock")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrips_through_servermsg() {
        let mut snap = Snapshot::default();
        snap.goals = vec!["⊢ True".into()];
        snap.in_tactic = true;
        snap.seq = 5;
        let bytes = ServerMsg::Snapshot(snap.clone()).to_json();
        match ServerMsg::from_json(&bytes).unwrap() {
            ServerMsg::Snapshot(got) => assert_eq!(got, snap),
        }
    }
}
