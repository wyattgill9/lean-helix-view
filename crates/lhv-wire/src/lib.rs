//! The proxy → viewer protocol: a single source of truth for both ends.
//!
//! The proxy holds one authoritative [`Snapshot`] and ships **full snapshots**
//! (never deltas) on every change and on connect. Snapshots are small,
//! idempotent, and last-write-wins, so a reconnecting viewer needs no
//! bookkeeping — it just renders the latest one.
//!
//! Messages travel over the viewer socket framed by `lhv-lsp`'s codec, with
//! [`ServerMsg`] as the JSON body. Both ends ship in one binary, so versions
//! match by construction and there is no protocol negotiation.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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

/// The complete, focus-centric state the viewer renders. `Default` is the empty
/// "waiting for goals" state a freshly connected viewer may receive before any
/// query has resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The document + version the goal fields describe, if known.
    pub doc: Option<DocRef>,
    /// The cursor position the snapshot describes (the focus).
    pub position: Option<Position>,
    /// `$/lean/plainGoal` goals. Empty vec = proof complete at this position;
    /// the distinction from "no tactic goals here" is carried by `in_tactic`.
    pub goals: Vec<String>,
    /// `$/lean/plainGoal.rendered`, the pre-rendered combined goal block.
    pub rendered: Option<String>,
    /// `$/lean/plainTermGoal.goal`: the expected type at the position.
    pub term_goal: Option<String>,
    /// Whether the position is inside tactic mode at all (plainGoal non-null).
    pub in_tactic: bool,
    /// True while `$/lean/fileProgress` reports a region as still elaborating.
    pub elaborating: bool,
    /// `textDocument/publishDiagnostics`, keyed by document URI.
    pub diagnostics: BTreeMap<String, Vec<Diagnostic>>,
    /// `$/lean/fileProgress` ranges currently being elaborated.
    pub progress: Vec<Range>,
    /// Monotonic counter, bumped on every state change. Lets a viewer ignore
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

// ---- Socket discovery -------------------------------------------------------
//
// The proxy and viewer meet at a per-workspace socket. The proxy keys it off the
// `rootUri` from `initialize` (canonicalized to a filesystem path); the viewer,
// lacking that, keys off its current directory. For the common case — Helix
// opened in the project root, `watch` run there too — both canonicalize to the
// same path and hash to the same socket. `--socket` overrides when they don't.

/// The directory holding per-workspace sockets.
pub fn socket_dir() -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join("lean-helix-view")
}

/// The socket path for a canonical workspace-root path.
pub fn socket_path_for_root(root_path: &str) -> PathBuf {
    socket_dir().join(format!("{:016x}.sock", fnv1a64(root_path.as_bytes())))
}

/// Walk up from `start` to the first directory containing a Lean workspace
/// marker (`lakefile.lean` / `lakefile.toml` / `lean-toolchain`). This lets the
/// viewer find the same root the proxy keyed its socket on, even from a nested
/// subdirectory.
pub fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    const MARKERS: [&str; 3] = ["lakefile.lean", "lakefile.toml", "lean-toolchain"];
    let mut dir = Some(start);
    while let Some(d) = dir {
        if MARKERS.iter().any(|m| d.join(m).exists()) {
            return Some(d.to_path_buf());
        }
        dir = d.parent();
    }
    None
}

/// The viewer's default socket: resolve the workspace root by walking up from
/// the current directory, canonicalize it, and key the socket off it (falling
/// back to the current directory if no marker is found).
pub fn workspace_socket_path() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let root = find_workspace_root(&cwd).unwrap_or(cwd);
    socket_path_for_root(&canonical(&root.to_string_lossy()))
}

/// Canonical filesystem path for a workspace, from an LSP `rootUri`. Falls back
/// to the (decoded) literal when the path can't be canonicalized.
pub fn root_path_from_uri(uri: &str) -> String {
    let raw = uri.strip_prefix("file://").unwrap_or(uri);
    canonical(&percent_decode(raw))
}

/// Canonical path of the current directory — the viewer's default root.
pub fn cwd_root() -> String {
    std::env::current_dir()
        .map(|p| canonical(&p.to_string_lossy()))
        .unwrap_or_else(|_| ".".to_string())
}

fn canonical(path: &str) -> String {
    std::fs::canonicalize(path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string())
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrips_through_servermsg() {
        let snap = Snapshot {
            goals: vec!["⊢ True".into()],
            in_tactic: true,
            position: Some(Position { line: 3, character: 1 }),
            seq: 5,
            ..Snapshot::default()
        };
        let bytes = ServerMsg::Snapshot(snap.clone()).to_json();
        match ServerMsg::from_json(&bytes).unwrap() {
            ServerMsg::Snapshot(got) => assert_eq!(got, snap),
        }
    }

    #[test]
    fn socket_path_is_deterministic_and_uri_agrees_with_path() {
        let a = socket_path_for_root("/home/u/proj");
        assert_eq!(a, socket_path_for_root("/home/u/proj"));
        assert_ne!(a, socket_path_for_root("/home/u/other"));
        // For a path that can't be canonicalized we fall back to the literal,
        // so a `file://` rootUri and a bare path for the same place agree.
        assert_eq!(socket_path_for_root(&root_path_from_uri("file:///home/u/proj")), a);
    }

    #[test]
    fn percent_decoding_in_root_uri() {
        assert_eq!(root_path_from_uri("file:///home/a%20b/p"), "/home/a b/p");
    }

    #[test]
    fn find_workspace_root_walks_up_to_the_marker() {
        let base = std::env::temp_dir().join(format!("lhv-root-{}", std::process::id()));
        let nested = base.join("sub/deep");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(base.join("lakefile.toml"), b"").unwrap();

        let found = find_workspace_root(&nested).expect("marker found by walking up");
        assert_eq!(
            std::fs::canonicalize(&found).unwrap(),
            std::fs::canonicalize(&base).unwrap()
        );

        // No marker anywhere above a temp file → None.
        let orphan = std::env::temp_dir();
        let _ = orphan; // (temp_dir itself may or may not have markers; don't assert)
        std::fs::remove_dir_all(&base).ok();
    }
}
