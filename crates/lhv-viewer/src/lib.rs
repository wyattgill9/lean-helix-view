//! The infoview viewer: the proxy↔viewer socket *client*.
//!
//! The proxy is the socket server; the viewer dials in and renders whatever
//! snapshots it receives. v1 is a minimal client that proves the seam — it
//! connects, decodes [`ServerMsg`]s with `lhv-lsp`'s codec, and prints each
//! snapshot. The ratatui panes (goals / expected-type / diagnostics / progress)
//! land in milestone 5; the `ratatui` + `crossterm` deps are staged for it.

use std::io;
use std::path::PathBuf;

use lhv_lsp::read_frame;
use lhv_wire::{ServerMsg, Snapshot, default_socket_path};
use tokio::io::BufReader;
use tokio::net::UnixStream;

/// Connect to the proxy socket and render snapshots until it disconnects.
pub async fn run(socket: Option<PathBuf>) -> io::Result<()> {
    let path = socket.unwrap_or_else(default_socket_path);
    let stream = UnixStream::connect(&path).await.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot connect to proxy socket {}: {e}", path.display()),
        )
    })?;
    println!("lean-helix-view: connected to {}", path.display());

    let mut reader = BufReader::new(stream);
    while let Some(frame) = read_frame(&mut reader).await? {
        match ServerMsg::from_json(frame.body()) {
            Ok(ServerMsg::Snapshot(snapshot)) => render(&snapshot),
            Err(e) => tracing::warn!(error = %e, "ignoring malformed snapshot"),
        }
    }
    println!("lean-helix-view: proxy disconnected");
    Ok(())
}

/// v1 placeholder render — milestone 5 replaces this with ratatui panes.
fn render(snapshot: &Snapshot) {
    println!("── goals ({}) ──", snapshot.goals.len());
    for goal in &snapshot.goals {
        println!("{goal}");
    }
    if let Some(term) = &snapshot.term_goal {
        println!("── expected type ──\n{term}");
    }
}
