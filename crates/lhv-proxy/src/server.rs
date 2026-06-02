//! Viewer socket server: a Unix-domain listener that publishes [`Snapshot`]s
//! from the shared state.
//!
//! Present but **not spawned** in v1 — milestone 5 wires it into [`crate::run`].
//! Unix-only for now; the listener type is the only transport-specific seam, so
//! TCP-on-localhost can slot in later.
//!
//! [`Snapshot`]: lhv_wire::Snapshot

use std::io;
use std::path::PathBuf;

use lhv_lsp::{Frame, write_frame};
use lhv_wire::ServerMsg;
use tokio::net::{UnixListener, UnixStream};

use crate::state::StateHandle;

/// Accept viewer connections forever, spawning a publisher task per client.
pub async fn serve(path: PathBuf, state: StateHandle) -> io::Result<()> {
    let _ = std::fs::remove_file(&path); // clear any stale socket
    let listener = UnixListener::bind(&path)?;
    tracing::info!(path = %path.display(), "viewer socket listening");
    loop {
        let (stream, _addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = publish_to(stream, state).await {
                tracing::debug!(error = %e, "viewer connection ended");
            }
        });
    }
}

/// Replay the latest snapshot immediately, then push a fresh one on every
/// change. `watch` collapses bursts to the latest, so a slow viewer is fine.
async fn publish_to(mut stream: UnixStream, state: StateHandle) -> io::Result<()> {
    let mut rx = state.subscribe();
    loop {
        let snapshot = rx.borrow_and_update().clone();
        let frame = Frame::from_body(&ServerMsg::Snapshot(snapshot).to_json());
        write_frame(&mut stream, &frame).await?;
        if rx.changed().await.is_err() {
            break; // the proxy (sender) is gone
        }
    }
    Ok(())
}
