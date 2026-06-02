//! Temporary headless sink for milestones 3–4: drains the viewer (state) watch
//! channel and appends each snapshot as one line of JSON to a file. This is the
//! first *consumer* of the now-produced viewer channel; the socket server +
//! ratatui viewer replace it in milestone 5 without touching the producer side.

use std::io;
use std::path::PathBuf;

use lhv_wire::Snapshot;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

/// Append a JSON-lines snapshot on every state change. `watch` already collapses
/// bursts to the latest, so this never backpressures the proxy.
pub async fn run_goal_sink(mut rx: watch::Receiver<Snapshot>, path: PathBuf) -> io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    tracing::info!(path = %path.display(), "goal sink writing JSON-lines snapshots");

    rx.borrow_and_update(); // skip the empty baseline; emit only on change
    while rx.changed().await.is_ok() {
        let snapshot = rx.borrow_and_update().clone();
        let mut line = serde_json::to_vec(&snapshot).unwrap_or_default();
        line.push(b'\n');
        file.write_all(&line).await?;
        file.flush().await?;
    }
    Ok(())
}
