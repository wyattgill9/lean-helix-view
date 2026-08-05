//! Viewer socket server: a Unix-domain listener that publishes [`Snapshot`]s
//! from the shared state.
//!
//! Decoupling is the whole point: each connection is its own task with its own
//! `watch::Receiver`. The publish side ([`StateHandle::update`]) never awaits a
//! viewer, and a slow/stuck/absent viewer blocks only its own socket write —
//! the `watch` coalesces to the latest behind it, so there is no unbounded
//! queue and zero backpressure onto Helix↔Lean.
//!
//! Unix-only for now; the listener type is the only transport-specific seam.
//!
//! [`Snapshot`]: lhv_wire::Snapshot
//! [`StateHandle::update`]: crate::state::StateHandle::update

use std::io;
use std::path::{Path, PathBuf};

use lhv_lsp::{Frame, write_frame};
use lhv_wire::ServerMsg;
use tokio::net::{UnixListener, UnixStream};

use crate::state::StateHandle;

/// Bind `path` and accept viewer connections forever, one publisher task each.
/// Removes the socket file on exit (when this future is dropped or errors).
pub async fn serve(path: PathBuf, state: StateHandle) -> io::Result<()> {
    let listener = reclaim_and_bind(&path)?;
    let _cleanup = SocketCleanup::of(&path);
    tracing::info!(path = %path.display(), "viewer socket listening");

    loop {
        let (stream, _addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = publish_to(stream, state).await {
                // EPIPE / abrupt disconnect — expected, never disturbs upstream.
                tracing::debug!(error = %e, "viewer connection ended");
            }
        });
    }
}

/// Replay the latest snapshot immediately, then push a fresh one on every
/// change. `watch` collapses bursts to the latest, so a slow viewer just sees
/// fewer (newer) snapshots — it never queues or backpressures.
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

/// Bind the socket, taking over whatever is at the path — a stale file from a
/// crashed proxy *or* a live listener from an older session for this workspace.
///
/// Newest-proxy-wins is the point: the Helix you just started is the one you're
/// looking at. Deferring to the incumbent instead left a second session silently
/// invisible — the viewer kept showing the older session's goals forever, with
/// only a log line to explain it. The previous owner's [`SocketCleanup`] is
/// inode-guarded, so it will not delete the socket we bind here.
fn reclaim_and_bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match UnixListener::bind(path) {
        Ok(listener) => Ok(listener),
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            tracing::info!(path = %path.display(), "taking over the workspace viewer socket");
            let _ = std::fs::remove_file(path);
            UnixListener::bind(path)
        }
        Err(e) => Err(e),
    }
}

/// Removes the socket file when the server task is dropped or returns — but
/// only while it is still *ours*. A newer proxy that took the path over has
/// replaced the inode, and unlinking that would leave it serving nothing.
struct SocketCleanup {
    path: PathBuf,
    inode: Option<u64>,
}

impl SocketCleanup {
    fn of(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            inode: inode_of(path),
        }
    }
}

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        if inode_of(&self.path) == self.inode {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn inode_of(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lhv_lsp::read_frame;
    use lhv_wire::Snapshot;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::{AsyncBufRead, BufReader};

    fn unique_socket() -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("lhv-test-{}-{n}.sock", std::process::id()))
    }

    async fn connect_retry(path: &Path) -> UnixStream {
        for _ in 0..200 {
            if let Ok(stream) = UnixStream::connect(path).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("could not connect to {}", path.display());
    }

    /// Connect until the socket's owner is the proxy publishing `goal` — the
    /// takeover is a bind on another task, so it is not instantaneous.
    async fn connect_until_goal(path: &Path, goal: &str) {
        for _ in 0..200 {
            let mut reader = BufReader::new(connect_retry(path).await);
            if read_snapshot(&mut reader).await.goals == vec![goal.to_string()] {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("socket never served {goal}");
    }

    async fn read_snapshot<R: AsyncBufRead + Unpin>(reader: &mut R) -> Snapshot {
        let frame = read_frame(reader).await.unwrap().expect("a frame");
        match ServerMsg::from_json(frame.body()).unwrap() {
            ServerMsg::Snapshot(s) => s,
        }
    }

    /// A freshly connected viewer immediately receives the current snapshot.
    #[tokio::test]
    async fn replay_on_connect() {
        let path = unique_socket();
        let state = StateHandle::new();
        state.update(|s| s.goals = vec!["A".into()]);
        let server = tokio::spawn(serve(path.clone(), state.clone()));

        let mut reader = BufReader::new(connect_retry(&path).await);
        let snap = read_snapshot(&mut reader).await;
        assert_eq!(snap.goals, vec!["A".to_string()]);

        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// A stuck (never-reading) viewer must not block the producer or other
    /// viewers — the publish path never awaits a viewer.
    #[tokio::test]
    async fn stuck_viewer_does_not_block_producer_or_others() {
        let path = unique_socket();
        let state = StateHandle::new();
        let server = tokio::spawn(serve(path.clone(), state.clone()));

        let _stuck = connect_retry(&path).await; // connect, never read

        let flooded = tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..10_000u64 {
                state.update(|_| {}); // bumps seq; must never block
            }
        })
        .await;
        assert!(flooded.is_ok(), "producer must not block on a stuck viewer");
        let latest = state.snapshot().seq;

        // A new viewer still sees the latest despite the stuck one.
        let mut reader = BufReader::new(connect_retry(&path).await);
        let snap = read_snapshot(&mut reader).await;
        assert_eq!(snap.seq, latest, "new viewer sees latest despite stuck one");

        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// Updates that occur between a consumer's reads coalesce to the latest;
    /// intermediates are dropped, never queued unboundedly.
    #[tokio::test]
    async fn slow_viewer_converges_to_latest() {
        let path = unique_socket();
        let state = StateHandle::new();
        let server = tokio::spawn(serve(path.clone(), state.clone()));

        let mut reader = BufReader::new(connect_retry(&path).await);
        let _ = read_snapshot(&mut reader).await; // initial replay

        // Synchronous flood: no awaits, so the publisher task can't drain mid-burst.
        for _ in 0..50 {
            state.update(|_| {});
        }
        let final_seq = state.snapshot().seq;

        let mut received = 0;
        let mut last = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while last < final_seq {
            let snap = tokio::time::timeout_at(deadline, read_snapshot(&mut reader))
                .await
                .expect("should converge to latest");
            last = snap.seq;
            received += 1;
        }
        assert_eq!(last, final_seq);
        assert!(received < 50, "intermediate snapshots dropped (received {received})");

        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// A viewer can drop and reconnect; it re-receives the latest, and the
    /// server stays healthy across the disconnect.
    #[tokio::test]
    async fn reconnect_resees_latest_and_server_survives() {
        let path = unique_socket();
        let state = StateHandle::new();
        let server = tokio::spawn(serve(path.clone(), state.clone()));

        state.update(|s| s.goals = vec!["first".into()]);
        {
            let mut reader = BufReader::new(connect_retry(&path).await);
            assert_eq!(read_snapshot(&mut reader).await.goals, vec!["first".to_string()]);
            // reader dropped here → viewer disconnects
        }

        state.update(|s| s.goals = vec!["second".into()]);
        let mut reader = BufReader::new(connect_retry(&path).await);
        assert_eq!(read_snapshot(&mut reader).await.goals, vec!["second".to_string()]);
        assert!(!server.is_finished(), "server survives viewer disconnect");

        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// A leftover socket file from a crashed proxy is reclaimed on bind.
    #[tokio::test]
    async fn stale_socket_file_is_reclaimed() {
        let path = unique_socket();
        std::fs::write(&path, b"leftover").unwrap(); // simulate a crashed proxy's file
        let bound = reclaim_and_bind(&path).expect("stale file must be reclaimed");
        drop(bound);
        let _ = std::fs::remove_file(&path);
    }

    /// A second proxy for the workspace takes the socket over, and the first
    /// one's cleanup does not then unlink the successor's socket.
    #[tokio::test]
    async fn newest_proxy_takes_over_and_predecessor_cleanup_spares_it() {
        let path = unique_socket();
        let state_old = StateHandle::new();
        state_old.update(|s| s.goals = vec!["old".into()]);
        let old = tokio::spawn(serve(path.clone(), state_old));
        let _ = connect_retry(&path).await; // the old proxy is live and serving

        let state_new = StateHandle::new();
        state_new.update(|s| s.goals = vec!["new".into()]);
        let new = tokio::spawn(serve(path.clone(), state_new));

        // Viewers connecting now reach the newest session.
        connect_until_goal(&path, "new").await;

        // The displaced proxy exiting must leave the live socket in place.
        old.abort();
        let _ = old.await;
        let mut reader = BufReader::new(connect_retry(&path).await);
        assert_eq!(read_snapshot(&mut reader).await.goals, vec!["new".to_string()]);

        new.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// Two workspaces → two distinct sockets → each viewer sees its own state.
    #[tokio::test]
    async fn two_roots_yield_independent_sockets() {
        let (path_a, path_b) = (unique_socket(), unique_socket());
        let state_a = StateHandle::new();
        state_a.update(|s| s.goals = vec!["A".into()]);
        let state_b = StateHandle::new();
        state_b.update(|s| s.goals = vec!["B".into()]);

        let sa = tokio::spawn(serve(path_a.clone(), state_a));
        let sb = tokio::spawn(serve(path_b.clone(), state_b));

        let mut ra = BufReader::new(connect_retry(&path_a).await);
        let mut rb = BufReader::new(connect_retry(&path_b).await);
        assert_eq!(read_snapshot(&mut ra).await.goals, vec!["A".to_string()]);
        assert_eq!(read_snapshot(&mut rb).await.goals, vec!["B".to_string()]);

        sa.abort();
        sb.abort();
        let _ = std::fs::remove_file(&path_a);
        let _ = std::fs::remove_file(&path_b);
    }

    /// Two viewers both receive updates; and the producer is fine with none.
    #[tokio::test]
    async fn two_viewers_both_receive_and_zero_is_fine() {
        let path = unique_socket();
        let state = StateHandle::new();
        let server = tokio::spawn(serve(path.clone(), state.clone()));

        // Zero viewers: updates must not panic or hang.
        for _ in 0..100 {
            state.update(|_| {});
        }
        assert!(!server.is_finished());

        let mut a = BufReader::new(connect_retry(&path).await);
        let mut b = BufReader::new(connect_retry(&path).await);
        let _ = read_snapshot(&mut a).await; // replays
        let _ = read_snapshot(&mut b).await;

        state.update(|s| s.goals = vec!["x".into()]);
        assert_eq!(read_snapshot(&mut a).await.goals, vec!["x".to_string()]);
        assert_eq!(read_snapshot(&mut b).await.goals, vec!["x".to_string()]);

        server.abort();
        let _ = std::fs::remove_file(&path);
    }
}
