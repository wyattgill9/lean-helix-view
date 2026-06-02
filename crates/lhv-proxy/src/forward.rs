//! The bidirectional forwarder: the sacred Helix↔Lean pipe.
//!
//! Three invariants hold absolutely:
//! 1. forwardable bytes are never altered — frames are re-emitted verbatim
//!    ([`Frame::as_bytes`]), and a frame that can't be parsed is still
//!    forwarded;
//! 2. two frames are never interleaved on one sink — each sink has exactly one
//!    writer task, fed by a channel;
//! 3. snoop / classification never stalls or corrupts the path — decoding is
//!    done on a borrowed *copy* of the body, after the forward is enqueued.
//!
//! Generic over its four streams, so the same core is exercised by an
//! in-memory transparency test (milestone 1) and by real stdio + the
//! `lake serve` child (milestone 2).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lhv_lsp::{Envelope, Frame, Id, read_frame, write_frame};
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::query::{InjectedIds, Querier, handle_injected_response};
use crate::state::StateHandle;

/// Frames buffered toward a sink before the producing pump experiences
/// backpressure. This bound applies only to the *sacred* path (legitimate
/// flow control); the viewer path is separately drop-to-latest.
const SINK_CAPACITY: usize = 1024;

/// Which direction closed first, selecting the shutdown choreography.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstClosed {
    /// Helix closed our stdin (clean editor shutdown, or Helix died).
    Client,
    /// `lake serve` closed its stdout (it exited or crashed).
    Server,
}

/// The single writer for one sink. Drains its channel; when every producer has
/// dropped (or a write fails), it half-closes the sink so the peer sees EOF.
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

/// Client→server pump. Forwards every frame verbatim; holds the `Querier` so
/// the inject channel stays live and its sender drops together with the
/// forward sender when the client closes (cleanly closing Lean's stdin).
async fn pump_c2s<R>(mut client_in: R, forward_tx: mpsc::Sender<Frame>, _querier: Querier)
where
    R: AsyncBufRead + Unpin,
{
    loop {
        match read_frame(&mut client_in).await {
            Ok(Some(frame)) => {
                // SACRED: enqueue the verbatim forward first, before anything else.
                if forward_tx.send(frame).await.is_err() {
                    tracing::debug!("c2s: to-Lean sink closed");
                    return;
                }
                // MILESTONE 4 SEAM: snoop a copy of the body off the hot path and,
                // on a position hit, call `_querier.request(..)`. Nothing here
                // may block or precede the forward above.
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

/// Server→client pump. Classifies each frame off a borrowed copy and forwards
/// it — *unless* it is a response to an id we injected, which is consumed.
async fn pump_s2c<R>(
    mut server_in: R,
    to_helix_tx: mpsc::Sender<Frame>,
    injected: InjectedIds,
    state: StateHandle,
) where
    R: AsyncBufRead + Unpin,
{
    loop {
        match read_frame(&mut server_in).await {
            Ok(Some(frame)) => {
                // Decode a copy for classification; this never gates forwarding.
                if let Some(env) = Envelope::parse(frame.body())
                    && env.is_response()
                    && let Some(Id::Str(id)) = &env.id
                {
                    let pending = injected.lock().unwrap().remove(id);
                    if let Some(pending) = pending {
                        // CONSUME: our own goal-query response — never reaches Helix.
                        tracing::trace!(%id, "consumed injected response");
                        handle_injected_response(&state, pending, frame.body());
                        continue;
                    }
                }
                // Notifications, server→client requests, foreign responses, and
                // unparseable frames all forward verbatim.
                if to_helix_tx.send(frame).await.is_err() {
                    tracing::debug!("s2c: to-Helix sink closed");
                    return;
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

/// A running forwarder: four tasks (two pumps, two writers) plus the shared
/// injected-id registry. The caller drives lifecycle via [`Forwarder::wait_first`]
/// and the teardown helpers.
pub struct Forwarder {
    c2s: JoinHandle<()>,
    s2c: JoinHandle<()>,
    lean_writer: JoinHandle<()>,
    helix_writer: JoinHandle<()>,
    injected: InjectedIds,
}

impl Forwarder {
    /// Wire up and spawn all four tasks.
    ///
    /// - `helix_in` / `helix_out`: our stdin / stdout (Helix's view of "Lean").
    /// - `lean_in` / `lean_out`: the child's stdin / stdout.
    pub fn spawn<HIn, HOut, LIn, LOut>(
        helix_in: HIn,
        helix_out: HOut,
        lean_in: LIn,
        lean_out: LOut,
        state: StateHandle,
    ) -> Forwarder
    where
        HIn: AsyncBufRead + Unpin + Send + 'static,
        HOut: AsyncWrite + Unpin + Send + 'static,
        LIn: AsyncWrite + Unpin + Send + 'static,
        LOut: AsyncBufRead + Unpin + Send + 'static,
    {
        let (to_lean_tx, to_lean_rx) = mpsc::channel::<Frame>(SINK_CAPACITY);
        let (to_helix_tx, to_helix_rx) = mpsc::channel::<Frame>(SINK_CAPACITY);
        let injected: InjectedIds = Arc::new(Mutex::new(HashMap::new()));

        // One writer per sink — the sole serializer of frames onto it.
        let lean_writer = tokio::spawn(writer(lean_in, to_lean_rx));
        let helix_writer = tokio::spawn(writer(helix_out, to_helix_rx));

        // Inject channel: the querier holds a clone of the to-Lean sender; the
        // C2S pump owns both that querier and the forward sender, so both drop
        // together at client close.
        let querier = Querier::new(to_lean_tx.clone(), injected.clone());
        let c2s = tokio::spawn(pump_c2s(helix_in, to_lean_tx, querier));
        let s2c = tokio::spawn(pump_s2c(lean_out, to_helix_tx, injected.clone(), state));

        Forwarder {
            c2s,
            s2c,
            lean_writer,
            helix_writer,
            injected,
        }
    }

    /// The shared injected-id registry. Exposed for milestone-4 producers and
    /// for tests that pre-register an id to prove the consume branch.
    pub fn injected(&self) -> InjectedIds {
        self.injected.clone()
    }

    /// Resolve when either direction's pump finishes.
    pub async fn wait_first(&mut self) -> FirstClosed {
        tokio::select! {
            _ = &mut self.c2s => FirstClosed::Client,
            _ = &mut self.s2c => FirstClosed::Server,
        }
    }

    /// Stop the client→server pump (used when the server closed first).
    pub fn abort_c2s(&self) {
        self.c2s.abort();
    }

    /// Await the server→client pump — lets Lean's final output flush to Helix
    /// after the client has closed.
    pub async fn join_s2c(&mut self) {
        let _ = (&mut self.s2c).await;
    }

    /// Drain both writers so each sink gets a clean half-close.
    pub async fn drain_writers(self) {
        let _ = self.lean_writer.await;
        let _ = self.helix_writer.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Pending, QueryKind};
    use lhv_wire::{DocRef, Position};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, duplex};

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// End-to-end transparency against an in-memory mock server: every client
    /// byte reaches the server verbatim, every server byte reaches the client
    /// verbatim — except a response to an id we injected, which is consumed and
    /// never reaches Helix.
    #[tokio::test]
    async fn transparent_passthrough_and_injected_consume() {
        // Four unidirectional pipes (using one direction of each duplex).
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
        );

        // Pre-register an injected id, as if the querier had sent a plainGoal.
        fwd.injected().lock().unwrap().insert(
            "lhv-q7".into(),
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
            },
        );

        // ---- client -> server ----
        let mut client_bytes = Vec::new();
        client_bytes.extend_from_slice(
            Frame::from_body(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
                .as_bytes(),
        );
        client_bytes.extend_from_slice(
            Frame::from_body(br#"{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{}}"#)
                .as_bytes(),
        );
        helix_tx.write_all(&client_bytes).await.unwrap();
        helix_tx.shutdown().await.unwrap(); // client closes stdin -> C2S EOF -> closes Lean stdin
        drop(helix_tx);

        let mut got_on_lean = Vec::new();
        lean_rx.read_to_end(&mut got_on_lean).await.unwrap();
        assert_eq!(
            got_on_lean, client_bytes,
            "client->server bytes must pass through verbatim"
        );

        // ---- server -> client (one injected response interleaved) ----
        let notif = Frame::from_body(
            br#"{"jsonrpc":"2.0","method":"textDocument/publishDiagnostics","params":{}}"#,
        );
        let response = Frame::from_body(br#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#);
        let injected = Frame::from_body(
            r#"{"jsonrpc":"2.0","id":"lhv-q7","result":{"goals":["⊢ True"],"rendered":"r"}}"#
                .as_bytes(),
        );

        let mut expected_on_helix = Vec::new();
        expected_on_helix.extend_from_slice(notif.as_bytes());
        expected_on_helix.extend_from_slice(response.as_bytes());

        lean_tx.write_all(notif.as_bytes()).await.unwrap();
        lean_tx.write_all(injected.as_bytes()).await.unwrap(); // must be swallowed
        lean_tx.write_all(response.as_bytes()).await.unwrap();
        lean_tx.shutdown().await.unwrap();
        drop(lean_tx);

        let mut got_on_helix = Vec::new();
        helix_rx.read_to_end(&mut got_on_helix).await.unwrap();
        assert_eq!(
            got_on_helix, expected_on_helix,
            "server->client verbatim, minus the consumed injected response"
        );
        assert!(
            !contains(&got_on_helix, b"lhv-q7"),
            "no injected id may ever reach Helix"
        );

        // The consumed response drove the (dormant) state path.
        let snap = state.snapshot();
        assert_eq!(snap.goals, vec!["⊢ True".to_string()]);
        assert!(snap.in_tactic);

        fwd.drain_writers().await;
    }
}
