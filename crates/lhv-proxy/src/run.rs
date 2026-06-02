//! Proxy entry point: own the `lake serve` child, wire stdio to the forwarder,
//! and map every shutdown path so no orphaned Lean server is left behind — and
//! so a first-run failure (no `lake`, project not built, wrong toolchain) fails
//! with a reason a human can act on.

use std::io;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::io::BufReader;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::forward::{FirstClosed, Forwarder, SnoopConfig};
use crate::sink::run_goal_sink;
use crate::snoop::resolve_triggers;
use crate::state::StateHandle;

/// How long to wait for the upstream to exit on its own before killing it.
const REAP_GRACE: Duration = Duration::from_secs(5);

/// Proxy configuration assembled from the CLI.
#[derive(Debug, Clone)]
pub struct Config {
    /// Idle debounce before an injected goal query fires.
    pub debounce: Duration,
    /// Raw trigger methods from the CLI (empty = default set); resolved here.
    pub triggers: Vec<String>,
    /// If set, record a client→server cadence capture (JSON-lines).
    pub capture_path: Option<PathBuf>,
    /// If set, also write goal snapshots (JSON-lines) to this debug sink.
    pub goal_sink_path: Option<PathBuf>,
    /// Override the viewer socket path (default: workspace-root-keyed).
    pub socket_path: Option<PathBuf>,
}

/// Run the proxy until Helix or Lean closes (or a termination signal arrives).
/// `upstream` is the configurable command (e.g. `["lake", "serve"]`).
pub async fn run(upstream: Vec<String>, config: Config) -> io::Result<()> {
    let (program, args) = upstream
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no upstream command given"))?;

    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // lake's stderr passes straight through to ours
        .spawn()
        .map_err(|e| io::Error::new(e.kind(), spawn_error_message(program, &e)))?;
    tracing::info!(%program, ?args, pid = ?child.id(), "spawned upstream Lean server");

    let lean_in = child.stdin.take().expect("child stdin is piped");
    let lean_out = child.stdout.take().expect("child stdout is piped");

    let state = StateHandle::new();

    // The snoop signals the workspace root (from `initialize`) once; the socket
    // binds lazily off that. A `--socket` override binds immediately instead.
    let (root_tx, root_rx) = oneshot::channel::<Option<String>>();
    spawn_socket_server(state.clone(), config.socket_path.clone(), root_rx);

    // Optional debug goal sink (the headless JSON-lines consumer from m3–4).
    if let Some(path) = config.goal_sink_path.clone() {
        let rx = state.subscribe();
        tokio::spawn(async move {
            if let Err(e) = run_goal_sink(rx, path).await {
                tracing::warn!(error = %e, "goal sink ended");
            }
        });
    }

    let snoop_config = SnoopConfig {
        debounce: config.debounce,
        triggers: resolve_triggers(config.triggers),
        capture_path: config.capture_path,
    };

    let mut fwd = Forwarder::spawn(
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        lean_in,
        BufReader::new(lean_out),
        state,
        snoop_config,
        Some(root_tx),
    );

    let first = tokio::select! {
        outcome = fwd.wait_first() => Some(outcome),
        _ = shutdown_signal() => None,
    };

    match first {
        Some(FirstClosed::Client) => {
            tracing::info!("Helix closed; reaping upstream and draining");
            reap(&mut child).await;
            fwd.join_s2c().await;
        }
        Some(FirstClosed::Server) => {
            let status = reap(&mut child).await;
            if fwd.server_spoke() {
                tracing::warn!("upstream exited; Helix will see its server stop");
            } else {
                // Never produced any LSP output → it died during startup.
                report_startup_failure(program, status);
            }
            fwd.abort_c2s();
        }
        None => {
            tracing::info!("received termination signal; shutting down");
            fwd.abort_c2s();
            reap(&mut child).await;
        }
    }

    fwd.drain_writers().await;
    tracing::info!("proxy exiting");
    Ok(())
}

/// A clear, specific reason a spawn failed — distinguishing "not found" (the
/// most common first-run failure) from other errors.
fn spawn_error_message(program: &str, e: &io::Error) -> String {
    if e.kind() == io::ErrorKind::NotFound {
        format!(
            "upstream command `{program}` not found — is Lean/elan installed and `{program}` on your PATH?"
        )
    } else {
        format!("failed to spawn upstream `{program}`: {e}")
    }
}

/// The upstream spawned but exited without ever talking LSP: a startup failure.
fn report_startup_failure(program: &str, status: Option<ExitStatus>) {
    let detail = match status {
        Some(s) => match s.code() {
            Some(code) => format!("exited with code {code}"),
            None => format!("was terminated ({s})"),
        },
        None => "did not exit cleanly".to_string(),
    };
    // stderr (which Helix captures) + the log — never stdout.
    eprintln!(
        "lean-helix-view: the upstream Lean server (`{program}`) {detail} without starting up.\n  \
         Likely causes: not inside a Lean project, the project isn't built (run `lake build`), \
         `{program}` is the wrong command, or the toolchain is missing.\n  \
         Any output from `{program}` itself is above; details are in the log."
    );
    tracing::error!(%program, ?status, "upstream exited without starting up");
}

/// Spawn the viewer socket server. With an explicit `override_path` it binds at
/// once; otherwise it waits for the snoop's `rootUri` signal and keys the path
/// off the workspace root. If `initialize` is never seen, no socket is created.
fn spawn_socket_server(
    state: StateHandle,
    override_path: Option<PathBuf>,
    root_rx: oneshot::Receiver<Option<String>>,
) {
    tokio::spawn(async move {
        let path = match override_path {
            Some(path) => path,
            None => match root_rx.await {
                Ok(root_uri) => {
                    let root = root_uri
                        .map(|uri| lhv_wire::root_path_from_uri(&uri))
                        .unwrap_or_else(lhv_wire::cwd_root);
                    lhv_wire::socket_path_for_root(&root)
                }
                Err(_) => {
                    tracing::debug!("no initialize observed; viewer socket not bound");
                    return;
                }
            },
        };
        if let Err(e) = crate::server::serve(path, state).await {
            tracing::warn!(error = %e, "viewer socket server ended");
        }
    });
}

/// Wait for the child to exit, killing it if it overstays the grace window —
/// the guard against orphaned Lean servers pegging a core. Returns the exit
/// status when the child exited on its own.
async fn reap(child: &mut Child) -> Option<ExitStatus> {
    match timeout(REAP_GRACE, child.wait()).await {
        Ok(Ok(status)) => {
            tracing::info!(?status, "upstream exited");
            Some(status)
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "error awaiting upstream");
            None
        }
        Err(_) => {
            tracing::warn!("upstream did not exit within grace; killing");
            let _ = child.start_kill();
            let _ = child.wait().await;
            None
        }
    }
}

/// Resolve when a termination signal (SIGTERM / SIGINT) arrives, so we can shut
/// down cleanly — reaping the child and dropping the socket task (which removes
/// its socket file) — instead of being killed mid-flight.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).ok();
    let mut interrupt = signal(SignalKind::interrupt()).ok();
    match (term.as_mut(), interrupt.as_mut()) {
        (Some(t), Some(i)) => {
            tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
        }
        (Some(t), None) => {
            t.recv().await;
        }
        (None, Some(i)) => {
            i.recv().await;
        }
        (None, None) => std::future::pending::<()>().await,
    }
}
