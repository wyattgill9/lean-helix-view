//! Proxy entry point: own the `lake serve` child, wire stdio to the forwarder,
//! and map every shutdown path so no orphaned Lean server is left behind.

use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::BufReader;
use tokio::process::{Child, Command};
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
    /// If set, write goal snapshots (JSON-lines) to this headless sink.
    pub goal_sink_path: Option<PathBuf>,
}

/// Run the proxy until Helix or Lean closes. `upstream` is the configurable
/// command (e.g. `["lake", "serve"]`), taken from the binary's trailing args.
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
        .map_err(|e| {
            io::Error::new(e.kind(), format!("failed to spawn upstream {program:?}: {e}"))
        })?;
    tracing::info!(%program, ?args, pid = ?child.id(), "spawned upstream Lean server");

    let lean_in = child.stdin.take().expect("child stdin is piped");
    let lean_out = child.stdout.take().expect("child stdout is piped");

    let state = StateHandle::new();

    // Headless goal sink (milestones 3–4): the first consumer of the viewer
    // channel. The socket server + ratatui viewer replace it in milestone 5.
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
    );

    match fwd.wait_first().await {
        FirstClosed::Client => {
            tracing::info!("Helix closed; reaping upstream and draining");
            reap(&mut child).await;
            fwd.join_s2c().await;
        }
        FirstClosed::Server => {
            tracing::warn!("upstream closed first; tearing down so Helix sees its server died");
            fwd.abort_c2s();
            reap(&mut child).await;
        }
    }

    fwd.drain_writers().await;
    tracing::info!("proxy exiting");
    Ok(())
}

/// Wait for the child to exit, killing it if it overstays the grace window —
/// the guard against orphaned Lean servers pegging a core.
async fn reap(child: &mut Child) {
    match timeout(REAP_GRACE, child.wait()).await {
        Ok(Ok(status)) => tracing::info!(?status, "upstream exited"),
        Ok(Err(e)) => tracing::warn!(error = %e, "error awaiting upstream"),
        Err(_) => {
            tracing::warn!("upstream did not exit within grace; killing");
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
}
