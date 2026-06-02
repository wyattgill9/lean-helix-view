//! Proxy entry point: own the `lake serve` child, wire stdio to the forwarder,
//! and map every shutdown path so no orphaned Lean server is left behind.

use std::io;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::BufReader;
use tokio::process::{Child, Command};
use tokio::time::timeout;

use crate::forward::{FirstClosed, Forwarder};
use crate::state::StateHandle;

/// How long to wait for the upstream to exit on its own before killing it.
const REAP_GRACE: Duration = Duration::from_secs(5);

/// Run the proxy until Helix or Lean closes. `upstream` is the configurable
/// command (e.g. `["lake", "serve"]`), taken from the binary's trailing args.
pub async fn run(upstream: Vec<String>) -> io::Result<()> {
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
    // MILESTONE 5 SEAM: spawn the viewer socket server from this same state:
    //   tokio::spawn(crate::server::serve(lhv_wire::default_socket_path(), state.clone()));

    let mut fwd = Forwarder::spawn(
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        lean_in,
        BufReader::new(lean_out),
        state,
    );

    match fwd.wait_first().await {
        FirstClosed::Client => {
            // Helix closed stdin: C2S has dropped Lean's stdin senders, so the
            // child's stdin is closing. Reap it (bounded), letting S2C flush
            // Lean's final output to Helix.
            tracing::info!("Helix closed; reaping upstream and draining");
            reap(&mut child).await;
            fwd.join_s2c().await;
        }
        FirstClosed::Server => {
            // Lean died/closed first: S2C dropped the to-Helix sender, so Helix
            // will see EOF once we exit. Stop reading Helix and reap.
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
