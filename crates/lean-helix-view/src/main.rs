//! `lean-helix-view`: a thin dispatcher over the proxy and the viewer.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_appender::non_blocking::WorkerGuard;

#[derive(Parser)]
#[command(
    name = "lean-helix-view",
    version,
    about = "Terminal-native Lean 4 infoview for Helix"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Transparent LSP proxy between Helix and the upstream Lean server.
    ///
    /// Give the upstream command as trailing args after `--`, e.g.
    /// `lean-helix-view proxy -- lake serve`.
    Proxy {
        /// Idle debounce before an injected goal query fires (milliseconds).
        #[arg(long, default_value_t = 120)]
        debounce_ms: u64,
        /// Record a client→server cadence capture (JSON-lines) to this path.
        #[arg(long, value_name = "PATH")]
        capture: Option<PathBuf>,
        /// Write goal-state snapshots (JSON-lines) to this path (headless sink).
        #[arg(long, value_name = "PATH")]
        goal_sink: Option<PathBuf>,
        /// Override the explicit-position trigger set (repeatable). Bare names
        /// are prefixed with `textDocument/`; omit entirely for the default set.
        #[arg(long = "trigger", value_name = "METHOD")]
        triggers: Vec<String>,
        /// Override the viewer socket path (default: workspace-root-keyed).
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
        #[arg(last = true, required = true, value_name = "UPSTREAM")]
        upstream: Vec<String>,
    },
    /// The infoview TUI; connects to a running proxy's socket.
    Watch {
        /// Override the proxy socket path.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let component = match &cli.command {
        Command::Proxy { .. } => "proxy",
        Command::Watch { .. } => "viewer",
    };
    let _guard = init_tracing(component);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        match cli.command {
            Command::Proxy {
                debounce_ms,
                capture,
                goal_sink,
                triggers,
                socket,
                upstream,
            } => {
                let config = lhv_proxy::Config {
                    debounce: Duration::from_millis(debounce_ms),
                    triggers,
                    capture_path: capture,
                    goal_sink_path: goal_sink,
                    socket_path: socket,
                };
                lhv_proxy::run(upstream, config).await
            }
            Command::Watch { socket } => lhv_viewer::run(socket).await,
        }
    })
}

/// Initialize tracing to a file in the XDG state dir.
///
/// **Never** stdout — that is the LSP channel. Not stderr either: for the proxy
/// it carries Lean's passed-through logs, and for the viewer it hosts the TUI.
fn init_tracing(component: &str) -> WorkerGuard {
    let dir = log_dir();
    let _ = std::fs::create_dir_all(&dir);
    let appender = tracing_appender::rolling::never(&dir, format!("{component}.log"));
    let (writer, guard) = tracing_appender::non_blocking(appender);

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .with_env_filter(filter)
        .init();
    guard
}

fn log_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(dir).join("lean-helix-view");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".local/state/lean-helix-view");
    }
    std::env::temp_dir().join("lean-helix-view")
}
