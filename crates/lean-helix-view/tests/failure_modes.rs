//! First-five-minutes failures must fail gracefully with a reason a human can
//! act on — never a hang or a silent nothing. Verified against the real binary.

use std::io::Read;
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_lean-helix-view");

/// Upstream command not found (e.g. `lake` not on PATH) → non-zero exit with a
/// clear, specific message naming the command — not a cryptic OS error.
#[test]
fn missing_upstream_command_fails_clearly() {
    let output = Command::new(BIN)
        .args(["proxy", "--", "definitely-not-a-real-binary-xyz-12345"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn the proxy binary");

    assert!(!output.status.success(), "must exit non-zero when it can't spawn");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not found"), "stderr should explain: {stderr}");
    assert!(
        stderr.contains("definitely-not-a-real-binary-xyz-12345"),
        "stderr should name the command: {stderr}"
    );
}

/// Upstream spawns but exits immediately without talking LSP (e.g. not a built
/// Lean project) → a clear "didn't start up" diagnostic on stderr, and the
/// proxy itself still exits cleanly (it propagated the death to Helix).
#[test]
fn upstream_that_exits_immediately_is_diagnosed() {
    let mut child = Command::new(BIN)
        .args(["proxy", "--", "false"]) // exits 1 at once, no output
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the proxy binary");

    // Hold stdin open so the *server* side closes first (the failure path),
    // not the client side.
    let _stdin = child.stdin.take().unwrap();

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    let status = child.wait().unwrap();

    assert!(status.success(), "proxy handles the death gracefully and exits 0");
    assert!(
        stderr.contains("without starting up"),
        "stderr should diagnose the early exit: {stderr}"
    );
    assert!(stderr.contains("lake build"), "stderr should hint a fix: {stderr}");
}
