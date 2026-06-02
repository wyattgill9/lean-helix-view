//! Milestone-2 "invisible proxy" check against the *real* compiled binary.
//!
//! Uses `cat` as a stand-in upstream: it echoes each client→server frame back
//! on the server→client path. No frame carries an injected (`lhv-q…`) id, so
//! the proxy must forward everything verbatim in both directions — and must
//! reap the child and exit cleanly when the client closes stdin.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn frame(body: &str) -> Vec<u8> {
    format!("Content-Length: {}\r\n\r\n{}", body.len(), body).into_bytes()
}

#[test]
fn proxy_is_byte_transparent_over_a_child_process() {
    let mut input = Vec::new();
    input.extend(frame(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"x":1}}"#,
    ));
    input.extend(frame(
        r#"{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{}}"#,
    ));
    input.extend(frame(
        r#"{"jsonrpc":"2.0","id":2,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///A.lean"},"position":{"line":3,"character":5}}}"#,
    ));

    let mut child = Command::new(env!("CARGO_BIN_EXE_lean-helix-view"))
        .args(["proxy", "--", "cat"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the proxy binary");

    {
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&input).unwrap();
        // Dropping stdin sends EOF to the proxy, triggering clean shutdown.
    }

    let mut output = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut output)
        .unwrap();

    // The proxy must exit on its own — no orphaned child.
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "proxy did not exit after the client closed"
        );
        std::thread::sleep(Duration::from_millis(20));
    };

    assert!(status.success(), "proxy exited unsuccessfully: {status:?}");
    assert_eq!(
        output, input,
        "the proxy must be byte-transparent in both directions"
    );
}
