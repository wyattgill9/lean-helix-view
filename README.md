# lean-helix-view

<img width="1470" height="892" alt="lean-tui-view" src="https://github.com/user-attachments/assets/d83b813d-d8ed-4cdd-8e31-cbce5276e4cb" />

A terminal-native Lean 4 infoview for [Helix](https://helix-editor.com), built
**without modifying Helix or Lean**. It shows live goal / expected-type /
diagnostics state in a tmux/zellij pane next to the editor.

It works by slipping a transparent LSP proxy between Helix and `lake serve`:
Helix talks to the proxy as if it were the Lean language server, the proxy
forwards everything untouched, and watches the position-carrying requests Helix
sends so it can issue its own goal queries and publish them to a viewer pane.

**Status: v1.0.** The proxy is byte-for-byte transparent (completions,
diagnostics, goto-def all behave exactly as if talking to `lake serve` directly)
with full lifecycle handling. The snoop tracks session + cursor focus; the
querier debounces and injects `plainGoal` / `plainTermGoal` through the shared
Lean-stdin writer; their responses are consumed (never leaked to Helix) and
folded into state with teed diagnostics + progress; and a Unix-socket server
publishes that state to the `watch` ratatui viewer. The publish path is fully
decoupled from the LSP pipe (a `watch` channel, drop-to-latest, never awaits a
viewer).

### How it updates — the one caveat

The view refreshes whenever Helix sends a position-carrying request: on **edits,
hover, completion, and goto-definition**, and on the idle requests Helix issues
when the cursor comes to rest (typically `textDocument/documentHighlight` after
`editor.idle-timeout`). It does **not** refresh on pure cursor motion while you
are still moving — Helix exposes no cursor-move event to external tools, so this
is a Helix design constraint, not a limitation we can fix from outside. In
practice the goals update within a moment of the cursor settling. (You can
measure your own Helix's cadence and tune the trigger set — see
[docs/cadence-capture.md](docs/cadence-capture.md).)

## Layout

A single binary, `lean-helix-view`, with `proxy` and `watch` subcommands, over a
Cargo workspace:

| crate            | role                                                              |
|------------------|-------------------------------------------------------------------|
| `lhv-lsp`        | `Content-Length` frame codec + thin envelope parse. Pure, no I/O policy. |
| `lhv-wire`       | serde types for the proxy↔viewer protocol. One source of truth.   |
| `lhv-proxy`      | forwarder, snoop, goal querier, state store, socket server.       |
| `lhv-viewer`     | ratatui TUI + socket client, with a reconnect loop.               |
| `lean-helix-view`| thin bin: clap arg parsing, dispatches `proxy` / `watch`.         |

### The sacred pipe

The forwarder upholds three invariants absolutely:

1. **Never alter forwardable bytes.** Each `Frame` owns its full on-wire bytes;
   forwarding re-emits them verbatim. A frame that can't be parsed is still
   forwarded. Snooping decodes a *copy* of the body, off the hot path.
2. **Never interleave two frames on one sink.** Each sink (Lean's stdin, Helix's
   stdout) has exactly one writer task, fed by a channel; "forward" and "inject"
   both enqueue, and the one task serializes.
3. **Never let snoop, viewer, or logging stall the path.** The viewer channel is
   `watch`-based (drop-to-latest, no backpressure); logging goes to a file.

## Install

From a clone of this repository:

```sh
cargo install --path crates/lean-helix-view
```

That puts `lean-helix-view` on your `PATH` (`~/.cargo/bin`). Requires a Rust
toolchain (edition 2024 / Rust ≥ 1.85) and, for use, a working Lean install
(`lake` on your `PATH`, via [elan](https://github.com/leanprover/elan)).

To build from source without installing:

```sh
nix develop          # or: direnv allow  (provides cargo + elan)
cargo build --release   # binary at target/release/lean-helix-view
cargo test --workspace
```

## Wiring into Helix

Helix already maps the `lean` language to a `lean` language server whose command
is `lake serve`. Override just that command in your
`~/.config/helix/languages.toml` to route through the proxy:

```toml
[language-server.lean]
command = "lean-helix-view"
args = ["proxy", "--", "lake", "serve"]

[[language]]
name = "lean"
language-servers = ["lean"]
```

- Everything after `--` is the upstream command; the proxy spawns it as a child,
  inheriting Helix's working directory (so `lake serve` runs in your project
  root, as it must).
- Use an absolute `command` path if the binary isn't on Helix's `PATH`.
- The proxy never writes to stdout except forwarded LSP traffic. Its own logs go
  to `$XDG_STATE_HOME/lean-helix-view/proxy.log` (default
  `~/.local/state/lean-helix-view/proxy.log`); `lake serve`'s stderr passes
  through to the proxy's stderr, where Helix captures it. Set `RUST_LOG=debug`
  for verbose tracing.
- Optional flags: `--capture <path>` (record client cadence, JSON-lines),
  `--goal-sink <path>` (write goal-state snapshots, JSON-lines),
  `--debounce-ms <n>` (default 120), `--trigger <method>` (repeatable; overrides
  the default position-request set). See
  [docs/cadence-capture.md](docs/cadence-capture.md).

After this, Helix should behave exactly as before — the proxy is invisible.

## Launching the viewer

Helix's config is unchanged (it just launches the `proxy` as above). The viewer
is a separate process you run in an adjacent pane:

```sh
# from your Lean project root (same dir Helix opened), in a tmux/zellij pane:
lean-helix-view watch
# or point it explicitly:
lean-helix-view watch --socket /run/user/$UID/lean-helix-view/<hash>.sock
```

The viewer auto-discovers the socket by hashing the workspace root (its current
directory must match Helix's `rootUri` — run it from the project root, or use
`--socket`). It connects whenever the proxy appears and reconnects across proxy
restarts, so launch order doesn't matter. Keys: `q`/`Esc` quit, `j`/`k` (or
arrows) scroll goals, `g`/`Home` jump to top. It renders Goals, Expected type,
Diagnostics, and a Progress (elaborating) indicator, with a connection-status
line.

## Troubleshooting

The proxy logs to `$XDG_STATE_HOME/lean-helix-view/proxy.log` (default
`~/.local/state/...`); `RUST_LOG=debug` adds detail. In Helix, the Lean server's
stderr (and the proxy's diagnostics) show via `:log-open`.

- **"The Lean server didn't start."** The proxy prints a specific reason to
  stderr (which Helix captures):
  - *`lake` not found* — install Lean via [elan](https://github.com/leanprover/elan)
    and make sure `lake` is on the `PATH` Helix sees. Use an absolute `command`
    in `languages.toml` if needed.
  - *exited without starting up* — usually the project isn't built or you're not
    in a Lean project. Run `lake build` in the project, then reopen.
- **Viewer shows "No proxy found for this workspace."** Helix isn't running for
  this project (no proxy bound its socket yet), or you launched `watch` from a
  different directory than the project root. Run it from the project root, or
  pass `--socket <path>` (the expected path is shown on that screen and in the
  proxy log).
- **Viewer shows "waiting / disconnected, retrying…"** Normal before the proxy
  starts or across a restart; it reconnects with backoff. If it never connects,
  check that the proxy is running (`:log-open` in Helix) and that the socket
  paths match.
- **A stale socket** from a crashed proxy is detected and reclaimed on the next
  start — no action needed.

## Roadmap

1. ✅ Scaffold + transport codec, proven by a byte-equality transparency test.
2. ✅ Real forwarder over `lake serve` + full lifecycle; the proxy is invisible.
3. ✅ Instrument (`--capture`): record every client→server method to JSON-lines
   to measure cadence; decide the update model from data, not assumption.
4. ✅ Snoop + goal querier + injection: focus tracking, debounce, supersession,
   consume-injected-id (no leak), tee diagnostics/progress, headless `--goal-sink`.
5. ✅ Socket server (rootUri-keyed, replay-on-connect, drop-to-latest) + the
   `watch` ratatui viewer: Goals / Expected type / Diagnostics / Progress.
6. ✅ Release hardening: clear failure diagnostics, reconnect backoff, workspace
   -root resolution, multi-instance sockets, stale-socket reclaim, cleanup on
   every exit, progress goal-gating, docs + release metadata. **→ v1.0**

### Not in v1 (future, not gaps)

Lean's interactive RPC session (`$/lean/rpc/connect`), interactive widgets, the
browser infoview, and goals-on-pure-cursor-motion (blocked by Helix regardless).
