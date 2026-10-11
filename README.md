<h1 align="center">lean-helix-view</h1>

<p align="center">
  A Lean 4 infoview for Helix, in a terminal pane.
  <br />
  <a href="#about">About</a>
  ·
  <a href="#installation">Installation</a>
  ·
  <a href="#how-it-works">How It Works</a>
  ·
  <a href="#troubleshooting">Troubleshooting</a>
  ·
  <a href="#developing">Developing</a>
</p>

<img width="1512" height="982" alt="lean-helix-view" src="https://github.com/user-attachments/assets/ee73e70a-37aa-40e3-bc45-262b04b32820" />

## About

lean-helix-view shows Lean 4 goals, expected types, and diagnostics in a
terminal pane next to [Helix](https://helix-editor.com), updating live as
you work.

Lean is hard to write without an infoview, and Helix doesn't have one. The
usual answers are to switch editors or patch Helix. I didn't want to do
either. lean-helix-view needs no changes to Helix or Lean. It sits between
them as a transparent LSP proxy and sends what it learns to a small TUI
viewer.

The project does one thing: it puts the goal state where you can see it.
Widgets, the browser infoview, and Lean's interactive RPC session are out of
scope.

## Installation

You need Rust 1.85 or newer, and `lake` on your `PATH` (install it with
[elan](https://github.com/leanprover/elan)).

```sh
cargo install --path crates/lean-helix-view
```

Then tell Helix to start Lean through the proxy. In
`~/.config/helix/languages.toml`:

```toml
[language-server.lean]
command = "lean-helix-view"
args = ["proxy", "--", "lake", "serve"]

[[language]]
name = "lean"
language-servers = ["lean"]
```

Finally, open a tmux or zellij pane in your Lean project root and run:

```sh
lean-helix-view watch
```

It doesn't matter which you start first. The viewer reconnects on its own.
In the viewer, `q` or `Esc` quits, `j`/`k` or the arrow keys scroll, and
`g` or `Home` jumps to the top.

## How It Works

Helix thinks it's talking to `lake serve`. It's actually talking to a proxy
that passes every byte through unchanged. Along the way, the proxy watches
for cursor positions, asks Lean for the goals at that position, and
publishes them over a Unix socket to the viewer.

The proxy follows three rules:

1. Forwarded bytes are never altered.
2. Each sink has exactly one writer, so messages never interleave.
3. Nothing downstream (the viewer, the snooper, logging) can stall the pipe.

There is one limitation you will notice. Goals refresh whenever Helix sends
a position request: edits, hover, completion, goto-definition, and the idle
ping Helix sends when the cursor stops moving. They don't refresh during
pure cursor motion, because Helix doesn't expose cursor movement to outside
tools. In practice, goals appear a moment after you stop moving. This can't
be fixed from outside Helix.

The code is split into a few crates:

| Crate             | Purpose                                     |
| ----------------- | ------------------------------------------- |
| `lhv-lsp`         | LSP frame codec                             |
| `lhv-wire`        | Message types shared by proxy and viewer    |
| `lhv-proxy`       | Forwarding, snooping, goal queries, socket  |
| `lhv-viewer`      | The ratatui TUI                             |
| `lean-helix-view` | The binary                                  |

## Troubleshooting

The proxy logs to `~/.local/state/lean-helix-view/proxy.log`. Set
`RUST_LOG=debug` for more detail. In Helix, `:log-open` shows the Lean
server's stderr.

| Symptom                                               | Fix                                                                                          |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| "The Lean server didn't start" and `lake` not found   | Install Lean with elan, or use an absolute `command` path in `languages.toml`.               |
| "The Lean server didn't start" and it exited at once  | Run `lake build` in the project, then reopen the file.                                       |
| "No proxy found for this workspace"                   | Run `watch` from the directory Helix opened, or pass the `--socket <path>` shown on screen.  |
| "waiting / disconnected, retrying…"                   | Expected before and after a proxy restart. If it never connects, check `:log-open`.          |
| A stale socket left behind by a crash                 | Nothing to do. It's cleaned up automatically.                                                |

The proxy also takes a few flags:

- `--debounce-ms <n>`: how long to wait before querying goals (default 120).
- `--trigger <method>`: an LSP method that triggers a goal query. Repeat it
  to build your own set, replacing the default.
- `--capture <path>`: record the timing of client requests.
- `--goal-sink <path>`: write goal snapshots to a file, with no viewer.

See [docs/cadence-capture.md](docs/cadence-capture.md) for how to tune these.

## Developing

The repo has a Nix flake. Run `nix develop` (or `direnv allow`), then:

```sh
cargo build --release
cargo test --workspace
```
