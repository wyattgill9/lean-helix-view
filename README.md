# lean-helix-view

**A Lean 4 infoview for [Helix](https://helix-editor.com), in a terminal pane.**
Goals, expected type, diagnostics — live, next to your editor. No patches to
Helix or Lean.

<img width="1512" height="982" alt="lean-helix-view" src="https://github.com/user-attachments/assets/ee73e70a-37aa-40e3-bc45-262b04b32820" />

## Setup (3 steps)

**1. Install**

```sh
cargo install --path crates/lean-helix-view
```

Needs Rust ≥ 1.85 and `lake` on your `PATH` (via [elan](https://github.com/leanprover/elan)).

**2. Point Helix at it** — `~/.config/helix/languages.toml`:

```toml
[language-server.lean]
command = "lean-helix-view"
args = ["proxy", "--", "lake", "serve"]

[[language]]
name = "lean"
language-servers = ["lean"]
```

**3. Run the viewer** in a tmux/zellij pane, from your Lean project root:

```sh
lean-helix-view watch
```

Done. Launch order doesn't matter — it reconnects on its own.

**Keys:** `q`/`Esc` quit · `j`/`k` or arrows scroll · `g`/`Home` top.

## The one catch

Goals refresh when Helix sends a position request — edits, hover, completion,
goto-def, and the idle ping when your cursor stops moving. **Not** during pure
cursor motion. Helix exposes no cursor-move event to outside tools, so nobody
can fix this from here. In practice: goals appear a beat after you stop moving.

## How it works (30 seconds)

Helix thinks it's talking to `lake serve`. It's talking to a proxy that forwards
every byte untouched, while watching for cursor positions so it can ask Lean for
goals on the side and publish them over a Unix socket to the viewer.

Rules the proxy never breaks: forwarded bytes are never altered, one writer per
sink (no interleaving), and nothing downstream — viewer, snoop, logs — can stall
the pipe.

Crates: `lhv-lsp` (frame codec) · `lhv-wire` (proxy↔viewer types) · `lhv-proxy`
(forward + snoop + query + serve) · `lhv-viewer` (ratatui TUI) ·
`lean-helix-view` (bin).

## Something's broken

Logs: `~/.local/state/lean-helix-view/proxy.log`. `RUST_LOG=debug` for more.
In Helix, `:log-open` shows the Lean server's stderr.

| Symptom | Fix |
|---|---|
| "The Lean server didn't start" — `lake` not found | Install via elan, or use an absolute `command` path in `languages.toml`. |
| "The Lean server didn't start" — exited immediately | `lake build` in the project, then reopen. |
| "No proxy found for this workspace" | Run `watch` from the same dir Helix opened, or pass `--socket <path>` (shown on that screen). |
| "waiting / disconnected, retrying…" | Normal before/after a proxy restart. If it never connects, check `:log-open`. |
| Stale socket from a crash | Reclaimed automatically. Ignore. |

## Flags & extras

`--debounce-ms <n>` (default 120) · `--trigger <method>` (repeatable, overrides
the default request set) · `--capture <path>` (record client cadence) ·
`--goal-sink <path>` (headless goal snapshots). Tuning guide:
[docs/cadence-capture.md](docs/cadence-capture.md).

Build without installing: `nix develop` (or `direnv allow`), then `cargo build
--release` and `cargo test --workspace`.

## Status

**v1.0.** Proxy is byte-for-byte transparent, full LSP lifecycle, everything on
the roadmap shipped.

Not in v1 (by choice, not gaps): Lean's interactive RPC session, widgets, the
browser infoview, goals on pure cursor motion (Helix-blocked).
