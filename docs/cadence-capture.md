# Cadence capture & the goal sink (milestones 3–4)

Helix exposes no cursor-move event to external tools, so the proxy learns the
cursor only from the LSP requests Helix sends. The open question is the real
*cadence* — in particular whether Helix fires a position-carrying request (e.g.
`documentHighlight`) shortly after the cursor stops, which would give
near-cursor-move goal updates for free. **We measure this rather than assume
it**, and the trigger set is configuration, not a hardcoded constant.

## Running a capture

1. Point Helix's `languages.toml` at the proxy with `--capture`:

   ```toml
   [language-server.lean]
   command = "lean-helix-view"
   args = ["proxy", "--capture", "/tmp/lhv-cadence.jsonl", "--", "lake", "serve"]

   [[language]]
   name = "lean"
   language-servers = ["lean"]
   ```

2. Open a real Lean file in Helix and drive a representative session: rest the
   cursor (no keys) at several spots, hover, type a bit, trigger completion, do
   a goto-definition. Jot down roughly what you did and when.

3. Quit Helix (this closes the proxy). Inspect `/tmp/lhv-cadence.jsonl`.

Each line is one client→server message:

```json
{"ts_unix_ms":1780427190820,"method":"textDocument/documentHighlight",
 "has_position":true,"uri":"file:///A.lean","line":12,"character":4,
 "version":null,"is_trigger":true,"would_focus":true}
```

- `is_trigger` — the method is in the active trigger set.
- `would_focus` — it produced a *legal* focus (session initialized + doc open),
  i.e. a goal query would fire for it after the debounce.

The snoop channel is best-effort (drop-on-full), so under a flood the capture
can have gaps; it never backpressures Helix↔Lean.

## Reading it (jq)

```sh
# Method histogram
jq -r .method cap.jsonl | sort | uniq -c | sort -rn

# Which methods actually carry a position
jq -r 'select(.has_position) | .method' cap.jsonl | sort | uniq -c

# Inter-arrival gaps (ms) of documentHighlight — the on-idle cadence
jq 'select(.method=="textDocument/documentHighlight") | .ts_unix_ms' cap.jsonl \
  | awk 'NR>1{print $1-p} {p=$1}'

# After you stop typing, does a positional request arrive on idle?
# Look for a would_focus:true line ~editor.idle-timeout after the last didChange.
jq -c 'select(.would_focus or .method=="textDocument/didChange")
       | {t:.ts_unix_ms, method, line, character}' cap.jsonl
```

## Hypothesis (for the human to confirm or adjust)

Helix issues `textDocument/documentHighlight` at the cursor after
`editor.idle-timeout` (default 250 ms) whenever the cursor comes to rest — even
with no edit. **If the capture confirms this**, the default trigger set already
delivers near-cursor-move updates: cursor stops → ~250 ms → `documentHighlight`
→ +debounce → goal query. `hover` / `completion` / `signatureHelp` add updates
when you explicitly invoke them.

**If it does not** fire on pure cursor rest in your Helix build/config, the
fallbacks still cover the common case: `didChange` refreshes goals at the edit
point as you type, and explicit hover/goto-definition still work. You would then
widen the trigger set and/or lean on idle-timeout tuning.

Either way the design is data-driven — confirm against your own capture.

## Tuning the trigger set

The active set is configurable; nothing is hardcoded.

```toml
# Restrict to what actually fires on idle (less injection noise):
args = ["proxy", "--trigger", "documentHighlight", "--", "lake", "serve"]
```

- Repeat `--trigger` per method. Bare names are prefixed with `textDocument/`;
  pass full method names (e.g. `$/lean/…`) verbatim. Omit `--trigger` entirely
  for the default six (`hover`, `completion`, `definition`, `references`,
  `documentHighlight`, `signatureHelp`).
- Match the debounce to the observed cadence: `--debounce-ms 150` (default 120).

## Inspecting goals without a TUI (milestone 4)

`--goal-sink /tmp/lhv-goals.jsonl` appends a full state snapshot — goals,
expected type, diagnostics-by-uri, progress — as JSON-lines on every change:

```toml
args = ["proxy", "--goal-sink", "/tmp/lhv-goals.jsonl", "--", "lake", "serve"]
```

```sh
tail -f /tmp/lhv-goals.jsonl | jq '{doc, in_tactic, goals, term_goal, elaborating}'
```

**Human check:** open a Lean file, move into a tactic block and rest the cursor;
a snapshot with the expected goals for that position should appear within a few
hundred ms. Type, and goals should refresh at the edit. Throughout, Helix must
stay invisible — completions, diagnostics, and goto-definition unaffected, and
no goal-query response ever shown to Helix.

Capture and goal-sink are diagnostics for milestones 3–5; the Unix socket +
ratatui viewer (milestone 5) replace the goal-sink. Neither ever writes to
stdout — that is the LSP wire.
