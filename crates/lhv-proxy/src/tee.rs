//! Tee handlers. `textDocument/publishDiagnostics` and `$/lean/fileProgress`
//! belong to Helix and keep flowing verbatim; these fold a *copy* into the
//! shared state so the sink/viewer sees them too. Best-effort — anything
//! unparseable is ignored, and forwarding has already happened by the time
//! these run.

use lhv_wire::{Diagnostic, Range, Severity};
use serde_json::Value;

use crate::json::parse_range;
use crate::state::StateHandle;

/// Fold a `publishDiagnostics` notification into `state.diagnostics[uri]`.
pub fn apply_diagnostics(state: &StateHandle, body: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let Some(params) = value.get("params") else { return };
    let Some(uri) = params.get("uri").and_then(Value::as_str) else {
        return;
    };
    let diagnostics: Vec<Diagnostic> = params
        .get("diagnostics")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(parse_diagnostic).collect())
        .unwrap_or_default();
    let uri = uri.to_owned();
    state.update(move |s| {
        s.diagnostics.insert(uri, diagnostics);
    });
}

/// Fold a `$/lean/fileProgress` notification into `state.progress`, and set the
/// coarse `elaborating` gate (refined per-position gating is milestone 6).
pub fn apply_progress(state: &StateHandle, body: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let Some(params) = value.get("params") else { return };
    let ranges: Vec<Range> = params
        .get("processing")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|p| p.get("range").and_then(parse_range))
                .collect()
        })
        .unwrap_or_default();
    let elaborating = !ranges.is_empty();
    state.update(move |s| {
        s.progress = ranges;
        s.elaborating = elaborating;
    });
}

fn parse_diagnostic(v: &Value) -> Option<Diagnostic> {
    Some(Diagnostic {
        range: parse_range(v.get("range")?)?,
        severity: Severity::from_lsp(v.get("severity").and_then(Value::as_i64)),
        message: v.get("message").and_then(Value::as_str)?.to_owned(),
        source: v.get("source").and_then(Value::as_str).map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_land_keyed_by_uri() {
        let state = StateHandle::new();
        apply_diagnostics(
            &state,
            br#"{"method":"textDocument/publishDiagnostics","params":{"uri":"file:///A.lean","diagnostics":[{"range":{"start":{"line":1,"character":0},"end":{"line":1,"character":3}},"severity":2,"message":"warn","source":"lean"}]}}"#,
        );
        let snap = state.snapshot();
        let diags = snap.diagnostics.get("file:///A.lean").unwrap();
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Severity::Warning);
        assert_eq!(diags[0].message, "warn");
        assert_eq!(diags[0].source.as_deref(), Some("lean"));
    }

    #[test]
    fn progress_sets_then_clears_elaborating() {
        let state = StateHandle::new();
        apply_progress(
            &state,
            br#"{"method":"$/lean/fileProgress","params":{"textDocument":{"uri":"file:///A.lean"},"processing":[{"range":{"start":{"line":0,"character":0},"end":{"line":2,"character":0}}}]}}"#,
        );
        assert!(state.snapshot().elaborating);
        apply_progress(
            &state,
            br#"{"method":"$/lean/fileProgress","params":{"textDocument":{"uri":"file:///A.lean"},"processing":[]}}"#,
        );
        let snap = state.snapshot();
        assert!(!snap.elaborating);
        assert!(snap.progress.is_empty());
    }
}
