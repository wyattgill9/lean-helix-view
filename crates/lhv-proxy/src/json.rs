//! Tiny shared parsers from LSP wire JSON into `lhv-wire` geometry types.
//!
//! Hand-written rather than serde-derived: LSP encodes severity as an integer
//! and positions show up nested in several shapes, and keeping these allocation
//! -light lets the snoop and the tee share them.

use lhv_wire::{Position, Range};
use serde_json::Value;

pub(crate) fn parse_position(v: &Value) -> Option<Position> {
    Some(Position {
        line: v.get("line")?.as_u64()? as u32,
        character: v.get("character")?.as_u64()? as u32,
    })
}

pub(crate) fn parse_range(v: &Value) -> Option<Range> {
    Some(Range {
        start: parse_position(v.get("start")?)?,
        end: parse_position(v.get("end")?)?,
    })
}
