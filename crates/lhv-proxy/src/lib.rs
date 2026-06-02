//! The transparent stdio LSP proxy between Helix and `lake serve`.
//!
//! [`run`] is the entry point. The forwarder ([`forward`]) is the sacred path.
//! As of milestones 3–4 the goal-view seams are *activated*: the [`snoop`]
//! reads a copy of the client stream and tracks session + focus; the [`query`]
//! querier debounces and injects `$/lean/plainGoal` / `plainTermGoal` through
//! the shared Lean-stdin writer; the consume-injected-id branch in [`forward`]
//! withholds those responses from Helix and folds them into [`state`]; and
//! [`tee`] copies diagnostics + progress into the same state. A headless
//! [`sink`] drains the state watch to a JSON-lines file. The socket [`server`]
//! and the ratatui viewer stay dormant until milestone 5.

pub mod forward;
pub mod query;
pub mod server;
pub mod sink;
pub mod snoop;
pub mod state;
pub mod tee;

mod json;
mod run;

pub use forward::{FirstClosed, Forwarder, SnoopConfig};
pub use run::{Config, run};
pub use snoop::default_triggers;
pub use state::StateHandle;
