//! The transparent stdio LSP proxy between Helix and `lake serve`.
//!
//! [`run`] is the entry point. The forwarder ([`forward`]) is the sacred path;
//! [`snoop`], [`query`], [`state`], and [`server`] are the seams the goal-view
//! features grow into — wired live where it matters (inject channel, the
//! consume-injected-id branch, the viewer state + watch channel), with no
//! producer attached yet.

pub mod forward;
pub mod query;
pub mod server;
pub mod snoop;
pub mod state;

mod run;

pub use forward::{FirstClosed, Forwarder};
pub use run::run;
pub use state::StateHandle;
