//! LSP transport: `Content-Length` frame codec + a thin envelope parse.
//!
//! Pure and testable; carries no I/O *policy* (no notion of "client" vs
//! "server", no forwarding rules). The proxy layers policy on top.
//!
//! The central invariant: a [`Frame`] owns the **full on-wire bytes** (headers
//! and body). Forwarding re-emits those bytes verbatim, so a frame that was
//! read can be written back byte-for-byte regardless of header order, extra
//! headers, or an unparseable body. Snooping decodes a *copy* of the body via
//! [`Envelope`], never disturbing the bytes that get forwarded.

pub mod codec;
pub mod envelope;

pub use codec::{Frame, read_frame, write_frame};
pub use envelope::{Envelope, Id};
