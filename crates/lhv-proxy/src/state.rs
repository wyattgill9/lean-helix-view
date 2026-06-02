//! The authoritative in-memory state and the viewer publish channel.
//!
//! Built on `tokio::sync::watch`, which gives exactly the semantics the spec
//! calls for: **full snapshots**, **drop-to-latest** (only the newest value is
//! retained), and **replay-on-connect** (a fresh subscriber sees the current
//! value). A slow or dead viewer can never apply backpressure to the producer.
//!
//! Live in v1, with no producer attached — `update` is wired in from
//! milestone 4 (goal-query responses) onward.

use lhv_wire::Snapshot;
use tokio::sync::watch;

/// A cloneable handle to the single authoritative [`Snapshot`].
#[derive(Clone)]
pub struct StateHandle {
    tx: watch::Sender<Snapshot>,
}

impl StateHandle {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(Snapshot::default());
        Self { tx }
    }

    /// A new subscription for the viewer socket. The first `borrow_and_update`
    /// yields the current snapshot (replay); subsequent `changed` calls wait
    /// for the next update.
    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.tx.subscribe()
    }

    /// Apply a mutation and publish it, bumping the monotonic `seq`. Works even
    /// with zero subscribers, so the latest state is always ready to replay.
    pub fn update(&self, f: impl FnOnce(&mut Snapshot)) {
        self.tx.send_modify(|s| {
            f(s);
            s.seq += 1;
        });
    }

    /// A clone of the current snapshot.
    pub fn snapshot(&self) -> Snapshot {
        self.tx.borrow().clone()
    }
}

impl Default for StateHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_bumps_seq_and_persists_for_late_subscribers() {
        let state = StateHandle::new();
        state.update(|s| s.goals = vec!["⊢ A".into()]);
        state.update(|s| s.term_goal = Some("A".into()));
        // A subscriber that arrives *after* the updates still sees the latest.
        let rx = state.subscribe();
        let snap = rx.borrow().clone();
        assert_eq!(snap.goals, vec!["⊢ A".to_string()]);
        assert_eq!(snap.term_goal.as_deref(), Some("A"));
        assert_eq!(snap.seq, 2);
    }
}
