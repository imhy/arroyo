//! The worker's acknowledged lifecycle fence, as the ownership generation a table reads
//! (plan M11.T10b.01, ruling M11.T10R6), and the deletion gate on it (M11.T10b.02 as amended
//! by the PR #200 review, 2026-09-26).
//!
//! A worker generation's lifecycle guard (`arroyo-worker`'s `lifecycle_fence::guard`) records
//! the highest lifecycle fence it has acknowledged. The value rises only when the guard admits a
//! fenced `START`, `FENCE_ONLY` or `REVOKE` directive, and it can rise while the worker's tasks
//! keep running: an already-running adoption is acknowledged with `FENCE_ONLY`/`REVOKE` and
//! restarts nothing (M11.T27). Under the legacy (unfenced) protocol nothing is ever
//! acknowledged and the value stays zero. A table whose deletions must stop when ownership
//! moves cannot therefore take the fence once, at construction: it has to read the value as it
//! is now.
//!
//! [`AcknowledgedFence`] is that read — a cheaply cloned, `Send + Sync` handle onto one cell —
//! and [`AcknowledgedFenceWriter`] is the cell's only writer. They are two types so that "only
//! the lifecycle guard moves the fence" is a property of the types rather than a convention:
//!
//! - a reader has no method that writes, and reaches the cell only through
//!   [`AcknowledgedFence::get`] and the deletion gate below;
//! - a writer is not `Clone`, and [`AcknowledgedFenceWriter::prepare`] takes `&mut self`, so the
//!   one writer a guard owns is raised only by code holding exclusive access to it — in
//!   `arroyo-worker`, under the lock that serialises fence advancement with start admission.
//!
//! # The deletion gate: every deletion linearizes before or after the acknowledgement
//!
//! A table deleting objects under the ownership generation it read must not keep deleting once
//! a higher fence is acknowledged: the controller may hand ownership to a successor the moment
//! it reads the acknowledgement. Reading the fence again before each request does not close
//! that, because a request already sent cannot be recalled. So a deleter asks the fence itself:
//!
//! - [`AcknowledgedFence::admit_deletion`] returns a [`DeletionGuard`] only while the
//!   acknowledged fence **equals** the generation the deleter's authorization was declared
//!   under **and** no raise is pending, checked and registered under the gate's one lock. The
//!   guard is held for the request; dropping it — the request returned, the deleter unwound, or
//!   its future was cancelled — deregisters it.
//! - A raise is two-phase. [`AcknowledgedFenceWriter::prepare`] first **closes** admission
//!   under the current fence ([`Raise::Drain`]): from then on no guard is admitted under it.
//!   The raise then **drains** — [`PendingRaise::drained`] waits, asynchronously, until every
//!   guard admitted before the close has dropped — and only a drained raise is
//!   [`Raise::Ready`] to **publish** the higher value. A raise dropped before it publishes
//!   reopens admission under the fence that is still acknowledged.
//!
//! So a deletion either holds its guard before the close — and the higher fence is not
//! published, let alone acknowledged, until its request returned — or asks after the close and
//! is refused ([`DeletionRefused::Rising`]), or asks after the publication and is refused
//! ([`DeletionRefused::Moved`]).
//!
//! ## What the gate cannot establish
//!
//! A guard is dropped when the **client's** request returns. A request the client stopped
//! waiting for — its own timeout or retry budget spent — returns an error while the server may
//! still apply a DELETE it had received; and a worker process that is killed takes its guards
//! with it while such a request is on the wire. Whether a store applies a request after its
//! client gave up is the store's property, not this gate's: the gate orders every deletion
//! whose client call is unresolved before the acknowledgement, never a server-side effect after
//! the client call ended. The drain therefore never gives up on its own: a stuck request keeps
//! the raise waiting (reported every [`LONG_DRAIN_WAIT`] by a warning and
//! [`DeletionGateStatus::long_drain_waits`]), and the acknowledgement — hence the ownership
//! transfer that waits for it — waits with it.
//!
//! ```
//! use arroyo_state::ownership::{AcknowledgedFenceWriter, DeletionRefused, Raise};
//!
//! let mut writer = AcknowledgedFenceWriter::unacknowledged();
//! let fence = writer.reader();
//! let kept = fence.clone();
//! assert_eq!(fence.get(), 0);
//!
//! // A deletion under the acknowledged fence holds the raise to 4 back.
//! let deleting = fence.admit_deletion(0).expect("admitted under the acknowledged fence");
//! let Raise::Drain(pending) = writer.prepare(4, None) else { unreachable!() };
//! assert_eq!(kept.get(), 0, "not published while the deletion is in flight");
//! assert_eq!(
//!     fence.admit_deletion(0).unwrap_err(),
//!     DeletionRefused::Rising { acknowledged: 0, requested: 0 },
//! );
//! let pending = pending.try_drained().unwrap_err();
//!
//! drop(deleting);
//! let drained = pending.try_drained().expect("the deletion returned");
//! let Raise::Ready(ready) = writer.prepare(4, Some(drained)) else { unreachable!() };
//! assert_eq!(ready.publish(), 4);
//! assert_eq!((fence.get(), kept.get()), (4, 4));
//! assert_eq!(
//!     fence.admit_deletion(0).unwrap_err(),
//!     DeletionRefused::Moved { acknowledged: 4, requested: 0 },
//! );
//!
//! // The fence only rises.
//! assert!(matches!(writer.prepare(3, None), Raise::NotAbove));
//! assert_eq!(kept.get(), 4);
//! ```
//!
//! A reader cannot write:
//!
//! ```compile_fail,E0599
//! use arroyo_state::ownership::AcknowledgedFenceWriter;
//!
//! let writer = AcknowledgedFenceWriter::unacknowledged();
//! let _ = writer.reader().prepare(5, None);
//! ```
//!
//! a writer cannot be duplicated (`clone` on it only copies a shared reference):
//!
//! ```compile_fail,E0308
//! use arroyo_state::ownership::AcknowledgedFenceWriter;
//!
//! let writer = AcknowledgedFenceWriter::unacknowledged();
//! let second: AcknowledgedFenceWriter = writer.clone();
//! ```
//!
//! and a shared reference to the writer cannot raise it:
//!
//! ```compile_fail,E0596
//! use arroyo_state::ownership::AcknowledgedFenceWriter;
//!
//! let writer = AcknowledgedFenceWriter::unacknowledged();
//! let shared = &writer;
//! let _ = shared.prepare(5, None);
//! ```

mod gate;
mod raise;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use gate::Gate;

pub use raise::{DrainedRaise, PendingRaise, Raise, ReadyRaise};

/// How long a raise waits for in-flight deletions before it reports the wait — a warning and
/// one [`DeletionGateStatus::long_drain_waits`] — and goes on waiting. It never stops waiting on
/// its own (module docs, "What the gate cannot establish").
pub const LONG_DRAIN_WAIT: Duration = Duration::from_secs(10);

/// A read-only handle onto a worker generation's highest acknowledged lifecycle fence, and its
/// deletion gate.
///
/// Zero means no fence has been acknowledged. Every clone reads the same cell, so a table
/// that keeps a clone reads the current value at any later time, and successive reads never
/// observe a lower value than an earlier one: the cell's only writer only raises it.
#[derive(Clone, Debug)]
pub struct AcknowledgedFence {
    gate: Arc<Gate>,
}

impl AcknowledgedFence {
    /// A handle no writer exists for: it reads zero — no fence acknowledged — for as long as
    /// it lives, and its deletion gate admits every deletion under zero.
    ///
    /// For executions that run under no lifecycle guard: the in-process engine
    /// (`Program::local_from_logical`) and tests.
    pub fn unfenced() -> Self {
        Self {
            gate: Arc::new(Gate::default()),
        }
    }

    /// The highest fence acknowledged so far; zero means none.
    pub fn get(&self) -> u64 {
        self.gate.acknowledged()
    }

    /// Admits one deletion under `generation` — the ownership generation the deleter's
    /// authorization was declared under — if the acknowledged fence equals it and no raise is
    /// pending; the check and the registration are one step under the gate's lock (module docs).
    ///
    /// Hold the guard for exactly the request it covers, and drop it when the request returns.
    ///
    /// # Errors
    ///
    /// [`DeletionRefused::Rising`] while a raise is pending, else [`DeletionRefused::Moved`]
    /// when the acknowledged fence is not `generation` — in either direction.
    pub fn admit_deletion(&self, generation: u64) -> Result<DeletionGuard, DeletionRefused> {
        self.gate.admit(generation)?;
        Ok(DeletionGuard {
            gate: Arc::clone(&self.gate),
        })
    }

    /// What the deletion gate holds now: the gauge a stuck drain shows up on.
    pub fn deletion_gate(&self) -> DeletionGateStatus {
        self.gate.status()
    }
}

/// The only writer of one [`AcknowledgedFence`] cell.
///
/// Not `Clone`, and raised through `&mut self`: whoever owns it is the one place the fence can
/// move. In a worker that owner is the lifecycle guard's fence state.
#[derive(Debug)]
pub struct AcknowledgedFenceWriter {
    gate: Arc<Gate>,
}

impl AcknowledgedFenceWriter {
    /// A new cell at zero — no fence acknowledged — and its writer.
    pub fn unacknowledged() -> Self {
        Self {
            gate: Arc::new(Gate::default()),
        }
    }

    /// The highest fence acknowledged so far; zero means none.
    pub fn get(&self) -> u64 {
        self.gate.acknowledged()
    }

    /// A read handle onto this writer's cell.
    pub fn reader(&self) -> AcknowledgedFence {
        AcknowledgedFence {
            gate: Arc::clone(&self.gate),
        }
    }

    /// One step of a raise to `fence` (module docs), given the drained raise an earlier step
    /// handed back, if any:
    ///
    /// - [`Raise::NotAbove`] when `fence` is not above the acknowledged fence — nothing to raise;
    ///   a `drained` raise handed in is dropped, which reopens the admission it closed;
    /// - [`Raise::Ready`] when `drained` is this gate's, drained for exactly `fence` — or when
    ///   closing admission finds no deletion in flight, so the whole raise is one step;
    /// - [`Raise::Drain`] otherwise: admission under the current fence is now closed, and the
    ///   pending raise must be drained and handed back.
    ///
    /// A `drained` raise of another writer, or for another fence, is dropped and a fresh close
    /// taken: it can never publish here.
    pub fn prepare(&mut self, fence: u64, drained: Option<DrainedRaise>) -> Raise<'_> {
        if fence <= self.gate.acknowledged() {
            drop(drained);
            return Raise::NotAbove;
        }
        let ticket = match drained {
            Some(drained) if drained.is_for(&self.gate, fence) => drained.into_ticket(),
            stale @ (Some(_) | None) => {
                // Dropped before the close, so a stale claim on this gate never overlaps the
                // fresh one.
                drop(stale);
                match PendingRaise::close(&self.gate, fence).try_drained() {
                    Ok(drained) => drained.into_ticket(),
                    Err(pending) => return Raise::Drain(pending),
                }
            }
        };
        Raise::Ready(ReadyRaise::new(self, ticket))
    }
}

/// One deletion admitted under the acknowledged fence (module docs). While it lives, no raise
/// of that fence publishes; dropping it deregisters the deletion.
///
/// `Send`, so a deleter may take it where the request runs — a blocking pool, another task.
/// Dropping it is how the deletion ends; `mem::forget` on it keeps every later raise of this
/// fence waiting, which fails closed — nothing is acknowledged — but is reported only by the
/// stuck drain's warning.
#[must_use = "a deletion guard admits one request; dropping it at once admits none"]
#[derive(Debug)]
pub struct DeletionGuard {
    gate: Arc<Gate>,
}

impl Drop for DeletionGuard {
    fn drop(&mut self) {
        self.gate.release();
    }
}

/// Why [`AcknowledgedFence::admit_deletion`] admitted nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeletionRefused {
    /// A raise above `acknowledged` has closed admission and not yet published or been
    /// abandoned.
    #[error(
        "a deletion under ownership generation {requested} was refused: a raise of the \
         acknowledged lifecycle fence {acknowledged} is draining its in-flight deletions"
    )]
    Rising {
        /// The fence acknowledged when the deletion asked.
        acknowledged: u64,
        /// The generation the deletion asked under.
        requested: u64,
    },
    /// The acknowledged fence is not the generation the deletion asked under.
    #[error(
        "a deletion under ownership generation {requested} was refused: the acknowledged \
         lifecycle fence is {acknowledged}"
    )]
    Moved {
        /// The fence acknowledged when the deletion asked.
        acknowledged: u64,
        /// The generation the deletion asked under.
        requested: u64,
    },
}

/// A snapshot of one deletion gate: the fence it guards, and what is holding a raise back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionGateStatus {
    /// The acknowledged fence.
    pub acknowledged: u64,
    /// Deletions admitted and not yet returned.
    pub in_flight: usize,
    /// Raises that closed admission and have neither published nor been abandoned.
    pub raises_pending: usize,
    /// Drain waits that ran past their budget — [`LONG_DRAIN_WAIT`] each in production —
    /// since the gate was made. Monotone and saturating.
    pub long_drain_waits: u64,
}
