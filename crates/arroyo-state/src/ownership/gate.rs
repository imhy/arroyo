//! The state one deletion gate keeps behind its lock (the parent module's docs say what it is
//! for).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tracing::warn;

use super::{DeletionGateStatus, DeletionRefused};

/// One acknowledged fence and its deletion gate.
///
/// `acknowledged` is written only under `state`'s lock, by [`Gate::publish`], so a deletion
/// admitted under that lock and a publication are totally ordered; it is an atomic so that
/// [`Gate::acknowledged`] — every table's per-barrier read — takes no lock.
#[derive(Debug, Default)]
pub(super) struct Gate {
    acknowledged: AtomicU64,
    state: Mutex<GateState>,
    /// Woken when the last in-flight deletion drops.
    drained: Notify,
}

/// What the lock guards.
///
/// Neither count can overflow: every deletion guard and every raise ticket it counts holds a
/// clone of this gate's `Arc`, and `Arc` aborts the process before its count passes
/// `isize::MAX`.
#[derive(Debug, Default)]
struct GateState {
    /// Deletion guards alive.
    in_flight: usize,
    /// Raise tickets alive: while any is, admission is closed.
    raises_pending: usize,
    long_drain_waits: u64,
}

impl Gate {
    /// The lock. Every critical section below is a few integer updates that cannot panic, so a
    /// poisoned lock still guards consistent counts and is taken as it is.
    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn acknowledged(&self) -> u64 {
        self.acknowledged.load(Ordering::Acquire)
    }

    /// Registers one deletion under `generation`, or says why not.
    pub(super) fn admit(&self, generation: u64) -> Result<(), DeletionRefused> {
        let mut state = self.lock();
        let acknowledged = self.acknowledged();
        if state.raises_pending > 0 {
            return Err(DeletionRefused::Rising {
                acknowledged,
                requested: generation,
            });
        }
        if acknowledged != generation {
            return Err(DeletionRefused::Moved {
                acknowledged,
                requested: generation,
            });
        }
        state.in_flight += 1;
        Ok(())
    }

    /// Deregisters one deletion, waking a drain if it was the last.
    pub(super) fn release(&self) {
        let mut state = self.lock();
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.drained.notify_waiters();
        }
    }

    /// Closes admission for one more raise.
    pub(super) fn close(&self) {
        self.lock().raises_pending += 1;
    }

    /// Whether no deletion is in flight. Stable once true while a raise is pending: nothing is
    /// admitted while one is.
    pub(super) fn is_drained(&self) -> bool {
        self.lock().in_flight == 0
    }

    /// Waits until no deletion is in flight. Registers for the wake-up before it looks, so a
    /// release between the look and the wait is not lost.
    pub(super) async fn until_drained(&self) {
        loop {
            let notified = self.drained.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.is_drained() {
                return;
            }
            notified.await;
        }
    }

    /// A raise to `target` waited `waited` and is still draining: report it.
    pub(super) fn note_long_wait(&self, target: u64, waited: Duration) {
        let in_flight = {
            let mut state = self.lock();
            state.long_drain_waits = state.long_drain_waits.saturating_add(1);
            state.in_flight
        };
        warn!(
            target_fence = target,
            acknowledged = self.acknowledged(),
            in_flight,
            waited_ms = waited.as_millis() as u64,
            "a lifecycle fence raise is waiting for deletions admitted under the acknowledged \
             fence; it is not acknowledged until every one of them returns"
        );
    }

    /// Publishes a drained raise to `target` and drops its ticket's claim. The value only rises.
    pub(super) fn publish(&self, target: u64) -> u64 {
        let mut state = self.lock();
        self.acknowledged.fetch_max(target, Ordering::Release);
        state.raises_pending -= 1;
        self.acknowledged()
    }

    /// Drops a raise ticket that did not publish: admission reopens once no other is pending.
    pub(super) fn abandon(&self) {
        self.lock().raises_pending -= 1;
    }

    pub(super) fn status(&self) -> DeletionGateStatus {
        let state = self.lock();
        DeletionGateStatus {
            acknowledged: self.acknowledged(),
            in_flight: state.in_flight,
            raises_pending: state.raises_pending,
            long_drain_waits: state.long_drain_waits,
        }
    }
}
