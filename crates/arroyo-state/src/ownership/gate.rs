//! The state one ownership gate keeps behind its lock (the parent module's docs say what it is
//! for).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tracing::warn;

use super::{AdmissionRefused, FencedRequest, GateStatus};

/// One acknowledged fence and its ownership gate.
///
/// `acknowledged` is written only under `state`'s lock, by [`Gate::publish`], so a request
/// admitted under that lock and a publication are totally ordered; it is an atomic so that
/// [`Gate::acknowledged`] — every table's per-barrier read — takes no lock.
#[derive(Debug, Default)]
pub(super) struct Gate {
    acknowledged: AtomicU64,
    state: Mutex<GateState>,
    /// Woken when the last in-flight request, of either kind, drops.
    drained: Notify,
}

/// What the lock guards.
///
/// No count can overflow: every request guard and every raise ticket it counts holds a clone of
/// this gate's `Arc`, and `Arc` aborts the process before its count passes `isize::MAX`.
#[derive(Debug, Default)]
struct GateState {
    /// Deletion guards alive.
    deletions: usize,
    /// Reservation guards alive.
    reservations: usize,
    /// Raise tickets alive: while any is, admission is closed.
    raises_pending: usize,
    long_drain_waits: u64,
}

impl GateState {
    /// The in-flight count of `request`'s kind.
    fn in_flight(&mut self, request: FencedRequest) -> &mut usize {
        match request {
            FencedRequest::Deletion => &mut self.deletions,
            FencedRequest::Reservation => &mut self.reservations,
        }
    }

    /// No request of either kind in flight: what a raise waits for.
    fn is_drained(&self) -> bool {
        self.deletions == 0 && self.reservations == 0
    }
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

    /// Registers one request of kind `request` under `generation`, or says why not.
    pub(super) fn admit(
        &self,
        generation: u64,
        request: FencedRequest,
    ) -> Result<(), AdmissionRefused> {
        let mut state = self.lock();
        let acknowledged = self.acknowledged();
        if state.raises_pending > 0 {
            return Err(AdmissionRefused::Rising {
                acknowledged,
                requested: generation,
                request,
            });
        }
        if acknowledged != generation {
            return Err(AdmissionRefused::Moved {
                acknowledged,
                requested: generation,
                request,
            });
        }
        *state.in_flight(request) += 1;
        Ok(())
    }

    /// Deregisters one request of kind `request`, waking a drain if nothing of either kind is in
    /// flight any more.
    ///
    /// Called only by `RequestGuard`'s drop, with the kind that guard was admitted for — after
    /// [`Gate::admit`] counted it under this same lock — so the count it takes from is at least
    /// one.
    pub(super) fn release(&self, request: FencedRequest) {
        let mut state = self.lock();
        *state.in_flight(request) -= 1;
        if state.is_drained() {
            self.drained.notify_waiters();
        }
    }

    /// Closes admission for one more raise.
    pub(super) fn close(&self) {
        self.lock().raises_pending += 1;
    }

    /// Whether no request of either kind is in flight. Stable once true while a raise is
    /// pending: nothing is admitted while one is.
    pub(super) fn is_drained(&self) -> bool {
        self.lock().is_drained()
    }

    /// Waits until no request of either kind is in flight. Registers for the wake-up before it
    /// looks, so a release between the look and the wait is not lost.
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
        let (deletions, reservations) = {
            let mut state = self.lock();
            state.long_drain_waits = state.long_drain_waits.saturating_add(1);
            (state.deletions, state.reservations)
        };
        warn!(
            target_fence = target,
            acknowledged = self.acknowledged(),
            deletions_in_flight = deletions,
            reservations_in_flight = reservations,
            waited_ms = waited.as_millis() as u64,
            "a lifecycle fence raise is waiting for deletions and reservations admitted under the \
             acknowledged fence; it is not acknowledged until every one of them returns"
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

    pub(super) fn status(&self) -> GateStatus {
        let state = self.lock();
        GateStatus {
            acknowledged: self.acknowledged(),
            deletions_in_flight: state.deletions,
            reservations_in_flight: state.reservations,
            raises_pending: state.raises_pending,
            long_drain_waits: state.long_drain_waits,
        }
    }
}
