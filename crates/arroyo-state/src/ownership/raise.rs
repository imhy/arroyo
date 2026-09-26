//! A raise of the acknowledged fence, one phase per type: closed ([`PendingRaise`]), drained
//! ([`DrainedRaise`]), matched to its writer ([`ReadyRaise`]) — the parent module's docs give
//! the protocol.

use std::sync::Arc;
use std::time::Duration;

use super::gate::Gate;
use super::{AcknowledgedFenceWriter, LONG_DRAIN_WAIT};

/// What one [`AcknowledgedFenceWriter::prepare`] step decided.
#[must_use = "a raise that is neither published nor drained reopens admission when dropped"]
#[derive(Debug)]
pub enum Raise<'a> {
    /// The fence asked for is not above the acknowledged one: nothing to raise.
    NotAbove,
    /// Admission under the acknowledged fence is closed and deletions admitted before the
    /// close are in flight: drain this, then hand the drained raise back to `prepare`.
    Drain(PendingRaise),
    /// Drained: publish it.
    Ready(ReadyRaise<'a>),
}

/// One raise's claim on its gate: while it lives, admission is closed. Dropped without
/// publishing, it reopens admission (unless another raise still holds it closed).
#[derive(Debug)]
pub(super) struct Ticket {
    gate: Arc<Gate>,
    target: u64,
    /// Set by [`ReadyRaise::publish`], whose publication released the claim.
    published: bool,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.published {
            self.gate.abandon();
        }
    }
}

/// A raise that has closed deletion admission under the acknowledged fence and has not yet
/// seen the deletions admitted before the close return.
///
/// `Send`, and it does not borrow the writer: the worker awaits the drain with its lifecycle
/// lock released. Dropping it — a cancelled handler, a refused directive — abandons the raise:
/// nothing was acknowledged, so admission under the fence still acknowledged reopens.
#[must_use = "dropping a pending raise abandons it and reopens deletion admission"]
#[derive(Debug)]
pub struct PendingRaise {
    ticket: Ticket,
}

impl PendingRaise {
    /// Closes admission on `gate` for a raise to `target`.
    pub(super) fn close(gate: &Arc<Gate>, target: u64) -> Self {
        gate.close();
        Self {
            ticket: Ticket {
                gate: Arc::clone(gate),
                target,
                published: false,
            },
        }
    }

    /// The fence this raise is for.
    pub fn target(&self) -> u64 {
        self.ticket.target
    }

    /// Drained now, or handed back.
    ///
    /// # Errors
    ///
    /// This raise, unchanged, while a deletion admitted before the close is in flight.
    pub fn try_drained(self) -> Result<DrainedRaise, PendingRaise> {
        if self.ticket.gate.is_drained() {
            Ok(DrainedRaise {
                ticket: self.ticket,
            })
        } else {
            Err(self)
        }
    }

    /// Waits at most `budget` for the drain. A wait that runs out is reported — a warning and
    /// one [`long_drain_waits`](super::DeletionGateStatus::long_drain_waits) — and hands the
    /// raise back, still pending: running out never publishes.
    ///
    /// # Errors
    ///
    /// This raise, still pending, when `budget` ran out first.
    pub async fn wait_for(self, budget: Duration) -> Result<DrainedRaise, PendingRaise> {
        let gate = Arc::clone(&self.ticket.gate);
        match tokio::time::timeout(budget, gate.until_drained()).await {
            Ok(()) => Ok(DrainedRaise {
                ticket: self.ticket,
            }),
            Err(_elapsed) => {
                gate.note_long_wait(self.ticket.target, budget);
                Err(self)
            }
        }
    }

    /// Waits for the drain, however long it takes, reporting every [`LONG_DRAIN_WAIT`]
    /// (the parent module's docs say why it never gives up). Returns at once — without touching
    /// a timer — when nothing is in flight.
    pub async fn drained(self) -> DrainedRaise {
        let mut pending = match self.try_drained() {
            Ok(drained) => return drained,
            Err(pending) => pending,
        };
        loop {
            pending = match pending.wait_for(LONG_DRAIN_WAIT).await {
                Ok(drained) => return drained,
                Err(still) => still,
            };
        }
    }
}

/// A raise whose close has been observed with no deletion in flight. Admission stayed closed
/// since, so none is in flight still: hand it to [`AcknowledgedFenceWriter::prepare`] to
/// publish. Dropping it abandons the raise.
#[must_use = "dropping a drained raise abandons it and reopens deletion admission"]
#[derive(Debug)]
pub struct DrainedRaise {
    ticket: Ticket,
}

impl DrainedRaise {
    /// The fence this raise is for.
    pub fn target(&self) -> u64 {
        self.ticket.target
    }

    /// Whether this raise closed `gate` for exactly `target`.
    pub(super) fn is_for(&self, gate: &Arc<Gate>, target: u64) -> bool {
        Arc::ptr_eq(&self.ticket.gate, gate) && self.ticket.target == target
    }

    pub(super) fn into_ticket(self) -> Ticket {
        self.ticket
    }
}

/// A drained raise, matched to the writer whose gate it closed: it borrows that writer, so it
/// can publish nowhere else, and nothing else can move the fence meanwhile.
#[must_use = "dropping a ready raise abandons it and reopens deletion admission"]
#[derive(Debug)]
pub struct ReadyRaise<'a> {
    writer: &'a mut AcknowledgedFenceWriter,
    ticket: Ticket,
}

impl<'a> ReadyRaise<'a> {
    pub(super) fn new(writer: &'a mut AcknowledgedFenceWriter, ticket: Ticket) -> Self {
        Self { writer, ticket }
    }

    /// The fence this raise publishes.
    pub fn target(&self) -> u64 {
        self.ticket.target
    }

    /// Publishes the raise: the acknowledged fence becomes its target, and this raise's hold on
    /// admission is released — admission is open under the new fence once no other raise holds
    /// it closed. Returns the fence now acknowledged.
    ///
    /// The ticket closed this writer's own gate — [`AcknowledgedFenceWriter::prepare`] builds a
    /// ready raise only from a ticket it closed there, or from a drained raise it matched to that
    /// gate and this target — so the claim released is the one this raise took.
    pub fn publish(mut self) -> u64 {
        self.ticket.published = true;
        self.writer.gate.publish(self.ticket.target)
    }
}
