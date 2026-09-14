//! The backend-neutral expiring-time-key view seam (design item M11.D13).
//!
//! [`ExpiringTimeKeyViewApi`] is the object-safe API that
//! [`TableManager::get_expiring_time_key_table`] hands out. The manager owns the view —
//! it caches a `Box<dyn ExpiringTimeKeyViewApi + Send>` and lends a `&mut dyn` out of
//! that box — so a view's lifetime is never tied to the `Arc<dyn ErasedTable>` it was
//! built from, and a backend other than parquet can supply one without `arroyo-state`
//! naming its type.
//!
//! Reading the view is a **drain**: [`ExpiringTimeKeyViewApi::begin_batch_drain`] mints a
//! [`BatchDrainToken`] and each [`ExpiringTimeKeyViewApi::next_drained_batch`] returns at
//! most one owned `(SystemTime, RecordBatch)`. The one-batch step is the primitive
//! because it is the only shape that both bounds transient memory (M11.D36) and lets a
//! caller that needs its own `&mut` state between batches — `InstantJoin::on_start`,
//! which replays each restored batch back through the operator's insert path — make
//! progress without holding a borrow of the view across the call.
//! [`ExpiringTimeKeyViewDrain::all_batches_for_watermark`] wraps the same two calls in the
//! [`ExpiringTimeKeyBatchStream`] that D13 specifies. It is deliberately not a method of
//! the view trait: a provided method is overridable, so a view could have replaced it with
//! one that materialised the whole table and still satisfied D13's signature. It lives
//! instead on a sealed extension trait carrying a single blanket implementation over every
//! `T: ExpiringTimeKeyViewApi + ?Sized` — the `dyn` the manager lends out included — so
//! coherence rejects any second implementation and no view can supply its own. One poll of
//! the stream is therefore one `next_drained_batch`, for every view that exists or will be
//! written.
//!
//! # Drain token invariant matrix
//!
//! A pull is a validate-then-act boundary: [`ExpiringTimeKeyViewApi::next_drained_batch`]
//! acts on state the caller named indirectly, by handing back a [`BatchDrainToken`]. Every
//! identity that has to agree for that act to be the one the caller asked for is listed
//! here, with the evidence an implementation must hold for it.
//!
//! | Boundary | Identity that must agree | Evidence that establishes it | Effect derived from the validated token |
//! |---|---|---|---|
//! | `next_drained_batch(token)` | **Which view** — the token was minted by *this* view | the view stores the token it minted and compares by equality; [`BatchDrainToken::mint`] draws from one process-wide monotonic counter, so no value another view holds can compare equal | the batch returned comes from this view's own state |
//! | `next_drained_batch(token)` | **Which drain of this view** — the live one, not a superseded one | `begin_batch_drain` mints a fresh token and replaces the drain record, so the previous token is stored nowhere and can never compare equal again | the position advanced belongs to the drain the caller started |
//! | `next_drained_batch(token)` | **Which range** — the retention cutoff and the pending bound fixed when that drain began | an implementation captures the range into the same record as the token, so validating the token validates the range with it; no range is recomputed per pull from a later watermark | the batch lies inside the range the caller asked for |
//! | `next_drained_batch(token)` | **Undisturbed** — nothing rewrote the part of that range still to come | the mutating methods mark the live drain record; a marked drain is refused | the sequence has no gap, and no batch the caller's own write put there |
//!
//! Nothing about a token is optional evidence: a caller cannot construct a value that
//! passes row one, because the only public constructor is `mint`, and a minted value is
//! fresh — it is not equal to any token any view is currently holding.
//!
//! [`TableManager::get_expiring_time_key_table`]: crate::tables::table_manager::TableManager::get_expiring_time_key_table

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use arroyo_rpc::errors::StateError;
use async_trait::async_trait;
use futures::{Stream, stream};

pub mod parquet_view;

#[cfg(test)]
mod tests;

/// A boxed, borrowing stream of the batches one drain of a view covers.
///
/// The stream borrows the view for `'a`, so it cannot outlive it and cannot be turned
/// into a `'static` snapshot by cloning the view's contents. Each item is one owned
/// `(SystemTime, RecordBatch)`; a `RecordBatch` owns `Arc`s over Arrow buffers, so
/// producing an item shares those buffers rather than copying them.
pub type ExpiringTimeKeyBatchStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(SystemTime, RecordBatch), StateError>> + Send + 'a>>;

/// The source of every [`BatchDrainToken`].
///
/// Write-only and monotonic: it is incremented to hand out a value and is never read
/// back, never reset, and never consulted by a lookup. It is therefore not the mutable
/// global that M11.T09t bars and risk M11.T09p describes — that clause is about a
/// resettable registry shared between tests and the record path, whose *contents* decide
/// behaviour. Nothing here decides behaviour; the counter only guarantees that two values
/// handed out in this process differ.
static NEXT_BATCH_DRAIN_TOKEN: AtomicU64 = AtomicU64::new(0);

/// Identifies one drain of one view.
///
/// A view mints a token in [`ExpiringTimeKeyViewApi::begin_batch_drain`], keeps it, and
/// refuses every other value in [`ExpiringTimeKeyViewApi::next_drained_batch`]. Because
/// [`Self::mint`] is the only public constructor and each mint yields a value distinct
/// from every other mint in this process, a token a caller obtains for itself is never
/// equal to a token any view is currently holding. A pull therefore succeeds only with
/// the value this view returned from its own live `begin_batch_drain`: neither a token
/// from another view, nor a token from a superseded drain, nor a self-minted one is
/// accepted.
///
/// The distinctness guarantee is exhaustible in principle — it would take 2^64 mints in
/// one process to wrap the counter — and is not otherwise conditional.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchDrainToken(u64);

impl BatchDrainToken {
    /// Mints a token distinct from every other token minted in this process.
    ///
    /// This is what an implementation of [`ExpiringTimeKeyViewApi::begin_batch_drain`]
    /// calls; there is no other way to obtain a token, and no way to derive one value
    /// from another.
    #[must_use]
    pub fn mint() -> Self {
        Self(NEXT_BATCH_DRAIN_TOKEN.fetch_add(1, Ordering::Relaxed))
    }
}

/// The object-safe view of one expiring time-key table (M11.D13).
///
/// Implementations are owned by [`TableManager`]'s typed view map. Every method that
/// reads state yields at most one `RecordBatch`, and no method returns a collection whose
/// size grows with the table's contents. That is the whole of what a view supplies: the
/// stream D13 specifies is [`ExpiringTimeKeyViewDrain::all_batches_for_watermark`], which
/// an implementation of this trait receives and cannot replace.
///
/// [`TableManager`]: crate::tables::table_manager::TableManager
#[async_trait]
pub trait ExpiringTimeKeyViewApi: Send {
    /// Buffers `batch` under `max_timestamp` until the next flush.
    fn insert(&mut self, max_timestamp: SystemTime, batch: RecordBatch) -> Result<(), StateError>;

    /// Writes every buffered batch to state and discards flushed data that `watermark`
    /// has retired.
    async fn flush(&mut self, watermark: Option<SystemTime>) -> Result<(), StateError>;

    /// Writes the batches buffered under `timestamp` to state.
    async fn flush_timestamp(&mut self, timestamp: SystemTime) -> Result<(), StateError>;

    /// Drops everything the view holds under `timestamp`.
    async fn expire_timestamp(&mut self, timestamp: SystemTime) -> Result<(), StateError>;

    /// The earliest timestamp the view holds anything under, flushed or buffered.
    fn get_min_time(&self) -> Option<SystemTime>;

    /// Starts a drain of the batches at or after `watermark`'s retention cutoff and
    /// returns the token that identifies it.
    ///
    /// Implementations mint the token with [`BatchDrainToken::mint`], so the returned
    /// value is fresh and belongs to this drain of this view alone. Any drain already in
    /// progress on this view is abandoned: its token stops being accepted. What the new
    /// drain covers is fixed here, so a batch inserted after this call is outside it.
    fn begin_batch_drain(&mut self, watermark: Option<SystemTime>) -> BatchDrainToken;

    /// Returns the next batch of the drain `token` identifies, or `None` once it is
    /// exhausted.
    ///
    /// # Errors
    ///
    /// Fails closed when `token` is not this view's live drain token — including a token
    /// from another view, from a superseded drain, or minted by the caller — and when the
    /// view was mutated in a way that could change the part of the drain not yet
    /// returned. No such case yields a batch, so a caller can never be handed another
    /// drain's items and can never read a truncated sequence as an exhausted one.
    async fn next_drained_batch(
        &mut self,
        token: BatchDrainToken,
    ) -> Result<Option<(SystemTime, RecordBatch)>, StateError>;
}

mod sealed {
    /// Closes [`super::ExpiringTimeKeyViewDrain`] over the views this crate's seam defines.
    ///
    /// The blanket implementation below is the only one, and this module is private, so a
    /// downstream crate can neither name this trait nor implement it for a type of its own.
    pub trait Sealed {}
    impl<T: super::ExpiringTimeKeyViewApi + ?Sized> Sealed for T {}
}

/// The one drain stream every [`ExpiringTimeKeyViewApi`] gets and none of them can replace.
///
/// This is an extension trait, not part of the view API, and that is the whole point. A
/// provided method on [`ExpiringTimeKeyViewApi`] would be overridable, and an override
/// could collect the table into memory before yielding a batch while still satisfying
/// D13's signature — exactly the state-sized owned collection D13 exists to forbid. The
/// blanket implementation below covers every `T: ExpiringTimeKeyViewApi + ?Sized`, so
/// coherence rejects any other implementation for a view; the private `sealed::Sealed`
/// supertrait closes the remaining door, a downstream implementation for a type that is
/// *not* a view.
///
/// The `?Sized` bound is load-bearing: the manager lends
/// `&mut (dyn ExpiringTimeKeyViewApi + Send)`, and the blanket implementation has to cover
/// that trait object as well as sized views.
pub trait ExpiringTimeKeyViewDrain: sealed::Sealed {
    /// The drain of [`ExpiringTimeKeyViewApi::begin_batch_drain`] as the boxed borrowing
    /// stream D13 specifies.
    ///
    /// One poll performs one [`ExpiringTimeKeyViewApi::next_drained_batch`], so the stream
    /// never holds more than the single batch it is about to yield — and because the
    /// implementation below is the only one that can exist, that holds for every view
    /// rather than for the views that chose not to override it. The returned stream
    /// borrows `self` for `'a`; the borrow ends when it is dropped, after which the view
    /// is usable again.
    fn all_batches_for_watermark<'a>(
        &'a mut self,
        watermark: Option<SystemTime>,
    ) -> ExpiringTimeKeyBatchStream<'a>;
}

impl<T: ExpiringTimeKeyViewApi + ?Sized> ExpiringTimeKeyViewDrain for T {
    fn all_batches_for_watermark<'a>(
        &'a mut self,
        watermark: Option<SystemTime>,
    ) -> ExpiringTimeKeyBatchStream<'a> {
        let token = self.begin_batch_drain(watermark);
        Box::pin(stream::try_unfold(self, move |view| async move {
            let next = view.next_drained_batch(token).await?;
            Ok(next.map(|batch| (batch, view)))
        }))
    }
}
