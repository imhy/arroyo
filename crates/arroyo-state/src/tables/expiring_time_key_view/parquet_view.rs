//! The parquet implementation of the expiring-time-key view seam.
//!
//! `ExpiringTimeKeyView` is the view `ExpiringTimeKeyTable::get_view` builds: two ordered
//! maps of `RecordBatch`es keyed by the batch's maximum timestamp — one already written
//! to state, one still buffered — plus the channel that writes to state. It was moved
//! here from `expiring_time_key_map` with its behaviour intact; what changed is the
//! signatures the seam fixes — the borrowed-iterator read became the one-batch-at-a-time
//! walk [`ExpiringTimeKeyViewApi`] defines, `insert` and `flush_timestamp` return
//! `Result`, and `expire_timestamp` no longer returns the batches it dropped, which no
//! caller read.

use std::{collections::BTreeMap, ops::Bound, time::SystemTime};

use arrow_array::RecordBatch;
use arroyo_rpc::errors::StateError;
use arroyo_types::print_time;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use tracing::debug;

use super::{BatchDrainToken, ExpiringTimeKeyViewApi};
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::{StateMessage, TableData};

/// Which of the view's two maps a drain is walking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainPhase {
    /// Batches already written to state, in timestamp order.
    Flushed,
    /// Batches still buffered, in timestamp order. Walked after [`Self::Flushed`], which
    /// is the order the borrowed iterator this replaced chained the two maps in.
    Pending,
    /// Every batch in range has been returned.
    Done,
}

/// One drain in progress.
///
/// Holds no batch and no per-timestamp collection: a position is one timestamp and one
/// index, so its size does not grow with the table.
#[derive(Debug)]
struct BatchDrain {
    token: BatchDrainToken,
    /// The retention cutoff computed when the drain began. Timestamps below it are out of
    /// range for the whole drain even if the watermark moves.
    cutoff: SystemTime,
    /// The highest buffered timestamp at or above `cutoff` when the drain began, or
    /// `None` if nothing was buffered in range. The pending phase stops there, so a batch
    /// buffered above it during the drain is outside the drain's range by construction.
    pending_upper: Option<SystemTime>,
    phase: DrainPhase,
    /// The `(timestamp, index within that timestamp)` last returned in the current phase.
    position: Option<(SystemTime, usize)>,
    /// Set when the view was mutated somewhere the rest of this drain could have covered.
    /// The drain then fails rather than returning a batch the caller's own write put
    /// there, or skipping one it displaced.
    disturbed: bool,
}

/// An expiring time-key view backed by parquet state files.
#[derive(Debug)]
pub struct ExpiringTimeKeyView {
    parent: ExpiringTimeKeyTable,
    flushed_batches_by_max_timestamp: BTreeMap<SystemTime, Vec<RecordBatch>>,
    batches_to_flush: BTreeMap<SystemTime, Vec<RecordBatch>>,
    state_tx: Sender<StateMessage>,
    drain: Option<BatchDrain>,
}

impl ExpiringTimeKeyView {
    pub(crate) fn new(
        parent: ExpiringTimeKeyTable,
        flushed_batches_by_max_timestamp: BTreeMap<SystemTime, Vec<RecordBatch>>,
        state_tx: Sender<StateMessage>,
    ) -> Self {
        Self {
            parent,
            flushed_batches_by_max_timestamp,
            batches_to_flush: BTreeMap::new(),
            state_tx,
            drain: None,
        }
    }

    /// The oldest timestamp `watermark` retains, which is where a read starts.
    fn cutoff(&self, watermark: Option<SystemTime>) -> SystemTime {
        watermark
            .map(|watermark| watermark - self.parent.retention())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }

    /// Records that the buffered map changed at `timestamp`, if that is inside the part
    /// of a live drain still to come.
    fn note_pending_write(&mut self, timestamp: SystemTime) {
        if let Some(drain) = self.drain.as_mut()
            && drain.phase != DrainPhase::Done
            && let Some(upper) = drain.pending_upper
            && drain.cutoff <= timestamp
            && timestamp <= upper
        {
            drain.disturbed = true;
        }
    }

    /// Records that batches moved between or out of the maps, which a live drain cannot
    /// walk through.
    fn note_map_rewrite(&mut self) {
        if let Some(drain) = self.drain.as_mut()
            && drain.phase != DrainPhase::Done
        {
            drain.disturbed = true;
        }
    }

    fn stale_drain(&self) -> StateError {
        StateError::Other {
            table: self.parent.table_name().to_string(),
            error: "batch drain token does not identify this view's current drain".to_string(),
        }
    }

    fn disturbed_drain(&self) -> StateError {
        StateError::Other {
            table: self.parent.table_name().to_string(),
            error: "the view was written to inside the range of a drain in progress".to_string(),
        }
    }
}

/// Returns the batch after `position` in `map`, restricted to `[cutoff, upper]`, and
/// advances `position` onto it.
///
/// Clones exactly the one batch it returns; the clone shares that batch's Arrow buffers.
fn next_in_map(
    map: &BTreeMap<SystemTime, Vec<RecordBatch>>,
    cutoff: SystemTime,
    upper: Bound<SystemTime>,
    position: &mut Option<(SystemTime, usize)>,
) -> Option<(SystemTime, RecordBatch)> {
    if let Some((timestamp, index)) = *position
        && let Some(batch) = map
            .get(&timestamp)
            .and_then(|batches| batches.get(index + 1))
    {
        *position = Some((timestamp, index + 1));
        return Some((timestamp, batch.clone()));
    }

    let lower = match *position {
        Some((timestamp, _)) => Bound::Excluded(timestamp),
        None => Bound::Included(cutoff),
    };
    for (timestamp, batches) in map.range((lower, upper)) {
        if let Some(batch) = batches.first() {
            *position = Some((*timestamp, 0));
            return Some((*timestamp, batch.clone()));
        }
    }
    None
}

#[async_trait]
impl ExpiringTimeKeyViewApi for ExpiringTimeKeyView {
    fn insert(&mut self, max_timestamp: SystemTime, batch: RecordBatch) -> Result<(), StateError> {
        self.note_pending_write(max_timestamp);
        self.batches_to_flush
            .entry(max_timestamp)
            .or_default()
            .push(batch);
        Ok(())
    }

    async fn flush(&mut self, watermark: Option<SystemTime>) -> Result<(), StateError> {
        self.note_map_rewrite();
        while let Some((max_timestamp, mut batches)) = self.batches_to_flush.pop_first() {
            if watermark
                .map(|watermark| max_timestamp < watermark - self.parent.retention())
                .unwrap_or(false)
            {
                continue;
            }
            for batch in &batches {
                self.state_tx
                    .send(StateMessage::TableData {
                        table: self.parent.table_name().to_string(),
                        data: TableData::RecordBatch(batch.clone()),
                    })
                    .await
                    .expect("queue closed");
            }
            self.flushed_batches_by_max_timestamp
                .entry(max_timestamp)
                .or_default()
                .append(&mut batches);
        }
        if let Some(watermark) = watermark {
            let cutoff = watermark - self.parent.retention();
            self.flushed_batches_by_max_timestamp =
                self.flushed_batches_by_max_timestamp.split_off(&cutoff);
        }
        Ok(())
    }

    async fn flush_timestamp(&mut self, timestamp: SystemTime) -> Result<(), StateError> {
        self.note_map_rewrite();
        let Some(batches_to_flush) = self.batches_to_flush.remove(&timestamp) else {
            return Ok(());
        };
        let flushed_vec = self
            .flushed_batches_by_max_timestamp
            .entry(timestamp)
            .or_default();
        for batch in batches_to_flush {
            flushed_vec.push(batch.clone());
            self.state_tx
                .send(StateMessage::TableData {
                    table: self.parent.table_name().to_string(),
                    data: TableData::RecordBatch(batch),
                })
                .await
                .expect("queue closed");
        }
        Ok(())
    }

    async fn expire_timestamp(&mut self, timestamp: SystemTime) -> Result<(), StateError> {
        self.note_map_rewrite();
        self.flushed_batches_by_max_timestamp.remove(&timestamp);
        self.batches_to_flush.remove(&timestamp);
        Ok(())
    }

    fn get_min_time(&self) -> Option<SystemTime> {
        match (
            self.batches_to_flush.keys().next(),
            self.flushed_batches_by_max_timestamp.keys().next(),
        ) {
            (None, None) => None,
            (None, Some(time)) | (Some(time), None) => Some(*time),
            (Some(buffered_time), Some(flushed_time)) => Some(*buffered_time.min(flushed_time)),
        }
    }

    fn begin_batch_drain(&mut self, watermark: Option<SystemTime>) -> BatchDrainToken {
        // TODO: decide how to manage hash range ownership. Previously this was done by
        // iterating over the contents of the record batch. Should we use statistics?
        let cutoff = self.cutoff(watermark);
        debug!("CUTOFF IS {}", print_time(cutoff));
        let token = BatchDrainToken::mint();
        let pending_upper = self
            .batches_to_flush
            .range(cutoff..)
            .next_back()
            .map(|(timestamp, _)| *timestamp);
        self.drain = Some(BatchDrain {
            token,
            cutoff,
            pending_upper,
            phase: DrainPhase::Flushed,
            position: None,
            disturbed: false,
        });
        token
    }

    async fn next_drained_batch(
        &mut self,
        token: BatchDrainToken,
    ) -> Result<Option<(SystemTime, RecordBatch)>, StateError> {
        match self.drain.as_ref() {
            Some(drain) if drain.token != token => return Err(self.stale_drain()),
            Some(drain) if drain.disturbed => return Err(self.disturbed_drain()),
            Some(_) => {}
            None => return Err(self.stale_drain()),
        }
        loop {
            let drain = self.drain.as_mut().expect("checked above");
            match drain.phase {
                DrainPhase::Flushed => {
                    let cutoff = drain.cutoff;
                    let mut position = drain.position;
                    let next = next_in_map(
                        &self.flushed_batches_by_max_timestamp,
                        cutoff,
                        Bound::Unbounded,
                        &mut position,
                    );
                    let drain = self.drain.as_mut().expect("checked above");
                    match next {
                        Some(batch) => {
                            drain.position = position;
                            return Ok(Some(batch));
                        }
                        None => {
                            drain.phase = DrainPhase::Pending;
                            drain.position = None;
                        }
                    }
                }
                DrainPhase::Pending => {
                    let Some(upper) = drain.pending_upper else {
                        drain.phase = DrainPhase::Done;
                        continue;
                    };
                    let cutoff = drain.cutoff;
                    let mut position = drain.position;
                    let next = next_in_map(
                        &self.batches_to_flush,
                        cutoff,
                        Bound::Included(upper),
                        &mut position,
                    );
                    let drain = self.drain.as_mut().expect("checked above");
                    match next {
                        Some(batch) => {
                            drain.position = position;
                            return Ok(Some(batch));
                        }
                        None => drain.phase = DrainPhase::Done,
                    }
                }
                DrainPhase::Done => return Ok(None),
            }
        }
    }
}
