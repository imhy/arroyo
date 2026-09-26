//! The worker's acknowledged lifecycle fence, as the ownership generation a table reads
//! (plan M11.T10b.01, ruling M11.T10R6).
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
//!   [`AcknowledgedFence::get`];
//! - a writer is not `Clone`, and [`AcknowledgedFenceWriter::raise`] takes `&mut self`, so the
//!   one writer a guard owns is raised only by code holding exclusive access to it — in
//!   `arroyo-worker`, under the lock that serialises fence advancement with start admission.
//!
//! ```
//! use arroyo_state::ownership::AcknowledgedFenceWriter;
//!
//! let mut writer = AcknowledgedFenceWriter::unacknowledged();
//! let fence = writer.reader();
//! let kept = fence.clone();
//! assert_eq!(fence.get(), 0);
//!
//! writer.raise(4);
//! assert_eq!((fence.get(), kept.get()), (4, 4));
//!
//! // The fence only rises.
//! writer.raise(3);
//! assert_eq!(kept.get(), 4);
//! ```
//!
//! A reader cannot write:
//!
//! ```compile_fail,E0599
//! use arroyo_state::ownership::AcknowledgedFenceWriter;
//!
//! let writer = AcknowledgedFenceWriter::unacknowledged();
//! writer.reader().raise(5);
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
//! shared.raise(5);
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A read-only handle onto a worker generation's highest acknowledged lifecycle fence.
///
/// Zero means no fence has been acknowledged. Every clone reads the same cell, so a table
/// that keeps a clone reads the current value at any later time, and successive reads never
/// observe a lower value than an earlier one: the cell's only writer only raises it.
#[derive(Clone, Debug)]
pub struct AcknowledgedFence {
    cell: Arc<AtomicU64>,
}

impl AcknowledgedFence {
    /// A handle no writer exists for: it reads zero — no fence acknowledged — for as long as
    /// it lives.
    ///
    /// For executions that run under no lifecycle guard: the in-process engine
    /// (`Program::local_from_logical`) and tests.
    pub fn unfenced() -> Self {
        Self {
            cell: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The highest fence acknowledged so far; zero means none.
    pub fn get(&self) -> u64 {
        self.cell.load(Ordering::Acquire)
    }
}

/// The only writer of one [`AcknowledgedFence`] cell.
///
/// Not `Clone`, and raised through `&mut self`: whoever owns it is the one place the fence can
/// move. In a worker that owner is the lifecycle guard's fence state.
#[derive(Debug)]
pub struct AcknowledgedFenceWriter {
    cell: Arc<AtomicU64>,
}

impl AcknowledgedFenceWriter {
    /// A new cell at zero — no fence acknowledged — and its writer.
    pub fn unacknowledged() -> Self {
        Self {
            cell: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The highest fence acknowledged so far; zero means none.
    pub fn get(&self) -> u64 {
        self.cell.load(Ordering::Acquire)
    }

    /// Raises the fence to `fence` if that is higher; a lower or equal value changes nothing.
    pub fn raise(&mut self, fence: u64) {
        self.cell.fetch_max(fence, Ordering::Release);
    }

    /// A read handle onto this writer's cell.
    pub fn reader(&self) -> AcknowledgedFence {
        AcknowledgedFence {
            cell: Arc::clone(&self.cell),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_shareable<T: Clone + Send + Sync + 'static>() {}

    #[test]
    fn a_reader_is_a_cheap_shareable_handle() {
        is_shareable::<AcknowledgedFence>();
    }

    #[test]
    fn every_reader_follows_the_writer_and_the_fence_only_rises() {
        let mut writer = AcknowledgedFenceWriter::unacknowledged();
        let early = writer.reader();
        assert_eq!((writer.get(), early.get()), (0, 0));

        writer.raise(4);
        let late = writer.reader();
        assert_eq!((writer.get(), early.get(), late.get()), (4, 4, 4));

        writer.raise(2);
        writer.raise(4);
        assert_eq!((writer.get(), early.get(), late.get()), (4, 4, 4));

        writer.raise(9);
        assert_eq!((early.get(), late.clone().get()), (9, 9));
    }

    #[test]
    fn an_unfenced_handle_reads_zero_and_is_no_writers_cell() {
        let mut writer = AcknowledgedFenceWriter::unacknowledged();
        let unfenced = AcknowledgedFence::unfenced();
        writer.raise(7);
        assert_eq!(unfenced.get(), 0);
        assert_eq!(unfenced.clone().get(), 0);
    }
}
