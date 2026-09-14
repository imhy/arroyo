//! The parquet implementation of the global key/value load seam (design items M11.D15c,
//! M11.D33; risk M11.T09o).
//!
//! This is `GlobalKeyedTable::load_with_version`'s file-and-batch walk, moved behind
//! [`GlobalKeyValueLoad`] and re-shaped into pages. What it reads is unchanged: the same
//! whole-object fetch per checkpoint file, the same `ParquetRecordBatchReaderBuilder`, the
//! same `key`/`value` binary columns, and the same refusal — with the same message — of a
//! null key or a null value. What changed is that the bytes leave in bounded pages instead
//! of accumulating into one `HashMap` inside the walk, and that the state version a file
//! was written under is now reported across the seam rather than acted on inside the walk,
//! because deciding what a version means is a decode question and the decode stays above
//! the seam.
//!
//! # Sources are files, in checkpoint order
//!
//! One source is one checkpoint object. A restored `GlobalKeyedTable` carries the union of
//! every predecessor subtask's file and loads all of them (M11.D15c), so this loader walks
//! `files` in the order the table holds them — which is the order the legacy walk read them
//! in, and therefore the order that decides which of two colliding keys wins.
//!
//! # What is held at once
//!
//! While a page is being filled the loader holds the current file's fetched bytes, the one
//! `RecordBatch` the parquet reader last produced, and the partially filled page. The first
//! two are arroyo's pre-existing shape — `load_with_version` held exactly the same two —
//! and the third is bounded by [`LoadPageLimits`]. Nothing here grows with the number of
//! files or with the size of the restored map.
//!
//! # Two orderings that are worth stating
//!
//! - A page's entries are validated as the page is *built*, so for a source that is both
//!   written under an unsupported state version and carries a null key, the version
//!   refusal above the seam still comes first: the source's announcing page reaches the
//!   decode before any of its rows are read. That is the announcing page's purpose.
//! - The parquet reader is built when a source is opened, before its announcing page is
//!   returned, so a file that is *both* undecodable by the parquet reader and written under
//!   an unsupported state version now fails with the reader's error where the legacy
//!   migratable walk raised the version error. Both are hard restore failures of the same
//!   file and neither is recoverable; no other ordering changes.

use std::vec;

use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_schema::ArrowError;
use arroyo_rpc::errors::StateError;
use arroyo_storage::StorageProviderRef;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use super::{
    GlobalKeyValueLoad, GlobalKeyValuePage, GlobalKeyValuePageBuilder, LoadPageLimits, LoadPagePeak,
};
use crate::tables::CheckpointParquetMetadata;

/// A bounded-page loader over one restored `GlobalKeyedTable`'s checkpoint files.
///
/// Owns everything it reads from — the file list, the storage handle and the table's name
/// — so it outlives the `Arc<dyn ErasedTable>` the provider built it from.
pub struct ParquetGlobalKeyValueLoad {
    table_name: String,
    storage: StorageProviderRef,
    files: vec::IntoIter<String>,
    open: Option<OpenSource>,
    limits: LoadPageLimits,
    peak: LoadPagePeak,
}

impl ParquetGlobalKeyValueLoad {
    /// A loader over `files`, read in the order given, with pages bounded by `limits`.
    ///
    /// `files` is the restored table's own file list; cloning it costs one string per
    /// predecessor subtask, which is a property of the job's parallelism and not of the
    /// state's size.
    pub fn new(
        table_name: String,
        storage: StorageProviderRef,
        files: Vec<String>,
        limits: LoadPageLimits,
    ) -> Self {
        Self {
            table_name,
            storage,
            files: files.into_iter(),
            open: None,
            limits,
            peak: LoadPagePeak::ZERO,
        }
    }

    /// Records `page` in the high-water mark and hands it back.
    fn emit(&mut self, page: GlobalKeyValuePage) -> Option<GlobalKeyValuePage> {
        self.peak.record(&page);
        Some(page)
    }
}

#[async_trait]
impl GlobalKeyValueLoad for ParquetGlobalKeyValueLoad {
    async fn next_page(&mut self) -> Result<Option<GlobalKeyValuePage>, StateError> {
        loop {
            if self.open.is_none() {
                let Some(file) = self.files.next() else {
                    return Ok(None);
                };
                let source = open_source(&self.storage, file, self.limits).await?;
                let header = source.pages.header();
                self.open = Some(source);
                return Ok(self.emit(header));
            }

            let filled = {
                let source = self.open.as_mut().expect("checked immediately above");
                fill_page(source, &self.table_name)?
            };
            if let Some(page) = filled {
                return Ok(self.emit(page));
            }

            // The source ran out of rows: emit whatever it had accumulated, then move on.
            let mut source = self.open.take().expect("checked immediately above");
            if let Some(page) = source.pages.take() {
                return Ok(self.emit(page));
            }
        }
    }

    fn page_limits(&self) -> LoadPageLimits {
        self.limits
    }

    fn peak_page(&self) -> LoadPagePeak {
        self.peak
    }
}

/// One checkpoint file, opened and being read.
struct OpenSource {
    reader: ParquetRecordBatchReader,
    rows: Option<RowCursor>,
    pages: GlobalKeyValuePageBuilder,
}

/// Fetches `file`, reads the state version its footer records, and prepares to page it.
async fn open_source(
    storage: &StorageProviderRef,
    file: String,
    limits: LoadPageLimits,
) -> Result<OpenSource, StateError> {
    let contents = storage.get(file.as_str()).await?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(contents)?;
    let state_version =
        CheckpointParquetMetadata::from(reader.metadata().file_metadata().key_value_metadata())
            .state_version;
    Ok(OpenSource {
        reader: reader.build()?,
        rows: None,
        pages: GlobalKeyValuePageBuilder::new(file, state_version, limits),
    })
}

/// Fills pages from `source` until one closes, or `None` when the source has no rows left.
///
/// Pages span record-batch boundaries within one file: a batch that ends mid-page is
/// dropped and the next one continues filling it, so how many pages a file produces depends
/// on its entries and the bound, not on how the writer happened to batch them.
fn fill_page(
    source: &mut OpenSource,
    table_name: &str,
) -> Result<Option<GlobalKeyValuePage>, StateError> {
    loop {
        let exhausted = match &source.rows {
            None => true,
            Some(rows) => rows.is_exhausted(),
        };
        if exhausted {
            let Some(batch) = source.reader.next() else {
                return Ok(None);
            };
            source.rows = Some(RowCursor::new(&batch?)?);
            continue;
        }

        let rows = source.rows.as_mut().expect("present and not exhausted");
        let (key, value) = rows.next_entry(table_name)?;
        if let Some(page) = source.pages.push(key, value) {
            return Ok(Some(page));
        }
    }
}

/// The `key` and `value` columns of one record batch, and how far through them the loader
/// has read.
struct RowCursor {
    keys: BinaryArray,
    values: BinaryArray,
    next: usize,
    rows: usize,
}

impl RowCursor {
    /// Extracts the two binary columns `batch` must have.
    ///
    /// These are `GlobalKeyedTable::get_key_value_iterator`'s four refusals, unchanged: a
    /// missing column is a schema error and a column of another type is a cast error.
    fn new(batch: &RecordBatch) -> Result<Self, StateError> {
        let keys = binary_column(batch, "key", "failed to downcast key to binary")?;
        let values = binary_column(
            batch,
            "value",
            "failed to downcast value column to BinaryArray",
        )?;
        // The legacy walk zipped the two column iterators, which stops at the shorter of
        // the two; `RecordBatch` gives every column the same length, so this is the same
        // count by another route.
        let rows = keys.len().min(values.len());
        Ok(Self {
            keys,
            values,
            next: 0,
            rows,
        })
    }

    const fn is_exhausted(&self) -> bool {
        self.next >= self.rows
    }

    /// The next `(key, value)` pair, refusing a null in either half.
    fn next_entry(&mut self, table_name: &str) -> Result<(Vec<u8>, Vec<u8>), StateError> {
        let row = self.next;
        self.next += 1;
        let key = cell(
            &self.keys,
            row,
            table_name,
            "unexpected null key from record batch",
        )?;
        let value = cell(
            &self.values,
            row,
            table_name,
            "unexpected null value from record batch",
        )?;
        Ok((key, value))
    }
}

/// One binary column of `batch`, by name.
fn binary_column(
    batch: &RecordBatch,
    name: &str,
    cast_error: &str,
) -> Result<BinaryArray, StateError> {
    let column = batch.column_by_name(name).ok_or_else(|| {
        StateError::ArrowError(ArrowError::SchemaError(format!("missing column '{name}'")))
    })?;
    column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .cloned()
        .ok_or_else(|| StateError::ArrowError(ArrowError::CastError(cast_error.to_string())))
}

/// One cell of a binary column, or `error` when it is null.
fn cell(
    column: &BinaryArray,
    row: usize,
    table_name: &str,
    error: &str,
) -> Result<Vec<u8>, StateError> {
    if column.is_null(row) {
        return Err(StateError::Other {
            table: table_name.to_string(),
            error: error.to_string(),
        });
    }
    Ok(column.value(row).to_vec())
}
