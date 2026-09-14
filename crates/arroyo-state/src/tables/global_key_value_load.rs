//! The backend-neutral bounded-page global key/value load seam (design item M11.D15c).
//!
//! [`GlobalKeyValueLoad`] is the object-safe loader a state backend supplies for a
//! [`TableKind::GlobalKeyValue`] table. It yields **pages** of opaque
//! `(key bytes, value bytes)` entries; nothing typed crosses it. `GlobalKeyedView<K, V>`
//! is generic over `K: Key, V: Data` and so cannot be a trait object, which is why the
//! seam is a loader rather than a view: the bincode decode and the view construction stay
//! above it, in `tables::global_keyed_map::restore`, unchanged for every backend.
//!
//! Bytes are the whole vocabulary. No Arrow type, no bincode, and no `K` or `V` appears in
//! any signature here, so a backend that stores these entries in a form that is not
//! parquet — M11.T17's `stateengine` implementation is the one this seam was shaped for —
//! implements it without `arroyo-state` naming its types.
//!
//! # What a page is, and what a source is
//!
//! A **source** is one place a backend reads entries from: for parquet that is one
//! checkpoint object, because restore reads the union of every predecessor subtask's file
//! (M11.D15c). A **page** is a bounded slice of one source's entries. Four rules make the
//! sequence of pages a contract a decoder can rely on:
//!
//! 1. **Pages come in source order, and a page belongs to exactly one source.** Its
//!    [`GlobalKeyValuePage::source`] names that source and its
//!    [`GlobalKeyValuePage::state_version`] is the version that source's entries were
//!    written under. A page therefore never mixes two versions.
//! 2. **A source is announced before it is read.** The first page a source yields carries
//!    its name and state version and *no entries*. That is what lets a decoder refuse a
//!    source written under a state version it cannot read before any of that source's
//!    entries have been touched — which is what arroyo's migratable decode does per file
//!    today, and which the loader's own per-entry validation must not preempt. A source
//!    with no entries at all still yields this one page, so its version is still checked.
//! 3. **A page is bounded**, by both of [`LoadPageLimits`]' units. Every page is built by
//!    [`GlobalKeyValuePageBuilder`], which is the only way to construct one, so the bound
//!    is a property of the type rather than of each implementation's care.
//! 4. **Exhaustion is stable.** Once [`GlobalKeyValueLoad::next_page`] has returned `None`
//!    it keeps returning `None`.
//!
//! # The page bound, and where its value comes from
//!
//! M11.D36 requires that no operation M11 introduces hold O(total live state) transient
//! memory, and that its limits and peak usage be observable. What this seam bounds is what
//! a loader holds *at once while producing a page*. It does not bound the restored map:
//! `GlobalKeyedTable` is a memory-resident `HashMap` by arroyo's own design (M11.D15c),
//! and the whole map is resident after the load either way.
//!
//! The bound has **two units**, because one of them alone is not a memory bound:
//!
//! ```text
//! cost(page) = Σ (key.len() + value.len())   +   entries × size_of::<(Vec<u8>, Vec<u8>)>()
//!              \_____ payload bytes _____/       \_______ per-entry bookkeeping _______/
//! ```
//!
//! Bounding only the payload leaves a page of a million one-byte entries costing tens of
//! megabytes of `Vec` headers; bounding only the count leaves one page of
//! [`MAX_LOAD_PAGE_ENTRIES`] hundred-megabyte values unbounded in bytes. Both are capped,
//! so a page costs at most `MAX_LOAD_PAGE_BYTES + MAX_LOAD_PAGE_ENTRIES × 48 B` ≈ 1.05 MiB
//! regardless of the table's size or the shape of its entries.
//!
//! The two values are derived from the seam's own input contract — an entry is opaque
//! bytes, and the caller's work per entry is two bincode decodes and one map insert — not
//! from what today's only implementation happens to read:
//!
//! - [`MAX_LOAD_PAGE_ENTRIES`] = 1024 amortises the *fixed* cost of a page — one
//!   trait-object call, one `Vec` allocation, one `Vec` drop — over a thousand entries'
//!   worth of decode work, which puts it around a percent of the load's cost. Raising it
//!   buys a fraction of a percent and costs transient memory linearly.
//! - [`MAX_LOAD_PAGE_BYTES`] = 1 MiB is the payload cap that makes the count safe for
//!   entries of any size. Together the two put the crossover at 1 KiB per entry: below
//!   that the count closes a page, above it the payload does. Global key/value entries are
//!   source offsets and commit descriptors — tens to hundreds of bytes — so the count is
//!   normally the binding unit and the payload cap is the guard for unusually large
//!   values.
//!
//! No arroyo configuration key carries this quantity. `pipeline.source-batch-size` is the
//! source operator's record-batching unit and has nothing to do with restore memory, so
//! the values are named constants here rather than a config key invented for them. An
//! implementation may bound its pages more tightly than the default; it reports what it
//! honours from [`GlobalKeyValueLoad::page_limits`], and
//! [`GlobalKeyValueLoad::peak_page`] reports the largest page it has actually produced,
//! which is the observability half of M11.D36.
//!
//! ## An entry larger than the bound
//!
//! A single entry whose key and value together exceed [`LoadPageLimits::max_payload_bytes`]
//! is delivered as a page of one. It is not split — half a key/value pair is not a value
//! any decoder can use — and it is not refused, because it is state a previous run wrote
//! and the legacy loader restored. So the payload cap is a rule about *when a page closes*,
//! and the guarantee [`LoadPageLimits::admits`] checks is: at most
//! [`LoadPageLimits::max_entries`] entries, and at most
//! [`LoadPageLimits::max_payload_bytes`] payload bytes unless the page holds exactly one
//! entry.
//!
//! # What this seam does not bound
//!
//! The parquet implementation fetches each checkpoint object whole
//! (`StorageProvider::get`) before reading record batches out of it, exactly as
//! `GlobalKeyedTable::load_with_version` did before the walk moved behind this seam.
//! Turning that into a ranged read would change parquet's I/O pattern, which M11.D33 holds
//! fixed and which is outside M11.T09c.01. The pages bound what crosses the seam and what
//! the loader stages while producing them; they do not make the fetch itself paged.
//!
//! [`TableKind::GlobalKeyValue`]: crate::provider::TableKind::GlobalKeyValue

use std::sync::Arc;

use arroyo_rpc::errors::StateError;
use async_trait::async_trait;

pub mod parquet_load;

#[cfg(test)]
mod tests;

/// The most entries one load page carries by default.
///
/// Derived in this module's documentation: it amortises a page's fixed cost over a
/// thousand entries' worth of decode work.
pub const MAX_LOAD_PAGE_ENTRIES: usize = 1024;

/// The most payload bytes one load page carries by default, before the single-entry
/// exception.
///
/// Derived in this module's documentation: it caps what a page holds when entries are
/// large, putting the crossover with [`MAX_LOAD_PAGE_ENTRIES`] at 1 KiB per entry.
pub const MAX_LOAD_PAGE_BYTES: usize = 1 << 20;

/// The bound one loader's pages honour.
///
/// Both units are upper bounds on a single page, never on a load: a load produces as many
/// pages as its entries need.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LoadPageLimits {
    max_entries: usize,
    max_payload_bytes: usize,
}

impl LoadPageLimits {
    /// The seam's own bound, [`MAX_LOAD_PAGE_ENTRIES`] and [`MAX_LOAD_PAGE_BYTES`].
    pub const DEFAULT: Self = Self {
        max_entries: MAX_LOAD_PAGE_ENTRIES,
        max_payload_bytes: MAX_LOAD_PAGE_BYTES,
    };

    /// A bound tighter than — or as tight as — the default.
    ///
    /// Returns `None` when either unit is zero, which would describe a page that can hold
    /// nothing and a load that can never finish, or when either exceeds
    /// [`Self::DEFAULT`]. Refusing the second case is what makes the seam's own maximum
    /// enforceable rather than advisory: [`Self::DEFAULT`] and this constructor are the
    /// only ways to obtain a `LoadPageLimits`, so every value that can exist is within
    /// [`MAX_LOAD_PAGE_ENTRIES`] and [`MAX_LOAD_PAGE_BYTES`], and a loader cannot declare
    /// a wider bound than the seam's in order to pass [`Self::admits`].
    pub const fn new(max_entries: usize, max_payload_bytes: usize) -> Option<Self> {
        if max_entries == 0
            || max_payload_bytes == 0
            || max_entries > MAX_LOAD_PAGE_ENTRIES
            || max_payload_bytes > MAX_LOAD_PAGE_BYTES
        {
            return None;
        }
        Some(Self {
            max_entries,
            max_payload_bytes,
        })
    }

    /// The most entries a page may carry.
    pub const fn max_entries(self) -> usize {
        self.max_entries
    }

    /// The most payload bytes a page may carry, unless it carries exactly one entry.
    pub const fn max_payload_bytes(self) -> usize {
        self.max_payload_bytes
    }

    /// Whether `page` is within this bound.
    ///
    /// This is the whole contract as a predicate, single-entry exception included. The
    /// decode above the seam checks every page against the limits its loader declared, so
    /// a backend that over-fills a page fails the load instead of quietly spending the
    /// memory.
    pub fn admits(self, page: &GlobalKeyValuePage) -> bool {
        page.len() <= self.max_entries
            && (page.payload_bytes() <= self.max_payload_bytes || page.len() <= 1)
    }
}

impl Default for LoadPageLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The largest page a loader has produced so far, in each unit independently.
///
/// This is M11.D36's "peak usage is observable" for this seam: after a load, the loader
/// still answers with the high-water mark its pages reached, so a test at 10× the entries
/// can assert the mark did not move with the table's size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct LoadPagePeak {
    entries: usize,
    payload_bytes: usize,
}

impl LoadPagePeak {
    /// No page produced yet.
    pub const ZERO: Self = Self {
        entries: 0,
        payload_bytes: 0,
    };

    /// Folds `page` into the mark.
    ///
    /// An implementation calls this for every page it hands out; the two units move
    /// independently, so the mark describes the largest count and the largest payload
    /// seen, which need not be the same page.
    pub fn record(&mut self, page: &GlobalKeyValuePage) {
        self.entries = self.entries.max(page.len());
        self.payload_bytes = self.payload_bytes.max(page.payload_bytes());
    }

    /// The most entries any one page carried.
    pub const fn entries(self) -> usize {
        self.entries
    }

    /// The most payload bytes any one page carried.
    pub const fn payload_bytes(self) -> usize {
        self.payload_bytes
    }
}

/// One bounded page of a global key/value load.
///
/// Constructed only by [`GlobalKeyValuePageBuilder`], which is what makes "a page is
/// within its loader's bound" true by construction rather than by convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalKeyValuePage {
    source: Arc<str>,
    state_version: u32,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    payload_bytes: usize,
}

impl GlobalKeyValuePage {
    /// The source these entries came from, for diagnostics: a checkpoint object's path for
    /// the parquet loader.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The state version this page's entries were written under.
    ///
    /// Every entry in one page shares it, so a decoder decides how to read the page once.
    pub const fn state_version(&self) -> u32 {
        self.state_version
    }

    /// The entries, as `(key bytes, value bytes)`.
    ///
    /// Empty on the page that announces a source (rule 2 in this module's documentation).
    pub fn entries(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.entries
    }

    /// The entries, owned.
    pub fn into_entries(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.entries
    }

    /// How many entries this page carries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether this page carries no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The sum of every key and value length in this page.
    ///
    /// This is the quantity [`LoadPageLimits::max_payload_bytes`] caps; the page's total
    /// cost is this plus the per-entry bookkeeping this module's documentation accounts
    /// for.
    pub const fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }
}

/// Accumulates one source's entries into bounded pages.
///
/// A loader makes one builder per source and pushes entries into it; the builder decides
/// where a page ends. Because [`GlobalKeyValuePage`] has no other constructor, no
/// implementation of [`GlobalKeyValueLoad`] can produce a page outside the bound it was
/// built with.
#[derive(Debug)]
pub struct GlobalKeyValuePageBuilder {
    source: Arc<str>,
    state_version: u32,
    limits: LoadPageLimits,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    payload_bytes: usize,
}

impl GlobalKeyValuePageBuilder {
    /// A builder for one source's entries.
    pub fn new(source: impl Into<Arc<str>>, state_version: u32, limits: LoadPageLimits) -> Self {
        Self {
            source: source.into(),
            state_version,
            limits,
            entries: Vec::new(),
            payload_bytes: 0,
        }
    }

    /// The page that announces this source: its name and state version, no entries.
    ///
    /// A loader returns this before reading any of the source's entries, which is rule 2
    /// of this module's page contract.
    pub fn header(&self) -> GlobalKeyValuePage {
        GlobalKeyValuePage {
            source: self.source.clone(),
            state_version: self.state_version,
            entries: Vec::new(),
            payload_bytes: 0,
        }
    }

    /// The bound this builder enforces.
    pub const fn limits(&self) -> LoadPageLimits {
        self.limits
    }

    /// Adds one entry, returning the page it closed.
    ///
    /// The entry never splits a page it belongs in: when it does not fit, the entries
    /// already accumulated become a page and the new entry starts the next one. An entry
    /// larger than [`LoadPageLimits::max_payload_bytes`] therefore ends up alone in a page
    /// of its own rather than being split or dropped.
    #[must_use = "the returned page is a page of the load; dropping it loses those entries"]
    pub fn push(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<GlobalKeyValuePage> {
        let size = key.len() + value.len();
        let full = self.entries.len() >= self.limits.max_entries
            || self.payload_bytes + size > self.limits.max_payload_bytes;
        let closed = if full { self.take() } else { None };
        self.entries.push((key, value));
        self.payload_bytes += size;
        closed
    }

    /// The entries accumulated so far as a page, or `None` when there are none.
    ///
    /// A loader calls this when its source runs out, to emit the partial page. It never
    /// produces an empty page: the only page with no entries is [`Self::header`].
    pub fn take(&mut self) -> Option<GlobalKeyValuePage> {
        if self.entries.is_empty() {
            return None;
        }
        Some(GlobalKeyValuePage {
            source: self.source.clone(),
            state_version: self.state_version,
            entries: std::mem::take(&mut self.entries),
            payload_bytes: std::mem::take(&mut self.payload_bytes),
        })
    }
}

/// The object-safe bounded-page loader of one global key/value table (M11.D15c).
///
/// A provider hands one out as `Box<dyn GlobalKeyValueLoad + Send>` from
/// [`GlobalKeyValueProvider::global_key_value_load`]; the decode above the seam drains it
/// and builds the `GlobalKeyedView<K, V>`. Implementations own everything they read from,
/// so a loader's lifetime is not tied to the `Arc<dyn ErasedTable>` it was made from.
///
/// [`GlobalKeyValueProvider::global_key_value_load`]: crate::provider::GlobalKeyValueProvider::global_key_value_load
#[async_trait]
pub trait GlobalKeyValueLoad: Send {
    /// The next page, or `None` once every source is exhausted.
    ///
    /// The four rules in this module's documentation are this method's contract: source
    /// order, one source per page, an announcing page before each source's entries, a
    /// bounded page, and a stable `None`.
    ///
    /// # Errors
    ///
    /// Returns the [`StateError`] raised by reading the backend's stored state — for
    /// parquet, a storage failure, an undecodable object, or an entry with a null key or
    /// value. A failed call ends the load: a caller does not retry a loader that errored.
    async fn next_page(&mut self) -> Result<Option<GlobalKeyValuePage>, StateError>;

    /// The bound this loader's pages honour.
    ///
    /// Declaring it is not optional: the decode above the seam checks every page against
    /// this value, so a loader that returns a wider bound than it keeps fails the load
    /// rather than silently spending the memory.
    fn page_limits(&self) -> LoadPageLimits;

    /// The largest page this loader has produced so far (M11.D36).
    fn peak_page(&self) -> LoadPagePeak;
}
