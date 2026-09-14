//! The page bound over real state: its size, its count, its high-water mark, and the check
//! that a loader's declared bound is verified rather than trusted (design item M11.D36,
//! work-plan item M11.P49d).

use async_trait::async_trait;

use arroyo_rpc::errors::StateError;

use super::{
    ENTRY_PAYLOAD_BYTES, drain, expected, loader_through_the_registry, pairs, restored,
    versioned_config, write_source,
};
use crate::BINCODE_CONFIG;
use crate::provider::tests::{TABLE, state_channel, storage};
use crate::tables::global_key_value_load::{
    GlobalKeyValueLoad, GlobalKeyValuePage, GlobalKeyValuePageBuilder, LoadPageLimits,
    LoadPagePeak, MAX_LOAD_PAGE_ENTRIES,
};

/// The page bound holds at 1× and at 10× the entries: the same peak, ten times the pages.
///
/// This is M11.P49d's bounded-load-page exit criterion. Both runs use the production
/// bound, and both counts are closed forms of it: a source of `n` entries yields one
/// announcing page and `ceil(n / 1024)` data pages, and the widest page holds 1024 entries
/// and `1024 × 14` payload bytes whichever run it came from.
#[tokio::test]
async fn load_pages_stay_bounded_at_one_and_ten_times_the_entries() {
    let storage = storage("global-load-scale").await;
    let config = versioned_config(0);

    let one_times = MAX_LOAD_PAGE_ENTRIES;
    let ten_times = 10 * MAX_LOAD_PAGE_ENTRIES;

    for (epoch, entries, expected_data_pages) in [(1, one_times, 1), (2, ten_times, 10)] {
        let file = write_source(&storage, &config, epoch, pairs(0..entries)).await;
        let table = restored(&storage, &config, vec![file]);
        let (pages, peak, limits) = drain(loader_through_the_registry(&table).await).await;

        assert_eq!(limits, LoadPageLimits::DEFAULT);
        assert_eq!(
            pages.len(),
            1 + expected_data_pages,
            "{entries} entries is one announcing page and {expected_data_pages} data pages"
        );
        assert_eq!(pages[0].len(), 0, "the first page announces the source");
        assert_eq!(
            pages[1..]
                .iter()
                .map(GlobalKeyValuePage::len)
                .sum::<usize>(),
            entries
        );
        assert_eq!(peak.entries(), MAX_LOAD_PAGE_ENTRIES);
        assert_eq!(
            peak.payload_bytes(),
            MAX_LOAD_PAGE_ENTRIES * ENTRY_PAYLOAD_BYTES
        );

        let (tx, _rx) = state_channel();
        let view = crate::tables::global_keyed_map::restore::view::<String, String>(
            TABLE,
            loader_through_the_registry(&table).await,
            tx,
        )
        .await
        .expect("a restored view");
        assert_eq!(view.get_all().len(), entries);
        assert_eq!(*view.get_all(), expected(0..entries));
    }
}

/// Pages follow the bound, not the parquet reader's batching: a bound that does not divide
/// the reader's 1024-row batch produces pages that start in one batch and finish in the
/// next.
///
/// 2500 entries read in batches of 1024, 1024 and 452, paged at 300: nine data pages of
/// 300, 300, 300, 300, 300, 300, 300, 300, 100 — a count that depends only on 2500 and 300.
#[tokio::test]
async fn pages_follow_the_bound_and_not_the_readers_batching() {
    let storage = storage("global-load-span").await;
    let config = versioned_config(0);
    let entries = 2500;
    let limits = LoadPageLimits::new(
        300,
        crate::tables::global_key_value_load::MAX_LOAD_PAGE_BYTES,
    )
    .expect("a bound tighter than the default");

    let file = write_source(&storage, &config, 1, pairs(0..entries)).await;
    let table = restored(&storage, &config, vec![file]);
    let loader: Box<dyn GlobalKeyValueLoad + Send> = Box::new(
        crate::tables::global_key_value_load::parquet_load::ParquetGlobalKeyValueLoad::new(
            TABLE.to_string(),
            storage.clone(),
            table.files.clone(),
            limits,
        ),
    );

    let (pages, peak, declared) = drain(loader).await;
    assert_eq!(declared, limits);
    assert_eq!(
        pages
            .iter()
            .map(GlobalKeyValuePage::len)
            .collect::<Vec<_>>(),
        vec![0, 300, 300, 300, 300, 300, 300, 300, 300, 100],
    );
    assert_eq!(peak.entries(), 300);
    assert_eq!(peak.payload_bytes(), 300 * ENTRY_PAYLOAD_BYTES);

    let restored_keys: Vec<Vec<u8>> = pages
        .iter()
        .flat_map(|page| page.entries().iter().map(|(key, _)| key.clone()))
        .collect();
    assert_eq!(restored_keys.len(), entries);
    assert_eq!(
        restored_keys[0],
        bincode::encode_to_vec("k00000".to_string(), BINCODE_CONFIG).unwrap()
    );
    assert_eq!(
        restored_keys[entries - 1],
        bincode::encode_to_vec(format!("k{:05}", entries - 1), BINCODE_CONFIG).unwrap()
    );
}

/// A loader that hands out a page wider than the bound it declared fails the restore
/// instead of being believed.
#[tokio::test]
async fn a_page_wider_than_the_declared_bound_fails_the_restore() {
    let (tx, _rx) = state_channel();
    let error = crate::tables::global_keyed_map::restore::view::<String, String>(
        TABLE,
        Box::new(LyingLoad::new()),
        tx,
    )
    .await
    .map(|_| ())
    .expect_err("the loader declared a bound of one entry and produced four");

    match error {
        StateError::Other { table, error } => {
            assert_eq!(table, TABLE);
            assert!(
                error.contains("produced a page of 4 entries")
                    && error.contains("bound it declared of 1 entries"),
                "the refusal must say what was produced and what was promised: {error}"
            );
        }
        other => panic!("expected the bound refusal, got {other:?}"),
    }
}

/// A loader whose pages are wider than the limits it reports.
///
/// It cannot be built by mis-using [`GlobalKeyValuePageBuilder`] — the builder enforces the
/// bound it is given — so it declares one bound and builds with another, which is the shape
/// a backend with a bug in its own paging would have.
struct LyingLoad {
    pages: std::vec::IntoIter<GlobalKeyValuePage>,
    peak: LoadPagePeak,
}

impl LyingLoad {
    fn new() -> Self {
        let mut builder = GlobalKeyValuePageBuilder::new("liar", 0, LoadPageLimits::DEFAULT);
        let mut pages = vec![builder.header()];
        for i in 0..4u8 {
            assert!(builder.push(vec![i], vec![i]).is_none());
        }
        pages.extend(builder.take());
        Self {
            pages: pages.into_iter(),
            peak: LoadPagePeak::ZERO,
        }
    }
}

#[async_trait]
impl GlobalKeyValueLoad for LyingLoad {
    async fn next_page(&mut self) -> Result<Option<GlobalKeyValuePage>, StateError> {
        let page = self.pages.next();
        if let Some(page) = &page {
            self.peak.record(page);
        }
        Ok(page)
    }

    fn page_limits(&self) -> LoadPageLimits {
        LoadPageLimits::new(1, 1).expect("a bound of one entry")
    }

    fn peak_page(&self) -> LoadPagePeak {
        self.peak
    }
}
