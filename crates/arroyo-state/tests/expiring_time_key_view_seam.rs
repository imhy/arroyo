//! The M11.D13 view seam, exercised the way another crate would use it.
//!
//! Nothing here touches parquet: the view under test implements `ExpiringTimeKeyViewApi`
//! and nothing else. That makes this file the evidence for D11's "external crates compile
//! against the seam" — if the trait stopped being object-safe, if a method stopped being
//! reachable outside `arroyo-state`, or if the batch stream started demanding `'static`
//! data, this test would not compile.
//!
//! It is also where an outside view shows it gets the D13 drain stream without writing one
//! and without being able to write one: `all_batches_for_watermark` lives on
//! `ExpiringTimeKeyViewDrain`, whose single blanket implementation over every
//! `T: ExpiringTimeKeyViewApi + ?Sized` coherence will not let a second implementation
//! join.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::errors::StateError;
use arroyo_state::tables::expiring_time_key_view::{
    BatchDrainToken, ExpiringTimeKeyViewApi, ExpiringTimeKeyViewDrain,
};
use async_trait::async_trait;
use futures::StreamExt;

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn batch(value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))])
        .expect("batch matches schema")
}

fn value_of(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 value column")
        .value(0)
}

/// A view with no backend at all.
///
/// It serves a fixed list of batches and counts how many it has actually handed out, so a
/// test can tell the difference between "produced on demand" and "produced up front".
struct FakeView {
    source: Vec<(SystemTime, RecordBatch)>,
    /// Incremented once per batch the fake actually yields.
    produced: Arc<AtomicUsize>,
    /// The live drain's token and how far into `source` it has reached.
    drain: Option<(BatchDrainToken, usize)>,
    /// What has been called on this view, in order, shared so a test can read it back
    /// through a `Box<dyn ExpiringTimeKeyViewApi + Send>`.
    calls: Arc<Mutex<Vec<String>>>,
    min_time: Option<SystemTime>,
}

impl FakeView {
    fn new(values: &[(u64, i64)]) -> Self {
        Self {
            source: values
                .iter()
                .map(|(seconds, value)| (at(*seconds), batch(*value)))
                .collect(),
            produced: Arc::new(AtomicUsize::new(0)),
            drain: None,
            calls: Arc::new(Mutex::new(vec![])),
            min_time: values.iter().map(|(seconds, _)| at(*seconds)).min(),
        }
    }

    fn record(&self, call: impl Into<String>) {
        self.calls.lock().expect("call log").push(call.into());
    }
}

fn seconds_of(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("after epoch")
        .as_secs()
}

#[async_trait]
impl ExpiringTimeKeyViewApi for FakeView {
    fn insert(&mut self, max_timestamp: SystemTime, batch: RecordBatch) -> Result<(), StateError> {
        self.record(format!(
            "insert@{}={}",
            seconds_of(max_timestamp),
            value_of(&batch)
        ));
        Ok(())
    }

    async fn flush(&mut self, _watermark: Option<SystemTime>) -> Result<(), StateError> {
        self.record("flush");
        Ok(())
    }

    async fn flush_timestamp(&mut self, _timestamp: SystemTime) -> Result<(), StateError> {
        self.record("flush_timestamp");
        Ok(())
    }

    async fn expire_timestamp(&mut self, timestamp: SystemTime) -> Result<(), StateError> {
        self.record(format!("expire_timestamp@{}", seconds_of(timestamp)));
        Ok(())
    }

    fn get_min_time(&self) -> Option<SystemTime> {
        self.record("get_min_time");
        self.min_time
    }

    fn begin_batch_drain(&mut self, _watermark: Option<SystemTime>) -> BatchDrainToken {
        self.record("begin_batch_drain");
        let token = BatchDrainToken::mint();
        self.drain = Some((token, 0));
        token
    }

    async fn next_drained_batch(
        &mut self,
        token: BatchDrainToken,
    ) -> Result<Option<(SystemTime, RecordBatch)>, StateError> {
        self.record("next_drained_batch");
        let Some((live, index)) = self.drain else {
            return Err(StateError::Other {
                table: "fake".to_string(),
                error: "no drain in progress".to_string(),
            });
        };
        if live != token {
            return Err(StateError::Other {
                table: "fake".to_string(),
                error: "stale drain token".to_string(),
            });
        }
        let Some(item) = self.source.get(index) else {
            return Ok(None);
        };
        self.drain = Some((live, index + 1));
        self.produced.fetch_add(1, Ordering::SeqCst);
        Ok(Some((item.0, item.1.clone())))
    }
}

#[test]
fn the_seam_is_object_safe_and_boxable() {
    fn assert_send<T: Send + ?Sized>() {}
    assert_send::<dyn ExpiringTimeKeyViewApi + Send>();

    let boxed: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(FakeView::new(&[(1000, 1)]));
    assert_eq!(boxed.get_min_time(), Some(at(1000)));
}

#[tokio::test]
async fn an_external_view_is_driven_through_every_method() {
    let fake = FakeView::new(&[(1000, 1), (1010, 10)]);
    let produced = Arc::clone(&fake.produced);
    let calls = Arc::clone(&fake.calls);
    let mut view: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(fake);

    view.insert(at(1020), batch(20)).expect("insert");
    view.flush(Some(at(1020))).await.expect("flush");
    view.flush_timestamp(at(1020))
        .await
        .expect("flush_timestamp");
    view.expire_timestamp(at(1020)).await.expect("expire");
    assert_eq!(view.get_min_time(), Some(at(1000)));

    let mut drained = vec![];
    {
        let mut stream = view.all_batches_for_watermark(None);
        while let Some(item) = stream.next().await {
            let (timestamp, batch) = item.expect("drain");
            drained.push((timestamp, value_of(&batch)));
        }
    }

    assert_eq!(drained, vec![(at(1000), 1), (at(1010), 10)]);
    assert_eq!(produced.load(Ordering::SeqCst), 2);
    assert_eq!(
        *calls.lock().expect("call log"),
        vec![
            "insert@1020=20",
            "flush",
            "flush_timestamp",
            "expire_timestamp@1020",
            "get_min_time",
            "begin_batch_drain",
            "next_drained_batch",
            "next_drained_batch",
            "next_drained_batch",
        ],
        "every seam method is reached, and the stream is three polls: two batches and the end"
    );
}

#[tokio::test]
async fn the_stream_produces_exactly_one_item_per_poll() {
    let fake = FakeView::new(&[(1000, 1), (1010, 2), (1020, 3), (1030, 4), (1040, 5)]);
    let produced = Arc::clone(&fake.produced);
    let mut view: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(fake);

    let mut stream = view.all_batches_for_watermark(None);
    assert_eq!(
        produced.load(Ordering::SeqCst),
        0,
        "building the stream must not produce anything"
    );

    for expected in 1..=5usize {
        let item = stream.next().await.expect("an item").expect("drain");
        assert_eq!(value_of(&item.1), expected as i64);
        assert_eq!(
            produced.load(Ordering::SeqCst),
            expected,
            "poll {expected} must have produced exactly {expected} batches"
        );
    }

    assert!(stream.next().await.is_none());
    assert_eq!(produced.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn a_cache_shaped_container_lends_the_view_and_takes_it_back() {
    let mut cache: HashMap<String, Box<dyn ExpiringTimeKeyViewApi + Send>> = HashMap::new();
    cache.insert(
        "t".to_string(),
        Box::new(FakeView::new(&[(1000, 1), (1010, 2)])),
    );

    let mut drained = vec![];
    {
        let view = cache.get_mut("t").expect("cached view");
        let mut stream = view.all_batches_for_watermark(None);
        while let Some(item) = stream.next().await {
            drained.push(value_of(&item.expect("drain").1));
        }
    }
    assert_eq!(drained, vec![1, 2]);

    // The stream's borrow has ended, so the same cached view takes a mutation.
    let view = cache.get_mut("t").expect("cached view");
    view.insert(at(1020), batch(3)).expect("insert");
    assert_eq!(view.get_min_time(), Some(at(1000)));
}

#[tokio::test]
async fn a_second_drain_retires_the_first_drains_token() {
    let mut view: Box<dyn ExpiringTimeKeyViewApi + Send> =
        Box::new(FakeView::new(&[(1000, 1), (1010, 2)]));

    let first = view.begin_batch_drain(None);
    let second = view.begin_batch_drain(None);
    assert_ne!(first, second);

    assert!(view.next_drained_batch(first).await.is_err());
    let item = view
        .next_drained_batch(second)
        .await
        .expect("live drain")
        .expect("a batch");
    assert_eq!(value_of(&item.1), 1);
}

#[tokio::test]
async fn two_views_do_not_share_a_first_drain_token() {
    let mut a: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(FakeView::new(&[(1000, 1)]));
    let mut b: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(FakeView::new(&[(1000, 9)]));

    // Each view's *first* drain. A per-view counter would make these equal.
    assert_ne!(a.begin_batch_drain(None), b.begin_batch_drain(None));
}

#[tokio::test]
async fn a_token_minted_by_another_view_is_refused() {
    let mut a: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(FakeView::new(&[(1000, 1)]));
    let mut b: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(FakeView::new(&[(1000, 9)]));

    let a_token = a.begin_batch_drain(None);
    let b_token = b.begin_batch_drain(None);

    assert!(
        b.next_drained_batch(a_token).await.is_err(),
        "another view's live token must not drain this view"
    );
    // B's own drain is untouched by the refusal: its first item is still waiting.
    let item = b
        .next_drained_batch(b_token)
        .await
        .expect("b's own drain")
        .expect("a batch");
    assert_eq!(value_of(&item.1), 9);
}

#[tokio::test]
async fn a_self_minted_token_is_refused() {
    let mut view: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(FakeView::new(&[(1000, 1)]));

    assert!(
        view.next_drained_batch(BatchDrainToken::mint())
            .await
            .is_err(),
        "a minted token must not start a drain that was never begun"
    );

    let token = view.begin_batch_drain(None);
    assert!(
        view.next_drained_batch(BatchDrainToken::mint())
            .await
            .is_err(),
        "a minted token must not pull from a live drain it did not start"
    );
    // The live drain still holds every item: the refusal consumed nothing.
    let item = view
        .next_drained_batch(token)
        .await
        .expect("live drain")
        .expect("a batch");
    assert_eq!(value_of(&item.1), 1);
}

/// Drains `view` through a bound that names only [`ExpiringTimeKeyViewApi`], asserting one
/// `next_drained_batch` per poll.
///
/// The bound is the point. `FakeView` implements that trait and declares no
/// `all_batches_for_watermark` of its own, so the stream this obtains can only be the one
/// blanket implementation of [`ExpiringTimeKeyViewDrain`]. `?Sized` covers the
/// `dyn ExpiringTimeKeyViewApi` the manager lends out as well as a sized view.
async fn drain_one_per_poll<V: ExpiringTimeKeyViewApi + ?Sized>(
    view: &mut V,
    produced: &AtomicUsize,
) -> Vec<i64> {
    let mut stream = view.all_batches_for_watermark(None);
    assert_eq!(
        produced.load(Ordering::SeqCst),
        0,
        "building the stream must not produce anything"
    );

    let mut drained = vec![];
    while let Some(item) = stream.next().await {
        drained.push(value_of(&item.expect("drain").1));
        assert_eq!(
            produced.load(Ordering::SeqCst),
            drained.len(),
            "poll {} must have produced exactly {} batches",
            drained.len(),
            drained.len()
        );
    }
    drained
}

#[tokio::test]
async fn an_outside_view_gets_the_one_batch_stream_it_cannot_replace() {
    // A view defined outside `arroyo-state` that supplies only the primitives. Every batch
    // below therefore came from the blanket implementation, which is the only one that can
    // exist: a second `impl ExpiringTimeKeyViewDrain for FakeView` here is rejected by
    // coherence (E0119), and `ExpiringTimeKeyViewDrain`'s sealed supertrait is unnameable
    // from this crate.
    let mut sized = FakeView::new(&[(1000, 1), (1010, 2), (1020, 3)]);
    let produced = Arc::clone(&sized.produced);
    assert_eq!(
        drain_one_per_poll(&mut sized, &produced).await,
        vec![1, 2, 3]
    );
    assert_eq!(produced.load(Ordering::SeqCst), 3);

    // The same single implementation reaches the erased view the manager actually lends,
    // which is what the `?Sized` bound on the blanket impl buys.
    let erased = FakeView::new(&[(1000, 7), (1010, 8)]);
    let produced = Arc::clone(&erased.produced);
    let mut boxed: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(erased);
    assert_eq!(drain_one_per_poll(&mut *boxed, &produced).await, vec![7, 8]);
    assert_eq!(produced.load(Ordering::SeqCst), 2);
}
