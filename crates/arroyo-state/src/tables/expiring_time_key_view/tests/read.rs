//! What a drain returns: order, the retention cutoff, and what it clones.

use super::*;

#[tokio::test]
async fn drain_returns_every_batch_in_timestamp_then_insertion_order() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1020, 20), (1000, 1), (1000, 2), (1010, 10)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    assert_eq!(
        drained(&mut view, None).await,
        vec![(1000, 1), (1000, 2), (1010, 10), (1020, 20)]
    );
}

#[tokio::test]
async fn drain_keeps_a_timestamp_exactly_at_the_retention_cutoff() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10), (1020, 20)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    // cutoff = 1020 - 10 = 1010, which is exactly the second timestamp.
    assert_eq!(
        drained(&mut view, Some(at(1020))).await,
        vec![(1010, 10), (1020, 20)]
    );
}

#[tokio::test]
async fn drain_drops_a_timestamp_just_below_the_retention_cutoff() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10), (1020, 20)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    // cutoff = 1021 - 10 = 1011, one second above the second timestamp.
    assert_eq!(drained(&mut view, Some(at(1021))).await, vec![(1020, 20)]);
}

#[tokio::test]
async fn drain_keeps_a_timestamp_just_above_the_retention_cutoff() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10), (1020, 20)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    // cutoff = 1019 - 10 = 1009, one second below the second timestamp.
    assert_eq!(
        drained(&mut view, Some(at(1019))).await,
        vec![(1010, 10), (1020, 20)]
    );
}

#[tokio::test]
async fn drain_of_an_empty_view_yields_nothing() {
    let (mut view, _rx) = view().await;
    assert_eq!(drained(&mut view, None).await, vec![]);
    assert_eq!(drained(&mut view, Some(at(1020))).await, vec![]);
}

#[tokio::test]
async fn drain_returns_flushed_batches_before_pending_ones() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10), (1020, 20)]);
    view.flush_timestamp(at(1010)).await.expect("flush 1010");
    written_to_state(&mut rx);

    assert_eq!(
        drained(&mut view, None).await,
        vec![(1010, 10), (1000, 1), (1020, 20)]
    );
}

#[tokio::test]
async fn drain_clones_only_the_batch_it_yields() {
    let (mut view, mut rx) = view().await;
    let columns: Vec<ArrayRef> = (0..4)
        .map(|i| {
            let (batch, column) = batch(i, at(1000 + i as u64));
            view.insert(at(1000 + i as u64), batch).expect("insert");
            column
        })
        .collect();
    view.flush(None).await.expect("flush");
    // The flush handed a clone of each batch to the state channel; drop them so the only
    // remaining holders are this test and the view's own map.
    written_to_state(&mut rx);
    for column in &columns {
        assert_eq!(Arc::strong_count(column), 2, "test + view map");
    }

    let token = view.begin_batch_drain(None);
    let (_timestamp, first) = view
        .next_drained_batch(token)
        .await
        .expect("drain")
        .expect("a first batch");

    assert!(
        Arc::ptr_eq(first.column(0), &columns[0]),
        "the yielded batch shares the stored array rather than copying it"
    );
    assert_eq!(
        Arc::strong_count(&columns[0]),
        3,
        "test + view map + yielded"
    );
    for column in &columns[1..] {
        assert_eq!(
            Arc::strong_count(column),
            2,
            "a batch the drain has not reached is not cloned"
        );
    }

    drop(first);
    for column in &columns {
        assert_eq!(Arc::strong_count(column), 2, "test + view map");
    }
}

#[tokio::test]
async fn the_view_is_usable_after_its_stream_is_dropped() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    {
        let mut stream = view.all_batches_for_watermark(None);
        assert!(stream.next().await.is_some());
    }

    // The borrow the stream held has ended, so the same view accepts a mutation.
    let (extra, _) = batch(2, at(1010));
    view.insert(at(1010), extra).expect("insert");
    assert_eq!(view.get_min_time(), Some(at(1000)));
    assert_eq!(drained(&mut view, None).await, vec![(1000, 1), (1010, 2)]);
}
