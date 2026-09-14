//! What `insert`, `flush`, `flush_timestamp`, `expire_timestamp` and `get_min_time`
//! leave behind, observed through a later drain and through the state channel.

use super::*;

#[tokio::test]
async fn flush_moves_pending_batches_to_flushed_and_writes_them_to_state() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1000, 2), (1010, 10)]);
    assert_eq!(written_to_state(&mut rx), Vec::<i64>::new());

    view.flush(None).await.expect("flush");

    assert_eq!(written_to_state(&mut rx), vec![1, 2, 10]);
    assert_eq!(view.get_min_time(), Some(at(1000)));
    assert_eq!(
        drained(&mut view, None).await,
        vec![(1000, 1), (1000, 2), (1010, 10)]
    );
}

#[tokio::test]
async fn flush_discards_pending_batches_the_watermark_has_retired() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10)]);

    // cutoff = 1020 - 10 = 1010; the 1000 batch is strictly below it.
    view.flush(Some(at(1020))).await.expect("flush");

    assert_eq!(written_to_state(&mut rx), vec![10]);
    assert_eq!(view.get_min_time(), Some(at(1010)));
    assert_eq!(drained(&mut view, None).await, vec![(1010, 10)]);
}

#[tokio::test]
async fn flush_drops_flushed_batches_below_the_retention_cutoff() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10), (1020, 20)]);
    view.flush(None).await.expect("first flush");
    written_to_state(&mut rx);

    view.flush(Some(at(1020))).await.expect("second flush");

    assert_eq!(written_to_state(&mut rx), Vec::<i64>::new());
    assert_eq!(view.get_min_time(), Some(at(1010)));
    assert_eq!(drained(&mut view, None).await, vec![(1010, 10), (1020, 20)]);
}

#[tokio::test]
async fn flush_timestamp_moves_only_its_own_timestamp() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1000, 2), (1010, 10)]);

    view.flush_timestamp(at(1000)).await.expect("flush 1000");

    assert_eq!(written_to_state(&mut rx), vec![1, 2]);
    assert_eq!(view.get_min_time(), Some(at(1000)));
    // 1000 now reads out of the flushed map, 1010 is still pending, so 1000 comes first.
    assert_eq!(
        drained(&mut view, None).await,
        vec![(1000, 1), (1000, 2), (1010, 10)]
    );
}

#[tokio::test]
async fn flush_timestamp_of_an_unknown_timestamp_changes_nothing() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1010, 10)]);

    view.flush_timestamp(at(1000)).await.expect("flush 1000");

    assert_eq!(written_to_state(&mut rx), Vec::<i64>::new());
    assert_eq!(drained(&mut view, None).await, vec![(1010, 10)]);
}

#[tokio::test]
async fn expire_timestamp_removes_that_timestamp_from_both_maps() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);
    insert_all(&mut view, &[(1000, 2), (1010, 10)]);

    view.expire_timestamp(at(1000)).await.expect("expire");

    assert_eq!(written_to_state(&mut rx), Vec::<i64>::new());
    assert_eq!(view.get_min_time(), Some(at(1010)));
    assert_eq!(drained(&mut view, None).await, vec![(1010, 10)]);
}

#[tokio::test]
async fn get_min_time_is_the_minimum_across_both_maps() {
    let (mut view, mut rx) = view().await;
    assert_eq!(view.get_min_time(), None);

    insert_all(&mut view, &[(1010, 10)]);
    assert_eq!(view.get_min_time(), Some(at(1010)), "pending only");

    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);
    assert_eq!(view.get_min_time(), Some(at(1010)), "flushed only");

    insert_all(&mut view, &[(1000, 1)]);
    assert_eq!(view.get_min_time(), Some(at(1000)), "pending below flushed");

    insert_all(&mut view, &[(1020, 20)]);
    assert_eq!(
        view.get_min_time(),
        Some(at(1000)),
        "unchanged by a later key"
    );
}
