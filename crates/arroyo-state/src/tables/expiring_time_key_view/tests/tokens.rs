//! The drain-token invariants of the seam's module matrix, and the fail-closed
//! behaviour when a live drain's range is written to.

use super::*;

#[tokio::test]
async fn a_drain_token_from_a_replaced_drain_is_refused() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    let first = view.begin_batch_drain(None);
    let second = view.begin_batch_drain(None);
    assert_ne!(first, second);

    let err = view
        .next_drained_batch(first)
        .await
        .expect_err("the replaced drain's token is refused");
    assert!(
        err.to_string().contains("current drain"),
        "unexpected error: {err}"
    );
    assert!(
        view.next_drained_batch(second)
            .await
            .expect("live drain")
            .is_some()
    );
}

#[tokio::test]
async fn a_pull_without_a_drain_is_refused() {
    let (mut view, _rx) = view().await;
    let err = view
        .next_drained_batch(BatchDrainToken::mint())
        .await
        .expect_err("no drain has begun");
    assert!(
        err.to_string().contains("current drain"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn two_views_first_drains_do_not_share_a_token() {
    let (mut first, _first_rx) = view().await;
    let (mut second, _second_rx) = view().await;

    // Both are the first drain of a freshly built view. A per-view counter would make
    // these equal, and every cross-view refusal below would silently stop holding.
    assert_ne!(
        first.begin_batch_drain(None),
        second.begin_batch_drain(None)
    );
}

#[tokio::test]
async fn a_token_minted_by_another_view_is_refused() {
    let (mut first, mut first_rx) = view().await;
    insert_all(&mut first, &[(1000, 1)]);
    first.flush(None).await.expect("flush");
    written_to_state(&mut first_rx);

    let (mut second, mut second_rx) = view().await;
    insert_all(&mut second, &[(1000, 9)]);
    second.flush(None).await.expect("flush");
    written_to_state(&mut second_rx);

    let first_token = first.begin_batch_drain(None);
    let second_token = second.begin_batch_drain(None);

    let err = second
        .next_drained_batch(first_token)
        .await
        .expect_err("the other view's token is refused");
    assert!(
        err.to_string().contains("current drain"),
        "unexpected error: {err}"
    );

    // Neither drain was consumed by the refusal.
    let (_, batch) = second
        .next_drained_batch(second_token)
        .await
        .expect("second view's own drain")
        .expect("a batch");
    assert_eq!(value_of(&batch), 9);
    let (_, batch) = first
        .next_drained_batch(first_token)
        .await
        .expect("first view's own drain")
        .expect("a batch");
    assert_eq!(value_of(&batch), 1);
}

#[tokio::test]
async fn a_freshly_minted_token_is_refused_by_a_live_drain() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    let token = view.begin_batch_drain(None);
    let err = view
        .next_drained_batch(BatchDrainToken::mint())
        .await
        .expect_err("a token the view did not mint is refused");
    assert!(
        err.to_string().contains("current drain"),
        "unexpected error: {err}"
    );

    // The refusal did not advance the live drain: both batches are still to come.
    assert_eq!(
        value_of(
            &view
                .next_drained_batch(token)
                .await
                .expect("live drain")
                .expect("a batch")
                .1
        ),
        1
    );
    assert_eq!(
        value_of(
            &view
                .next_drained_batch(token)
                .await
                .expect("live drain")
                .expect("a batch")
                .1
        ),
        10
    );
    assert!(
        view.next_drained_batch(token)
            .await
            .expect("live drain")
            .is_none()
    );
}

#[tokio::test]
async fn a_pending_write_inside_a_live_drain_fails_it_closed() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);
    // Pending content at 1020 puts the drain's pending bound above 1010.
    insert_all(&mut view, &[(1020, 20)]);

    let token = view.begin_batch_drain(None);
    assert!(
        view.next_drained_batch(token)
            .await
            .expect("first")
            .is_some()
    );
    let (extra, _) = batch(10, at(1010));
    view.insert(at(1010), extra).expect("insert");

    let err = view
        .next_drained_batch(token)
        .await
        .expect_err("the drain range was written to");
    assert!(
        err.to_string().contains("drain in progress"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn a_flush_during_a_live_drain_fails_it_closed() {
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1010, 10)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    let token = view.begin_batch_drain(None);
    assert!(
        view.next_drained_batch(token)
            .await
            .expect("first")
            .is_some()
    );
    view.flush(None).await.expect("flush");

    let err = view
        .next_drained_batch(token)
        .await
        .expect_err("the maps were rewritten under the drain");
    assert!(
        err.to_string().contains("drain in progress"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn replayed_inserts_do_not_re_enter_the_drain() {
    // The `InstantJoin::on_start` shape: every batch pulled is written straight back into
    // the view it came from. The replay must terminate after exactly the batches the view
    // held when the drain began.
    let (mut view, mut rx) = view().await;
    insert_all(&mut view, &[(1000, 1), (1000, 2), (1010, 10)]);
    view.flush(None).await.expect("flush");
    written_to_state(&mut rx);

    let token = view.begin_batch_drain(None);
    let mut replayed = vec![];
    while let Some((timestamp, batch)) = view.next_drained_batch(token).await.expect("drain") {
        replayed.push(value_of(&batch));
        view.insert(timestamp, batch).expect("replay insert");
    }

    assert_eq!(replayed, vec![1, 2, 10]);
    // The replay is now buffered as well as flushed, so a fresh drain sees both copies.
    assert_eq!(
        drained(&mut view, None).await,
        vec![
            (1000, 1),
            (1000, 2),
            (1010, 10),
            (1000, 1),
            (1000, 2),
            (1010, 10)
        ]
    );
}
