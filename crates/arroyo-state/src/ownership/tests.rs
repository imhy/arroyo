//! The acknowledged fence and its deletion gate (M11.T10b.01/.02).
//!
//! The gate's rows, each identity varied alone against a control that admits: the acknowledged
//! fence against the generation asked under (below, equal, above), a pending raise, a guard
//! dropped normally, by an unwinding deleter and by a cancelled future, and a guard that is never
//! dropped. Then the raise itself: one in flight holds the publication back and one asked for
//! after the close is refused; a raise that runs out of wait budget reports and keeps waiting;
//! one abandoned reopens admission under the fence still acknowledged; two at once publish in
//! either order and never lower the fence; a drained raise handed to another writer, or for
//! another fence, cannot publish there.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use super::*;

/// Long enough never to run out in a test that waits for something that has already happened.
const FOREVER: Duration = Duration::from_secs(3600);

fn is_shareable<T: Clone + Send + Sync + 'static>() {}
fn is_send<T: Send + 'static>() {}

/// Raises `writer` to `fence` with nothing in flight: one step.
fn raise_now(writer: &mut AcknowledgedFenceWriter, fence: u64) -> u64 {
    match writer.prepare(fence, None) {
        Raise::Ready(ready) => ready.publish(),
        Raise::NotAbove => writer.get(),
        Raise::Drain(pending) => panic!("{} deletions in flight", pending.target()),
    }
}

/// A fence acknowledged at `fence`, and a reader.
fn acknowledged_at(fence: u64) -> (AcknowledgedFenceWriter, AcknowledgedFence) {
    let mut writer = AcknowledgedFenceWriter::unacknowledged();
    raise_now(&mut writer, fence);
    let reader = writer.reader();
    (writer, reader)
}

fn status(fence: &AcknowledgedFence) -> (u64, usize, usize) {
    let status = fence.deletion_gate();
    (status.acknowledged, status.in_flight, status.raises_pending)
}

fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn a_reader_is_a_cheap_shareable_handle_and_every_token_moves_across_threads() {
    is_shareable::<AcknowledgedFence>();
    is_send::<AcknowledgedFenceWriter>();
    is_send::<DeletionGuard>();
    is_send::<PendingRaise>();
    is_send::<DrainedRaise>();
}

#[test]
fn every_reader_follows_the_writer_and_the_fence_only_rises() {
    let mut writer = AcknowledgedFenceWriter::unacknowledged();
    let early = writer.reader();
    assert_eq!((writer.get(), early.get()), (0, 0));

    assert_eq!(raise_now(&mut writer, 4), 4);
    let late = writer.reader();
    assert_eq!((writer.get(), early.get(), late.get()), (4, 4, 4));

    for lower in [2, 4] {
        assert!(matches!(writer.prepare(lower, None), Raise::NotAbove));
    }
    assert_eq!((writer.get(), early.get(), late.get()), (4, 4, 4));
    assert_eq!(status(&early), (4, 0, 0), "a refused raise closed nothing");

    assert_eq!(raise_now(&mut writer, 9), 9);
    assert_eq!((early.get(), late.clone().get()), (9, 9));
}

#[test]
fn an_unfenced_handle_reads_zero_is_no_writers_cell_and_admits_under_zero_only() {
    let mut writer = AcknowledgedFenceWriter::unacknowledged();
    let unfenced = AcknowledgedFence::unfenced();
    raise_now(&mut writer, 7);
    assert_eq!(unfenced.get(), 0);
    assert_eq!(unfenced.clone().get(), 0);
    drop(unfenced.admit_deletion(0).expect("admitted under zero"));
    assert_eq!(
        unfenced.admit_deletion(7).unwrap_err(),
        DeletionRefused::Moved {
            acknowledged: 0,
            requested: 7
        }
    );
}

/// The admission row: the generation asked under against the acknowledged fence, each way, and
/// a pending raise — which refuses even the generation that would otherwise match — against the
/// control that registers exactly one deletion until its guard drops.
#[test]
fn admission_needs_the_acknowledged_fence_and_no_pending_raise() {
    let (mut writer, fence) = acknowledged_at(5);
    for asked in [4, 6, 0, u64::MAX] {
        assert_eq!(
            fence.admit_deletion(asked).unwrap_err(),
            DeletionRefused::Moved {
                acknowledged: 5,
                requested: asked
            },
            "{asked}"
        );
    }
    assert_eq!(status(&fence), (5, 0, 0));

    let control = fence.admit_deletion(5).expect("the control");
    let second = fence
        .admit_deletion(5)
        .expect("admission counts, it does not exclude");
    assert_eq!(status(&fence), (5, 2, 0));
    drop((control, second));
    assert_eq!(status(&fence), (5, 0, 0));

    let Raise::Ready(ready) = writer.prepare(6, None) else {
        panic!("nothing in flight: one step")
    };
    // Not yet published: the close alone refuses every generation.
    for asked in [5, 6] {
        assert_eq!(
            fence.admit_deletion(asked).unwrap_err(),
            DeletionRefused::Rising {
                acknowledged: 5,
                requested: asked
            },
            "{asked}"
        );
    }
    assert_eq!(ready.publish(), 6);
    assert_eq!(status(&fence), (6, 0, 0));
    drop(
        fence
            .admit_deletion(6)
            .expect("open under the published fence"),
    );
}

/// The reviewer's case at the gate: a deletion in flight when the raise closes holds the
/// publication back — the reader still sees the old fence — and nothing asked for after the close
/// is admitted under any generation; once it returns, the raise publishes, and only the new fence
/// admits.
#[test]
fn a_deletion_in_flight_holds_the_raise_back_and_none_is_admitted_after_the_close() {
    let (mut writer, fence) = acknowledged_at(3);
    let in_flight = fence.admit_deletion(3).expect("admitted before the close");

    let Raise::Drain(pending) = writer.prepare(4, None) else {
        panic!("a deletion is in flight")
    };
    assert_eq!(pending.target(), 4);
    assert_eq!(status(&fence), (3, 1, 1));
    assert!(fence.admit_deletion(3).is_err() && fence.admit_deletion(4).is_err());
    let pending = pending.try_drained().expect_err("still in flight");
    assert_eq!(fence.get(), 3, "not published");

    drop(in_flight);
    assert_eq!(status(&fence), (3, 0, 1), "drained, still closed");
    assert!(
        fence.admit_deletion(3).is_err(),
        "closed until it publishes"
    );
    let drained = pending.try_drained().expect("drained");
    let Raise::Ready(ready) = writer.prepare(4, Some(drained)) else {
        panic!("drained for exactly 4")
    };
    assert_eq!(ready.publish(), 4);
    assert_eq!(status(&fence), (4, 0, 0));
    assert_eq!(
        fence.admit_deletion(3).unwrap_err(),
        DeletionRefused::Moved {
            acknowledged: 4,
            requested: 3
        }
    );
    drop(fence.admit_deletion(4).expect("open under 4"));
}

/// A guard dropped by an unwinding deleter, and one owned by a future that is cancelled, both
/// deregister; one that is never dropped keeps the raise waiting — fail-closed.
#[tokio::test]
async fn a_guard_deregisters_on_unwind_and_cancellation_and_a_leaked_one_keeps_the_raise_waiting() {
    let (mut writer, fence) = acknowledged_at(2);

    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _guard = fence.admit_deletion(2).expect("admitted");
        panic!("the store call unwound");
    }));
    assert!(unwound.is_err());
    assert_eq!(status(&fence), (2, 0, 0), "deregistered by the unwind");

    let reader = fence.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
    let deleting = tokio::spawn(async move {
        let _guard = reader.admit_deletion(2).expect("admitted");
        entered_tx.send(()).expect("the test waits");
        std::future::pending::<()>().await;
    });
    entered_rx.await.expect("the deleter holds its guard");
    assert_eq!(status(&fence), (2, 1, 0));
    deleting.abort();
    assert!(deleting.await.expect_err("aborted").is_cancelled());
    assert_eq!(
        status(&fence),
        (2, 0, 0),
        "deregistered by the cancellation"
    );

    std::mem::forget(fence.admit_deletion(2).expect("admitted"));
    let Raise::Drain(pending) = writer.prepare(3, None) else {
        panic!("the leaked guard is in flight")
    };
    let pending = pending
        .wait_for(Duration::from_millis(1))
        .await
        .expect_err("a leaked guard never drains");
    assert_eq!(fence.get(), 2, "and nothing is acknowledged");
    assert_eq!(fence.deletion_gate().long_drain_waits, 1);
    drop(pending);
    assert_eq!(
        status(&fence),
        (2, 1, 0),
        "abandoned: open again, the leak still counted"
    );
}

/// The timeout case: a request that does not return within the wait budget keeps the raise
/// waiting — reported, never published — and a deletion asked for meanwhile under either
/// generation is refused; once the request returns, the same raise publishes and the new fence
/// admits.
#[tokio::test]
async fn a_raise_that_runs_out_of_budget_reports_keeps_waiting_and_never_publishes() {
    let (mut writer, fence) = acknowledged_at(7);
    let stuck = fence.admit_deletion(7).expect("admitted before the close");
    let Raise::Drain(mut pending) = writer.prepare(8, None) else {
        panic!("in flight")
    };
    for waits in 1..=3 {
        pending = pending
            .wait_for(Duration::from_millis(1))
            .await
            .expect_err("the request has not returned");
        let now = fence.deletion_gate();
        assert_eq!(
            (
                now.acknowledged,
                now.in_flight,
                now.raises_pending,
                now.long_drain_waits
            ),
            (7, 1, 1, waits)
        );
        for asked in [7, 8] {
            assert!(matches!(
                fence.admit_deletion(asked),
                Err(DeletionRefused::Rising {
                    acknowledged: 7,
                    ..
                })
            ));
        }
    }

    // `drained` waits as long as it takes: here, until the request returns.
    let mut draining = pin!(pending.drained());
    assert!(poll_once(draining.as_mut()).is_pending());
    drop(stuck);
    let drained = tokio::time::timeout(FOREVER, draining)
        .await
        .expect("woken by the release");
    let Raise::Ready(ready) = writer.prepare(8, Some(drained)) else {
        panic!("drained for 8")
    };
    assert_eq!(ready.publish(), 8);
    drop(fence.admit_deletion(8).expect("open under 8"));
    assert_eq!(fence.deletion_gate().long_drain_waits, 3);
}

/// A raise dropped at each phase — closed, drained, ready — publishes nothing and reopens
/// admission under the fence still acknowledged.
#[test]
fn an_abandoned_raise_reopens_admission_under_the_fence_still_acknowledged() {
    let (mut writer, fence) = acknowledged_at(1);

    let guard = fence.admit_deletion(1).expect("admitted");
    let Raise::Drain(pending) = writer.prepare(2, None) else {
        panic!("in flight")
    };
    drop(pending);
    assert_eq!(status(&fence), (1, 1, 0));
    drop(guard);

    let Raise::Ready(ready) = writer.prepare(2, None) else {
        panic!("nothing in flight")
    };
    drop(ready);
    assert_eq!(status(&fence), (1, 0, 0));

    let guard = fence.admit_deletion(1).expect("admitted");
    let Raise::Drain(pending) = writer.prepare(2, None) else {
        panic!("in flight")
    };
    drop(guard);
    drop(pending.try_drained().expect("drained"));
    assert_eq!(status(&fence), (1, 0, 0));
    drop(fence.admit_deletion(1).expect("open under 1 again"));
}

/// Two raises at once, as two concurrent directives would make: admission stays closed until
/// both have published or been abandoned, the fence ends at the higher target whichever order they
/// publish in, and a lower target handed back after a higher one published is `NotAbove` — its
/// drained raise dropped, never published.
#[test]
fn two_raises_at_once_close_until_both_settle_and_never_lower_the_fence() {
    for higher_first in [true, false] {
        let (mut writer, fence) = acknowledged_at(4);
        let guard = fence.admit_deletion(4).expect("admitted");
        let Raise::Drain(low) = writer.prepare(5, None) else {
            panic!("in flight")
        };
        let Raise::Drain(high) = writer.prepare(7, None) else {
            panic!("in flight")
        };
        assert_eq!(status(&fence), (4, 1, 2));
        drop(guard);
        let (low, high) = (
            low.try_drained().expect("drained"),
            high.try_drained().expect("drained"),
        );
        if higher_first {
            let Raise::Ready(ready) = writer.prepare(7, Some(high)) else {
                panic!("ready")
            };
            assert_eq!(ready.publish(), 7);
            assert_eq!(status(&fence), (7, 0, 1), "the other still holds it closed");
            assert!(fence.admit_deletion(7).is_err());
            assert!(matches!(writer.prepare(5, Some(low)), Raise::NotAbove));
        } else {
            let Raise::Ready(ready) = writer.prepare(5, Some(low)) else {
                panic!("ready")
            };
            assert_eq!(ready.publish(), 5);
            assert_eq!(status(&fence), (5, 0, 1));
            let Raise::Ready(ready) = writer.prepare(7, Some(high)) else {
                panic!("ready")
            };
            assert_eq!(ready.publish(), 7);
        }
        assert_eq!(status(&fence), (7, 0, 0), "higher first: {higher_first}");
        drop(fence.admit_deletion(7).expect("open under 7"));
    }
}

/// A drained raise is matched to the writer and the fence it closed: handed to another writer,
/// or back for another fence, it is dropped and a fresh close taken — its own gate reopens, and it
/// publishes nothing anywhere.
#[test]
fn a_drained_raise_publishes_only_on_its_own_writer_for_its_own_fence() {
    let (mut mine, my_fence) = acknowledged_at(2);
    let (mut other, other_fence) = acknowledged_at(2);

    let Raise::Drain(pending) = ({
        let guard = my_fence.admit_deletion(2).expect("admitted");
        let raise = mine.prepare(3, None);
        drop(guard);
        raise
    }) else {
        panic!("in flight at the close")
    };
    let foreign = pending.try_drained().expect("drained");
    let in_flight = other_fence
        .admit_deletion(2)
        .expect("admitted on the other gate");
    let Raise::Drain(fresh) = other.prepare(3, Some(foreign)) else {
        panic!("the other gate's own close finds its deletion in flight")
    };
    assert_eq!(
        status(&my_fence),
        (2, 0, 0),
        "mine abandoned, not published"
    );
    assert_eq!(status(&other_fence), (2, 1, 1), "the other closed afresh");
    drop((fresh, in_flight));

    let Raise::Drain(pending) = ({
        let guard = my_fence.admit_deletion(2).expect("admitted");
        let raise = mine.prepare(3, None);
        drop(guard);
        raise
    }) else {
        panic!("in flight at the close")
    };
    let for_three = pending.try_drained().expect("drained");
    let Raise::Ready(ready) = mine.prepare(9, Some(for_three)) else {
        panic!("a fresh close for 9, nothing in flight")
    };
    assert_eq!(
        status(&my_fence),
        (2, 0, 1),
        "one claim: the stale one released first"
    );
    assert_eq!(ready.publish(), 9);
    assert_eq!(status(&my_fence), (9, 0, 0));
}

#[test]
fn every_refusal_renders_its_whole_sentence() {
    assert_eq!(
        DeletionRefused::Rising {
            acknowledged: 4,
            requested: 4
        }
        .to_string(),
        "a deletion under ownership generation 4 was refused: a raise of the acknowledged \
         lifecycle fence 4 is draining its in-flight deletions"
    );
    assert_eq!(
        DeletionRefused::Moved {
            acknowledged: 5,
            requested: 4
        }
        .to_string(),
        "a deletion under ownership generation 4 was refused: the acknowledged lifecycle fence \
         is 5"
    );
}
