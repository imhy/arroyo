//! The raise's rows: running out of wait budget without publishing, abandoned at each phase, two
//! at once, and a drained raise handed to the wrong writer or fence — with a request of each kind
//! in flight where the row needs one.

use std::pin::pin;
use std::time::Duration;

use super::{
    FOREVER, KINDS, acknowledged_at, moved_for_both, only, poll_once, rising_for_both, status,
};
use crate::ownership::{FencedRequest, Raise};

/// The timeout case, for each kind: a request that does not return within the wait budget keeps
/// the raise waiting — reported, never published — and a request of either kind asked for
/// meanwhile under either generation is refused; once the request returns, the same raise
/// publishes and the new fence admits.
#[tokio::test]
async fn a_raise_that_runs_out_of_budget_reports_keeps_waiting_and_never_publishes() {
    for request in KINDS {
        let (mut writer, fence) = acknowledged_at(7);
        let stuck = fence.admit(7, request).expect("admitted before the close");
        let Raise::Drain(mut pending) = writer.prepare(8, None) else {
            panic!("in flight")
        };
        let (deletions, reservations) = only(request, 1);
        for waits in 1..=3 {
            pending = pending
                .wait_for(Duration::from_millis(1))
                .await
                .expect_err("the request has not returned");
            let now = fence.gate_status();
            assert_eq!(
                (
                    now.acknowledged,
                    now.deletions_in_flight,
                    now.reservations_in_flight,
                    now.raises_pending,
                    now.long_drain_waits
                ),
                (7, deletions, reservations, 1, waits),
                "{request}"
            );
            for asked in [7, 8] {
                rising_for_both(&fence, 7, asked);
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
        for other in KINDS {
            drop(fence.admit(8, other).expect("open under 8"));
        }
        assert_eq!(fence.gate_status().long_drain_waits, 3, "{request}");
    }
}

/// A raise dropped at each phase — closed, drained, ready — publishes nothing and reopens
/// admission of both kinds under the fence still acknowledged.
#[test]
fn an_abandoned_raise_reopens_admission_under_the_fence_still_acknowledged() {
    for request in KINDS {
        let (mut writer, fence) = acknowledged_at(1);

        let guard = fence.admit(1, request).expect("admitted");
        let Raise::Drain(pending) = writer.prepare(2, None) else {
            panic!("in flight")
        };
        drop(pending);
        let (deletions, reservations) = only(request, 1);
        assert_eq!(status(&fence), (1, deletions, reservations, 0), "{request}");
        drop(guard);

        let Raise::Ready(ready) = writer.prepare(2, None) else {
            panic!("nothing in flight")
        };
        drop(ready);
        assert_eq!(status(&fence), (1, 0, 0, 0), "{request}");

        let guard = fence.admit(1, request).expect("admitted");
        let Raise::Drain(pending) = writer.prepare(2, None) else {
            panic!("in flight")
        };
        drop(guard);
        drop(pending.try_drained().expect("drained"));
        assert_eq!(status(&fence), (1, 0, 0, 0), "{request}");
        for other in KINDS {
            drop(fence.admit(1, other).expect("open under 1 again"));
        }
    }
}

/// Two raises at once, as two concurrent directives would make: admission stays closed until
/// both have published or been abandoned, the fence ends at the higher target whichever order they
/// publish in, and a lower target handed back after a higher one published is `NotAbove` — its
/// drained raise dropped, never published.
#[test]
fn two_raises_at_once_close_until_both_settle_and_never_lower_the_fence() {
    for (request, higher_first) in KINDS.into_iter().flat_map(|k| [(k, true), (k, false)]) {
        let label = format!("{request}, higher first: {higher_first}");
        let (mut writer, fence) = acknowledged_at(4);
        let guard = fence.admit(4, request).expect("admitted");
        let Raise::Drain(low) = writer.prepare(5, None) else {
            panic!("in flight")
        };
        let Raise::Drain(high) = writer.prepare(7, None) else {
            panic!("in flight")
        };
        let (deletions, reservations) = only(request, 1);
        assert_eq!(status(&fence), (4, deletions, reservations, 2), "{label}");
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
            assert_eq!(
                status(&fence),
                (7, 0, 0, 1),
                "{label}: the other still closes it"
            );
            rising_for_both(&fence, 7, 7);
            assert!(matches!(writer.prepare(5, Some(low)), Raise::NotAbove));
        } else {
            let Raise::Ready(ready) = writer.prepare(5, Some(low)) else {
                panic!("ready")
            };
            assert_eq!(ready.publish(), 5);
            assert_eq!(status(&fence), (5, 0, 0, 1), "{label}");
            let Raise::Ready(ready) = writer.prepare(7, Some(high)) else {
                panic!("ready")
            };
            assert_eq!(ready.publish(), 7);
        }
        assert_eq!(status(&fence), (7, 0, 0, 0), "{label}");
        moved_for_both(&fence, 7, 4);
        drop(fence.admit(7, request).expect("open under 7"));
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
        let guard = my_fence
            .admit(2, FencedRequest::Reservation)
            .expect("admitted");
        let raise = mine.prepare(3, None);
        drop(guard);
        raise
    }) else {
        panic!("in flight at the close")
    };
    let foreign = pending.try_drained().expect("drained");
    let in_flight = other_fence
        .admit(2, FencedRequest::Deletion)
        .expect("admitted on the other gate");
    let Raise::Drain(fresh) = other.prepare(3, Some(foreign)) else {
        panic!("the other gate's own close finds its deletion in flight")
    };
    assert_eq!(
        status(&my_fence),
        (2, 0, 0, 0),
        "mine abandoned, not published"
    );
    assert_eq!(
        status(&other_fence),
        (2, 1, 0, 1),
        "the other closed afresh"
    );
    drop((fresh, in_flight));

    let Raise::Drain(pending) = ({
        let guard = my_fence
            .admit(2, FencedRequest::Reservation)
            .expect("admitted");
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
        (2, 0, 0, 1),
        "one claim: the stale one released first"
    );
    assert_eq!(ready.publish(), 9);
    assert_eq!(status(&my_fence), (9, 0, 0, 0));
}
