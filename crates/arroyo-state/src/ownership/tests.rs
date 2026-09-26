//! The acknowledged fence and its ownership gate (M11.T10b.01/.02), for each [`FencedRequest`]
//! kind.
//!
//! The gate's rows, each identity varied alone against a control that admits: the acknowledged
//! fence against the generation asked under (below, equal, above) and the kind, independently; a
//! pending raise; the two kinds counted apart, each guard naming and releasing exactly what it was
//! admitted for; a request of either kind holding a raise back, and a raise waiting for both; a
//! guard dropped normally, by an unwinding requester and by a cancelled future, and a guard that
//! is never dropped. `tests/raise.rs` has the raise itself: running out of wait budget, abandoned
//! at each phase, two at once, and a drained raise handed to the wrong writer or fence.

mod raise;

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use super::*;

/// Long enough never to run out in a test that waits for something that has already happened.
const FOREVER: Duration = Duration::from_secs(3600);

const KINDS: [FencedRequest; 2] = [FencedRequest::Deletion, FencedRequest::Reservation];

fn is_shareable<T: Clone + Send + Sync + 'static>() {}
fn is_send<T: Send + 'static>() {}

/// Raises `writer` to `fence` with nothing in flight: one step.
fn raise_now(writer: &mut AcknowledgedFenceWriter, fence: u64) -> u64 {
    match writer.prepare(fence, None) {
        Raise::Ready(ready) => ready.publish(),
        Raise::NotAbove => writer.get(),
        Raise::Drain(pending) => panic!("requests in flight: a raise to {}", pending.target()),
    }
}

/// A fence acknowledged at `fence`, and a reader.
fn acknowledged_at(fence: u64) -> (AcknowledgedFenceWriter, AcknowledgedFence) {
    let mut writer = AcknowledgedFenceWriter::unacknowledged();
    raise_now(&mut writer, fence);
    let reader = writer.reader();
    (writer, reader)
}

/// `(acknowledged, deletions, reservations, raises pending)`.
fn status(fence: &AcknowledgedFence) -> (u64, usize, usize, usize) {
    let status = fence.gate_status();
    (
        status.acknowledged,
        status.deletions_in_flight,
        status.reservations_in_flight,
        status.raises_pending,
    )
}

/// `n` requests of `kind` in flight, as `(deletions, reservations)`.
fn only(kind: FencedRequest, n: usize) -> (usize, usize) {
    match kind {
        FencedRequest::Deletion => (n, 0),
        FencedRequest::Reservation => (0, n),
    }
}

/// A raise is pending over `acknowledged`: every admission of either kind under `asked` is
/// refused as rising, naming its own kind and generation.
fn rising_for_both(fence: &AcknowledgedFence, acknowledged: u64, asked: u64) {
    for request in KINDS {
        assert_eq!(
            fence.admit(asked, request).unwrap_err(),
            AdmissionRefused::Rising {
                acknowledged,
                requested: asked,
                request
            },
            "{request} under {asked}"
        );
    }
}

/// The fence is `acknowledged`: every admission of either kind under `asked` is refused as
/// moved.
fn moved_for_both(fence: &AcknowledgedFence, acknowledged: u64, asked: u64) {
    for request in KINDS {
        assert_eq!(
            fence.admit(asked, request).unwrap_err(),
            AdmissionRefused::Moved {
                acknowledged,
                requested: asked,
                request
            },
            "{request} under {asked}"
        );
    }
}

fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn a_reader_is_a_cheap_shareable_handle_and_every_token_moves_across_threads() {
    is_shareable::<AcknowledgedFence>();
    is_send::<AcknowledgedFenceWriter>();
    is_send::<RequestGuard>();
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
    assert_eq!(
        status(&early),
        (4, 0, 0, 0),
        "a refused raise closed nothing"
    );

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
    for request in KINDS {
        drop(unfenced.admit(0, request).expect("admitted under zero"));
    }
    moved_for_both(&unfenced, 0, 7);
}

/// The admission row, for each kind: the generation asked under against the acknowledged fence,
/// each way — the refusal names the kind and the generation asked — and a pending raise, which
/// refuses even the generation that would otherwise match, against the control that registers
/// exactly one request of its kind until its guard drops.
#[test]
fn admission_needs_the_acknowledged_fence_and_no_pending_raise() {
    for request in KINDS {
        let (mut writer, fence) = acknowledged_at(5);
        for asked in [4, 6, 0, u64::MAX] {
            assert_eq!(
                fence.admit(asked, request).unwrap_err(),
                AdmissionRefused::Moved {
                    acknowledged: 5,
                    requested: asked,
                    request
                },
                "{request} under {asked}"
            );
        }
        assert_eq!(
            status(&fence),
            (5, 0, 0, 0),
            "{request}: a refusal counts nothing"
        );

        let control = fence.admit(5, request).expect("the control");
        let second = fence
            .admit(5, request)
            .expect("admission counts, it does not exclude");
        let (deletions, reservations) = only(request, 2);
        assert_eq!(status(&fence), (5, deletions, reservations, 0), "{request}");
        drop((control, second));
        assert_eq!(status(&fence), (5, 0, 0, 0), "{request}");

        let Raise::Ready(ready) = writer.prepare(6, None) else {
            panic!("nothing in flight: one step")
        };
        // Not yet published: the close alone refuses every generation, of either kind.
        for asked in [5, 6] {
            rising_for_both(&fence, 5, asked);
        }
        assert_eq!(ready.publish(), 6);
        assert_eq!(status(&fence), (6, 0, 0, 0));
        moved_for_both(&fence, 6, 5);
        drop(
            fence
                .admit(6, request)
                .expect("open under the published fence"),
        );
    }
}

/// A guard names the kind and the generation it was admitted for — while it lives, the
/// acknowledged fence — and its drop takes one from its own kind's count and never the other's.
#[test]
fn each_kind_is_counted_apart_and_a_guard_names_what_it_was_admitted_for() {
    let (_writer, fence) = acknowledged_at(5);
    let deletion = fence.admit(5, FencedRequest::Deletion).expect("deletion");
    let reservation = fence
        .admit(5, FencedRequest::Reservation)
        .expect("reservation");
    let second = fence.admit(5, FencedRequest::Reservation).expect("another");
    assert_eq!(
        (deletion.request(), deletion.generation()),
        (FencedRequest::Deletion, 5)
    );
    assert_eq!(
        (reservation.request(), reservation.generation()),
        (FencedRequest::Reservation, 5)
    );
    assert_eq!(status(&fence), (5, 1, 2, 0));
    drop(deletion);
    assert_eq!(status(&fence), (5, 0, 2, 0));
    drop(reservation);
    assert_eq!(status(&fence), (5, 0, 1, 0));
    drop(second);
    assert_eq!(status(&fence), (5, 0, 0, 0));
}

/// The reviewer's case at the gate, for each kind: a request in flight when the raise closes
/// holds the publication back — the reader still sees the old fence, the guard's generation — and
/// nothing of either kind asked for after the close is admitted under any generation; once it
/// returns, the raise publishes, and only the new fence admits.
#[test]
fn a_request_of_either_kind_in_flight_holds_the_raise_back_and_none_is_admitted_after_the_close() {
    for request in KINDS {
        let (mut writer, fence) = acknowledged_at(3);
        let in_flight = fence.admit(3, request).expect("admitted before the close");

        let Raise::Drain(pending) = writer.prepare(4, None) else {
            panic!("a {request} is in flight")
        };
        assert_eq!(pending.target(), 4);
        let (deletions, reservations) = only(request, 1);
        assert_eq!(status(&fence), (3, deletions, reservations, 1), "{request}");
        for asked in [3, 4] {
            rising_for_both(&fence, 3, asked);
        }
        let pending = pending.try_drained().expect_err("still in flight");
        assert_eq!(
            fence.get(),
            in_flight.generation(),
            "{request}: not published"
        );

        drop(in_flight);
        assert_eq!(status(&fence), (3, 0, 0, 1), "drained, still closed");
        rising_for_both(&fence, 3, 3);
        let drained = pending.try_drained().expect("drained");
        let Raise::Ready(ready) = writer.prepare(4, Some(drained)) else {
            panic!("drained for exactly 4")
        };
        assert_eq!(ready.publish(), 4);
        assert_eq!(status(&fence), (4, 0, 0, 0));
        moved_for_both(&fence, 4, 3);
        for other in KINDS {
            drop(fence.admit(4, other).expect("open under 4"));
        }
    }
}

/// `is_drained` needs both counts at zero: with one request of each kind in flight, releasing
/// either leaves the raise pending, in either order, and the asynchronous drain wakes only for
/// the last.
#[tokio::test]
async fn a_raise_waits_for_both_kinds() {
    for deletion_first in [true, false] {
        let label = format!("deletion first: {deletion_first}");
        let (mut writer, fence) = acknowledged_at(2);
        let deletion = fence.admit(2, FencedRequest::Deletion).expect("deletion");
        let reservation = fence
            .admit(2, FencedRequest::Reservation)
            .expect("reservation");
        let Raise::Drain(pending) = writer.prepare(3, None) else {
            panic!("both in flight")
        };
        let mut draining = std::pin::pin!(pending.drained());
        assert!(poll_once(draining.as_mut()).is_pending(), "{label}");
        let (remaining, left) = if deletion_first {
            drop(deletion);
            (reservation, (2, 0, 1, 1))
        } else {
            drop(reservation);
            (deletion, (2, 1, 0, 1))
        };
        assert_eq!(status(&fence), left, "{label}");
        assert!(
            poll_once(draining.as_mut()).is_pending(),
            "{label}: the other kind is in flight"
        );
        drop(remaining);
        let drained = tokio::time::timeout(FOREVER, draining)
            .await
            .expect("woken by the last release");
        let Raise::Ready(ready) = writer.prepare(3, Some(drained)) else {
            panic!("drained for 3")
        };
        assert_eq!(ready.publish(), 3, "{label}");
    }
}

/// For each kind: a guard dropped by an unwinding requester, and one owned by a future that is
/// cancelled, both deregister; one that is never dropped keeps the raise waiting — fail-closed —
/// and stays counted under its kind.
#[tokio::test]
async fn a_guard_deregisters_on_unwind_and_cancellation_and_a_leaked_one_keeps_the_raise_waiting() {
    for request in KINDS {
        let (mut writer, fence) = acknowledged_at(2);

        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let _guard = fence.admit(2, request).expect("admitted");
            panic!("the store call unwound");
        }));
        assert!(unwound.is_err());
        assert_eq!(
            status(&fence),
            (2, 0, 0, 0),
            "{request}: deregistered by the unwind"
        );

        let reader = fence.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let requesting = tokio::spawn(async move {
            let _guard = reader.admit(2, request).expect("admitted");
            entered_tx.send(()).expect("the test waits");
            std::future::pending::<()>().await;
        });
        entered_rx.await.expect("the requester holds its guard");
        let (deletions, reservations) = only(request, 1);
        assert_eq!(status(&fence), (2, deletions, reservations, 0), "{request}");
        requesting.abort();
        assert!(requesting.await.expect_err("aborted").is_cancelled());
        assert_eq!(
            status(&fence),
            (2, 0, 0, 0),
            "{request}: deregistered by the cancellation"
        );

        std::mem::forget(fence.admit(2, request).expect("admitted"));
        let Raise::Drain(pending) = writer.prepare(3, None) else {
            panic!("the leaked guard is in flight")
        };
        let pending = pending
            .wait_for(Duration::from_millis(1))
            .await
            .expect_err("a leaked guard never drains");
        assert_eq!(fence.get(), 2, "{request}: and nothing is acknowledged");
        assert_eq!(fence.gate_status().long_drain_waits, 1);
        drop(pending);
        assert_eq!(
            status(&fence),
            (2, deletions, reservations, 0),
            "{request}: abandoned — open again, the leak still counted under its kind"
        );
    }
}

/// Every refusal, for every kind, renders its whole sentence — an exhaustive table, so a new
/// variant or kind fails to compile here until its sentence is pinned.
#[test]
fn every_refusal_renders_its_whole_sentence() {
    for request in KINDS {
        let word = match request {
            FencedRequest::Deletion => "deletion",
            FencedRequest::Reservation => "reservation",
        };
        assert_eq!(request.to_string(), word);
        let rows = [
            (
                AdmissionRefused::Rising {
                    acknowledged: 4,
                    requested: 3,
                    request,
                },
                format!(
                    "a {word} under ownership generation 3 was refused: a raise of the \
                     acknowledged lifecycle fence 4 is draining its in-flight requests"
                ),
            ),
            (
                AdmissionRefused::Moved {
                    acknowledged: 5,
                    requested: 4,
                    request,
                },
                format!(
                    "a {word} under ownership generation 4 was refused: the acknowledged \
                     lifecycle fence is 5"
                ),
            ),
        ];
        for (refusal, sentence) in rows {
            match refusal {
                AdmissionRefused::Rising { .. } | AdmissionRefused::Moved { .. } => {
                    assert_eq!(refusal.to_string(), sentence);
                }
            }
        }
    }
}
