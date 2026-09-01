//! Status-publication funnel tests (M11.T26b/M11.T26h, design M11.D39d).
//!
//! The funnel used to make one choice between two write forms; M11.T26h's activation change
//! removed the unconditional one, so what these run is the single conditional write against the
//! schema the SQLite migrations produce. The fixture is [`super::fence_tests`]'s, shared rather
//! than copied: the properties are properties of the columns.

use super::LifecycleMode;
use super::fence::LifecycleAuthority;
use super::fence_tests::{JOB, adopt, cold_status, migrated_job, stored_authority, stored_state};
use super::publication::{StatusPublication, publish_status};

/// A status carrying a *wrong* authority: one no read of this row ever produced.
///
/// This is what the conditional write refuses, and refusing it is the whole reason the
/// unconditional write — which named only the job's id, and so could not have refused it — is
/// gone.
fn with_a_wrong_authority(status: &mut crate::JobStatus) {
    status.authority = LifecycleAuthority::from_parts(JOB, 999, "an epoch nobody installed");
}

/// A status whose authority the row does not carry is refused, and the row is left exactly as
/// it was.
///
/// This is the successor to `the_legacy_arm_performs_the_landed_unconditional_write` and
/// `the_fenced_arm_refuses_exactly_what_the_legacy_arm_publishes`, which asserted the same
/// status reaching opposite outcomes through the two arms. With one arm left there is one
/// outcome, and it is the refusing one: a superseded controller publishes nothing.
#[tokio::test]
async fn a_status_whose_authority_the_row_does_not_carry_is_refused() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;
    let held = status.authority().clone();
    let (fence_before, epoch_before) = stored_authority(&connection);

    with_a_wrong_authority(&mut status);
    status.state = "Stopped".to_string();
    let outcome = publish_status(&status, &database)
        .await
        .expect("a refused conditional write is an outcome, not a failure");
    let StatusPublication::Superseded(stale) = outcome else {
        panic!("the one write form must refuse an authority the row does not carry");
    };
    assert_eq!(stale.job_id, JOB);
    assert_eq!(stale.operation, "publish the job's status");
    assert_eq!(stale.presented_fence.get(), 999);
    assert_eq!(stale.presented_epoch, "an epoch nobody installed");
    assert_eq!(
        stored_state(&connection),
        "Running",
        "a refused conditional write must leave the row exactly as it was"
    );
    assert_eq!(
        stored_authority(&connection),
        (fence_before, epoch_before),
        "including the authority columns, which a status publication never writes"
    );

    // The control: the authority the row does carry publishes through the same call.
    status.authority = held;
    assert!(matches!(
        publish_status(&status, &database).await,
        Ok(StatusPublication::Published)
    ));
    assert_eq!(stored_state(&connection), "Stopped");
}

/// Zero updated rows is a *value*, not an error: a job another controller holds and a job whose
/// row is gone reach the caller the same way, and standing down is the safe answer to both.
///
/// Before M11.T26h the two were distinguishable, because the unconditional write answered a
/// missing row with `Err("Job status does not exist")`. The conditional statement cannot tell
/// them apart — the predicate names the id *and* the authority — so it does not pretend to.
#[tokio::test]
async fn zero_rows_is_an_outcome_and_not_a_failure() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;

    connection
        .lock()
        .unwrap()
        .execute("DELETE FROM job_statuses WHERE id = ?1", [JOB])
        .expect("the fixture must be editable");

    let outcome = publish_status(&status, &database)
        .await
        .expect("zero rows is an outcome on the conditional write");
    assert!(matches!(outcome, StatusPublication::Superseded(_)));
}

/// A write the database could not perform is an error, and never a superseded authority.
///
/// The other side of `zero_rows_is_an_outcome_and_not_a_failure`, and the pair is the whole
/// contract of the funnel: a *refused* write means somebody else holds the job and this
/// controller stops administering it for good; a write that could not be performed means
/// nothing about who holds the job and the caller retries. Collapsing the two — by answering
/// `Superseded` when the statement fails — would stand a controller down from every job it
/// administers the first time its database blinked.
///
/// The whole table is gone rather than the row, because deleting the row is the case above: the
/// conditional statement runs and matches nothing. A missing table is the statement itself
/// failing.
#[tokio::test]
async fn a_write_the_database_refuses_is_an_error_and_never_a_superseded_authority() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;

    connection
        .lock()
        .unwrap()
        .execute_batch("DROP TABLE job_statuses;")
        .expect("the fixture must be editable");

    let error = publish_status(&status, &database)
        .await
        .expect_err("a statement the database refuses is a failure, not a lost duel");
    assert!(
        matches!(
            error,
            crate::AuthorityWriteError::Database {
                ref job_id,
                operation: "publish the job's status",
                ..
            } if job_id == JOB
        ),
        "{error}"
    );
}

/// Every authority a status can present is answered, and only the one the row carries publishes.
///
/// The successor to `the_legacy_arm_never_reports_a_superseded_authority`, inverted: what that
/// row asserted was that the *unconditional* write ignored all four of these, which is exactly
/// the property M11.T26h removed. Each of the three wrong ones is now refused, and the right one
/// is the control.
#[tokio::test]
async fn only_the_authority_the_row_carries_publishes() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;
    let held = status.authority().clone();

    for (case, authority) in [
        (
            "an authority no read produced",
            LifecycleAuthority::from_parts(JOB, 999, "elsewhere"),
        ),
        (
            "the unadopted authority",
            LifecycleAuthority::unadopted(JOB),
        ),
        (
            "an authority over another job",
            LifecycleAuthority::from_parts("job_xyz", held.fence().get(), held.epoch()),
        ),
    ] {
        status.authority = authority;
        status.state = format!("Refused-{case}");
        match publish_status(&status, &database).await {
            Ok(StatusPublication::Superseded(_)) => {}
            Ok(StatusPublication::Published) => {
                panic!("{case}: this authority must not be able to publish")
            }
            Err(e) => panic!("{case}: the write must be performable: {e}"),
        }
        assert_eq!(
            stored_state(&connection),
            "Running",
            "{case}: and the row is untouched"
        );
    }

    status.authority = held;
    status.state = "Published".to_string();
    assert!(matches!(
        publish_status(&status, &database).await,
        Ok(StatusPublication::Published)
    ));
    assert_eq!(stored_state(&connection), "Published");
}

/// There is one write form, and it is the conditional one — asserted against the funnel rather
/// than against a source count.
///
/// The successor to `every_mode_publishes_through_one_of_the_two_write_forms`. That row
/// quantified over `LifecycleMode::ALL` because the funnel took a mode; this one asserts the
/// stronger fact the activation change produced — the mode is gone, so no mode can select a
/// write that ignores the authority, and the same status behaves identically however the job
/// that holds it was built.
#[tokio::test]
async fn there_is_one_write_form_and_no_mode_can_choose_another() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;

    for round in 0..LifecycleMode::ALL.len() {
        status.state = format!("Published-{round}");
        let outcome = publish_status(&status, &database)
            .await
            .unwrap_or_else(|e| panic!("the write must be performable: {e}"));
        assert!(
            matches!(outcome, StatusPublication::Published),
            "a status published under the authority its row carries must land"
        );
        assert_eq!(stored_state(&connection), format!("Published-{round}"));
    }

    with_a_wrong_authority(&mut status);
    status.state = "Never".to_string();
    assert!(
        matches!(
            publish_status(&status, &database).await,
            Ok(StatusPublication::Superseded(_))
        ),
        "and there is no second form for a wrong authority to reach"
    );
    assert_ne!(stored_state(&connection), "Never");
}
