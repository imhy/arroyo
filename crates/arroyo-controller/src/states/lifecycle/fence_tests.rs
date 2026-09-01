//! Durable execution authority tests (M11.T26b, design M11.D39d).
//!
//! Everything here runs against the schema the SQLite migrations actually produce — the same
//! files `build.rs` feeds to cornucopia — rather than against a fixture that mirrors it. That
//! is deliberate: the properties under test are properties of the *columns* (their defaults,
//! their behaviour under a conditional predicate), and a hand-written fixture is a second
//! opinion about them that cannot fail when a migration changes.

use std::sync::{Arc, Mutex};

use cornucopia_async::DatabaseSource;
use cornucopia_async::rusqlite::Connection;

use super::root::RootCandidate;
use super::root_tests::{CandidateStore, EPOCH, GENERATION, candidate_for, validated_metadata};
use crate::queries::controller_queries;
use crate::states::lifecycle::fence::{
    AuthorityOutcome, LifecycleAuthority, LifecycleFence, MalformedAuthority,
};
use crate::states::scheduling::START_EXECUTION_RECONCILE_ATTEMPTS;
use crate::states::scheduling::fanout::IssuedAttempts;
use arroyo_rpc::fencing::MAX_ATTEMPT_ID_CHARS;
use arroyo_rpc::metadata_root::{MAX_CONTROLLER_EPOCH_CHARS, MetadataRoot};
use arroyo_types::WorkerId;

const SQLITE_MIGRATIONS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../arroyo-api/sqlite_migrations"
);

const POSTGRES_FENCE_MIGRATION: &str =
    include_str!("../../../../arroyo-api/migrations/V34__add_job_status_lifecycle_fence.sql");
const SQLITE_FENCE_MIGRATION: &str = include_str!(
    "../../../../arroyo-api/sqlite_migrations/V12__add_job_status_lifecycle_fence.sql"
);

/// The migration that introduced the fence and epoch columns.
const FENCE_MIGRATION: u32 = 12;

pub(super) const JOB: &str = "job_abc";

/// The `job_configs`/`job_statuses`/`pipelines` rows the controller's poll reads, inserted
/// with only the columns that have no default so that every defaulted column is exercised as
/// an upgraded deployment would have it.
const FIXTURE: &str = "\
    INSERT INTO pipelines (id, organization_id, created_by, name, type, pub_id, textual_repr, program)
        VALUES (1, 'org', 'user', 'pipeline', 'sql', 'pl_1', 'select 1', x'');
    INSERT INTO job_configs (id, organization_id, pipeline_name, created_by, pipeline_id)
        VALUES ('job_abc', 'org', 'pipeline', 'user', 1);
    INSERT INTO job_statuses (id, organization_id, pub_id, state)
        VALUES ('job_abc', 'org', 'js_1', 'Running');";

/// Runs every SQLite migration up to and including `through`.
///
/// Refinery records what it has applied, so calling this twice with a higher bound is exactly
/// an upgrade of an existing deployment: the rows inserted between the two calls are rows that
/// already existed when the second migration ran.
fn migrate_through(connection: &mut Connection, through: u32) {
    let mut migrations = refinery::load_sql_migrations(SQLITE_MIGRATIONS)
        .expect("the SQLite migrations must be loadable");
    migrations.sort_by_key(|m| m.version());
    migrations.retain(|m| m.version() <= through);
    refinery::Runner::new(&migrations)
        .run(connection)
        .unwrap_or_else(|e| panic!("migrating through V{through} must succeed: {e}"));
}

/// A fully migrated database holding one running job, and a handle on the same connection for
/// the raw assertions that read columns no query surface exposes.
///
/// Shared with [`super::publication_tests`] and [`super::root_tests`] rather than copied: the
/// properties under test are properties of the columns the migrations produce, and a second
/// fixture is a second opinion about them that cannot fail when a migration changes.
pub(super) fn migrated_job() -> (DatabaseSource, Arc<Mutex<Connection>>) {
    migrated_job_named(JOB)
}

/// The same, for one job named `job_id`.
///
/// M11.T26f's rows need it because the fencing metrics are labelled by job id and the Prometheus
/// registry is process-wide: a row asserting a gauge for `job_abc` would be reading a series
/// every other row in this binary was also writing, at up to sixteen threads. A job of its own
/// is what makes those assertions closed-form.
pub(super) fn migrated_job_named(job_id: &str) -> (DatabaseSource, Arc<Mutex<Connection>>) {
    let mut connection = Connection::open_in_memory().expect("an in-memory database");
    migrate_through(&mut connection, u32::MAX);
    connection
        .execute_batch(&FIXTURE.replace(JOB, job_id))
        .expect("the fixture rows must insert");
    let shared = Arc::new(Mutex::new(connection));
    (DatabaseSource::Sqlite(Arc::clone(&shared)), shared)
}

/// The two authority columns as the row holds them.
pub(super) fn stored_authority(connection: &Mutex<Connection>) -> (i64, String) {
    connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT lifecycle_fence, controller_epoch FROM job_statuses WHERE id = ?1",
            [JOB],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("the job's row must be readable")
}

/// One column of the job's status, for the writes that must and must not land.
pub(super) fn stored_state(connection: &Mutex<Connection>) -> String {
    connection
        .lock()
        .unwrap()
        .query_row("SELECT state FROM job_statuses WHERE id = ?1", [JOB], |r| {
            r.get(0)
        })
        .expect("the job's row must be readable")
}

/// The job as the controller's poll sees it: through `all_jobs`, then through the same
/// per-row classification the update thread runs.
pub(super) async fn polled(database: &DatabaseSource) -> (crate::PolledJob, crate::JobStatus) {
    let client = database.client().await.expect("a database handle");
    let mut jobs = controller_queries::fetch_all_jobs(&client)
        .await
        .expect("all_jobs must run against the migrated schema");
    assert_eq!(jobs.len(), 1, "the fixture holds exactly one job");
    crate::classify_polled_job(jobs.remove(0)).expect("the fixture job must classify")
}

/// The job's status with the authority its row currently carries — a controller that has just
/// started and read the row for the first time.
pub(super) async fn cold_status(database: &DatabaseSource) -> crate::JobStatus {
    polled(database).await.1
}

/// An uncontested adoption, asserted rather than unwrapped: the outcome is `#[must_use]`
/// precisely because ignoring it is the mistake, and a test that ignored it would be
/// modelling the mistake.
pub(super) async fn adopt(status: &mut crate::JobStatus, database: &DatabaseSource) {
    assert_eq!(
        status.adopt_lifecycle_authority(database).await,
        Ok(AuthorityOutcome::Applied(())),
        "an uncontested adoption must be applied"
    );
}

/// M11.T26n's first clause, on the upgrade path it is actually about: a `job_statuses` row
/// that existed *before* V12 ran takes the column defaults, and those defaults are the
/// authority no controller holds.
///
/// The row is inserted between the two migration runs, so it is not a row the new migration
/// created — which is the only version of this property that says anything about a deployment
/// that already has jobs.
#[test]
fn a_row_written_before_the_migration_takes_the_unadopted_authority() {
    let mut connection = Connection::open_in_memory().expect("an in-memory database");
    migrate_through(&mut connection, FENCE_MIGRATION - 1);
    connection
        .execute_batch(FIXTURE)
        .expect("the fixture rows must insert");
    assert!(
        connection
            .query_row(
                "SELECT lifecycle_fence FROM job_statuses WHERE id = ?1",
                [JOB],
                |r| r.get::<_, i64>(0)
            )
            .is_err(),
        "the columns must not exist before the migration that adds them, or this test is \
         asserting nothing about an upgrade"
    );

    migrate_through(&mut connection, u32::MAX);

    let (fence, epoch) = stored_authority(&Mutex::new(connection));
    assert_eq!(fence, 0, "a pre-existing row takes the fence default");
    assert_eq!(epoch, "", "a pre-existing row takes the epoch default");
}

/// The same defaults on the other backend, pinned against the SQL rather than executed: there
/// is no in-process PostgreSQL here, and a column added to one backend with a different
/// default from the other is exactly how a fence means one thing on a deployment and something
/// else on the next.
#[test]
fn both_migrations_add_the_same_columns_with_the_same_defaults() {
    for (name, migration, fence_type) in [
        ("V34 (PostgreSQL)", POSTGRES_FENCE_MIGRATION, "BIGINT"),
        ("V12 (SQLite)", SQLITE_FENCE_MIGRATION, "INTEGER"),
    ] {
        assert_eq!(
            migration
                .matches(&format!(
                    "ALTER TABLE job_statuses ADD COLUMN lifecycle_fence {fence_type} NOT NULL \
                     DEFAULT 0;"
                ))
                .count(),
            1,
            "{name} must add the fence to job_statuses, not null, defaulting to 0"
        );
        assert_eq!(
            migration
                .matches(
                    "ALTER TABLE job_statuses ADD COLUMN controller_epoch TEXT NOT NULL DEFAULT '';"
                )
                .count(),
            1,
            "{name} must add the epoch to job_statuses, not null, defaulting to the empty string"
        );
    }
}

/// Cold adoption: the fence rises by exactly one, the epoch becomes one no row had, and the
/// authority the call returns is the one the row now holds — so a controller never has to
/// re-read to learn what it may present.
#[tokio::test]
async fn adoption_raises_the_fence_by_one_and_installs_a_fresh_epoch() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    assert_eq!(status.authority().fence(), LifecycleFence::UNADOPTED);
    assert_eq!(status.authority().epoch(), "");

    let mut previous = status.authority().fence();
    for expected in 1..=3u64 {
        assert_eq!(
            status.adopt_lifecycle_authority(&database).await,
            Ok(AuthorityOutcome::Applied(())),
            "an uncontested adoption must be applied"
        );
        assert_eq!(status.authority().fence().get(), expected);
        assert!(
            status.authority().fence() > previous,
            "the fence is monotonic across adoptions"
        );
        previous = status.authority().fence();

        let (fence, epoch) = stored_authority(&connection);
        assert_eq!(fence, expected as i64, "the row carries what was installed");
        assert_eq!(
            epoch,
            status.authority().epoch(),
            "and the epoch the caller holds is the one in the row"
        );
        assert!(
            !epoch.is_empty() && epoch.chars().all(|c| c.is_ascii_hexdigit()),
            "a minted epoch is hexadecimal, and is never the empty value an unadopted row \
             carries: {epoch:?}"
        );
    }
}

/// The fence duel, at its smallest: two controllers read the same row, and exactly one of them
/// adopts it. The loser is told so as a value it must handle, and the row it lost still carries
/// the winner's authority rather than a merge of the two.
#[tokio::test]
async fn only_one_of_two_controllers_reading_the_same_row_adopts_it() {
    let (database, connection) = migrated_job();
    let mut winner = cold_status(&database).await;
    let mut loser = cold_status(&database).await;
    assert_eq!(
        winner.authority(),
        loser.authority(),
        "both controllers must start from the same read, or this proves nothing"
    );

    assert_eq!(
        winner.adopt_lifecycle_authority(&database).await,
        Ok(AuthorityOutcome::Applied(()))
    );

    let held_before = loser.authority().clone();
    let outcome = loser
        .adopt_lifecycle_authority(&database)
        .await
        .expect("losing a duel is an outcome, not a failure");
    let AuthorityOutcome::Stale(stale) = outcome else {
        panic!("the second adoption of one row must be refused");
    };
    assert_eq!(stale.job_id, JOB);
    assert_eq!(stale.operation, "adopt the job's lifecycle authority");
    assert_eq!(stale.presented_fence, LifecycleFence::UNADOPTED);
    assert_eq!(stale.presented_epoch, "");
    assert_eq!(
        loser.authority(),
        &held_before,
        "a controller that lost must not install the authority it would have taken"
    );

    let (fence, epoch) = stored_authority(&connection);
    assert_eq!(fence, 1, "exactly one adoption reached the row");
    assert_eq!(epoch, winner.authority().epoch());
}

/// The authority is a pair, and a write must match both halves of it. Each row below breaks
/// the agreement in a different way — including the two that are individually right and
/// together describe no read that ever happened.
#[tokio::test]
async fn a_status_write_is_refused_unless_the_fence_and_the_epoch_both_match() {
    let (database, connection) = migrated_job();

    // Two adoptions, so that there are two real authorities to cross-breed.
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;
    let first = status.authority().clone();
    adopt(&mut status, &database).await;
    let current = status.authority().clone();
    assert_ne!(first.fence(), current.fence());
    assert_ne!(first.epoch(), current.epoch());

    fn published(status: &mut crate::JobStatus, authority: LifecycleAuthority, state: &str) {
        status.authority = authority;
        status.state = state.to_string();
    }

    for (case, authority) in [
        (
            "a stale fence with the current epoch",
            LifecycleAuthority::from_parts(JOB, first.fence().get(), current.epoch()),
        ),
        (
            "the current fence with a stale epoch",
            LifecycleAuthority::from_parts(JOB, current.fence().get(), first.epoch()),
        ),
        ("both halves, each from a different read", first.clone()),
        (
            "an authority for another job entirely",
            LifecycleAuthority::from_parts("job_xyz", current.fence().get(), current.epoch()),
        ),
    ] {
        published(&mut status, authority.clone(), "Failed");
        let outcome = status
            .update_db_under_authority(&database)
            .await
            .unwrap_or_else(|e| panic!("{case}: a refused write is an outcome, not an error: {e}"));
        let AuthorityOutcome::Stale(stale) = outcome else {
            panic!("{case}: the write must be refused");
        };
        assert_eq!(stale.operation, "publish the job's status");
        assert_eq!(stale.presented_fence, authority.fence(), "{case}");
        assert_eq!(stale.presented_epoch, authority.epoch(), "{case}");
        assert_eq!(
            stored_state(&connection),
            "Running",
            "{case}: a refused write must leave the row exactly as it was"
        );
    }

    // The control, through the same call: the authority the row actually carries publishes.
    published(&mut status, current.clone(), "Stopped");
    assert_eq!(
        status.update_db_under_authority(&database).await,
        Ok(AuthorityOutcome::Applied(()))
    );
    assert_eq!(stored_state(&connection), "Stopped");
}

/// A controller restart recovers the authority from the row, and only from the row. The
/// authority the *previous* controller held before it adopted is refused afterwards, which is
/// what makes a restarted process unable to publish under a fence it has already superseded.
#[tokio::test]
async fn an_adopted_authority_is_recovered_by_re_reading_the_row() {
    let (database, connection) = migrated_job();
    let before_adoption = cold_status(&database).await.authority().clone();

    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;
    let adopted = status.authority().clone();
    drop(status);

    // The restart: nothing survives but the row.
    let mut restarted = cold_status(&database).await;
    assert_eq!(
        restarted.authority(),
        &adopted,
        "a restarted controller reads back exactly what the adoption installed"
    );

    restarted.state = "Stopped".to_string();
    assert_eq!(
        restarted.update_db_under_authority(&database).await,
        Ok(AuthorityOutcome::Applied(()))
    );
    assert_eq!(stored_state(&connection), "Stopped");

    restarted.authority = before_adoption;
    restarted.state = "Failed".to_string();
    assert!(
        matches!(
            restarted.update_db_under_authority(&database).await,
            Ok(AuthorityOutcome::Stale(_))
        ),
        "the pre-adoption authority must be permanently unusable"
    );
    assert_eq!(stored_state(&connection), "Stopped");
}

/// M11.D96 row 15, first test: the job's selector authority is its durable record, and
/// adoption does not become a second way for the editable `job_configs` row to become one.
///
/// Adoption writes the two authority columns and nothing else, so the selector still comes
/// from `state_context` and is still refused when the configuration row disagrees (M11.T08d)
/// — before the adoption and after it. Row 15's other test,
/// `fence_duel_installs_exactly_one_authoritative_root`, is below.
#[tokio::test]
async fn selector_authority_is_durable_record() {
    let (database, connection) = migrated_job();
    connection
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE job_configs SET state_backend = 'parquet' WHERE id = 'job_abc';
             UPDATE job_statuses
                SET state_context = '{\"version\": 1, \"execution_selector\": \"stateengine\"}'
              WHERE id = 'job_abc';",
        )
        .expect("the fixture must be editable");

    let (before, _) = polled(&database).await;
    assert_eq!(
        before.execution_selector,
        arroyo_rpc::state_backend::StateBackendSelector::StateEngine
    );

    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;

    let (after, status_after) = polled(&database).await;
    assert_eq!(
        after.execution_selector,
        arroyo_rpc::state_backend::StateBackendSelector::StateEngine,
        "adoption must not move the job onto the backend its configuration row now names"
    );
    assert!(
        after.refusal.is_some(),
        "and the row's disagreement must still be refused rather than adopted"
    );
    assert_eq!(
        status_after.state_context.execution_selector.as_deref(),
        Some("stateengine"),
        "the durable record itself is untouched by adoption"
    );
    assert_eq!(status_after.state_context.fencing, None);
}

/// A fence this build cannot interpret skips the job it belongs to, and only that job — the
/// same fail-closed answer an unusable execution record gets, for the same reason.
#[tokio::test]
async fn a_negative_lifecycle_fence_skips_only_that_job() {
    let (database, connection) = migrated_job();
    connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE job_statuses SET lifecycle_fence = -1 WHERE id = ?1",
            [JOB],
        )
        .expect("the fixture must be editable");

    let client = database.client().await.unwrap();
    let mut jobs = controller_queries::fetch_all_jobs(&client).await.unwrap();
    let row = jobs.remove(0);
    assert_eq!(
        LifecycleAuthority::observed(&row),
        Err(MalformedAuthority {
            job_id: JOB.to_string(),
            fence: -1,
        })
    );
    assert!(
        crate::classify_polled_job(row).is_none(),
        "a job whose authority cannot be interpreted must be skipped, not guessed at"
    );
}

/// The two conditions that are *not* a stale authority. Both are retryable and neither says
/// anything about who holds the job, which is the whole reason they are a different type from
/// [`AuthorityOutcome::Stale`]: a controller that retried a stale authority would be a
/// superseded controller retrying to overwrite a live one.
#[tokio::test]
async fn a_database_failure_and_an_exhausted_fence_are_errors_rather_than_stale_authorities() {
    let (database, connection) = migrated_job();
    let status = cold_status(&database).await;

    // A fence that cannot be raised without leaving the column's range. Unreachable through
    // adoption — which only ever stores what it read plus one — and therefore checked rather
    // than asserted: a panic here would be an availability bug, not a caught mistake.
    let exhausted = LifecycleAuthority::from_parts(JOB, u64::MAX, status.authority().epoch());
    assert_eq!(
        exhausted.adopt(&database).await,
        Err(crate::AuthorityWriteError::Exhausted {
            job_id: JOB.to_string(),
        })
    );

    // The same fence presented to the *status* write, which converts it separately. Both
    // conversions are checked because they are two statements: a status write that wrapped a
    // fence the adoption refused would publish under an authority no row can hold.
    let mut unwritable = cold_status(&database).await;
    unwritable.authority =
        LifecycleAuthority::from_parts(JOB, u64::MAX, status.authority().epoch());
    assert_eq!(
        unwritable.update_db_under_authority(&database).await,
        Err(crate::AuthorityWriteError::Exhausted {
            job_id: JOB.to_string(),
        })
    );

    // And a row that is not there at all is the database's answer, not the job's.
    connection
        .lock()
        .unwrap()
        .execute_batch("DROP TABLE job_statuses;")
        .expect("the fixture must be editable");
    let error = status
        .update_db_under_authority(&database)
        .await
        .expect_err("a missing table is a failure, not a lost fence duel");
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

    // The same for the *first* write of a takeover. Adoption's own statement is the one place a
    // refused database could be mistaken for a lost duel with real consequences: a takeover that
    // read "no rows matched" would stand the controller down from a job nobody else holds.
    let error = status
        .authority()
        .adopt(&database)
        .await
        .expect_err("a missing table is a failure, not a lost fence duel");
    assert!(
        matches!(
            error,
            crate::AuthorityWriteError::Database {
                ref job_id,
                operation: "adopt the job's lifecycle authority",
                ..
            } if job_id == JOB
        ),
        "{error}"
    );
}

/// A database the controller cannot reach is a retryable failure, and never a lost duel.
///
/// The other arm of [`DatabaseSource`]. Every fixture above is SQLite, whose `client()` hands
/// back the connection it already holds and cannot fail; a Postgres pool pointed at a port
/// nothing listens on is the only way to reach the branch that names the failure. It matters
/// because the two answers have opposite consequences: `Stale` stands a controller down from a
/// job for good, and a controller that read "the database is unreachable" as "somebody else
/// holds this job" would abandon every job it administers the moment its pool went down.
#[tokio::test]
async fn a_database_the_controller_cannot_reach_is_an_error_and_never_a_lost_duel() {
    let mut pool_config = deadpool_postgres::Config::new();
    pool_config.host = Some("127.0.0.1".to_string());
    // Port 1 is reserved and nothing binds it, so the connection is refused rather than timing
    // out: the row asserts a classification, and waiting for a deadline to assert it would make
    // the row's duration a property of the network.
    pool_config.port = Some(1);
    pool_config.dbname = Some("arroyo".to_string());
    pool_config.user = Some("arroyo".to_string());
    let pool = pool_config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .expect("a pool that has not connected to anything yet");
    let unreachable = DatabaseSource::Postgres(pool);

    let authority = LifecycleAuthority::from_parts(JOB, 4, "epoch-4");
    // Matched rather than `expect_err`: a `Database` handle has no `Debug`, and the point of the
    // row is which of the two answers arrives.
    let error = match authority.client(&unreachable).await {
        Ok(_) => panic!("a database that cannot be reached must not hand back a client"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            crate::AuthorityWriteError::Database {
                ref job_id,
                operation: "reach the database",
                ..
            } if job_id == JOB
        ),
        "{error}"
    );
    assert!(
        authority.adopt(&unreachable).await.is_err(),
        "and the adoption that begins a takeover reports it rather than standing down"
    );
}

/// The top of the fence column's range: the last value that can be adopted, and the one that
/// cannot.
///
/// The neighbouring pair, because the refusal is about the boundary and not about large numbers.
/// `i64::MAX - 1` adopts, storing `i64::MAX`; presenting `i64::MAX` is refused *before* the
/// statement runs, so the row a controller cannot advance is a row it also cannot damage. The
/// column is `bigint`, so this is the point at which raising the fence would leave the range the
/// row can hold rather than a Rust-level overflow.
#[tokio::test]
async fn the_last_representable_fence_adopts_and_the_one_above_it_refuses_before_writing() {
    let (database, connection) = migrated_job();
    connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE job_statuses SET lifecycle_fence = ?1, controller_epoch = ?2 WHERE id = ?3",
            cornucopia_async::rusqlite::params![i64::MAX - 1, "penultimate", JOB],
        )
        .expect("the fixture row must be editable");

    let penultimate = LifecycleAuthority::from_parts(JOB, (i64::MAX - 1) as u64, "penultimate");
    let adopted = penultimate
        .adopt(&database)
        .await
        .expect("the last representable fence is adoptable")
        .applied()
        .expect("the row still carried the presented authority");
    assert_eq!(
        adopted.fence().get(),
        i64::MAX as u64,
        "adoption stores what it read plus one, and that value still fits the column"
    );
    assert_eq!(
        stored_authority(&connection).0,
        i64::MAX,
        "and the row carries it"
    );

    assert_eq!(
        adopted.adopt(&database).await,
        Err(crate::AuthorityWriteError::Exhausted {
            job_id: JOB.to_string(),
        }),
        "the successor of the last representable fence is not representable, so the adoption is \
         refused rather than wrapped"
    );
    assert_eq!(
        stored_authority(&connection),
        (i64::MAX, adopted.epoch().to_string()),
        "and the refused adoption wrote nothing: the row still carries the authority the \
         successful one installed"
    );
}

/// The whole classification of what a conditional statement did, over the only three answers a
/// row count can give.
///
/// The third arm is the one production cannot produce: `job_statuses.id` is the table's primary
/// key and every conditional statement in this module names it, so at most one row can match.
/// It is classified rather than assumed away because "one of the rows I updated was the one I
/// meant" is not something the caller could check afterwards, and a schema change that made it
/// possible must not read as success.
#[test]
fn a_row_count_the_primary_key_cannot_produce_is_ambiguous_rather_than_applied() {
    let authority = LifecycleAuthority::from_parts(JOB, 4, "epoch-4");
    assert_eq!(
        authority
            .outcome(1, "publish the job's status", || "written")
            .expect("one row is an outcome"),
        AuthorityOutcome::Applied("written"),
        "exactly one row is the write this authority asked for"
    );
    assert_eq!(
        authority
            .outcome(0, "publish the job's status", || unreachable!())
            .expect("zero rows is an outcome"),
        AuthorityOutcome::Stale(crate::StaleAuthority {
            job_id: JOB.to_string(),
            operation: "publish the job's status",
            presented_fence: authority.fence(),
            presented_epoch: "epoch-4".to_string(),
        }),
        "no row is another controller holding the job, which is an outcome and not an error"
    );
    assert_eq!(
        authority.outcome(2, "publish the job's status", || unreachable!()),
        Err(crate::AuthorityWriteError::Ambiguous {
            job_id: JOB.to_string(),
            operation: "publish the job's status",
            rows: 2,
        }),
        "more rows than the primary key can select is an error, and never a successful write"
    );
}

/// The convenience reading of an outcome, which discards *which* authority was refused but not
/// the fact that one was.
#[test]
fn an_applied_outcome_is_the_only_one_that_yields_a_value() {
    let authority = LifecycleAuthority::unadopted(JOB);
    assert_eq!(
        AuthorityOutcome::Applied(7u8).applied(),
        Some(7),
        "an applied write hands back what it produced"
    );
    let stale: AuthorityOutcome<u8> = authority
        .outcome(0, "publish the job's status", || unreachable!())
        .expect("zero rows is an outcome");
    assert_eq!(stale.applied(), None);
}

/// The derivation behind the durable record's capacity, pinned where it comes from: however
/// much of its reconcile budget a fan-out spends on one target, the ledger holds one issued
/// identifier for it. That is why the record's target capacity is also its identifier capacity
/// and there is no second bound to state.
#[test]
fn the_ledger_holds_one_identifier_per_target_however_much_budget_is_spent() {
    let mut issued = IssuedAttempts::default();
    let attempt_id = format!("{:016x}{:016x}", u64::MAX, 0u64);
    assert_eq!(
        attempt_id.chars().count(),
        MAX_ATTEMPT_ID_CHARS,
        "the durable record's identifier bound must fit the identifier the fan-out mints"
    );

    for _ in 0..=START_EXECUTION_RECONCILE_ATTEMPTS {
        issued.issued(WorkerId(7), attempt_id.clone());
    }
    assert_eq!(issued.issued_count(), 1);
    assert_eq!(issued.outstanding_count(), 1);

    issued.issued(WorkerId(8), attempt_id.clone());
    assert_eq!(
        issued.issued_count(),
        2,
        "a second target is a second entry; a replayed identifier is not"
    );
}

/// The status write is conditional on the job's durable authority, and has been since the
/// activation change (M11.T26b/M11.T26g/M11.T26h).
///
/// This row has been rewritten twice rather than replaced, because the requirement it carries
/// has not changed: **a job's status reaches its row through exactly one funnel, and which write
/// form that funnel performs cannot be changed by half.** M11.T26b's version counted direct
/// `status.update_db(` calls in each publishing state; M11.T26g's moved it up to the funnel and
/// made the mode-vs-arm relationship a *co-occurrence*; M11.T26h's — this one — is that same
/// co-occurrence read from the other side of the flag day.
///
/// **The co-occurrence, clause 4.** `FencedV2` is available in production if and only if the
/// funnel has no unconditional arm. Activating without removing the unconditional write fails
/// it, and removing the unconditional write without activating fails it. That is deliberately
/// one assertion rather than two: two independent clauses let a half-applied activation read as
/// a stale test, and one equality makes it read as a contradiction.
///
/// **What else it catches.**
///
/// * A *sixth* publishing state. This counts direct writes across the whole crate and finds
///   them wherever they are, rather than checking a list of files someone remembered to name.
/// * A state that publishes through the funnel and then ignores what it answered. Every
///   `Superseded` arm in production must call `stand_down(`, which is the difference between
///   acting on a lost authority and logging it.
/// * Adoption and candidate publication reaching a production path other than the M11.D39b
///   preamble.
///
/// The intended reading of a failure here is not "the test is stale" but "say which half of the
/// activation this change is, and where the other half is".
#[test]
fn the_production_status_write_is_conditional_since_the_activation_change() {
    // Every file that publishes a job's status, how many times, and the funnel that decides
    // how. `states/mod.rs` publishes twice: once at the state-machine boundary after every
    // transition, and once when a recovered job's state machine writes the state it came back
    // into — before its task, and therefore before any `JobContext`, exists.
    const PUBLISHING: [(&str, &str, usize); 5] = [
        ("states/mod.rs", include_str!("../mod.rs"), 2),
        ("states/scheduling.rs", include_str!("../scheduling.rs"), 1),
        ("states/running.rs", include_str!("../running.rs"), 1),
        (
            "states/leader_running.rs",
            include_str!("../leader_running.rs"),
            1,
        ),
        (
            "states/scheduling/admission.rs",
            include_str!("../scheduling/admission.rs"),
            1,
        ),
    ];
    let funnel = include_str!("publication.rs");

    // 1. One funnel. No publishing state names either write form; each reaches exactly one
    //    status publication, and it goes through `JobContext::publish_status`.
    for (name, source, publications) in PUBLISHING {
        let source = production_half(source);
        assert_eq!(
            source.matches("status.update_db(").count(),
            0,
            "{name} must not publish a status through an unconditional write: there is no \
             longer such a write, and re-adding one here would put a second write form back \
             outside the funnel"
        );
        assert_eq!(
            source.matches("update_db_under_authority(").count(),
            0,
            "{name} must not publish a status under a lifecycle authority directly either"
        );
        assert_eq!(
            source.matches(".publish_status(").count(),
            publications,
            "{name} publishes a status exactly {publications} time(s), through the funnel"
        );
    }

    // 2. The funnel holds both write forms and nothing else does. Counted over the whole
    //    crate, so a publishing state added later is found wherever it is put.
    assert_eq!(
        funnel.matches("status.update_db(").count(),
        0,
        "the funnel has no unconditional arm: M11.T26h removed it, and `JobStatus::update_db` \
         with it"
    );
    assert_eq!(
        funnel.matches("status.update_db_under_authority(").count(),
        1,
        "and the one write form it does perform is the conditional one"
    );
    for (name, kind, sites) in [
        (
            "an unconditional status write",
            "status.update_db(",
            Vec::<&str>::new(),
        ),
        (
            "the conditional write",
            "status.update_db_under_authority(",
            vec!["src/states/lifecycle/publication.rs"],
        ),
        (
            "lifecycle adoption",
            "status.adopt_lifecycle_authority(",
            vec!["src/states/scheduling/admission.rs"],
        ),
        (
            "candidate publication",
            "candidate.publish(",
            vec!["src/states/scheduling/admission/root.rs"],
        ),
        (
            "root installation",
            "status.install_metadata_root(",
            vec!["src/states/scheduling/admission/root.rs"],
        ),
    ] {
        assert_eq!(
            production_call_sites(kind),
            sites,
            "{name} has exactly these production call sites"
        );
    }

    // 3. Every state that learns it has been superseded acts on it, in the one way there is:
    //    one arm per publication, and one stand-down per arm. Counting both against the number
    //    of publications rather than against each other is what stops the equality from being
    //    satisfied by a file that has neither.
    for (name, source, publications) in PUBLISHING {
        let source = production_half(source);
        assert_eq!(
            source.matches("StatusPublication::Superseded(").count(),
            publications,
            "{name}: every one of its status publications answers the superseded outcome"
        );
        assert_eq!(
            source.matches("stand_down(").count(),
            publications,
            "{name}: and answers it by standing down, not by logging it and continuing"
        );
    }

    // 4. The co-occurrence. `FencedV2` is available in production if and only if the funnel's
    //    unconditional arm is gone — so neither half of M11.T26h's activation can stand alone,
    //    in either direction.
    let availability = function_body(
        include_str!("mode.rs"),
        "const fn is_available_in_production(self) -> bool {",
    );
    let activated = availability.contains("LifecycleMode::FencedV2 => true,");
    let legacy_arm_present = funnel.contains("status.update_db(");
    assert_eq!(
        activated, !legacy_arm_present,
        "activating `FencedV2` and removing the funnel's unconditional arm are one change. \
         Activated: {activated}; unconditional arm still present: {legacy_arm_present}"
    );
    // And the state this build is in, so that the equality above cannot be satisfied by neither
    // half having moved.
    assert!(
        activated && !legacy_arm_present,
        "M11.T26h has landed: `FencedV2` is what production selects, and the conditional write \
         is the only form a status can be published through"
    );
}

/// Everything in a file before its test module, so a mention inside a test does not count as a
/// production one.
fn production_half(source: &'static str) -> &'static str {
    match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    }
}

/// The body of a named function, by brace matching from its signature.
pub(crate) fn function_body(source: &'static str, signature: &str) -> String {
    let at = source
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` must exist"));
    let body = &source[at + signature.len()..];
    let mut depth = 1usize;
    for (i, c) in body.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return body[..i].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("`{signature}` has no closing brace")
}

/// Every production `.rs` file in this crate that contains `needle`, as a repository-relative
/// path, sorted.
///
/// Walks the crate's own source rather than a list of files, which is what makes clause 2 above
/// a statement about the whole crate instead of about the files someone remembered to name.
pub(super) fn production_call_sites(needle: &str) -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory)
            .expect("the crate's own source must be readable")
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            // Test modules are named `*_tests.rs` in this crate, and every other file's tests
            // are behind a `#[cfg(test)]` the halving above removes.
            if name.ends_with("_tests.rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("a readable source file");
            let production = match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source.as_str(),
            };
            if production.contains(needle) {
                found.push(format!(
                    "src/{}",
                    path.strip_prefix(&root).unwrap().to_string_lossy()
                ));
            }
        }
    }
    found.sort();
    found
}

/// M11.D96 row 15, second test: a fence duel installs exactly one authoritative root.
///
/// The sequence is the one M11.D39d is about. A controller adopts the job, publishes its
/// generation metadata as a candidate object, and is superseded by a replacement before it can
/// root it. The object store ends up with **two** immutable candidates — neither overwrote the
/// other, which is what the fence in the name is for — and the row names exactly one of them.
///
/// The loser's candidate is not cleaned up here, and that is the design: M11.D39d leaves it
/// unrooted for the grace collector, and
/// `stale_controller_candidate_never_becomes_authoritative_root` is what pins that it can never
/// become the root afterwards.
#[tokio::test]
async fn fence_duel_installs_exactly_one_authoritative_root() {
    let store = CandidateStore::new("duel");
    let provider = store.provider().await;
    let (database, _connection) = migrated_job();

    // The first controller adopts and publishes a candidate for its generation.
    let mut loser = cold_status(&database).await;
    loser.generation = GENERATION;
    adopt(&mut loser, &database).await;
    let loser_candidate = candidate_for(&loser);
    loser_candidate
        .publish(&provider)
        .await
        .expect("a candidate is published before it is rooted");

    // A replacement controller reads the row the first one left and adopts it in turn. This is
    // the duel: both hold an authority the row *did* carry, and only one of them still does.
    let mut winner = cold_status(&database).await;
    winner.generation = GENERATION;
    assert_eq!(
        winner.authority().fence(),
        loser.authority().fence(),
        "the replacement reads the authority the first controller installed"
    );
    adopt(&mut winner, &database).await;
    let winner_candidate = candidate_for(&winner);
    assert_ne!(
        winner_candidate.key(),
        loser_candidate.key(),
        "two controllers must not be able to write the same candidate object"
    );
    winner_candidate
        .publish(&provider)
        .await
        .expect("the replacement publishes its own");

    // Both try to root. Exactly one row update matches.
    assert_eq!(
        winner
            .install_metadata_root(&database, &winner_candidate)
            .await
            .expect("the winner's candidate agrees with its own status"),
        Ok(AuthorityOutcome::Applied(())),
        "the controller that holds the row installs the root"
    );
    let refused = loser
        .install_metadata_root(&database, &loser_candidate)
        .await
        .expect("the loser's candidate agrees with the authority the loser holds")
        .expect("losing the duel is an outcome, not a failure");
    assert!(
        matches!(refused, AuthorityOutcome::Stale(_)),
        "the superseded controller's root update must match no row"
    );

    // One root, and it is the winner's. Two candidates, and both are still there.
    let (_, rooted) = polled(&database).await;
    let root = rooted
        .state_context
        .metadata_root
        .expect("the row carries the root the winner installed");
    assert_eq!(root.object(), winner_candidate.key());
    assert_eq!(root.fence(), winner.authority().fence().get());
    assert_eq!(root.generation(), GENERATION);
    assert!(root.roots(&winner_candidate.key()));
    assert!(
        !root.roots(&loser_candidate.key()),
        "the loser's candidate is unrooted"
    );
    let mut expected = vec![winner_candidate.key(), loser_candidate.key()];
    expected.sort();
    assert_eq!(
        store.keys(),
        expected,
        "both candidates are still in the store: a losing controller leaves one behind for the \
         grace collector rather than deleting or replacing anything"
    );
    assert_eq!(
        loser.state_context.metadata_root, None,
        "and the losing controller does not go on believing it installed one"
    );
}

/// M11.D96 row 25: a stale controller's candidate never becomes the authoritative root.
///
/// Row 15 shows the duel resolving once. This shows that it stays resolved: the superseded
/// controller re-validates its metadata through M11.T25's `Validated<T>`, mints candidates
/// under every authority it can name, publishes each of them, and tries again. None reaches the
/// row, and the root the winner installed is the root throughout — so validation is *necessary
/// and not sufficient*, which is the whole reason M11.D39d puts the conditional update behind
/// the token rather than instead of it.
#[tokio::test]
async fn stale_controller_candidate_never_becomes_authoritative_root() {
    let store = CandidateStore::new("stale");
    let provider = store.provider().await;
    let (database, _connection) = migrated_job();

    let mut stale = cold_status(&database).await;
    stale.generation = GENERATION;
    adopt(&mut stale, &database).await;
    let held_before = stale.authority().clone();

    let mut winner = cold_status(&database).await;
    winner.generation = GENERATION;
    adopt(&mut winner, &database).await;
    let winner_candidate = candidate_for(&winner);
    winner_candidate
        .publish(&provider)
        .await
        .expect("the winner publishes");
    assert_eq!(
        winner
            .install_metadata_root(&database, &winner_candidate)
            .await
            .expect("the winner's own candidate"),
        Ok(AuthorityOutcome::Applied(()))
    );
    let installed = winner_candidate.key();

    // The stale controller, trying everything it can construct. Every row builds a real
    // `Validated<GenerationRoot>`: the metadata is perfectly valid in each of them.
    for (case, authority) in [
        ("the authority it held before the duel", held_before.clone()),
        (
            "the winner's fence, with its own epoch",
            LifecycleAuthority::from_parts(
                JOB,
                winner.authority().fence().get(),
                held_before.epoch(),
            ),
        ),
        (
            "the winner's epoch, with its own fence",
            LifecycleAuthority::from_parts(
                JOB,
                held_before.fence().get(),
                winner.authority().epoch(),
            ),
        ),
        (
            "a fence it guessed at",
            LifecycleAuthority::from_parts(JOB, 99, EPOCH),
        ),
    ] {
        stale.authority = authority;
        let candidate = candidate_for(&stale);
        assert_ne!(
            candidate.key(),
            installed,
            "{case}: a stale controller cannot even name the winner's object"
        );
        // Publishing again is allowed and harmless: the object is immutable and nobody points
        // at it.
        candidate
            .publish(&provider)
            .await
            .expect("a candidate may always be published");
        let outcome = stale
            .install_metadata_root(&database, &candidate)
            .await
            .unwrap_or_else(|e| panic!("{case}: the candidate agrees with the authority: {e}"))
            .unwrap_or_else(|e| panic!("{case}: a refused root is an outcome, not a failure: {e}"));
        assert!(
            matches!(outcome, AuthorityOutcome::Stale(_)),
            "{case}: a stale controller's candidate must not reach the row"
        );

        let (_, current) = polled(&database).await;
        assert_eq!(
            current
                .state_context
                .metadata_root
                .expect("the winner's root is still there")
                .object(),
            installed,
            "{case}: the root the winner installed must be untouched"
        );
    }

    // The unadopted authority cannot even name a candidate, so the loop above is not the whole
    // of what a stale controller can try: this is the other end of it.
    stale.authority = LifecycleAuthority::unadopted(JOB);
    assert!(
        RootCandidate::mint(stale.authority(), &validated_metadata()).is_err(),
        "an unadopted authority names no candidate at all"
    );

    // And every candidate that was published is still in the store, with exactly one of them
    // rooted.
    let keys = store.keys();
    assert!(
        keys.len() > 1,
        "the stale controller left candidates: {keys:?}"
    );
    let (_, final_row) = polled(&database).await;
    let root = final_row.state_context.metadata_root.expect("one root");
    assert_eq!(
        keys.iter().filter(|key| root.roots(key)).count(),
        1,
        "exactly one of the candidates in the store is the authoritative root: {keys:?}"
    );
}

/// The controller epoch an adoption mints fits the bound a metadata root is under.
///
/// The bound is stated in `arroyo-rpc` as an upper limit on what may be persisted, deliberately
/// not as a restatement of the minting format; this is the test that ties the two together, so
/// a change to `ControllerEpoch::fresh` that widened the value fails here rather than in a
/// candidate name nobody could write.
#[tokio::test]
async fn the_minted_controller_epoch_fits_the_metadata_root_bound() {
    let (database, _connection) = migrated_job();
    let mut status = cold_status(&database).await;
    for _ in 0..64 {
        adopt(&mut status, &database).await;
        let epoch = status.authority().epoch();
        assert_eq!(
            epoch.chars().count(),
            MAX_CONTROLLER_EPOCH_CHARS,
            "a minted epoch is exactly the bounded width: {epoch:?}"
        );
        assert!(
            epoch.bytes().all(|b| b.is_ascii_hexdigit()),
            "and is hexadecimal, which is what keeps it a single path segment: {epoch:?}"
        );
        assert!(
            MetadataRoot::mint("pl_1", JOB, 1, status.authority().fence().get(), epoch).is_ok(),
            "so every adoption can name a candidate: {epoch:?}"
        );
    }
}

/// A controller that loses the adoption causes no effect at all — not even the first one.
///
/// M11.D39d puts cold adoption before every effect, and this is the half of that which is about
/// the *losing* controller: after a refused adoption it still holds the authority it read, so
/// the very next thing the preamble would do — persist the raised generation — is refused too,
/// and the row is exactly as the winner left it.
#[tokio::test]
async fn a_controller_that_loses_the_adoption_causes_no_effect() {
    let (database, connection) = migrated_job();
    let mut winner = cold_status(&database).await;
    let mut loser = cold_status(&database).await;
    adopt(&mut winner, &database).await;

    let held_before = loser.authority().clone();
    assert!(
        matches!(
            loser.adopt_lifecycle_authority(&database).await,
            Ok(AuthorityOutcome::Stale(_))
        ),
        "the second adoption of one row is refused"
    );
    assert_eq!(
        loser.authority(),
        &held_before,
        "a refused adoption installs nothing, so the loser cannot present what it would have \
         held"
    );

    // The preamble's next effect, attempted under the authority the loser still holds.
    loser.generation += 1;
    loser.state = "Failed".to_string();
    assert!(
        matches!(
            loser.update_db_under_authority(&database).await,
            Ok(AuthorityOutcome::Stale(_))
        ),
        "and every write after a refused adoption is refused too"
    );
    assert_eq!(stored_state(&connection), "Running");
    let (fence, epoch) = stored_authority(&connection);
    assert_eq!(fence, winner.authority().fence().get() as i64);
    assert_eq!(epoch, winner.authority().epoch());
}

// ---------------------------------------------------------------------------------------------
// M11.T26h — the activation change, as one co-occurrence
// ---------------------------------------------------------------------------------------------

/// Every `.rs` file in this crate that contains `needle`, as a repository-relative path, sorted.
///
/// Unlike [`production_call_sites`] this does **not** skip test files or stop at a
/// `#[cfg(test)]`: what it is for is asking whether a definition still exists anywhere at all,
/// and a mechanism that lives on inside a test module is a mechanism that lives on.
pub(crate) fn crate_sources_containing(needle: &str) -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory)
            .expect("the crate's own source must be readable")
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            if std::fs::read_to_string(&path)
                .expect("a readable source file")
                .contains(needle)
            {
                found.push(format!(
                    "src/{}",
                    path.strip_prefix(&root).unwrap().to_string_lossy()
                ));
            }
        }
    }
    found.sort();
    found
}

/// **M11.T26h's structural test.** Selecting the fence and removing the M11.T08 mechanisms it
/// supersedes are one change, and this is the single equality that rejects either half alone.
///
/// # Why one assertion and not several
///
/// Two independent clauses — "the fence is selected" and "the guards are gone" — let a
/// half-applied activation satisfy one of them, and a reader then has a *passing* test beside a
/// failing one and no way to tell a stale expectation from a contradiction. An equality has no
/// such reading: `activated == superseded_and_removed` fails in both directions and its message
/// names which side moved. That is the idiom
/// `the_production_status_write_is_conditional_since_the_activation_change` established for the
/// publication funnel; this is the same idiom over the whole activation.
///
/// # What each side is
///
/// **Activated** is the exhaustive answer in `LifecycleMode::is_available_in_production`: both
/// arms, because "`FencedV2` is available" and "`LegacyT08` is not" are the two halves of one
/// selection and a build with both available would select the first in `ALL`, which is the
/// legacy one.
///
/// **Superseded and removed** is the three mechanisms M11.D75 and M11.D39b name, asked for by
/// *definition* rather than by mention — the prose in this crate deliberately still describes
/// them in the past tense, and describing a removed thing is not keeping it:
///
/// * the M11.T08 refusal-gate quartet — `admit_publication`, `admit_scheduling`, `publish`,
///   `withdraw` — and the two per-task methods that made "applied once" true, `take` and
///   `disarm`, together with the type that held them;
/// * `settle_under_admission`, the cancellation rescue, and the `SettlementRescue` /
///   `SettlingUnderAdmission` machinery it needed;
/// * the T08-era source-grep admission regression, which M11.D39b says is deleted **only**
///   here, once the type, transfer, recovery and fault evidence subsumes it.
///
/// **And the protocol evidence**, because removing the guards without it would be the other
/// unsafe half: the fence protocol and the aggregate D96 runner have to exist for the fence to
/// be worth selecting. A `false` on this side is what makes the equality fail if somebody
/// removes the guards from a build that cannot fence.
///
/// The needles are built from fragments so that this row's own source does not contain them —
/// otherwise it would find itself and never be able to report a removal.
#[test]
fn the_activation_change_selects_the_fence_and_removes_every_superseded_t08_guard() {
    let availability = function_body(
        include_str!("mode.rs"),
        "const fn is_available_in_production(self) -> bool {",
    );
    let activated = availability.contains("LifecycleMode::FencedV2 => true,")
        && availability.contains("LifecycleMode::LegacyT08 => false,");

    // The three superseded mechanisms, by the shape of their definitions. The `impl` / `fn` /
    // `struct` / `type` prefixes are what make these definitions rather than mentions, and each
    // needle is assembled from fragments so that this row's own source never contains one —
    // otherwise it would find itself and could never report a removal.
    //
    // The gate's six methods are covered by its two type definitions rather than by six
    // separate needles: `publish` and `take` are names too common to search for on their own,
    // and a method cannot outlive the `impl` block it is in. The two that *are* unique —
    // `withdraw` and `disarm` — are named as well, so a re-introduction that spelled the type
    // differently is still caught.
    let def = |kind: &str, name: &str| format!("{kind} {name}");
    let gate = "RefusalGate";
    let mut superseded: Vec<(String, Vec<String>)> = vec![
        (def("impl", &format!("{gate} {{")), Vec::new()),
        (def("struct", &format!("{gate} {{")), Vec::new()),
        (def("fn", "admit_publication"), Vec::new()),
        (def("fn", "admit_scheduling"), Vec::new()),
        (def("fn", "withdraw"), Vec::new()),
        (def("fn", "disarm"), Vec::new()),
        (def("fn", "settle_under_admission"), Vec::new()),
        (def("type", "SettlementRescue"), Vec::new()),
        (def("struct", "SettlingUnderAdmission"), Vec::new()),
        (
            def(
                "fn",
                "the_source_of_scheduling_next_keeps_every_irreversible_effect",
            ),
            Vec::new(),
        ),
    ];
    for (needle, sites) in &mut superseded {
        *sites = crate_sources_containing(needle);
    }
    let still_present: Vec<&(String, Vec<String>)> =
        superseded.iter().filter(|(_, at)| !at.is_empty()).collect();

    // The evidence that makes selecting the fence safe, by the same rule: each of these is a
    // production definition or call site that has to exist for the D39 path to be a path.
    let evidence: Vec<(&str, bool)> = vec![
        (
            "the conditional status write, in the one funnel",
            production_call_sites("status.update_db_under_authority(")
                == ["src/states/lifecycle/publication.rs"],
        ),
        (
            "the active fence handshake, reached from the fan-out's admitted region",
            !production_call_sites("advance_fence(").is_empty(),
        ),
        (
            "the durable fencing obligation a controller loss recovers from",
            !production_call_sites(&def("fn", "persist_obligation")).is_empty(),
        ),
        (
            "the job's settlement owner, the only `SettlementOwner` in the crate",
            production_call_sites(&def("impl", "SettlementOwner for"))
                .iter()
                .any(|at| at == "src/states/lifecycle/settlement.rs"),
        ),
        (
            "the aggregate M11.D96 registry and runner",
            registry_row_count() == 37 && matrix_runner_exists(),
        ),
    ];
    let missing: Vec<&str> = evidence
        .iter()
        .filter(|(_, present)| !present)
        .map(|(what, _)| *what)
        .collect();

    let superseded_and_removed = still_present.is_empty() && missing.is_empty();

    assert_eq!(
        activated, superseded_and_removed,
        "M11.T26h is one change: selecting `FencedV2` and removing the M11.T08 mechanisms it \
         supersedes cannot land apart, and neither can land without the protocol and the \
         aggregate proof that make the fence worth selecting.\n\
         \x20 activated (both arms of `is_available_in_production`): {activated}\n\
         \x20 superseded mechanisms still defined: {still_present:#?}\n\
         \x20 protocol/proof evidence missing: {missing:?}"
    );

    // Which side of the equality this build is on, so that it cannot be satisfied by nothing
    // having happened at all.
    assert!(
        activated && superseded_and_removed,
        "M11.T26h has landed: the fence is what production selects, and the refusal-gate \
         quartet, the `settle_under_admission` rescue and the source-grep admission regression \
         are gone"
    );
}

/// How many rows `scripts/m11-d39-registry.json` declares, counted without a JSON parser.
///
/// The registry is data this crate does not depend on, so it is read as text. A row's `id` is
/// the only *numeric* one — the two topologies carry string ids — so counting `"id": <digit>`
/// counts rows and nothing else, and M11.T26m fixes that count at 37.
fn registry_row_count() -> usize {
    let registry = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/m11-d39-registry.json"),
    )
    .expect("the D96 registry must be readable from the crate");
    let key = "\"id\": ";
    registry
        .match_indices(key)
        .filter(|(at, _)| {
            registry.as_bytes()[at + key.len()..]
                .first()
                .is_some_and(u8::is_ascii_digit)
        })
        .count()
}

/// Whether the aggregate D96 runner exists and is executable as a shell script.
fn matrix_runner_exists() -> bool {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/m11-d39-matrix.sh");
    std::fs::read_to_string(path).is_ok_and(|s| s.contains("ARROYO__JOB_CONTROLLER"))
}
