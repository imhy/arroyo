//! The durable execution authority (M11.T26b, design M11.D39d/M11.D23).
//!
//! A controller may write a job's row only while it is the controller that adopted the job.
//! M11.D39d makes that checkable rather than assumed: `job_statuses` carries a monotonic
//! `lifecycle_fence` and a `controller_epoch`, adoption raises the fence and installs a fresh
//! epoch in one conditional update, and every later write repeats both predicates. Two
//! controller processes therefore cannot both publish an authoritative status, generation or
//! metadata root for one job — the loser updates zero rows and is told so.
//!
//! # The authority is one value, not three
//!
//! The row's identity, its fence and its epoch are a triple that only means anything together:
//! a fence read from one poll paired with an epoch read from another describes no row that
//! ever existed. [`LifecycleAuthority`] is that triple, and it can be obtained in exactly two
//! ways — [`LifecycleAuthority::observed`], which reads all three from one row, and
//! [`LifecycleAuthority::adopt`], which returns the triple a successful adoption installed.
//! There is no constructor that takes three loose values, so "the caller got them from the
//! same read" is a property of the type rather than a convention a comment has to defend.
//!
//! # Zero rows is an answer
//!
//! A conditional update that matches nothing has not failed and has not succeeded: another
//! controller holds the job. That is [`AuthorityOutcome::Stale`], a value the caller must
//! handle, rather than `Ok(())` — which would let a stale controller carry on believing it had
//! published — or a generic error, which would put losing a fence duel in the same bucket as
//! the database being unreachable. Retrying the second is right; retrying the first is how a
//! superseded controller keeps trying to overwrite a live one.
//!
//! # What is here and what is beside it
//!
//! This module owns the authority: the value types, the adoption CAS and the outcome taxonomy.
//! The conditional *status* write lives on [`JobStatus`](crate::JobStatus), because a write can
//! present no authority other than the one its own row was read with. Where a job publishes it
//! from is [`super::publication`]'s, the candidate-then-root protocol is [`super::root`]'s, and
//! recovery into `Fencing` is [`super::recovery`]'s.
//!
//! Two children sit beside it. [`obligation`] is the projection that turns a live fencing
//! obligation into the durable record M11.T26b defined, failing closed rather than truncating; [`metrics`] is what an operator sees while one stands, including the alert
//! M11.D39g's deliberate unbounded wait has to be visible through.
//!
//! Every production status write goes through the funnel next door and presents the authority
//! this module defines. It did not until M11.T26h's activation change, which removed the
//! unconditional write in the same edit that selected the fence — see
//! `the_production_status_write_is_conditional_since_the_activation_change`, which pins both
//! halves of that as one co-occurrence.

pub(crate) mod metrics;
pub(crate) mod obligation;

use std::sync::Arc;

use cornucopia_async::DatabaseSource;
use thiserror::Error;
use tracing::warn;

use crate::queries::controller_queries;

/// A job's monotonic durable lifecycle fence.
///
/// It is raised by exactly one operation — [`LifecycleAuthority::adopt`], which stores
/// `lifecycle_fence + 1` — so the value a controller holds is the value it installed, and a
/// controller that has adopted nothing holds the column's own default.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LifecycleFence(u64);

impl LifecycleFence {
    /// The fence a row that no controller has adopted carries, which is the `DEFAULT 0` the
    /// V34/V12 migrations give the column.
    ///
    /// It is below every fence an adoption can install, because adoption writes
    /// `lifecycle_fence + 1` and so never writes zero itself.
    ///
    /// `#[cfg(test)]` for the same reason [`LifecycleAuthority::unadopted`] is, and since the
    /// same change: production reads an authority out of a row it has just read, so it never
    /// needs to name the unadopted fence as a value. M11.T26h narrowed the crate-root
    /// re-export of these types, which is what made that visible.
    #[cfg(test)]
    pub const UNADOPTED: LifecycleFence = LifecycleFence(0);

    /// The fence as the column holds it.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for LifecycleFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// The controller adoption a fence belongs to.
///
/// Two controllers that read the same fence and both raise it would otherwise be
/// indistinguishable in the row they left behind; the epoch is what makes the winner nameable.
/// It is minted, never chosen: [`Self::fresh`] is the only way to obtain one that is not
/// already in a row, so a caller cannot re-install an epoch it read somewhere else.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerEpoch(String);

impl ControllerEpoch {
    /// A fresh epoch for one adoption.
    ///
    /// Two `u64`s of randomness in hexadecimal, the same shape and the same source as the
    /// `start_execution_id` the fan-out mints, so that a value appearing in a log is
    /// recognizable as an identifier rather than as a counter. It is never the empty string
    /// the column defaults to, which is what keeps "no controller has adopted this job" a
    /// value no adoption can produce.
    fn fresh() -> Self {
        Self(format!(
            "{:016x}{:016x}",
            rand::random::<u64>(),
            rand::random::<u64>()
        ))
    }

    /// The epoch as the column holds it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A `job_statuses` row whose durable authority cannot be interpreted.
///
/// The fence column is a signed `BIGINT` and the fence is a count, so a negative value is a
/// row no controller in this build wrote — adoption only ever stores `lifecycle_fence + 1`
/// over a value it read. It is refused rather than clamped, for the same reason an
/// unrecognized execution selector is: the controller cannot say what authority the job is
/// under, and guessing would be guessing on behalf of whichever controller holds it.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("job {job_id} carries a negative lifecycle fence ({fence})")]
pub struct MalformedAuthority {
    /// The job whose row carried it.
    pub job_id: String,
    /// The value the column held.
    pub fence: i64,
}

/// The durable authority one controller holds over one job.
///
/// The three fields are the whole `WHERE` clause of every conditional write in M11.D39d, and
/// they are private so that the only triples that exist are ones a single row produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleAuthority {
    job_id: Arc<String>,
    fence: LifecycleFence,
    epoch: ControllerEpoch,
}

impl LifecycleAuthority {
    /// The authority carried by one `job_statuses` row, as `all_jobs` read it.
    ///
    /// Taking the row rather than its columns is the point: it is the only production
    /// constructor, so an authority always describes a row that existed at one instant.
    ///
    /// # Errors
    ///
    /// [`MalformedAuthority`] if the row's fence is negative.
    pub fn observed(row: &controller_queries::Job) -> Result<Self, MalformedAuthority> {
        let fence = u64::try_from(row.lifecycle_fence).map_err(|_| MalformedAuthority {
            job_id: row.id.clone(),
            fence: row.lifecycle_fence,
        })?;
        Ok(Self {
            job_id: Arc::new(row.id.clone()),
            fence: LifecycleFence(fence),
            epoch: ControllerEpoch(row.controller_epoch.clone()),
        })
    }

    /// The authority a job no controller has adopted carries: the two column defaults.
    ///
    /// Test-only. Production reads an authority from a row or receives one from
    /// [`Self::adopt`]; a build that could name the unadopted authority for an arbitrary job
    /// could present it as though it had been read.
    #[cfg(test)]
    pub fn unadopted(job_id: &str) -> Self {
        Self {
            job_id: Arc::new(job_id.to_string()),
            fence: LifecycleFence::UNADOPTED,
            epoch: ControllerEpoch(String::new()),
        }
    }

    /// An authority assembled from loose values, for tests that need to present a *wrong* one.
    ///
    /// Test-only for the reason the whole type exists: the production constructors above are
    /// what stop a fence and an epoch from different reads being paired, and a test that
    /// proves the pairing matters has to be able to break it.
    #[cfg(test)]
    pub fn from_parts(job_id: &str, fence: u64, epoch: &str) -> Self {
        Self {
            job_id: Arc::new(job_id.to_string()),
            fence: LifecycleFence(fence),
            epoch: ControllerEpoch(epoch.to_string()),
        }
    }

    /// The job this authority is over.
    pub fn job_id(&self) -> &Arc<String> {
        &self.job_id
    }

    /// The fence this authority holds.
    pub fn fence(&self) -> LifecycleFence {
        self.fence
    }

    /// The controller epoch this authority holds.
    pub fn epoch(&self) -> &str {
        self.epoch.as_str()
    }

    /// Cold adoption: raises the job's fence by one and installs a fresh epoch, conditional on
    /// the row still carrying the authority this value was read with.
    ///
    /// This is the first write of every controller takeover and precedes every effect
    /// (M11.D39d). The returned authority is the one the row now holds — the fence is
    /// necessarily this one's successor, because the statement stores `lifecycle_fence + 1`
    /// over the row it matched — so a caller never has to read the row back to learn what it
    /// may present next.
    ///
    /// The epoch is minted here rather than passed in: an adoption that installed an epoch its
    /// caller had read somewhere else would be an adoption two controllers could perform with
    /// the same result.
    ///
    /// # Errors
    ///
    /// [`AuthorityWriteError`] for a database failure, for a fence that cannot be raised
    /// further, or for a statement that matched more rows than the job's primary key can
    /// select. Losing the row to another controller is *not* an error: it is
    /// [`AuthorityOutcome::Stale`].
    pub async fn adopt(
        &self,
        database: &DatabaseSource,
    ) -> Result<AuthorityOutcome<LifecycleAuthority>, AuthorityWriteError> {
        let observed = i64::try_from(self.fence.0).map_err(|_| AuthorityWriteError::Exhausted {
            job_id: (*self.job_id).clone(),
        })?;
        if observed == i64::MAX {
            return Err(AuthorityWriteError::Exhausted {
                job_id: (*self.job_id).clone(),
            });
        }
        let adopted = LifecycleAuthority {
            job_id: Arc::clone(&self.job_id),
            fence: LifecycleFence(self.fence.0 + 1),
            epoch: ControllerEpoch::fresh(),
        };

        let client = self.client(database).await?;
        let rows = controller_queries::execute_adopt_job_lifecycle(
            &client,
            &adopted.epoch.0,
            &*self.job_id,
            &observed,
            &self.epoch.0,
        )
        .await
        .map_err(|e| AuthorityWriteError::Database {
            job_id: (*self.job_id).clone(),
            operation: "adopt the job's lifecycle authority",
            report: format!("{e:?}"),
        })?;

        self.outcome(rows, "adopt the job's lifecycle authority", || adopted)
    }

    /// A database handle, with this job's identity attached to whatever went wrong.
    pub(crate) async fn client<'a>(
        &self,
        database: &'a DatabaseSource,
    ) -> Result<cornucopia_async::Database<'a>, AuthorityWriteError> {
        database
            .client()
            .await
            .map_err(|e| AuthorityWriteError::Database {
                job_id: (*self.job_id).clone(),
                operation: "reach the database",
                report: format!("{e:?}"),
            })
    }

    /// Classifies what a conditional statement under this authority did.
    ///
    /// `applied` is a closure so that the value a successful write produces is built only when
    /// there was one; a stale write must not be able to hand back a value describing a row it
    /// did not touch.
    pub(crate) fn outcome<T>(
        &self,
        rows: u64,
        operation: &'static str,
        applied: impl FnOnce() -> T,
    ) -> Result<AuthorityOutcome<T>, AuthorityWriteError> {
        match rows {
            0 => {
                warn!(
                    job_id = %self.job_id,
                    fence = %self.fence,
                    epoch = %self.epoch.as_str(),
                    operation,
                    "a conditional write matched no row: another controller holds this job's \
                     lifecycle authority, or its row is gone"
                );
                Ok(AuthorityOutcome::Stale(StaleAuthority {
                    job_id: (*self.job_id).clone(),
                    operation,
                    presented_fence: self.fence,
                    presented_epoch: self.epoch.0.clone(),
                }))
            }
            1 => Ok(AuthorityOutcome::Applied(applied())),
            // The predicate names the primary key, so more than one row is a schema this build
            // does not understand rather than a race. It is raised instead of being treated as
            // success, because "one of the rows I updated was the one I meant" is not something
            // the caller could check afterwards.
            rows => Err(AuthorityWriteError::Ambiguous {
                job_id: (*self.job_id).clone(),
                operation,
                rows,
            }),
        }
    }
}

/// What a write conditional on a job's durable authority did.
///
/// Deliberately not a `Result`: losing the job to another controller is an outcome the caller
/// must act on, not a failure to be propagated past. `#[must_use]` because the one way to get
/// this wrong is to ignore it and carry on as though the write had landed.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "a conditional write may have updated nothing; handle the stale-authority outcome"]
pub enum AuthorityOutcome<T> {
    /// The row still carried the presented authority, and the write applied under it.
    Applied(T),
    /// The write matched no row. Another controller holds this job.
    Stale(StaleAuthority),
}

impl<T> AuthorityOutcome<T> {
    /// The value a successful write produced, or `None` if the authority was stale.
    ///
    /// For callers that have already decided what a stale authority means; anything that has
    /// to *distinguish* the two matches on the enum instead — and every production caller does,
    /// because standing down and carrying on are opposite answers. `#[cfg(test)]` since
    /// M11.T26h narrowed the crate-root re-export of these types and made that visible.
    #[cfg(test)]
    pub fn applied(self) -> Option<T> {
        match self {
            AuthorityOutcome::Applied(value) => Some(value),
            AuthorityOutcome::Stale(_) => None,
        }
    }
}

/// A conditional write that updated no rows, and the authority it presented.
///
/// It names the authority rather than only the job because that is what an operator comparing
/// two controllers' logs needs: the fence and epoch the losing controller believed it held say
/// which read it was working from.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error(
    "job {job_id} rejected an attempt to {operation} under lifecycle fence {presented_fence} and \
     controller epoch {presented_epoch:?}: another controller holds it, or its row is gone"
)]
pub struct StaleAuthority {
    /// The job whose row refused the write.
    pub job_id: String,
    /// What the write was trying to do, for the operator reading the message.
    pub operation: &'static str,
    /// The fence the write presented.
    pub presented_fence: LifecycleFence,
    /// The controller epoch the write presented.
    pub presented_epoch: String,
}

/// Why a conditional write could not be performed at all.
///
/// Distinct from [`StaleAuthority`] on purpose: everything here is a condition that says
/// nothing about who holds the job, and retrying is the right response to each. Retrying a
/// stale authority is how a superseded controller keeps trying to overwrite a live one.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AuthorityWriteError {
    /// The database refused or could not be reached.
    #[error("job {job_id} could not {operation}: {report}")]
    Database {
        /// The job the write was for.
        job_id: String,
        /// What the write was trying to do.
        operation: &'static str,
        /// The database's own report, preserved rather than replaced. Not named `source`:
        /// `thiserror` would take that as an error source, and this is the text of one.
        report: String,
    },
    /// A statement keyed by the job's primary key matched more than one row.
    #[error(
        "job {job_id} matched {rows} rows while trying to {operation}, which its primary key cannot"
    )]
    Ambiguous {
        /// The job the write was for.
        job_id: String,
        /// What the write was trying to do.
        operation: &'static str,
        /// How many rows the statement reported.
        rows: u64,
    },
    /// The fence cannot be raised without leaving the range the column can hold.
    #[error("job {job_id} has exhausted its lifecycle fence")]
    Exhausted {
        /// The job whose fence is at its limit.
        job_id: String,
    },
}
