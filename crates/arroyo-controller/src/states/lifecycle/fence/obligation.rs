//! The durable image of a live fencing obligation (M11.T26f, design M11.D39d).
//!
//! M11.T26b built [`Fencing`] — the record that survives a controller process — and wrote none.
//! This is the projection that produces one, and it is a **child** of [`super`] rather than a
//! sibling because what it projects is that module's subject: the obligation a controller holds
//! over a job, expressed durably instead of in memory.
//!
//! # What is persisted, and what deliberately is not
//!
//! Persisted: every target worker generation, what each has done about the fence, the
//! `start_execution_id` each was issued, the address each was reached at, the candidate object
//! the attempt published and never rooted, and the instant the obligation began.
//!
//! **Not** persisted: the [`Admission`](crate::states::Admission). M11.T26f's brief is explicit
//! — *"without persisting an in-process token"* — and it is not an oversight that there is no
//! field for one. An admission is this process's exclusive right to publish about this job; a
//! serialized one would be a right two processes could hold, which is the exact failure the
//! durable fence exists to prevent. What a recovering controller re-acquires is not the token
//! but the *authority*, by re-adopting the row — and that adoption is a CAS exactly one
//! controller wins. Nor is a settlement owner, a phase, a channel or a client persisted: all of
//! them are ways of *reaching* the obligation, and the obligation is what survives.
//!
//! # Why the projection can refuse
//!
//! It fails closed rather than truncating. A record that dropped a target because the
//! collection was full would be an obligation whose worker nothing will ever fence — and it
//! would look settled. So an obligation that will not fit, or whose parts disagree about which
//! generation they belong to, is refused with [`ObligationRefusal`] and the attempt reports
//! that instead of persisting a lie.

use std::collections::HashMap;

use arroyo_rpc::fence_wire::WorkerIncarnation;
use arroyo_rpc::fencing::{FenceTarget, Fencing, FencingRecordError};
use arroyo_types::WorkerId;
use thiserror::Error;

use crate::states::scheduling::fanout::IssuedAttempts;
use crate::states::scheduling::fencing::FenceTargets;

/// Where one registered worker generation was reached, and which process answered there.
///
/// One value rather than two maps because the two are read together and are only correct
/// together: a discharge that paired one worker's address with another's incarnation would
/// address a live process under a directive meant for a different one, and the receiving
/// generation would refuse a fence it should have taken (M11.D39d, PR #167 round 6).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordedEndpoint {
    /// The address the attempt reached this generation at.
    pub(crate) rpc_address: String,
    /// The process that answered its registration, or `None` if it named none.
    pub(crate) incarnation: Option<WorkerIncarnation>,
}

/// Why a live obligation could not be described durably.
///
/// Two shapes, and neither is recoverable by writing a smaller record: one says the obligation
/// is larger or stranger than the durable record may carry, and the other says the parts handed
/// in do not describe one attempt.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ObligationRefusal {
    /// The durable record refused the obligation.
    ///
    /// Every rule [`Fencing`] is under reaches here: the target capacity, an identifier or
    /// address wider than one this build can mint, a duplicate worker, a fencing origin that
    /// names no instant.
    #[error("this job's fencing obligation cannot be recorded durably: {0}")]
    Unrecordable(#[from] FencingRecordError),
    /// The issued-attempt inventory and the target set describe different scheduling
    /// generations.
    ///
    /// The identifiers, the generation they addressed and the authority they were issued under
    /// are one fact (M11.D39e(v)). Two of them arriving from different attempts is not a record
    /// to write with a note attached: it is a caller that has assembled an obligation out of two
    /// obligations, and the identifier of one would then be recorded against the targets of the
    /// other.
    #[error(
        "this job's fencing obligation names generation {addressed} while the identifiers it \
         issued were addressed to generation {issued}"
    )]
    GenerationDisagrees {
        /// The generation the target set was addressed under.
        addressed: u64,
        /// The generation the inventory says its identifiers went to.
        issued: u64,
    },
}

/// Projects one live obligation onto the durable record, or answers `None` if it owes nothing.
///
/// `None` is not an empty record. It means this attempt has no targets and left no candidate,
/// so there is nothing for a later controller to discharge — and the caller writes nothing at
/// all, leaving whatever the row already carries alone. That distinction matters on exactly one
/// path: an attempt that was interrupted by a *recovered* obligation it could not discharge has
/// itself addressed nobody, and must not overwrite the record it was interrupted by. See
/// [`recovery`](super::super::recovery), which is the only other writer of this column.
///
/// `since_millis` is a parameter rather than a clock read here, so that a controller
/// republishing an obligation it recovered carries the origin forward. That is what makes the
/// age metric measure how long the *job* has been fencing rather than how long this process has
/// been running — see [`Fencing::fencing_since_millis`].
///
/// # Errors
///
/// [`ObligationRefusal`]. Both variants fail closed: the caller reports, and nothing partial is
/// written.
pub(crate) fn describe(
    generation: u64,
    targets: &FenceTargets,
    issued: &IssuedAttempts,
    endpoints: &HashMap<WorkerId, RecordedEndpoint>,
    candidate_root: Option<&str>,
    since_millis: Option<u64>,
) -> Result<Option<Fencing>, ObligationRefusal> {
    // The relationship, checked before anything is assembled from the parts. An inventory that
    // issued nothing addressed nothing, and its generation sentinel says so rather than
    // disagreeing; one that issued something must have issued it to the generation these
    // targets are.
    if issued.issued_count() > 0 && issued.generation() != generation {
        return Err(ObligationRefusal::GenerationDisagrees {
            addressed: generation,
            issued: issued.generation(),
        });
    }
    if targets.count() == 0 && candidate_root.is_none() {
        return Ok(None);
    }

    let recorded: Vec<FenceTarget> = targets
        .each()
        .map(|(worker, state)| FenceTarget {
            worker_id: worker.0,
            generation,
            // The identifier this target was issued, taken from the inventory that issued it
            // rather than from anything this function composes. A target the fan-out never
            // reached carries none, which is what M11.T26b's `Option` is for: it was addressable
            // by the fence and was never issued a start.
            attempt_id: issued
                .record(worker)
                .map(|record| record.attempt_id.clone()),
            rpc_address: endpoints
                .get(&worker)
                .map(|endpoint| endpoint.rpc_address.clone()),
            // The process this target is owed by, so that a controller which did not start it
            // can still address a fence to it. Read from the same entry as the address, so a
            // record can never carry one worker's endpoint with another's incarnation.
            incarnation: endpoints
                .get(&worker)
                .and_then(|endpoint| endpoint.incarnation)
                .map(|incarnation| incarnation.into()),
            state,
        })
        .collect();

    Ok(Some(Fencing::record(
        recorded,
        candidate_root.map(str::to_string),
        since_millis,
    )?))
}
