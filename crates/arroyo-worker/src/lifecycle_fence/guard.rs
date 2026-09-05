//! The worker's same-guard fence advancement and `StartExecution` admission (M11.T26d,
//! design M11.D39d/M11.D39e).
//!
//! # The property this file exists to make structural
//!
//! M11.D39d: *"The worker serializes fence advancement with `StartExecution` admission under the
//! same non-blocking phase guard, records the highest acknowledged fence, revokes all named
//! lower-fence outstanding IDs, and only then acknowledges. Thus a start either linearizes
//! before the fence acknowledgement (and is reported applied, requiring observed generation
//! teardown) or after it (and is rejected stale); there is no validate→apply gap."*
//!
//! Three things carry that, and none of them is a rule a caller has to follow:
//!
//! 1. [`WorkerLifecycle`] owns *both* the execution phase and the fence state, and it is what
//!    `WorkerState`'s one mutex holds. There is no handle to the fence that does not go through
//!    that lock, so fence advancement and start admission cannot interleave — not because the
//!    handler remembers to take one lock, but because there is only one thing to lock.
//! 2. [`WorkerLifecycle::admit_start`] is the single operation. It advances the fence, revokes,
//!    decides, and moves the phase in one call, so there is no window in which a request has
//!    been validated but not yet applied, and no way to advance the fence without deciding the
//!    request that came with it.
//! 3. `WorkerExecutionPhase::Initializing` carries a [`StartAdmitted`] whose field is private to
//!    this module, so no other code can put the worker into the phase that means "a start was
//!    admitted"; and the [`AppliedStart`] that admission returns yields its response only by
//!    running the initialization, so an admitted start cannot be answered without being started.
//!
//! # Commits are decided here too, and by the same rule
//!
//! M11.D39d carries the fence and the target generation on *"`StartExecutionReq` and commit
//! directives"* alike, and M11.D39's safety invariant names commit publication among the effects
//! a refusal must not newly admit. [`WorkerLifecycle::admit_commit`] is that decision, taken
//! under the same lock and through the same
//! [`FenceState::addressed_to_this_generation`] the fenced start path uses — one function,
//! because "is this fence, addressed to that generation, still one I answer for?" is one
//! question and two answers to it could drift apart. What a commit does *not* do is advance the
//! fence; [`WorkerLifecycle::admit_commit`] says why, and its `&self` receiver is what makes it
//! so.
//!
//! # What is not decided here
//!
//! Nothing about the wire shape: `arroyo_rpc::fence_wire` turns the flat protobuf fields back
//! into one directive, and this module reads only that. And nothing about settlement: the worker
//! reports what it observed, and which issued attempts that settles is the controller's
//! accounting (M11.T26e).

use crate::WorkerExecutionPhase;
use crate::lifecycle_fence::attempt_ids::{AttemptDisposition, AttemptIdRefusal, AttemptIds};
use arroyo_rpc::fence_wire::{
    CommitAuthority, CommitDirective, FenceAddress, LifecycleTarget, StartDirective,
    WorkerIncarnation, commit_directive, start_directive,
};
use arroyo_rpc::grpc::rpc::{
    CommitReq, LifecycleOperation, OperatorCommitData, StartExecutionOutcome, StartExecutionReq,
    StartExecutionResp,
};
use std::collections::HashMap;
use std::time::SystemTime;
use tonic::Status;

/// How an incarnation reads in a refusal: its value, or `none` when the side in question names
/// no process.
///
/// Both sides of the identity comparison can legitimately name none — an address from a sender
/// predating the field, and a generation built without one — and an operator reading the refusal
/// has to be able to tell "a different process" from "no process named" without decoding zero.
fn describe_incarnation(incarnation: Option<WorkerIncarnation>) -> String {
    incarnation.map_or_else(|| "none".to_string(), |i| i.get().to_string())
}

/// Proof that a start was admitted by [`WorkerLifecycle::admit_start`].
///
/// `WorkerExecutionPhase::Initializing` carries one, and the field below is private to this
/// module, so that phase is unconstructible anywhere else. The worker cannot be put into the
/// state that means "a start has been admitted" by code that did not admit one.
#[derive(Debug)]
pub(crate) struct StartAdmitted(());

/// What the guard decided about a `StartExecution` request.
///
/// There is no third answer: either the request applies — and the phase has already moved, so
/// nothing can overtake it — or the guard has settled it and produced the response to send. A
/// refusal is a `Status` and never reaches this type.
#[must_use]
#[derive(Debug)]
pub(crate) enum StartAdmission {
    /// The request applies. The phase is already `Initializing`.
    Apply(AppliedStart),
    /// Nothing to apply; answer with this response.
    Settled(StartExecutionResp),
}

/// An admitted start, which yields its response only by starting the execution.
///
/// [`AppliedStart::start`] is the only way to get the response out, and it runs the caller's
/// initialization first. A caller therefore cannot answer "applied" to a request whose execution
/// it never began — the phase says `Initializing` and the response says `APPLIED` exactly when
/// the initialization has been handed off.
#[must_use]
#[derive(Debug)]
pub(crate) struct AppliedStart {
    response: StartExecutionResp,
    authority: CommitAuthority,
}

impl AppliedStart {
    /// Begins the admitted execution and returns the response for the request that admitted it.
    ///
    /// The initialization is *handed* the authority this start conferred rather than reading it
    /// back off the request, and that is the only route it takes. A worker admitted as a leader
    /// commits to its job's other workers under the fence its own start carried
    /// (M11.D39d); deriving that from a second read of the same request would be a second
    /// decision about which fence this execution runs under, and the two could differ — the
    /// generation's *highest acknowledged* fence, in particular, rises when a replacement
    /// controller handshakes this worker, and committing under that would be committing on an
    /// authority this execution was never given.
    pub(crate) fn start(self, begin: impl FnOnce(CommitAuthority)) -> StartExecutionResp {
        begin(self.authority);
        self.response
    }
}

/// A commit this generation may apply, and the only way to reach what it commits.
///
/// [`WorkerLifecycle::admit_commit`] takes the whole [`CommitReq`] and gives back one of these,
/// so the committing data cannot be read by a handler that did not put the request through the
/// fence decision first. That is the difference between a check and a funnel: there is no
/// `CommitReq` in scope for a caller to reach past the guard for.
#[must_use]
#[derive(Debug)]
pub(crate) struct AdmittedCommit {
    epoch: u64,
    committing_data: HashMap<String, OperatorCommitData>,
}

impl AdmittedCommit {
    /// The epoch this commit publishes and the operator data it publishes for it.
    pub(crate) fn into_parts(self) -> (u64, HashMap<String, OperatorCommitData>) {
        (self.epoch, self.committing_data)
    }
}

/// The execution phase and the lifecycle-fence state of one worker generation, together.
///
/// Together because M11.D39d requires them to be advanced under one guard; see the module
/// documentation. The phase is reachable for the handlers that legitimately move it
/// (`job_finished`, `job_controller_init`, initialization) through [`WorkerLifecycle::execution`]
/// and [`WorkerLifecycle::execution_mut`], and none of them can express the one transition that
/// admitting a start makes.
pub(crate) struct WorkerLifecycle {
    execution: WorkerExecutionPhase,
    fence: FenceState,
}

/// One worker generation's fence state.
///
/// Private to this module and reachable only through [`WorkerLifecycle`], so "the highest
/// acknowledged fence" and "the applied/revoked identifiers" are always read and written under
/// the same lock as the phase.
#[derive(Debug)]
struct FenceState {
    /// This generation's own identity — worker, generation and *this process* — or `None` when
    /// it addresses no generation.
    ///
    /// `LifecycleTarget::addressed` answers `None` for generation zero, which no live worker
    /// generation runs under. A worker that reports one is a worker no fence can be addressed
    /// to, so it refuses every fenced directive rather than matching one by accident.
    ///
    /// The incarnation is the third part and the one a restart cannot reconstruct. Everything
    /// else here is rebuilt by a successor process at the same worker id and generation — that
    /// is `WorkerFault::Restart` — so every check below is a check against reconstructed state
    /// and a directive delayed from before the restart passes it. The incarnation does not
    /// survive the process that minted it, so a directive addressed to the predecessor names a
    /// process this one is not (PR #167 round 6, finding 3).
    identity: Option<LifecycleTarget>,
    /// Whether this generation has announced itself to a controller (M11.D39e(i)).
    ///
    /// It turns on when the worker *issues* its `RegisterWorkerReq`, not when it applies the
    /// answer, and the difference is the whole of what makes the fenced protocol usable:
    /// `ControllerGrpc::register_worker` puts the `WorkerConnect` that makes this generation
    /// schedulable on the job's queue **before** it returns `RegisterWorkerResp`, so the
    /// controller's own fence handshake can reach this worker while that response is still in
    /// flight. A gate on the answer refuses it, definitively, and fails an otherwise healthy
    /// scheduling attempt.
    ///
    /// It gates the fenced protocol only. Both of the switches that turn [`Self::strict`] on —
    /// a registration response requiring fences, and acknowledging a fenced operation — are
    /// reachable only once this is true, so strict mode implies an announced generation and the
    /// two rules cannot disagree about one.
    announced: bool,
    /// Whether this generation requires a fence on every start (M11.D39e(i)).
    strict: bool,
    /// The highest fence this generation has acknowledged; zero means none.
    ///
    /// Zero is the same reading `StartExecutionResp::observed_lifecycle_fence` gives it: no
    /// controller adopts fence zero, because cold adoption increments the durable fence before
    /// causing any effect, so "none acknowledged" and "fence zero" cannot be confused.
    acknowledged: u64,
    ids: AttemptIds,
}

/// What the guard will do, decided before anything is changed.
///
/// Every arm carries the `FenceAddress` the directive was fenced under, or `None` for an
/// unfenced one — and an address in a plan is one that has already been checked against this
/// generation's identity and against the fence it has already acknowledged.
#[derive(Debug)]
enum AdmissionPlan<'a> {
    /// Apply the request's program, recording `attempt_id` if it carries one.
    Apply {
        address: Option<FenceAddress>,
        revoke: &'a [String],
        attempt_id: Option<&'a str>,
    },
    /// Acknowledge the fence, and the revocations if `revoking`, without applying anything.
    ///
    /// Reachable only from a fenced directive, which is why the address is not optional: an
    /// acknowledging outcome always has a non-zero fence to report.
    Acknowledge {
        address: FenceAddress,
        revoke: &'a [String],
        revoking: bool,
    },
    /// This generation already applied this identifier; acknowledge it again.
    AlreadyApplied {
        address: Option<FenceAddress>,
        revoke: &'a [String],
    },
}

/// Proof that a generation announced itself to a controller before applying an answer from one.
///
/// Unconstructible outside this module and produced only by [`WorkerLifecycle::announce`], so
/// "the fenced protocol opens when the registration request goes out, not when its answer comes
/// back" is a property of the type rather than of the order two statements happen to be written
/// in. A build that applies a registration answer has necessarily announced the generation.
#[must_use = "the registration answer is applied through this proof"]
pub(crate) struct Announced(());

impl WorkerLifecycle {
    /// An idle generation that has not announced itself and has acknowledged no fence.
    ///
    /// `incarnation` is this *process*'s, minted once by whoever builds the worker and reported
    /// on its `RegisterWorkerReq`. It is taken rather than minted here so that the value the
    /// controller is told and the value directives are checked against are one value.
    pub(crate) fn idle(worker_id: u64, generation: u64, incarnation: WorkerIncarnation) -> Self {
        Self {
            execution: WorkerExecutionPhase::Idle,
            fence: FenceState {
                identity: LifecycleTarget::addressed(worker_id, generation, incarnation.get()),
                announced: false,
                strict: false,
                acknowledged: 0,
                ids: AttemptIds::default(),
            },
        }
    }

    /// This generation's execution phase.
    pub(crate) fn execution(&self) -> &WorkerExecutionPhase {
        &self.execution
    }

    /// This generation's execution phase, for the handlers that move it after a start.
    pub(crate) fn execution_mut(&mut self) -> &mut WorkerExecutionPhase {
        &mut self.execution
    }

    /// Announces this generation to a controller: the fenced protocol opens here.
    ///
    /// Called immediately before the worker issues its `RegisterWorkerReq`, because that request
    /// is the only thing that tells a controller this generation exists and where it answers —
    /// and a controller may address it from the moment it holds one, without waiting for its own
    /// response to be applied. See [`FenceState::announced`].
    pub(crate) fn announce(&mut self) -> Announced {
        self.fence.announced = true;
        Announced(())
    }

    /// Applies the registration answer, and whether it activates strict mode.
    ///
    /// Strict mode is monotone: `requires_lifecycle_fence` can only add it, never clear it, so a
    /// later registration against a legacy controller cannot take a fenced generation back out
    /// of strict mode (M11.D39e(i)).
    ///
    /// It does not open the fenced protocol — [`Self::announce`] did that, one round trip
    /// earlier — and taking that proof by value is what says so.
    pub(crate) fn registered(&mut self, _announced: Announced, requires_lifecycle_fence: bool) {
        self.fence.strict |= requires_lifecycle_fence;
    }

    /// Advances the fence, applies the revocations, and admits or refuses the request — in that
    /// order, in one call, under the caller's lock on this value.
    ///
    /// # Errors
    ///
    /// A definitive `Status` for every refusal. None of the codes is one the controller's
    /// ambiguous-transport table retries (`Cancelled`, `Unknown`, `DeadlineExceeded`,
    /// `Unavailable`): a refusal here is an authoritative answer about this generation, not a
    /// transport outcome, and re-sending the same identifier can never change it.
    #[allow(clippy::result_large_err)]
    pub(crate) fn admit_start(
        &mut self,
        req: &StartExecutionReq,
    ) -> Result<StartAdmission, Status> {
        let plan = self.plan(req)?;
        self.commit(plan)
    }

    /// Admits or refuses a commit directive, under the caller's lock on this value.
    ///
    /// # Why this takes `&self`, and why that is the answer to "does a commit advance the fence"
    ///
    /// It does not, and the signature is how that is enforced rather than remembered.
    ///
    /// M11.D39d gives fence advancement one purpose: *"records the highest acknowledged fence,
    /// revokes all named lower-fence outstanding IDs, and only then acknowledges"*, so that a
    /// start either linearizes before the acknowledgement or is refused after it. The
    /// acknowledgement is the point — it is evidence the issuing controller *reads*, on
    /// `StartExecutionResp::observed_lifecycle_fence`, and M11.D39e(v) makes that reading one of
    /// the three things that can settle an issued attempt.
    ///
    /// `CommitResp` carries no such field, and M11.P54a's list of what the commit message gains
    /// ends at the fence itself. A fence advanced here would therefore be a state change no
    /// controller could observe: a delayed duplicate of a superseded controller's commit would
    /// silently raise this generation's floor, the live controller's own fenced start would then
    /// be refused as stale, and nothing in the protocol would say why. Arbitrary in-transit
    /// delay and duplication are inside M11.D39g's declared fault model, so that is a wedge and
    /// not a hypothetical.
    ///
    /// It would also flip strict mode, which M11.D39e(i) makes monotonic for the generation —
    /// a commit would then decide the flag-day question for later *starts*.
    ///
    /// So the fence on a commit is read as a guard and never as an instruction: this generation
    /// answers whether the sender still holds the authority it claims, and changes nothing.
    /// Advancing is the fenced start path's, where it is asked for and answered.
    ///
    /// # Errors
    ///
    /// A definitive `Status` for every refusal, for the reason [`Self::admit_start`] gives.
    #[allow(clippy::result_large_err)]
    pub(crate) fn admit_commit(&self, req: CommitReq) -> Result<AdmittedCommit, Status> {
        match commit_directive(&req).map_err(|e| Status::invalid_argument(e.to_string()))? {
            CommitDirective::Unfenced => self.fence.unfenced_is_still_admissible()?,
            // Deliberately *not* `acknowledged_this_fence`, which the start path requires. A
            // commit is issued by whatever controller is administering the job now, and a
            // controller that adopts an already-running job holds a fence above the one its
            // workers acknowledged without ever re-handshaking them — that is a takeover, not a
            // forgery, and refusing it would make a running job uncommittable by its own owner.
            // A start is different: one is only ever issued out of an `AcknowledgedTarget`, so
            // requiring the acknowledgement costs a live controller nothing.
            CommitDirective::Fenced(address) => self.fence.addressed_to_this_generation(address)?,
        }
        Ok(AdmittedCommit {
            epoch: req.epoch,
            committing_data: req.committing_data,
        })
    }

    /// Decides what to do, reading state and changing none of it.
    #[allow(clippy::result_large_err)]
    fn plan<'a>(&self, req: &'a StartExecutionReq) -> Result<AdmissionPlan<'a>, Status> {
        let directive =
            start_directive(req).map_err(|e| Status::invalid_argument(e.to_string()))?;

        // The operations that apply no program are answered from inside the fenced arm, where the
        // address they acknowledge is in scope without an option to unwrap: `start_directive`
        // gives `Unfenced` no operation and no revocations to carry.
        let (address, revoke) = match directive {
            StartDirective::Unfenced => {
                self.fence.unfenced_is_still_admissible()?;
                (None, &[][..])
            }
            StartDirective::Fenced {
                address,
                operation,
                revoked_execution_ids,
            } => {
                self.fence.addressed_to_this_generation(address)?;
                match operation {
                    LifecycleOperation::FenceOnly => {
                        if !revoked_execution_ids.is_empty() {
                            return Err(Status::invalid_argument(format!(
                                "a fence-only directive names {} execution identifiers to revoke",
                                revoked_execution_ids.len()
                            )));
                        }
                        return Ok(AdmissionPlan::Acknowledge {
                            address,
                            revoke: &[],
                            revoking: false,
                        });
                    }
                    LifecycleOperation::Revoke => {
                        if revoked_execution_ids.is_empty() {
                            return Err(Status::invalid_argument(
                                "a revoke directive names no execution identifier to revoke",
                            ));
                        }
                        return Ok(AdmissionPlan::Acknowledge {
                            address,
                            revoke: revoked_execution_ids,
                            revoking: true,
                        });
                    }
                    LifecycleOperation::Start => {
                        // M11.D39d's active replacement handshake, required here rather than
                        // assumed. A controller cannot build a fenced start without an
                        // `AcknowledgedTarget`, so every start it sends is under a fence this
                        // generation has *already* acknowledged; asking for that is what makes a
                        // start unforgeable by a state this generation no longer holds.
                        //
                        // It is what a restart costs, and the reason this is not merely
                        // symmetry. A restarted process is the same worker id and generation with
                        // none of the fence state its predecessor held (`WorkerFault::Restart`),
                        // so under a `>=` rule a start delayed from before a refusal — revoked
                        // and fenced past by the controller that published that refusal — would
                        // be admitted by the successor, which holds no record of either. Under
                        // this rule the successor has acknowledged nothing, so it authorises
                        // nothing, and the only fence it can be brought to is the live
                        // controller's own (PR #167 round 2).
                        self.fence.acknowledged_this_fence(address)?;
                    }
                }
                (Some(address), revoked_execution_ids)
            }
        };

        let attempt_id =
            (!req.start_execution_id.is_empty()).then_some(req.start_execution_id.as_str());
        match attempt_id.map(|id| self.fence.ids.disposition(id)) {
            Some(AttemptDisposition::Revoked) => {
                return Err(Status::failed_precondition(format!(
                    "execution {} is permanently revoked for this worker generation",
                    req.start_execution_id
                )));
            }
            // A lost response is not a lost decision: the controller keeps the same admission
            // while retrying this identifier, and this acknowledgement is what lets it resolve an
            // otherwise ambiguous client timeout without applying the request twice.
            Some(AttemptDisposition::Applied) => {
                return Ok(AdmissionPlan::AlreadyApplied { address, revoke });
            }
            Some(AttemptDisposition::Unknown) | None => {}
        }

        match &self.execution {
            WorkerExecutionPhase::Idle => Ok(AdmissionPlan::Apply {
                address,
                revoke,
                attempt_id,
            }),
            // `Unavailable` is reserved for ambiguous transport outcomes, which the controller
            // retries under its admission. These are authoritative application responses for
            // another attempt, so they must use a definitive status.
            WorkerExecutionPhase::Initializing { .. } => {
                Err(Status::failed_precondition("Worker is initializing"))
            }
            WorkerExecutionPhase::WaitingOnLeader { .. } => {
                Err(Status::failed_precondition("Worker is waiting for leader"))
            }
            WorkerExecutionPhase::Running(_) => {
                Err(Status::failed_precondition("Worker is already running"))
            }
            WorkerExecutionPhase::Failed { .. } => {
                Err(Status::failed_precondition("Worker is in failed state"))
            }
        }
    }

    /// Carries out a plan.
    ///
    /// The identifier record is written first because it is the only step that can still fail:
    /// a refusal here leaves the acknowledged fence and the execution phase exactly as the plan
    /// found them, so a directive the worker could not record is a directive it did not
    /// acknowledge either.
    #[allow(clippy::result_large_err)]
    fn commit(&mut self, plan: AdmissionPlan<'_>) -> Result<StartAdmission, Status> {
        let (address, revoke, apply) = match &plan {
            AdmissionPlan::Apply {
                address,
                revoke,
                attempt_id,
            } => (*address, *revoke, *attempt_id),
            AdmissionPlan::Acknowledge {
                address, revoke, ..
            } => (Some(*address), *revoke, None),
            AdmissionPlan::AlreadyApplied { address, revoke } => (*address, *revoke, None),
        };

        self.fence.ids.record(revoke, apply).map_err(refusal)?;
        self.fence.acknowledge(address);

        Ok(match plan {
            AdmissionPlan::Apply { .. } => {
                self.execution = WorkerExecutionPhase::Initializing {
                    started_at: SystemTime::now(),
                    _admitted: StartAdmitted(()),
                };
                StartAdmission::Apply(AppliedStart {
                    response: self.fence.settlement(StartExecutionOutcome::Applied),
                    authority: address
                        .map_or_else(CommitAuthority::unfenced, FenceAddress::commit_authority),
                })
            }
            AdmissionPlan::Acknowledge { revoking, .. } => {
                StartAdmission::Settled(self.fence.settlement(if revoking {
                    StartExecutionOutcome::Revoked
                } else {
                    StartExecutionOutcome::FenceAcknowledged
                }))
            }
            AdmissionPlan::AlreadyApplied { .. } => {
                StartAdmission::Settled(self.fence.settlement(StartExecutionOutcome::Applied))
            }
        })
    }
}

impl FenceState {
    /// Whether a directive carrying no lifecycle fields may still be acted on.
    ///
    /// The pre-flag-day route (M11.D39e(i), M11.D75). Before strict mode this is the whole
    /// protocol and the directive is admitted exactly as it was before the fields existed;
    /// after it, a fence-less directive is one this generation can no longer attribute to any
    /// controller, so it fails closed.
    ///
    /// # Errors
    ///
    /// `FailedPrecondition` once this generation is strict.
    #[allow(clippy::result_large_err)]
    fn unfenced_is_still_admissible(&self) -> Result<(), Status> {
        if self.strict {
            return Err(Status::failed_precondition(
                "Worker generation requires a lifecycle fence and this request carries none",
            ));
        }
        Ok(())
    }

    /// Whether `address` names this generation, under a fence it has not already moved past.
    ///
    /// One function for both directives, because it is one question. A fence and the generation
    /// it addresses are a single statement — "you, that generation, under my authority" — and
    /// answering it in two places is what would let a start and a commit disagree about which
    /// generation this worker is or which fences it has left behind. The checks are ordered
    /// from the widest: a generation that has not announced itself has no fenced protocol at
    /// all, one that addresses no generation cannot be a target, one that is a *different*
    /// target is a
    /// different worker generation, and only then does the fence itself matter.
    ///
    /// # Errors
    ///
    /// `FailedPrecondition` for each of those four, every one of them a definitive statement
    /// about this generation that re-sending cannot change.
    #[allow(clippy::result_large_err)]
    fn addressed_to_this_generation(&self, address: FenceAddress) -> Result<(), Status> {
        // M11.D39e(i)/M11.T26c: announcing gates the *fenced* protocol, not the legacy route.
        //
        // The gate is the registration *request* and not its response. A controller learns this
        // generation's identity and address from the `RegisterWorkerReq` it is answering, and
        // `ControllerGrpc::register_worker` puts the `WorkerConnect` that makes the generation
        // schedulable onto the job's queue before it returns that answer — so this controller's
        // own `FENCE_ONLY` handshake legitimately arrives while the answer is still in flight.
        // Refusing it there would fail a healthy scheduling attempt and would do it
        // definitively, because `FailedPrecondition` is not in the controller's ambiguous table
        // and nothing re-offers the directive.
        //
        // Before the request goes out nothing outside this process knows this generation exists,
        // so refusing a fenced directive there is fail-closed. A fence-less directive is the
        // pre-flag-day route and is admitted exactly as it was before this protocol existed;
        // refusing it here would turn a compatible increment into a live one, in the window
        // between this worker answering the gRPC port and announcing itself. After the flag day
        // the question does not arise: strict mode refuses fence-less directives, and strict
        // mode implies an announced generation because both of its on-switches are below this
        // line.
        if !self.announced {
            return Err(Status::failed_precondition(
                "Worker generation has not begun registration",
            ));
        }
        let identity = self.identity.ok_or_else(|| {
            Status::failed_precondition("Worker generation is not addressable by a lifecycle fence")
        })?;
        // Endpoint reuse is distinguished by generation, not by address or worker id: a
        // restarted worker answering at its predecessor's address is a different generation and
        // must not answer for its predecessor's requests (M11.D39d). Generation reuse is
        // distinguished by incarnation, for the same reason one step down: a restart under the
        // same generation is a different *process*, and everything else this guard holds is
        // state that process reconstructed (PR #167 round 6, finding 3).
        //
        // Whole-value equality, so an address that names no incarnation is refused by a
        // generation that has one. That fails closed and costs nothing deployable: the only
        // sender that cannot name an incarnation is one predating
        // `StartExecutionReq::target_worker_incarnation`, and M11.D75's worker-first ordering
        // means no such sender ever addresses a fence — a controller old enough to omit the
        // field is old enough to send no fence at all, which is the unfenced route above.
        if address.target() != identity {
            return Err(Status::failed_precondition(format!(
                "request is addressed to worker {} generation {} incarnation {}, and this is \
                 worker {} generation {} incarnation {}",
                address.target().worker_id(),
                address.target().generation(),
                describe_incarnation(address.target().incarnation()),
                identity.worker_id(),
                identity.generation(),
                describe_incarnation(identity.incarnation()),
            )));
        }
        if address.fence() < self.acknowledged {
            return Err(Status::failed_precondition(format!(
                "lifecycle fence {} is older than fence {} this worker generation has acknowledged",
                address.fence(),
                self.acknowledged,
            )));
        }
        Ok(())
    }

    /// Whether this generation has already acknowledged the exact fence `address` names.
    ///
    /// The worker end of M11.D39d's active handshake. `addressed_to_this_generation` asks the
    /// monotonicity question — is this fence one I have moved past? — and that is the right
    /// question for the handshake itself, which is how a generation *learns* a fence. It is the
    /// wrong question for a directive that applies a program or publishes a commit: those are
    /// only ever issued under a fence the issuer has already heard this generation acknowledge,
    /// so accepting one under any other fence accepts a directive no live controller could have
    /// sent (PR #167 round 2).
    ///
    /// # Errors
    ///
    /// `FailedPrecondition`, definitive like every other refusal here, and worded for the two
    /// cases an operator has to tell apart: a generation that has acknowledged nothing at all —
    /// a fresh process, which is what a restart produces — and one holding a different fence.
    #[allow(clippy::result_large_err)]
    fn acknowledged_this_fence(&self, address: FenceAddress) -> Result<(), Status> {
        if address.fence() == self.acknowledged {
            return Ok(());
        }
        Err(Status::failed_precondition(if self.acknowledged == 0 {
            format!(
                "this worker generation has acknowledged no lifecycle fence, so no handshake \
                 authorises a request under fence {}",
                address.fence(),
            )
        } else {
            format!(
                "lifecycle fence {} is not fence {}, the one this worker generation acknowledged, \
                 so no handshake of that authority authorises this request",
                address.fence(),
                self.acknowledged,
            )
        }))
    }

    /// Raises the highest acknowledged fence and turns strict mode on.
    ///
    /// Monotone in both: the fence only rises, and acknowledging a fenced operation is one of the
    /// two things that activate strict mode for a generation (M11.D39e(i)). An unfenced directive
    /// acknowledges nothing and changes neither.
    fn acknowledge(&mut self, address: Option<FenceAddress>) {
        if let Some(address) = address {
            self.acknowledged = self.acknowledged.max(address.fence());
            self.strict = true;
        }
    }

    /// The response this generation gives for `outcome`.
    ///
    /// `observed_lifecycle_fence` is the highest fence acknowledged *after* the directive was
    /// carried out, which is what makes it usable as evidence: a controller that reads its own
    /// fence back knows this generation will refuse everything older.
    fn settlement(&self, outcome: StartExecutionOutcome) -> StartExecutionResp {
        StartExecutionResp {
            observed_lifecycle_fence: self.acknowledged,
            outcome: outcome as i32,
        }
    }
}

/// Turns a record's refusal into the definitive status that reports it.
///
/// Capacity exhaustion is `ResourceExhausted` and not `Unavailable`: the controller's ambiguous
/// table retries `Unavailable` with the same identifier, and retrying is exactly what cannot help
/// here. A worker whose bounded state is full has answered authoritatively.
fn refusal(refusal: AttemptIdRefusal) -> Status {
    match refusal {
        AttemptIdRefusal::MalformedId { .. } => Status::invalid_argument(refusal.to_string()),
        AttemptIdRefusal::Overflow { .. } => Status::resource_exhausted(refusal.to_string()),
        AttemptIdRefusal::AlreadyApplied { .. } | AttemptIdRefusal::RevokesApplied { .. } => {
            Status::failed_precondition(refusal.to_string())
        }
    }
}

#[cfg(test)]
impl WorkerLifecycle {
    /// The highest fence this generation has acknowledged; zero means none.
    pub(crate) fn acknowledged_fence(&self) -> u64 {
        self.fence.acknowledged
    }

    /// Whether this generation requires a fence on every start.
    pub(crate) fn is_strict(&self) -> bool {
        self.fence.strict
    }

    /// Whether this generation has announced itself to a controller.
    pub(crate) fn is_announced(&self) -> bool {
        self.fence.announced
    }

    /// What this generation has done about `id`.
    pub(crate) fn disposition(&self, id: &str) -> AttemptDisposition {
        self.fence.ids.disposition(id)
    }

    /// The identifier this generation applied, if it applied one.
    pub(crate) fn applied(&self) -> Option<&str> {
        self.fence.ids.applied()
    }

    /// How many identifiers this generation's record holds.
    pub(crate) fn tracked_ids(&self) -> usize {
        self.fence.ids.len()
    }
}
