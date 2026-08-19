//! Which mechanism decides a job's lifecycle transitions (M11.T25f, design M11.D39).
//!
//! Two mechanisms exist in this crate at once, and exactly one of them is selected.

/// The mechanism a controller decides and publishes a job's lifecycle transitions with.
///
/// M11.T08 landed a *cross-task* mechanism: the configuration-update thread classifies a
/// polled row, decides what to do about it, and publishes the decision — to the job's
/// message queue and to its [`RefusalGate`](crate::states::RefusalGate) — while the job's
/// own state task consumes what it finds. Two tasks therefore share the decision, and the
/// gate, its admission mutex and its per-task `acted` watermark exist to make the two of
/// them linearize.
///
/// M11.D39a specifies the opposite arrangement: the job's state task is the *only*
/// component that decides and publishes, the update thread only enqueues a classified,
/// versioned intent, and there is consequently no cross-task gate, mutex or counter to
/// linearize at all. That mechanism is built by M11.T25 and activated by M11.T26.
///
/// # Why the two coexist rather than one replacing the other
///
/// D39's safety claim rests on a durable fence and a worker acknowledgement protocol that
/// M11.T26 owns. Landing the controller half without them would replace a mechanism that
/// is proven against sixteen review rounds with one whose settlement story is not yet
/// written. So the substrate is built, tested and reviewed while
/// [`Self::LegacyT08`] stays selected, and T26 — which supplies the missing durable half —
/// is the only owner allowed to change [`Self::SELECTED`] and then remove the superseded
/// T08 guards.
///
/// # Not a setting
///
/// This is deliberately not a configuration file key, an environment variable or a Cargo
/// feature. A deployment cannot select [`Self::FencedV2`], and neither can a test of the
/// production entry points: the value is fixed at compile time by [`Self::SELECTED`],
/// which is derived below rather than written as a literal so that it is exhaustive over
/// the enum. What a test *can* do is construct the D39a machinery directly — see
/// [`JobLifecycle::for_mode`](super::JobLifecycle::for_mode) — which is how the new path
/// is exercised without production ever being able to reach it.
///
/// # What deploying this costs (M11.T25g, design M11.D75)
///
/// M11.T25 adds no wire format, no message field, no database schema and no migration, and
/// it therefore adds **no rollout constraint of its own**. A controller carrying it talks to
/// workers exactly as the controller it was cut from did; it can be deployed before, after
/// or alongside them, and it can be rolled back, because nothing it writes is read by
/// anything else and [`Self::SELECTED`] means every job it runs is administered by the
/// mechanism M11.T08 landed. The one M11.T25 mechanism that *is* live on the production path
/// — the M11.D39c validate-then-act tokens — is a compile-time discipline over in-process
/// call sites and changes no stored bytes.
///
/// The one ordering requirement in force is the one that was already in force before this
/// todo: a worker must advertise `reconciles_start_execution` before a controller of this
/// version will send it a `StartExecution`, so worker images are upgraded first, or
/// together. That is M11.T08's round-15 fix; it is enforced loudly in
/// [`Scheduling`](crate::states::scheduling::Scheduling)'s own body, it is covered by
/// `a_worker_predating_the_reconciliation_contract_is_never_sent_a_start_execution`, and
/// M11.T25 neither weakens nor extends it.
///
/// **The flag day is M11.T26's.** Selecting [`Self::FencedV2`] makes a durable fence and an
/// acknowledged worker protocol part of what a job's safety rests on, so from that change
/// onward a controller and a worker disagreeing about whether fencing is in force is a real
/// mixed-version state rather than an inert one. T26 owns that boundary: the durable schema
/// and its migrations, the activation itself, and — in the same change, after the aggregate
/// M11.D96 proof — the removal of the T08 guards this substrate supersedes. **M11.T11**,
/// which installs the state-backend providers into the production process roles, re-confirms
/// the worker-first ordering at installation time, because that is the point at which a
/// deployment's own images are chosen.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum LifecycleMode {
    /// M11.T08's landed mechanism: the update thread decides and publishes through the
    /// [`RefusalGate`](crate::states::RefusalGate); the state task applies what it finds.
    ///
    /// This is [`Self::SELECTED`] for the whole of M11.T25.
    LegacyT08,
    /// M11.D39a's mechanism: the update thread validates, classifies and enqueues a
    /// versioned intent; the job's state task is the sole decider and publisher.
    ///
    /// Unreachable from production through M11.T25. M11.T26 activates it, after the
    /// durable fence (D39d) and the worker acknowledgement protocol (D39e) exist to make
    /// its settlement claim true.
    FencedV2,
}

impl LifecycleMode {
    /// Every mode this controller knows about.
    ///
    /// Exists so that [`Self::SELECTED`] is a *search* over the whole enum rather than a
    /// literal, and so that a test can quantify over the modes instead of sampling one
    /// call. A variant added without being listed here is not selectable at all, which is
    /// the safe direction for the omission to fail in.
    pub(crate) const ALL: [LifecycleMode; 2] = [LifecycleMode::LegacyT08, LifecycleMode::FencedV2];

    /// The mode this build runs jobs under.
    ///
    /// Evaluated at compile time from [`Self::ALL`] and
    /// [`Self::is_available_in_production`], so "production selects `LegacyT08`" is a
    /// consequence of an exhaustive match over the enum rather than of a literal that
    /// happens to name one variant. If every mode were ever marked unavailable this would
    /// fail to compile rather than fall back to something.
    pub(crate) const SELECTED: LifecycleMode = LifecycleMode::select();

    /// Whether a production controller may run under this mode.
    ///
    /// The match is exhaustive on purpose: adding a variant to [`LifecycleMode`] without
    /// answering this question does not compile, so a new mechanism cannot become
    /// selectable by omission.
    ///
    /// M11.T26 is the only owner allowed to change an answer here, and only together with
    /// the durable fence, the worker acknowledgement protocol and the aggregate D96 proof.
    const fn is_available_in_production(self) -> bool {
        match self {
            LifecycleMode::LegacyT08 => true,
            // M11.T25 builds the D39a/D39b substrate; it does not activate it. See the
            // type documentation for why the two are separate changes.
            LifecycleMode::FencedV2 => false,
        }
    }

    /// The first mode in [`Self::ALL`] a production controller may run under.
    ///
    /// A `const fn` rather than a runtime lookup so that [`Self::SELECTED`] is a compile-
    /// time constant: there is no point at which a process could be persuaded to compute a
    /// different answer, and the "no mode is available" arm is a build failure rather than
    /// a panic some job eventually hits.
    const fn select() -> LifecycleMode {
        let mut i = 0;
        while i < LifecycleMode::ALL.len() {
            let mode = LifecycleMode::ALL[i];
            if mode.is_available_in_production() {
                return mode;
            }
            i += 1;
        }
        panic!("no lifecycle mode is available in production")
    }
}
