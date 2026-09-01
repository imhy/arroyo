//! Which mechanism decides a job's lifecycle transitions (M11.T25f/M11.T26h, design M11.D39).
//!
//! Two mechanisms are named by this enum. Exactly one of them is selected, and since
//! M11.T26h's activation change that one is [`LifecycleMode::FencedV2`].

/// The mechanism a controller decides and publishes a job's lifecycle transitions with.
///
/// M11.T08 landed a *cross-task* mechanism: the configuration-update thread classified a
/// polled row, decided what to do about it, and published the decision — to the job's
/// message queue and to a cross-task refusal gate — while the job's own state task consumed
/// what it found. Two tasks therefore shared the decision, and the gate, its admission mutex
/// and its per-task `acted` watermark existed to make the two of them linearize.
///
/// M11.D39a specifies the opposite arrangement: the job's state task is the *only*
/// component that decides and publishes, the update thread only enqueues a classified,
/// versioned intent, and there is consequently no cross-task gate, mutex or counter to
/// linearize at all. That mechanism was built by M11.T25, completed by M11.T26 and selected
/// by M11.T26h — which is also the change that removed the gate, because a mechanism that is
/// superseded and still compiled is a second thing that could decide a job.
///
/// # Why both variants still exist
///
/// [`Self::LegacyT08`] is no longer a mechanism this build runs. It is retained as the
/// *description of a peer*: a controller or a worker from before the flag day. M11.D75's
/// declared compatibility window is a real deployment state — a fence-capable worker
/// registered to a fence-less controller must keep accepting fence-less starts — and
/// [`Self::requires_lifecycle_fence`] answering `false` for this variant is how that window
/// is expressed in the type system rather than in prose. The mixed-version rollout harness
/// (`arroyo-worker`'s `lifecycle_fence::rollout_tests`) instantiates it for exactly that
/// purpose.
///
/// # Not a setting
///
/// This is deliberately not a configuration file key, an environment variable or a Cargo
/// feature. A deployment cannot select a mode at all: the value is fixed at compile time by
/// [`Self::SELECTED`], which is derived below rather than written as a literal so that it is
/// exhaustive over the enum. What a test *can* do is construct either mechanism directly —
/// see [`JobLifecycle::for_mode`](super::JobLifecycle::for_mode) — which is how the
/// pre-flag-day peer is exercised without a deployment ever being able to be one.
///
/// # What deploying this costs (M11.T26h, design M11.D75)
///
/// **Worker-first, and the flag day is one-way.** A controller carrying this build requires
/// the lifecycle fence of every worker generation it registers, so worker images are upgraded
/// first — or together, on the schedulers that launch workers from `current_exe()`. A worker
/// generation put into strict mode by a registration response never leaves it, so rolling a
/// controller back to one that can emit fence-less starts locks those generations out of
/// every start it could send. Before the flag day, rollback to M11.T25 is unconditional;
/// after it, rollback is only to a fence-capable build or through a coordinated stop.
/// The whole of it is `docs/lifecycle-fence-rollout.md`, and **M11.T11** re-confirms the
/// ordering when it installs the state-backend providers into the production process roles,
/// because that is the point at which a deployment's own images are chosen.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum LifecycleMode {
    /// M11.T08's mechanism: the update thread decides and publishes; the state task applies
    /// what it finds.
    ///
    /// **Not selectable since M11.T26h.** What it now names is a peer from before the flag
    /// day — see the type documentation — which is the only reason it is still here.
    LegacyT08,
    /// M11.D39a's mechanism: the update thread validates, classifies and enqueues a
    /// versioned intent; the job's state task is the sole decider and publisher, under a
    /// durable fence (D39d) and an acknowledged worker protocol (D39e).
    ///
    /// This is [`Self::SELECTED`] since M11.T26h's activation change.
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
    /// [`Self::is_available_in_production`], so "production selects `FencedV2`" is a
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
    /// **M11.T26h moved both answers, in the change that also removed the M11.T08 mechanisms
    /// this one supersedes.** That is the flag day, and it is one edit rather than two: an
    /// activation without the removals leaves two mechanisms claiming one job, and the
    /// removals without the activation leave a production path with neither.
    /// `the_activation_change_selects_the_fence_and_removes_every_superseded_t08_guard` is
    /// the single co-occurrence that rejects either half on its own.
    const fn is_available_in_production(self) -> bool {
        match self {
            // Retained, and deliberately not selectable. `LegacyT08` is no longer a mechanism
            // this controller runs; it is the *description of a peer* — a controller or a
            // worker built before the flag day — which is what the mixed-version rollout
            // harness instantiates and what [`Self::requires_lifecycle_fence`] answers `false`
            // for. Removing the variant would delete M11.D75's declared compatibility window
            // from the type system along with the ability to test it.
            LifecycleMode::LegacyT08 => false,
            LifecycleMode::FencedV2 => true,
        }
    }

    /// The mode a job is *already* running under, from whether its transitions are decided by
    /// the M11.D39a single writer.
    ///
    /// Not a selection, and deliberately in this module rather than at its call site. The
    /// choice was made once, when the job's [`JobLifecycle`](super::JobLifecycle) was built
    /// from [`Self::SELECTED`]; this reads that choice back so that a job cannot run the D39a
    /// writer and the pre-fence wire protocol at the same time. Writing the same translation at
    /// a call site would put the name of the mode on a production path outside the module that
    /// defines it, which is exactly what `every_production_path_selects_the_fenced_v2_lifecycle`
    /// counts — and it would be a second thing that could answer differently.
    pub(crate) fn of_job(runs_fenced_lifecycle: bool) -> Self {
        if runs_fenced_lifecycle {
            LifecycleMode::FencedV2
        } else {
            LifecycleMode::LegacyT08
        }
    }

    /// Whether a controller running under this mode requires the worker generations it
    /// registers to fence (M11.D39e(i), M11.D75).
    ///
    /// This is the flag day, and it is one answer rather than two. It is what
    /// `RegisterWorkerResp::requires_lifecycle_fence` carries, and it is what decides whether
    /// this controller's own start and commit directives are fenced — so a controller cannot
    /// tell a worker generation "require a fence of me" and then send it a fence-less start, or
    /// send a fence to a generation it never put into strict mode. A worker's strict mode is
    /// monotone once either switch is thrown, which is exactly why the two must be one
    /// decision: a controller that required fences at registration and then stopped sending
    /// them would have locked its own workers out.
    ///
    /// Exhaustive on purpose, like [`Self::is_available_in_production`]: a mode added without
    /// answering this question does not compile, and cannot inherit an answer by omission.
    pub(crate) const fn requires_lifecycle_fence(self) -> bool {
        match self {
            // The pre-flag-day peer. A controller that sends no fence cannot require one: a
            // worker put into strict mode by such a registration would refuse the very next
            // start it was sent. This is M11.D75's declared compatibility window, and it is
            // the reason the variant outlived its selection.
            LifecycleMode::LegacyT08 => false,
            LifecycleMode::FencedV2 => true,
        }
    }

    /// Whether a job running under this mode records its fencing obligation durably, and
    /// recovers one a previous attempt left (M11.T26f, design M11.D39d).
    ///
    /// One answer for both halves, because they are one mechanism: a controller that wrote
    /// obligations and never recovered them would leave every interrupted job wedged behind a
    /// record nothing discharges, and one that recovered but never wrote would find nothing to
    /// recover. Splitting them into two questions would make "half activated" a state that
    /// compiles.
    ///
    /// Exhaustive on purpose, like [`Self::is_available_in_production`] and
    /// [`Self::requires_lifecycle_fence`]: a mode added without answering this question does not
    /// compile. `LegacyT08` — the pre-flag-day peer — writes no durable fencing record and takes
    /// no recovery path, which is what
    /// `the_legacy_mechanism_neither_writes_nor_recovers_a_durable_fencing_obligation` checks
    /// against the row rather than against this function; the selected mode's own answer, and
    /// that it is now the fenced one, is
    /// `the_selected_mechanism_writes_and_recovers_a_durable_fencing_obligation`.
    pub(crate) const fn recovers_a_durable_fencing_obligation(self) -> bool {
        match self {
            LifecycleMode::LegacyT08 => false,
            LifecycleMode::FencedV2 => true,
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
