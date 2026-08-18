// The environment `states/scheduling/phases.rs` is compiled against by the compile-fail
// harness (M11.T25b, D96 rows 12 and 13).
//
// This is a *stub*, and only a stub: every type here stands in for one the controller
// supplies, so that the phase graph's own source can be compiled by a plain `rustc` with no
// crate dependencies at all. Nothing in it can make a restriction hold or fail — the
// restrictions are properties of `phases.rs`, which is included verbatim below — but a stub
// that no longer matches the real environment would stop `phases.rs` compiling, and the
// positive fixtures exist to make that a loud failure rather than a quiet loss of coverage.
//
// `@PHASES@` is replaced by the harness with an absolute path to the copy of `phases.rs`
// under test, which is either the real file or the real file with one named weakening applied.
#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    unused_mut,
    unused_must_use,
    clippy::all
)]

/// Stands in for `crate::JobMessage`.
pub struct JobMessage;

pub mod states {
    /// Stands in for the landed `states::Admission`: an owned, un-cloneable capability.
    ///
    /// Un-cloneable is the part that matters. The real one owns a
    /// `tokio::sync::OwnedMutexGuard`, so a phase cannot duplicate its authority; a stub that
    /// derived `Clone` would let a fixture do what no real phase can.
    pub struct Admission;

    pub struct StateError;

    pub struct Transition;

    pub struct JobContext<'a> {
        pub marker: std::marker::PhantomData<&'a mut ()>,
    }
}

pub mod scheduling {
    pub mod admission {
        use super::fanout::{IssuedAttempts, SettlementOwner};
        use super::fencing::Interrupted;
        use crate::states::{Admission, JobContext, StateError, Transition};

        pub enum PhaseWait {
            Continue,
            Leave(Transition),
        }

        pub enum Admitted {
            Region(Admission),
            Leave(Transition),
        }

        pub struct PhaseContext<'a, 'ctx> {
            marker: std::marker::PhantomData<&'a mut JobContext<'ctx>>,
        }

        impl<'a, 'ctx> PhaseContext<'a, 'ctx> {
            pub fn new(_ctx: &'a mut JobContext<'ctx>) -> Self {
                unimplemented!()
            }
            pub fn stop_if_desired(&self) -> Option<Transition> {
                unimplemented!()
            }
            pub async fn admit(&mut self) -> Result<Admitted, StateError> {
                unimplemented!()
            }
            pub fn observe_intent_in_wait(&mut self) -> Result<PhaseWait, StateError> {
                unimplemented!()
            }
            pub fn observe_before_phase(&mut self) -> Result<PhaseWait, StateError> {
                unimplemented!()
            }
            pub fn begin_wait(&mut self) {}
            pub async fn persist_generation(&mut self, _a: &Admission) -> Result<(), StateError> {
                unimplemented!()
            }
            pub async fn tear_down_existing_cluster(&mut self, _a: &Admission) {}
            pub async fn start_replacement_workers(
                &mut self,
                _a: &Admission,
            ) -> Result<(), StateError> {
                unimplemented!()
            }
            pub async fn prepare_recovery_checkpoint(
                &mut self,
                _a: &Admission,
            ) -> Result<(), StateError> {
                unimplemented!()
            }
            pub async fn await_message_from_workers(&mut self) -> Result<PhaseWait, StateError> {
                unimplemented!()
            }
            pub fn workers_are_sufficient(&self) -> bool {
                unimplemented!()
            }
            pub async fn await_worker_channels(&mut self) -> Result<(), StateError> {
                unimplemented!()
            }
            pub fn require_reconciling_workers(&self) -> Result<(), StateError> {
                unimplemented!()
            }
            pub async fn fan_out_start_execution(
                &mut self,
                _a: Admission,
            ) -> (Admission, IssuedAttempts, Result<(), StateError>) {
                unimplemented!()
            }
            pub fn settlement_owner(&self) -> Option<std::sync::Arc<dyn SettlementOwner>> {
                None
            }
            pub async fn await_message_from_tasks(&mut self) -> Result<PhaseWait, StateError> {
                unimplemented!()
            }
            pub fn tasks_are_all_started(&self) -> bool {
                unimplemented!()
            }
            pub async fn prepare_handover(&mut self) {}
            pub fn needs_restored_commits(&self) -> bool {
                unimplemented!()
            }
            pub async fn publish_restored_commits(&mut self, _a: &Admission) {}
            pub async fn into_transition(self) -> Result<Transition, (Self, StateError)> {
                unimplemented!()
            }
            pub fn into_fencing(
                self,
                _reason: StateError,
                _outstanding: IssuedAttempts,
            ) -> Interrupted<'a, 'ctx> {
                unimplemented!()
            }
        }
    }

    pub mod fanout {
        use crate::states::Admission;

        #[derive(Default)]
        pub struct IssuedAttempts;

        pub struct SettlementBundle;

        pub struct SettlementReceipt;

        impl SettlementReceipt {
            pub fn outstanding(&self) -> usize {
                0
            }
        }

        pub trait SettlementOwner: Send + Sync {
            fn take_over(&self, bundle: SettlementBundle);
        }

        pub enum SettlementOutcome {
            Transferred(SettlementReceipt),
            SettledInPlace(Admission, IssuedAttempts),
        }

        impl SettlementBundle {
            pub fn new(_admission: Admission, _issued: IssuedAttempts) -> Self {
                unimplemented!()
            }
        }

        pub fn hand_over(
            _bundle: SettlementBundle,
            _owner: Option<&dyn SettlementOwner>,
        ) -> SettlementOutcome {
            unimplemented!()
        }
    }

    pub mod fencing {
        use super::admission::PhaseContext;
        use crate::states::{StateError, Transition};

        pub struct Fencing<'a, 'ctx> {
            marker: std::marker::PhantomData<PhaseContext<'a, 'ctx>>,
        }

        impl Fencing<'_, '_> {
            pub fn note_transferred(&mut self, _attempts: usize) {}
        }

        pub struct Interrupted<'a, 'ctx> {
            marker: std::marker::PhantomData<Fencing<'a, 'ctx>>,
        }

        impl<'a, 'ctx> Interrupted<'a, 'ctx> {
            pub fn fencing_mut(&mut self) -> &mut Fencing<'a, 'ctx> {
                unimplemented!()
            }
            pub fn reconcile_and_report(self) -> Result<Transition, StateError> {
                unimplemented!()
            }
        }
    }

    pub mod phases {
        include!("@PHASES@");
    }
}
