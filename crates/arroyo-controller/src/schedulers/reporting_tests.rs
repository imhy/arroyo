//! What each scheduler's live-worker listing says about termination (M11.T26f, design
//! M11.D39e(v)).
//!
//! [`Scheduler::generation_termination_reporting`] is the one place the controller decides
//! whether "this scheduler does not list that worker" may be read as *"that worker generation
//! has terminated"* — one of the exactly three facts M11.D39e(v) allows to settle an issued
//! `StartExecution`. Every implementation answers it, and the answers are not interchangeable:
//! reading an untracking scheduler's empty listing as a termination would settle every target of
//! every job the moment it was asked, which is the false settlement the whole fence exists to
//! prevent.
//!
//! The row below is a table over every implementation in the crate rather than one assertion per
//! scheduler, because the property is a property of the *set*: a scheduler added later that
//! answers `Authoritative` without listing by [`WorkerId`] is the mistake, and a table is what
//! makes the new row visible next to the ones it has to justify itself against.

use arroyo_rpc::config::config;

use super::embedded::EmbeddedScheduler;
use super::kubernetes::KubernetesScheduler;
use super::{
    GenerationTerminationReporting, ManualScheduler, NodeScheduler, ProcessScheduler, Scheduler,
};

/// Every scheduler states whether its listing can observe a worker generation's termination,
/// and the two that cannot say so with the report an operator reads.
///
/// The `Untracked` arms are asserted whole — scheduler name and reason — rather than by variant,
/// because the reason is the payload: a job that will not leave `Fencing` because its
/// deployment's scheduler keeps no worker registry is a different report from one held by a
/// partitioned worker, and this is the only place the difference is written down.
///
/// Kubernetes is `Untracked` because its pod listing maps every pod to `WorkerId(1)`. That is a
/// live availability cost — a K8s deployment discharges a recovered obligation only by
/// acknowledgement — and it is recorded here so that "fix the listing" and "flip this answer"
/// stay one change rather than two.
#[test]
fn every_scheduler_says_whether_its_listing_can_observe_a_generation_termination() {
    let process = ProcessScheduler::new();
    let node = NodeScheduler::new();
    let embedded = EmbeddedScheduler::new();
    let manual = ManualScheduler::new();
    let kubernetes = KubernetesScheduler::with_config(None, config().kubernetes_scheduler.clone());

    let answers: Vec<(&str, GenerationTerminationReporting)> = vec![
        ("process", process.generation_termination_reporting()),
        ("node", node.generation_termination_reporting()),
        ("embedded", embedded.generation_termination_reporting()),
        ("manual", manual.generation_termination_reporting()),
        ("kubernetes", kubernetes.generation_termination_reporting()),
    ];

    assert_eq!(
        answers,
        vec![
            ("process", GenerationTerminationReporting::Authoritative),
            ("node", GenerationTerminationReporting::Authoritative),
            ("embedded", GenerationTerminationReporting::Authoritative),
            (
                "manual",
                GenerationTerminationReporting::Untracked {
                    scheduler: "manual",
                    why: "its workers are started by an operator and it keeps no registry of \
                          them, so an empty listing says nothing about whether a generation has \
                          terminated",
                },
            ),
            (
                "kubernetes",
                GenerationTerminationReporting::Untracked {
                    scheduler: "kubernetes",
                    why: "its pod listing does not carry the worker id the controller assigned, \
                          so it cannot say that a particular worker generation has terminated",
                },
            ),
        ],
        "every scheduler in the crate answers, and the three that own their worker processes by \
         id are the only ones whose empty listing settles anything"
    );
}
