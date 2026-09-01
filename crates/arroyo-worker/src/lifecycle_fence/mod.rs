//! The worker half of the lifecycle-fence protocol (M11.T26d, design M11.D39d/M11.D39e).
//!
//! [`guard`] serializes fence advancement, revocation and `StartExecution` admission under the
//! one lock the worker's execution phase already lives behind; [`attempt_ids`] is the bounded
//! per-generation record of which attempt identifiers this generation applied and which are
//! permanently non-applicable. Between them they carry M11.D39d's closing claim: a start either
//! linearizes before the fence acknowledgement and is reported applied, or after it and is
//! rejected stale, with no validate→apply gap in between.
//!
//! Everything here is scoped to *one worker generation*, because the process is one: its
//! identity is fixed when `WorkerServer` is built, and a directive addressed to any other
//! generation is refused rather than answered.

pub(crate) mod attempt_ids;
pub(crate) mod guard;

#[cfg(test)]
mod attempt_ids_tests;
#[cfg(test)]
mod commit_tests;
/// M11.D39g's declared fault model, as named reusable injections (M11.T26g).
#[cfg(test)]
mod faults;
#[cfg(test)]
mod faults_tests;
#[cfg(test)]
mod guard_tests;
#[cfg(test)]
mod refusal_tests;
#[cfg(test)]
mod rollout_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wiring_tests;
