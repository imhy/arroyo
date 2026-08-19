// NEGATIVE fixture for D96 row 12, `irreversible_phases_consume_admission`, second half:
// a phase that holds no admission exposes no irreversible effect (DoD M11.T25h).
//
// The intended diagnostic is E0599, "no method named `persist_generation` found for struct
// `AwaitingWorkers`": the wait for workers is token-free, so the effect is not merely
// forbidden to it, it does not exist on it.

use crate::scheduling::phases::AwaitingWorkers;

async fn effect_without_a_token(mut awaiting: AwaitingWorkers<'_, '_>) {
    let _ = awaiting.persist_generation().await;
}
