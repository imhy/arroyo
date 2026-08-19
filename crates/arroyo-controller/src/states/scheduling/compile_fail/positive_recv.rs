// POSITIVE fixture for D96 row 13, `token_owning_phase_cannot_recv`.
//
// The permitted use: a token-free phase waits on the job's channel. This must compile.

use crate::scheduling::phases::AwaitingWorkers;

async fn a_token_free_phase_may_wait(mut awaiting: AwaitingWorkers<'_, '_>) -> bool {
    let _ = awaiting.await_message().await;
    awaiting.workers_are_sufficient()
}
