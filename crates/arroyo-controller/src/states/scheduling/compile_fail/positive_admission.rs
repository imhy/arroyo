// POSITIVE fixture for D96 row 12, `irreversible_phases_consume_admission`.
//
// The permitted use of the phase API: each irreversible effect consumes the phase that holds
// the admission and hands a fresh one back, so a preamble that wants to do two things threads
// the second through the result of the first. This must compile.

use crate::scheduling::phases::{AwaitingWorkers, Preamble};

async fn two_effects_threaded_through_the_token<'a, 'ctx>(
    preamble: Preamble<'a, 'ctx>,
) -> Option<AwaitingWorkers<'a, 'ctx>> {
    let preamble = preamble.persist_generation().await.ok()?;
    let preamble = preamble.tear_down_existing_cluster().await.ok()?;
    Some(preamble.release())
}
