// NEGATIVE fixture for D96 row 13, `token_owning_phase_cannot_recv`.
//
// A phase that holds the admission tries to wait on the job's channel. The intended diagnostic
// is E0599, "no method named `await_message` found for struct `Preamble`": the wait is not
// merely discouraged while a token is held, it is unreachable, because the only route to the
// job's channel is a `PhaseContext` the preamble owns privately and exposes no wait through.
//
// Holding an admission across a wait would make the job unrefusable for exactly as long as it
// waited, and could not terminate if what it waited for was the refusal.

use crate::scheduling::phases::Preamble;

async fn a_token_owning_phase_waits(mut preamble: Preamble<'_, '_>) {
    let _ = preamble.await_message().await;
}
