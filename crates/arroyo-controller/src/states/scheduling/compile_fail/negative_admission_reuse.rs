// NEGATIVE fixture for D96 row 12, `irreversible_phases_consume_admission`.
//
// Two irreversible effects from one admission, without threading the phase the first one
// returned. The intended diagnostic is a *move* error — E0382, "use of moved value" — because
// `persist_generation` takes `self` and therefore consumes the `Admission` inside it.
//
// `preamble` is declared `mut` so that the only thing standing between this and a successful
// compilation is the by-value receiver: under the weakening that makes `persist_generation`
// borrow instead of consume, this same fixture compiles.

use crate::scheduling::phases::Preamble;

async fn two_effects_from_one_admission(mut preamble: Preamble<'_, '_>) {
    let _ = preamble.persist_generation().await;
    let _ = preamble.tear_down_existing_cluster().await;
}
