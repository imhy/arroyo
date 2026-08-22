// The permitted shape of a settlement owner: it keeps the whole obligation.
//
// This is what M11.T26's owner must look like, and it is what the two test doubles in
// `phase_tests.rs` and `fanout_tests.rs` do. Reading what the obligation lists borrows it and
// takes nothing out, so an owner can decide what it owes and still be holding all of it.
use crate::scheduling::fanout::settlement::{SettlementBundle, SettlementOwner};

pub struct KeepingOwner {
    held: std::sync::Mutex<Option<SettlementBundle>>,
}

impl SettlementOwner for KeepingOwner {
    fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle> {
        let _outstanding = bundle.issued().outstanding_count();
        *self.held.lock().unwrap() = Some(bundle);
        Ok(())
    }
}
