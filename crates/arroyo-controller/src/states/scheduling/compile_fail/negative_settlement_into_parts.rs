// Review comment `5369004357`, written out: an owner takes the obligation apart, keeps the
// authority, drops the inventory, and still answers `Ok(())`.
//
// Before the fix this compiled and was reported as a transfer — `into_parts` clears the field
// `Drop` reads, so nothing raised the flag `transfer_to` inspects, and a `SettlementReceipt`
// was issued for attempts no owner had a record of.
use crate::scheduling::fanout::settlement::{SettlementBundle, SettlementOwner};
use crate::states::Admission;

pub struct AuthorityOnlyOwner {
    held: std::sync::Mutex<Option<Admission>>,
}

impl SettlementOwner for AuthorityOnlyOwner {
    fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle> {
        let (admission, issued) = bundle.into_parts();
        drop(issued);
        *self.held.lock().unwrap() = Some(admission);
        Ok(())
    }
}
