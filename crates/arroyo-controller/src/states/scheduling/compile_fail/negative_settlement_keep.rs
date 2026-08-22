// The other half of the same finding, under the sibling name: an owner keeps the inventory,
// drops the *authority* that stands behind it, and answers `Ok(())`.
//
// `keep` is `into_parts` with a different name, so closing one door and leaving this one open
// would have closed nothing. This is also the more damaging half: the job's publication lock is
// released while attempts are outstanding, and the receipt says they moved.
use crate::scheduling::fanout::IssuedAttempts;
use crate::scheduling::fanout::settlement::{SettlementBundle, SettlementOwner};

pub struct InventoryOnlyOwner {
    listed: std::sync::Mutex<Option<IssuedAttempts>>,
}

impl SettlementOwner for InventoryOnlyOwner {
    fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle> {
        let (admission, issued) = bundle.keep();
        drop(admission);
        *self.listed.lock().unwrap() = Some(issued);
        Ok(())
    }
}
