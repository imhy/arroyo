// The environment `states/scheduling/fanout/settlement.rs` is compiled against by the
// compile-fail harness (review comment `5369004357`).
//
// This is a *stub*, and only a stub, for the same reason `stub.rs` is one: every type here
// stands in for one the controller supplies, so that the transfer seam's own source can be
// compiled by a plain `rustc` with no crate dependencies at all. Nothing in it can make the
// restriction hold or fail — the restriction is that `SettlementBundle::into_parts` and
// `SettlementBundle::keep` are private, which is a property of the file included below — but a
// stub that no longer matches the real environment would stop that file compiling, and the
// positive fixture exists to make that a loud failure rather than a quiet loss of coverage.
//
// `@SETTLEMENT@` is replaced by the harness with an absolute path to the copy of
// `settlement.rs` under test, which is either the real file or the real file with one named
// weakening applied.
#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    unused_mut,
    unused_must_use,
    clippy::all
)]

// Stands in for the `tracing` crate, without adding one.
//
// `use tracing::{error, info, warn}` needs `tracing` to *be* a crate — a module of that name
// does not satisfy a `use` path's first segment — so the fixture crate is aliased to the name
// and exports the three macros the seam logs through. They discard their arguments rather than
// formatting them: what is under test is which operations a caller outside the module may
// reach, and a log line is neither.
extern crate self as tracing;

#[macro_export]
macro_rules! error {
    ($($ignored:tt)*) => {};
}
#[macro_export]
macro_rules! info {
    ($($ignored:tt)*) => {};
}
#[macro_export]
macro_rules! warn {
    ($($ignored:tt)*) => {};
}

pub mod states {
    /// Stands in for the landed `states::Admission`: an owned, un-cloneable capability.
    ///
    /// Un-cloneable is the part that matters here too. The real one owns a
    /// `tokio::sync::OwnedMutexGuard`, so an owner cannot duplicate the authority it was
    /// handed; a stub that derived `Clone` would let a fixture keep a copy of the very thing
    /// the seam is watching it part with.
    pub struct Admission;
}

pub mod scheduling {
    pub mod fanout {
        /// Stands in for the fan-out's issued-attempt inventory.
        ///
        /// Only the three operations `settlement.rs` performs on it are supplied: it is built
        /// empty, it is moved out of a bundle with `mem::take`, and it is asked what it owes.
        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub struct IssuedAttempts {
            outstanding: usize,
            issued: usize,
        }

        impl IssuedAttempts {
            pub fn outstanding_count(&self) -> usize {
                self.outstanding
            }

            pub fn issued_count(&self) -> usize {
                self.issued
            }
        }

        pub mod settlement {
            include!("@SETTLEMENT@");
        }
    }
}
