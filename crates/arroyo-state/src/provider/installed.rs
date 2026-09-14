//! The one process cell that says which [`ProviderRegistry`] this process runs on
//! (risk M11.T09p).
//!
//! The registry itself is an ordinary immutable value — see [`mod@super::registry`] — and every
//! test builds one directly. This module is the narrow part that production needs and tests
//! do not: a write-once cell, reconciling the design's "process-global registry" with risk
//! M11.T09p's "no resettable global".
//!
//! - [`registry`] initialises the cell to [`ProviderRegistry::parquet_default`] if nothing
//!   has been installed, so a process that never installs anything behaves exactly as
//!   Arroyo did before the seam existed.
//! - [`install`] fails if the cell already holds a registry — including one that
//!   [`registry`] defaulted into it — because a late install that silently did not take
//!   effect is how a job ends up running on a backend nobody selected. The two cases are
//!   different [`InstallError`] variants, since they call for different fixes.
//! - There is no reset, under `cfg(test)` or otherwise.
//!
//! A refused [`install`] leaves the installed registry exactly as it was: the refusal tells
//! a caller that *its* registry is not the one in force, and nothing about the process is
//! left half-changed. That is the sense in which installing twice is a sequencing mistake
//! rather than a corrupted process.

use std::sync::OnceLock;

use super::ProviderRegistry;
use thiserror::Error;

/// How the process cell came to hold the registry it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// A caller installed it through [`install`].
    Installed,
    /// [`registry`] defaulted it in because nothing had been installed when a provider
    /// was first needed.
    DefaultedByFirstUse,
}

/// The registry in force, and how it got there.
struct Installed {
    registry: ProviderRegistry,
    origin: Origin,
}

/// The one process cell. Written at most once; every read after that write is an atomic
/// load, so no lookup on a running job waits on anything.
static INSTALLED: OnceLock<Installed> = OnceLock::new();

/// The registry this process runs on.
///
/// If nothing was installed, this installs [`ProviderRegistry::parquet_default`] and
/// returns that — the first call decides, and every later call returns the same value.
/// Calling this before [`install`] therefore does not merely read early: it *fixes* the
/// registry, which is why a later [`install`] is refused with
/// [`InstallError::DefaultedByFirstUse`].
pub fn registry() -> &'static ProviderRegistry {
    &INSTALLED
        .get_or_init(|| Installed {
            registry: ProviderRegistry::parquet_default(),
            origin: Origin::DefaultedByFirstUse,
        })
        .registry
}

/// Installs `registry` as the one this process runs on.
///
/// Call this before anything constructs a table or opens a view — before any operator
/// context is built, and before any checkpoint is loaded.
///
/// # Errors
///
/// Returns [`InstallError::AlreadyInstalled`] if a registry was installed earlier, and
/// [`InstallError::DefaultedByFirstUse`] if [`registry`] already defaulted one in. In
/// both cases the registry already in force is unchanged and still serving, and the
/// `registry` argument is dropped: the error says that this call had no effect, not that
/// the process is inconsistent.
pub fn install(registry: ProviderRegistry) -> Result<(), InstallError> {
    if INSTALLED
        .set(Installed {
            registry,
            origin: Origin::Installed,
        })
        .is_err()
    {
        return Err(
            match INSTALLED
                .get()
                .expect("`set` fails only when the cell is already written")
                .origin
            {
                Origin::Installed => InstallError::AlreadyInstalled,
                Origin::DefaultedByFirstUse => InstallError::DefaultedByFirstUse,
            },
        );
    }
    Ok(())
}

/// Why an [`install`] did not take effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InstallError {
    /// Another registry was installed first. Two installs in one process means two pieces
    /// of code each believe they choose the backends; one of them is wrong.
    #[error(
        "a provider registry is already installed in this process; a registry is \
         installed once, before any table is constructed"
    )]
    AlreadyInstalled,

    /// A provider was looked up before this install ran, which defaulted the parquet
    /// registry into the cell. The fix is to install earlier, not to install again.
    #[error(
        "this process already defaulted to the parquet provider registry, because a \
         provider was looked up before this install; install the registry before any \
         table is constructed"
    )]
    DefaultedByFirstUse,
}
