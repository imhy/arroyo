//! The one walk that turns a global key/value load into a [`GlobalKeyedView`] (design
//! item M11.D15c).
//!
//! Everything typed about a global keyed table lives here: the bincode decode, the
//! migration decision, and the view the two [`TableManager`] getters hand out. Below this
//! module there are only bytes — [`GlobalKeyValueLoad`] yields bounded pages of
//! `(key bytes, value bytes)` and knows nothing about `K`, `V` or Arrow. Above it the view
//! is unchanged.
//!
//! # There is one walk, and two decoders
//!
//! `GlobalKeyedTable::load_with_version` and `GlobalKeyedTable::memory_view_migratable`
//! used to be two copies of the same file-and-batch walk that differed only in how they
//! turned two byte slices into a `(K, V)` — one decoded, the other decoded and migrated.
//! Both copies are gone. [`drain`] is the only walk over a load, and the difference between
//! the two restore paths is the [`PageDecode`] it is given:
//!
//! | Restore path | Decoder | Version handling | View version |
//! |---|---|---|---|
//! | [`view`] | [`Plain`] | ignored, as `load_with_version(.., 0)` ignored it | `0` |
//! | [`view_migratable`] | [`Migrating`] | one step back is migrated, further back is refused | `V::VERSION` |
//!
//! # Validate the page, then commit it
//!
//! A page reaches the map only after every one of its entries has decoded. The staging
//! vector is one page's worth, so it is bounded by the same limits the page is, and a page
//! that fails half-way through contributes nothing: the map under construction is a local
//! that the error drops, so there is no half-built view for a caller to observe — a caller
//! receives a `GlobalKeyedView` or a [`StateError`], never a partial one. `TableManager`
//! caches the view only on the success path, so a failed restore also leaves nothing behind
//! to be found by the next call.
//!
//! Each page is checked against the bound its loader declared before it is decoded, so a
//! backend that over-fills a page fails the restore rather than spending the memory
//! silently (M11.D36).
//!
//! [`TableManager`]: crate::tables::table_manager::TableManager

use std::collections::HashMap;
use std::marker::PhantomData;

use arroyo_rpc::errors::StateError;
use arroyo_types::{Data, Key};
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

use super::GlobalKeyedView;
use crate::tables::MigratableState;
use crate::tables::global_key_value_load::{
    GlobalKeyValueLoad, GlobalKeyValuePage, LoadPageLimits,
};
use crate::{BINCODE_CONFIG, StateMessage};

/// Restores the view `TableManager::get_global_keyed_state` hands out.
///
/// The state version each source was written under is not consulted, which is what
/// `load_with_version(state_tx, 0)` did: this path reads values that are already in the
/// shape `V` describes.
///
/// # Errors
///
/// Returns the [`StateError`] the loader raises, a bincode failure from a key or value that
/// is not a `K` or a `V`, and [`StateError::Other`] for a page wider than its loader's
/// declared bound.
pub(crate) async fn view<K: Key, V: Data>(
    table_name: &str,
    loader: Box<dyn GlobalKeyValueLoad + Send>,
    state_tx: Sender<StateMessage>,
) -> Result<GlobalKeyedView<K, V>, StateError> {
    let decode = Plain::<K, V>::new();
    let version = decode.view_version();
    let data = drain(table_name, loader, decode).await?;
    Ok(GlobalKeyedView {
        table_name: table_name.to_string(),
        data,
        state_tx,
        version,
    })
}

/// Restores the view `TableManager::get_global_keyed_state_migratable` hands out,
/// migrating sources written one version back.
///
/// # Errors
///
/// Everything [`view`] returns, plus [`StateError::UnsupportedStateVersion`] for a source
/// more than one version behind `V::VERSION`, and whatever `V::migrate` returns.
pub(crate) async fn view_migratable<K: Key, V: MigratableState>(
    table_name: &str,
    loader: Box<dyn GlobalKeyValueLoad + Send>,
    state_tx: Sender<StateMessage>,
) -> Result<GlobalKeyedView<K, V>, StateError> {
    let decode = Migrating::<K, V>::new(table_name);
    let version = decode.view_version();
    let data = drain(table_name, loader, decode).await?;
    Ok(GlobalKeyedView {
        table_name: table_name.to_string(),
        data,
        state_tx,
        version,
    })
}

/// Drains `loader` and decodes every page into one map.
async fn drain<K: Key, V: Data, D: PageDecode<K, V>>(
    table_name: &str,
    mut loader: Box<dyn GlobalKeyValueLoad + Send>,
    mut decode: D,
) -> Result<HashMap<K, V>, StateError> {
    let limits = loader.page_limits();
    let mut data = HashMap::new();

    while let Some(page) = loader.next_page().await? {
        if !limits.admits(&page) {
            return Err(oversized_page(table_name, &page, limits));
        }
        decode.admit(&page)?;

        let mut decoded = Vec::with_capacity(page.len());
        for (key, value) in page.entries() {
            decoded.push(decode.entry(key, value)?);
        }
        data.extend(decoded);
    }

    let peak = loader.peak_page();
    debug!(
        table = table_name,
        entries = data.len(),
        peak_page_entries = peak.entries(),
        peak_page_payload_bytes = peak.payload_bytes(),
        "restored global key/value state"
    );
    Ok(data)
}

/// A loader handed out a page wider than the bound it declared.
fn oversized_page(
    table_name: &str,
    page: &GlobalKeyValuePage,
    limits: LoadPageLimits,
) -> StateError {
    StateError::Other {
        table: table_name.to_string(),
        error: format!(
            "the state backend loading {table_name} produced a page of {} entries and {} \
             payload bytes from {}, which exceeds the bound it declared of {} entries and \
             {} payload bytes",
            page.len(),
            page.payload_bytes(),
            page.source(),
            limits.max_entries(),
            limits.max_payload_bytes(),
        ),
    }
}

/// How one restore path turns a page's bytes into `(K, V)`.
///
/// [`Self::admit`] sees each page before any of its entries are decoded, which is what lets
/// a decoder refuse a source's state version before reading it — the reason the loader
/// announces a source with a page of its own.
trait PageDecode<K, V> {
    /// Accepts `page`'s state version, or refuses the whole restore.
    fn admit(&mut self, page: &GlobalKeyValuePage) -> Result<(), StateError>;

    /// Decodes one entry under the version last admitted.
    fn entry(&self, key: &[u8], value: &[u8]) -> Result<(K, V), StateError>;

    /// The version the restored view reports to its checkpointer.
    fn view_version(&self) -> u32;
}

/// The decode of a table whose values are already in `V`'s shape.
struct Plain<K, V> {
    typed: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Plain<K, V> {
    const fn new() -> Self {
        Self { typed: PhantomData }
    }
}

impl<K: Key, V: Data> PageDecode<K, V> for Plain<K, V> {
    /// Accepts every version, because this path never read one.
    fn admit(&mut self, _page: &GlobalKeyValuePage) -> Result<(), StateError> {
        Ok(())
    }

    fn entry(&self, key: &[u8], value: &[u8]) -> Result<(K, V), StateError> {
        Ok((
            bincode::decode_from_slice(key, BINCODE_CONFIG)?.0,
            bincode::decode_from_slice(value, BINCODE_CONFIG)?.0,
        ))
    }

    fn view_version(&self) -> u32 {
        0
    }
}

/// The decode of a table whose sources may be one version behind `V`.
///
/// `migrating` is set from each page's own state version, so a load that mixes a
/// current-version source with a one-version-old source reads each of them correctly — the
/// same per-file decision the migratable walk made, taken per page instead.
struct Migrating<K, V> {
    table_name: String,
    migrating: bool,
    announced: Option<String>,
    typed: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Migrating<K, V> {
    fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            migrating: false,
            announced: None,
            typed: PhantomData,
        }
    }
}

impl<K: Key, V: MigratableState> PageDecode<K, V> for Migrating<K, V> {
    /// Decides whether this page's entries need migrating, refusing a version this build
    /// cannot read.
    ///
    /// Only one step back is supported, which is the rule the migratable walk enforced per
    /// file. The log line fires once per source, when a source that needs migrating is
    /// first seen.
    fn admit(&mut self, page: &GlobalKeyValuePage) -> Result<(), StateError> {
        let found = page.state_version();
        self.migrating = if found == V::VERSION {
            false
        } else if found + 1 == V::VERSION {
            true
        } else {
            // we only support 1 step migration at this point
            return Err(StateError::UnsupportedStateVersion {
                table: self.table_name.clone(),
                found,
                expected: V::VERSION,
            });
        };

        if self.announced.as_deref() != Some(page.source()) {
            if self.migrating {
                info!(
                    "Migrating state for table '{}' in {} from version {} to version {}",
                    self.table_name,
                    page.source(),
                    found,
                    V::VERSION
                );
            }
            self.announced = Some(page.source().to_string());
        }
        Ok(())
    }

    fn entry(&self, key: &[u8], value: &[u8]) -> Result<(K, V), StateError> {
        if self.migrating {
            let decoded_key: K = bincode::decode_from_slice(key, BINCODE_CONFIG)?.0;
            let old_value: V::PreviousVersion =
                bincode::decode_from_slice(value, BINCODE_CONFIG)?.0;
            Ok((decoded_key, V::migrate(old_value)?))
        } else {
            Ok((
                bincode::decode_from_slice(key, BINCODE_CONFIG)?.0,
                bincode::decode_from_slice(value, BINCODE_CONFIG)?.0,
            ))
        }
    }

    fn view_version(&self) -> u32 {
        V::VERSION
    }
}
