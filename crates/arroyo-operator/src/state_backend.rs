//! The one place a job's state-backend selector is written into its table configs.
//!
//! Operators declare their tables with `fn tables()`, and none of those ~25
//! implementations knows or sets a backend: they all converge on
//! [`OperatorContext::new`](crate::context::OperatorContext::new), which calls
//! [`apply_job_state_backend`] before the configs are used for anything. Keeping the
//! selector out of `tables()` is deliberate (design item M11.D13b) — a table helper
//! cannot forget a field it never writes.

use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::state_backend::{
    SelectorScope, StateBackendError, StateBackendSelector, validate_agreement,
};
use std::collections::HashMap;

/// Checks every table config that states a backend against the job's selector, then
/// stamps that selector into all of them.
///
/// `job` is the authoritative value the job was started with, carried down through
/// `TaskInfo`; it is already normalized and is never re-derived here.
///
/// This is the point at which a table config *acquires* its selector, so an empty field
/// here means "the producer has not stated one" rather than "parquet". Every `tables()`
/// implementation leaves it empty by design, so reading empty as parquet would make a
/// stateengine job reject its own tables. Empty entries are therefore not validated —
/// they are simply stamped, which is how the job's value gets there in the first place —
/// while every entry that *does* state a backend is checked against the job by
/// [`validate_agreement`]. The compatibility guarantee is unaffected: a config written
/// before the field existed is empty, and under a parquet job it leaves here as
/// `"parquet"`, exactly what [`StateBackendSelector::normalize`] would have made of it.
/// A persisted selector that is read back rather than produced here — checkpoint
/// metadata — is normalized in the usual way by its own reader.
///
/// On success every config in `tables` carries the job's selector explicitly, so nothing
/// downstream — the table manager, the writers, the checkpointers — has to re-derive it
/// or fall back to a default. On failure `tables` is left exactly as it was found: the
/// caller must abort, and a half-stamped map would be a lie about what was validated.
///
/// # Errors
///
/// Returns the [`StateBackendError`] that [`validate_agreement`] raised: an unknown
/// value, two tables selecting different backends, or a table disagreeing with the job.
/// Every variant names the offending table. The error is returned rather than defaulted
/// or logged, because it must reach the controller before any state is created or read.
pub(crate) fn apply_job_state_backend(
    job: StateBackendSelector,
    tables: &mut HashMap<String, TableConfig>,
) -> Result<(), StateBackendError> {
    validate_agreement(
        job,
        SelectorScope::Table,
        tables
            .iter()
            .map(|(name, config)| (name.as_str(), config.state_backend.as_str()))
            .filter(|(_, stated)| !stated.is_empty()),
    )?;

    for config in tables.values_mut() {
        config.state_backend.clear();
        config.state_backend.push_str(job.as_str());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::apply_job_state_backend;
    use arroyo_rpc::grpc::rpc::{TableConfig, TableEnum};
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use prost::Message;
    use std::collections::HashMap;

    fn config(state_backend: &str) -> TableConfig {
        TableConfig {
            table_type: TableEnum::GlobalKeyValue as i32,
            config: vec![],
            state_version: 0,
            state_backend: state_backend.to_string(),
        }
    }

    fn tables<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> HashMap<String, TableConfig> {
        entries
            .into_iter()
            .map(|(name, backend)| (name.to_string(), config(backend)))
            .collect()
    }

    /// The property the whole design rests on: `tables()` implementations never write the
    /// field, so the boundary must fill it in for *every* table it is handed, not just
    /// the first. Asserting over a whole map means a table helper added later is covered
    /// by construction — it cannot omit a field it never touches. Both jobs are exercised
    /// because both get the same all-empty input: a stateengine job's tables arrive as
    /// blank as a parquet job's, and must leave stamped `"stateengine"`.
    #[test]
    fn every_table_config_leaves_the_boundary_with_the_job_selector() {
        for job in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            let mut map = tables([
                ("s:source", ""),
                ("w:window", ""),
                ("g:global", ""),
                ("j:join_left", ""),
                ("j:join_right", ""),
            ]);
            let names: Vec<_> = {
                let mut n: Vec<_> = map.keys().cloned().collect();
                n.sort();
                n
            };

            apply_job_state_backend(job, &mut map).unwrap();

            for name in &names {
                assert_eq!(
                    map[name].state_backend,
                    job.as_str(),
                    "table {name:?} did not leave the boundary carrying {job}"
                );
            }
            // Stamping rewrites one field and nothing else.
            let mut after: Vec<_> = map.keys().cloned().collect();
            after.sort();
            assert_eq!(after, names);
            assert!(map.values().all(|c| c.state_version == 0));
        }
    }

    /// The compatibility guarantee. proto3 omits default-valued scalars, so a
    /// `TableConfig` serialized before this field existed is byte-identical to one
    /// serialized now with an empty selector — neither carries a field-4 tag. It decodes
    /// as empty, empty means parquet, and a parquet job accepts it and leaves it saying
    /// so explicitly.
    #[test]
    fn table_config_written_before_the_field_existed_is_parquet() {
        assert!(TableConfig::default().state_backend.is_empty());
        assert_eq!(
            StateBackendSelector::normalize("", "table \"g:global\"").unwrap(),
            StateBackendSelector::Parquet
        );

        let encoded = config("").encode_to_vec();
        assert!(
            !encoded.contains(&0x22),
            "an empty selector must not reach the wire at all: {encoded:?}"
        );

        let decoded = TableConfig::decode(&encoded[..]).unwrap();
        assert!(decoded.state_backend.is_empty());

        let mut map = HashMap::from([("g:global".to_string(), decoded)]);
        apply_job_state_backend(StateBackendSelector::Parquet, &mut map).unwrap();
        assert_eq!(map["g:global"].state_backend, "parquet");
    }

    /// A stamped config survives the trip to the checkpoint metadata and back, so the
    /// value a restored job reads is the value its predecessor wrote — and passing it
    /// back through the boundary is a no-op, because it now agrees with its own job.
    #[test]
    fn explicit_table_selector_round_trips_through_the_wire() {
        for job in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            let mut map = tables([("w:window", "")]);
            apply_job_state_backend(job, &mut map).unwrap();

            let encoded = map["w:window"].encode_to_vec();
            let decoded = TableConfig::decode(&encoded[..]).unwrap();
            assert_eq!(decoded.state_backend, job.as_str());
            assert_eq!(decoded, map["w:window"]);

            let mut again = HashMap::from([("w:window".to_string(), decoded)]);
            apply_job_state_backend(job, &mut again).unwrap();
            assert_eq!(again, map);
        }
    }

    /// A table config that names a backend other than the job's is a hard failure that
    /// names the table — never a config the job quietly overwrites.
    #[test]
    fn table_disagreeing_with_the_job_fails_typed() {
        let mut map = tables([("w:window", "stateengine")]);
        let err = apply_job_state_backend(StateBackendSelector::Parquet, &mut map).unwrap_err();

        assert_eq!(
            err,
            StateBackendError::TableMismatch {
                label: "table \"w:window\"".to_string(),
                found: StateBackendSelector::StateEngine,
                job: StateBackendSelector::Parquet,
            }
        );
        // Nothing was stamped: a rejected map is left exactly as it arrived.
        assert_eq!(map["w:window"].state_backend, "stateengine");
    }

    /// An unrecognized value is rejected, not defaulted to parquet and not laundered into
    /// the job's selector by the stamping step.
    #[test]
    fn unknown_table_selector_fails_typed() {
        let mut map = tables([("s:source", "rocksdb")]);
        let err = apply_job_state_backend(StateBackendSelector::Parquet, &mut map).unwrap_err();

        assert_eq!(
            err,
            StateBackendError::UnknownValue {
                label: "table \"s:source\"".to_string(),
                value: "rocksdb".to_string(),
            }
        );
        assert_eq!(map["s:source"].state_backend, "rocksdb");
    }

    /// A job may use exactly one state backend. Two tables that disagree with each other
    /// are rejected even though one of them agrees with the job, so a mixed-backend job
    /// can never reach the table manager. Which of the two is named depends on the map's
    /// iteration order, so the assertion pins the contract — typed, unstamped, and naming
    /// the table that broke the agreement — rather than one particular ordering.
    #[test]
    fn mixed_table_selectors_fail_typed() {
        // A fresh `HashMap` picks a fresh iteration order, so repeating exercises both
        // orders; the contract below must hold for either one.
        for _ in 0..16 {
            let mut map = tables([("w:window", "parquet"), ("s:source", "stateengine")]);
            let err = apply_job_state_backend(StateBackendSelector::Parquet, &mut map).unwrap_err();

            assert!(
                matches!(
                    err,
                    StateBackendError::MixedSelectors { .. }
                        | StateBackendError::TableMismatch { .. }
                ),
                "{err:?}"
            );
            let message = err.to_string();
            assert!(message.contains("s:source"), "{message}");
            assert!(message.contains("stateengine"), "{message}");

            assert_eq!(map["w:window"].state_backend, "parquet");
            assert_eq!(map["s:source"].state_backend, "stateengine");
        }
    }
}
