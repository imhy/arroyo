-- The state backend a job stores its operator state in ('parquet' or 'stateengine').
-- The default is the empty string, not 'parquet': it is byte-identical to the protobuf
-- default for a string field, so rows written before this column existed and start
-- requests sent by an older controller are indistinguishable, and exactly one place in
-- Rust (StateBackendSelector::normalize) maps that empty value to 'parquet'.
ALTER TABLE job_configs ADD COLUMN state_backend TEXT NOT NULL DEFAULT '';
