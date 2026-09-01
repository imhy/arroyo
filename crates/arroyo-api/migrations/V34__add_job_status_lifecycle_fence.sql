-- The durable authority a controller must hold to write this job's row (M11.D39d).
-- `lifecycle_fence` is monotonic and is incremented by the controller that adopts the job;
-- `controller_epoch` names that adoption. They are one value in two columns: every
-- lifecycle, status, generation and authoritative-root write matches on both, and a write
-- that matches neither updates zero rows rather than overwriting a live controller's work.
--
-- The defaults are the values a row written before these columns existed carries, and each
-- is chosen so that such a row is indistinguishable from one this build has not adopted
-- yet. 0 is below every fence an adoption can install, because adoption stores
-- `lifecycle_fence + 1` and so never writes 0 itself; and '' is byte-identical to the
-- protobuf default for a string field, so a never-adopted row and a fence-less request from
-- an older controller carry the same epoch. Neither value is ever a controller's own
-- authority: exactly one place in Rust (`LifecycleAuthority::adopt`) turns the pair a read
-- observed into the pair a write may present.
ALTER TABLE job_statuses ADD COLUMN lifecycle_fence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE job_statuses ADD COLUMN controller_epoch TEXT NOT NULL DEFAULT '';
