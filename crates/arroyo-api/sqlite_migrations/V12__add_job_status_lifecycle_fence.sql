-- The SQLite spelling of V34. The defaults must match that migration's exactly, and its
-- comment is where the reason they are 0 and '' is recorded: a row that predates these
-- columns has to read as a job no controller has adopted, on both backends.
ALTER TABLE job_statuses ADD COLUMN lifecycle_fence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE job_statuses ADD COLUMN controller_epoch TEXT NOT NULL DEFAULT '';
