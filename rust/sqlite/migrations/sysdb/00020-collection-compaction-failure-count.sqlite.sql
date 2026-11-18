-- The failed compactions of a collection in a row, as distributed Chroma keeps them (compaction_failure_count of its
-- collections): one more at each failed flush, back to 0 at the next flush that succeeds.
ALTER TABLE collections ADD COLUMN compaction_failure_count INTEGER NOT NULL DEFAULT 0;
