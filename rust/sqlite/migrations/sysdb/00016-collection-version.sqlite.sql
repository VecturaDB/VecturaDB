-- The version of a collection, as distributed Chroma keeps it: 0 for a collection before its first flush, one more at
-- each flush.
ALTER TABLE collections ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
