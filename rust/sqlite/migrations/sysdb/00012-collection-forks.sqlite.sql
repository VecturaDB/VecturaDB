-- The lineage of forked collections, as the lineage file of the root collection in distributed Chroma: one row per
-- fork, kept when collections are deleted, so the fork count of a lineage never goes down.
CREATE TABLE collection_forks (
    target_collection_id TEXT PRIMARY KEY NOT NULL,
    source_collection_id TEXT NOT NULL,
    root_collection_id TEXT NOT NULL
);
CREATE INDEX collection_forks_root ON collection_forks (root_collection_id);
