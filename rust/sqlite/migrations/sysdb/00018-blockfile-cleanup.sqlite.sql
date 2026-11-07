-- The folders of the segments of SPANN collections that a deletion leaves to remove, recorded in the same transaction as
-- the deletion of the segments, as index_cleanup records the indexes (00011): absence from the catalog alone must never
-- authorize deleting files. The folder of a segment is under the folder of its collection, which the row keeps, so that
-- the sweep looks for it there and only there.
CREATE TABLE blockfile_cleanup (
    segment_id TEXT PRIMARY KEY NOT NULL,
    collection_id TEXT NOT NULL
);
