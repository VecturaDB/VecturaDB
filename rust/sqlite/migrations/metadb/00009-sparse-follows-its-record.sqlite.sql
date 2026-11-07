-- The sparse vector of a record goes with the record, whoever deletes it. Chroma 1.5.9 knows nothing of the sparse
-- tables (00007, 00008): it deletes a record with the rows of embeddings alone, and SQLite gives the highest id freed to
-- the next record, which then had the sparse vector of the deleted one. A trigger lives in the database file, so it runs
-- for every server that writes the file; the trigger of the terms (00008) follows. The sparse rows of the records that
-- are already gone go once, here.
DELETE FROM embedding_metadata_sparse WHERE id NOT IN (SELECT id FROM embeddings);

CREATE TRIGGER embedding_metadata_sparse_record_delete AFTER DELETE ON embeddings
BEGIN
    DELETE FROM embedding_metadata_sparse WHERE id = OLD.id;
END;
