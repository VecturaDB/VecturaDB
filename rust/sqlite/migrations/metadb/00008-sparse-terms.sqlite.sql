-- One row per dimension of every stored sparse vector, kept in step with embedding_metadata_sparse by triggers:
-- the records that share a dimension with a query, and the number of records with each dimension (n_t for BM25),
-- come from an index lookup instead of a scan of every vector of the key.
CREATE TABLE embedding_metadata_sparse_term (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    dim INTEGER NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (key, dim, id)
);
CREATE INDEX embedding_metadata_sparse_term_id ON embedding_metadata_sparse_term (id, key);

INSERT INTO embedding_metadata_sparse_term (id, key, dim, value)
SELECT sparse.id, sparse.key, dims.value, vals.value
FROM embedding_metadata_sparse AS sparse,
     json_each(sparse.value, '$.indices') AS dims
     JOIN json_each(sparse.value, '$.values') AS vals ON vals.key = dims.key;

CREATE TRIGGER embedding_metadata_sparse_term_insert AFTER INSERT ON embedding_metadata_sparse
BEGIN
    INSERT INTO embedding_metadata_sparse_term (id, key, dim, value)
    SELECT NEW.id, NEW.key, dims.value, vals.value
    FROM json_each(NEW.value, '$.indices') AS dims
         JOIN json_each(NEW.value, '$.values') AS vals ON vals.key = dims.key;
END;

CREATE TRIGGER embedding_metadata_sparse_term_delete AFTER DELETE ON embedding_metadata_sparse
BEGIN
    DELETE FROM embedding_metadata_sparse_term WHERE id = OLD.id AND key = OLD.key;
END;

CREATE TRIGGER embedding_metadata_sparse_term_update AFTER UPDATE ON embedding_metadata_sparse
BEGIN
    DELETE FROM embedding_metadata_sparse_term WHERE id = OLD.id AND key = OLD.key;
    INSERT INTO embedding_metadata_sparse_term (id, key, dim, value)
    SELECT NEW.id, NEW.key, dims.value, vals.value
    FROM json_each(NEW.value, '$.indices') AS dims
         JOIN json_each(NEW.value, '$.values') AS vals ON vals.key = dims.key;
END;
