-- The state of the records before each apply of the log of a collection with an asynchronous attached function: the
-- run of the function materializes its records against the state at its completion offset, as distributed Chroma
-- materializes them against the version of the record segment at that offset, which the single node does not keep
-- (Federico, 04/11). For each apply, from the first record it applies, a row for each id it touches: whether the record
-- existed, and its metadata and document then. A row goes once every asynchronous function of the collection has run
-- past it, or with the last of them when it is dead-lettered or detached.
CREATE TABLE attached_function_prior (
    input_collection_id TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    id TEXT NOT NULL,
    existed BOOLEAN NOT NULL,
    metadata TEXT,
    document TEXT,
    PRIMARY KEY (input_collection_id, window_start, id)
);

-- From which record of its log a collection keeps the state before its applies: a function behind it runs as its
-- backfill, as distributed Chroma does for a function behind what its log still holds.
CREATE TABLE attached_function_prior_since (
    input_collection_id TEXT PRIMARY KEY NOT NULL,
    since INTEGER NOT NULL
);
