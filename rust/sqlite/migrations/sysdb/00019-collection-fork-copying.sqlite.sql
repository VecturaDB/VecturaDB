-- A fork on the single node copies the records of its source into its target after the target is in the catalog; while
-- the copy runs, the target is hidden from the reads of the users, which see a fork whole or not at all, as the fork of
-- distributed Chroma, which shares the files of its source in one go. A target still copying at the start is what a stop
-- left halfway: it goes with the normal deletion, and its row here too, since that fork never happened.
ALTER TABLE collection_forks ADD COLUMN copying BOOLEAN NOT NULL DEFAULT FALSE;
