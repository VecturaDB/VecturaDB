-- The files of a segment, as in distributed Chroma: a JSON object from a name to its paths, empty for the segments that
-- keep no files of their own.
ALTER TABLE segments ADD COLUMN file_paths TEXT NOT NULL DEFAULT '{}';
