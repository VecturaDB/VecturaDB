-- The attached functions, as the sysdb of distributed Chroma keeps them: its tables functions, attached_functions and
-- global_functions as its migrations leave them (go/pkg/sysdb/metastore/db/migrations, from 20251023154800 to
-- 20260810120000), with the types of SQLite: uuid, text and jsonb as TEXT, boolean as BOOLEAN, timestamp as TIMESTAMP.
CREATE TABLE functions (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    is_incremental BOOLEAN NOT NULL,
    return_type TEXT NOT NULL,
    is_async BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE attached_functions (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    database_id TEXT NOT NULL,
    input_collection_id TEXT NOT NULL,
    output_collection_name TEXT NOT NULL,
    output_collection_id TEXT DEFAULT NULL,
    function_id TEXT NOT NULL,
    function_params TEXT NOT NULL,
    completion_offset INTEGER NOT NULL DEFAULT 0,
    last_run TIMESTAMP NULL DEFAULT NULL,
    min_records_for_invocation INTEGER NOT NULL DEFAULT 100,
    current_attempts INTEGER NOT NULL DEFAULT 0,
    is_alive BOOLEAN NOT NULL DEFAULT TRUE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    global_parent TEXT NULL,
    oldest_written_nonce TEXT DEFAULT NULL,
    is_ready BOOLEAN NOT NULL DEFAULT FALSE,
    heap_entry_pending BOOLEAN NOT NULL DEFAULT FALSE,
    failure_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (id, input_collection_id),
    CONSTRAINT unique_attached_function_per_collection UNIQUE (input_collection_id, name)
);

CREATE TABLE global_functions (
    id TEXT PRIMARY KEY NOT NULL,
    tenant_id TEXT NOT NULL,
    database_id TEXT NOT NULL,
    name TEXT NOT NULL,
    function_id TEXT NOT NULL,
    params TEXT NOT NULL DEFAULT '{}',
    output_collection_pattern TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_global_function_per_tenant_db UNIQUE (tenant_id, database_id, name)
);

-- The built-in functions of distributed Chroma, with its ids and values (20251023154800, 20251029223300,
-- 20260501105846, 20260512120000, 20260525150000, 20260604123000 and 20260617150000).
INSERT INTO functions (id, name, is_incremental, return_type) VALUES (
    'ccf2e3ba-633e-43ba-9394-46b0c54c61e3',
    'record_counter',
    FALSE,
    '{"type": "object", "properties": {"count": {"type": "integer", "description": "Number of records processed"}}}'
);
INSERT INTO functions (id, name, is_incremental, return_type) VALUES (
    '304b58ad-a5cb-41dc-b88f-36dd3bf1d401',
    'statistics',
    FALSE,
    '{"type": "object", "properties": {"key": {"type": "string", "description": "Metadata key"}, "type": {"type": "string", "description": "Value type"}, "value": {"type": "string", "description": "Value"}, "count": {"type": "integer", "description": "Frequency count"}}}'
);
INSERT INTO functions (id, name, is_incremental, return_type, is_async) VALUES (
    '1db3d179-37a7-4c44-a301-687c1da69d7b',
    'dummy_async',
    TRUE,
    '{}',
    TRUE
);
INSERT INTO functions (id, name, is_incremental, return_type, is_async) VALUES (
    '9e3c7540-4ddd-40a2-bbff-ad9cb3f06efc',
    'http_generate',
    TRUE,
    '{}',
    TRUE
);
INSERT INTO functions (id, name, is_incremental, return_type, is_async) VALUES (
    '2df4342c-5b5a-49aa-8345-c46503e85509',
    'revision_history',
    TRUE,
    '{}',
    FALSE
);
INSERT INTO functions (id, name, is_incremental, return_type, is_async) VALUES (
    'eb125f49-1e8b-45d9-bb20-e84f2eae4e92',
    'count_to_file_async',
    TRUE,
    '{}',
    TRUE
);
INSERT INTO functions (id, name, is_incremental, return_type, is_async) VALUES (
    '63fdf220-eefb-4ad9-a686-9f719655aeb3',
    'http_currents',
    TRUE,
    '{}',
    TRUE
);
