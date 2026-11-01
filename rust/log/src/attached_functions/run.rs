// The run of the synchronous attached functions on the single node, as the attached function workflow of a compaction
// of distributed Chroma (rust/worker/src/execution/orchestration/compact.rs, run_attached_function_workflow and
// run_backfill_attached_function_workflow, and attached_function_orchestrator.rs, at 1cc7c750):
// - a function runs on the records of each apply of the log of its input collection, materialized against the state of
//   those records before the apply, as the worker materializes them against the record segment before its compaction;
//   the single node writes that state into a record segment in memory and calls materialize_logs and hydrate of the
//   worker unchanged;
// - a function whose completion offset is behind the window, as after a restart or right after its attach (the records
//   BackfillFn that the attach writes), runs as the backfill of the worker does: on every current record of its input
//   collection, as new, with no output read (materialize_logs_for_rebuild);
// - its output goes through the log of its output collection, where the worker applies it to the segments of the
//   output collection: the single node fills its segments only from the log, by seq_id (Federico's choice, 01/11);
// - its completion offset moves to the end of what it ran on, as finish_attached_function.rs of the worker.

use super::{sync_executor, HydratedInputBatch, OutputRecords};
use crate::local_compaction_manager::BackfillMessage;
use crate::{Log, PushLogsError};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::{RecordSegmentReaderShard, RecordSegmentWriterShard};
use chroma_segment::sqlite_metadata::SqliteMetadataReader;
use chroma_segment::types::{materialize_logs, materialize_logs_for_rebuild};
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::ReceiverForMessage;
use chroma_types::operator::{Filter, Limit, Projection, ProjectionRecord, Scan};
use chroma_types::plan::Get;
use chroma_types::{
    AttachedFunction, AttachedFunctionUpdateInfo, Chunk, CollectionAndSegments, CollectionUuid,
    DatabaseName, LogRecord, Metadata, Operation, OperationRecord, ScalarEncoding, Segment,
    SegmentScope, SegmentShard, SegmentType, SegmentUuid, UpdateMetadataValue,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

// How long the window of an attach waits for its function to be ready.
const ATTACH_WAIT_SECONDS: u32 = 30;

// How long a run waits for its output to be applied to the segments of its output collection.
const OUTPUT_APPLY_WAIT: Duration = Duration::from_secs(60);

/// The records of one apply of the log of a collection with ready synchronous functions: the records applied, and the
/// records of the same ids as they were before the apply.
pub struct FunctionWindow {
    pub input: CollectionAndSegments,
    pub logs: Chunk<LogRecord>,
    pub before: Vec<ProjectionRecord>,
    /// The log offset of the last record of the window.
    pub end: u64,
}

/// The state, read before the apply, of the records of the ids of the logs given.
pub async fn read_before(
    sqlite_db: &SqliteDb,
    input: &CollectionAndSegments,
    logs: &Chunk<LogRecord>,
) -> Result<Vec<ProjectionRecord>, Box<dyn ChromaError>> {
    let mut ids = logs
        .iter()
        .map(|(record, _)| record.record.id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    get_records(sqlite_db, input, Some(ids)).await
}

async fn get_records(
    sqlite_db: &SqliteDb,
    collection: &CollectionAndSegments,
    ids: Option<Vec<String>>,
) -> Result<Vec<ProjectionRecord>, Box<dyn ChromaError>> {
    let result = SqliteMetadataReader::new(sqlite_db.clone())
        .get(records_plan(collection, ids))
        .await
        .map_err(|e| e.boxed())?;
    Ok(result.result.records)
}

// A get of the records of the ids given, or of every record without ids, with their documents and metadata.
fn records_plan(collection: &CollectionAndSegments, ids: Option<Vec<String>>) -> Get {
    Get {
        scan: Scan {
            collection_and_segments: collection.clone(),
            shard_index: 0,
            num_shards: 1,
            log_upper_bound_offset: 0,
        },
        filter: Filter {
            query_ids: ids,
            where_clause: None,
        },
        limit: Limit {
            offset: 0,
            limit: None,
        },
        proj: Projection {
            document: true,
            embedding: false,
            metadata: true,
        },
    }
}

// The output collection as the executors read it, from its SQLite metadata.
struct SqliteOutputRecords {
    sqlite_db: SqliteDb,
    output: CollectionAndSegments,
}

#[async_trait]
impl OutputRecords for SqliteOutputRecords {
    async fn metadata_of(&self, user_id: &str) -> Option<Option<Metadata>> {
        get_records(
            &self.sqlite_db,
            &self.output,
            Some(vec![user_id.to_string()]),
        )
        .await
        .ok()?
        .into_iter()
        .next()
        .map(|record| record.metadata)
    }

    async fn all_metadata(&self) -> Result<Vec<Option<Metadata>>, Box<dyn ChromaError>> {
        Ok(get_records(&self.sqlite_db, &self.output, None)
            .await?
            .into_iter()
            .map(|record| record.metadata)
            .collect())
    }
}

/// What a run needs, out of the actor of the compaction manager.
#[derive(Clone)]
pub struct FunctionRunner {
    pub log: Log,
    pub sysdb: SysDb,
    pub sqlite_db: SqliteDb,
    /// The backfill of the compaction manager, which applies the output to the segments of its output collection.
    pub backfill: Option<Box<dyn ReceiverForMessage<BackfillMessage>>>,
}

impl FunctionRunner {
    /// The ready synchronous attached functions of a collection.
    pub async fn ready_sync_functions(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<Vec<AttachedFunction>, Box<dyn ChromaError>> {
        Ok(self
            .sysdb
            .clone()
            .get_attached_functions(None, Some(collection_id), vec![], true)
            .await
            .map_err(|e| e.boxed())?
            .into_iter()
            .filter(|function| !function.is_async)
            .collect())
    }

    /// Runs the ready synchronous functions of the input collection of a window on it.
    pub async fn run(&self, window: FunctionWindow) -> Result<(), Box<dyn ChromaError>> {
        let input_id = window.input.collection.collection_id;
        let mut functions = self.ready_sync_functions(input_id).await?;
        // The window of an attach waits for its function, which the frontend makes ready right after writing the records
        // of the attach, as the compaction of distributed Chroma comes after the attach.
        let attaches = window
            .logs
            .iter()
            .any(|(record, _)| record.record.operation == Operation::BackfillFn);
        let mut waited = 0;
        while attaches && functions.is_empty() && waited < ATTACH_WAIT_SECONDS {
            tokio::time::sleep(Duration::from_secs(1)).await;
            waited += 1;
            functions = self.ready_sync_functions(input_id).await?;
        }
        for function in functions {
            if function.completion_offset >= window.end {
                continue;
            }
            let Some(executor) = sync_executor(&function).map_err(|e| {
                Box::new(chroma_error::TonicError(tonic::Status::invalid_argument(e)))
                    as Box<dyn ChromaError>
            })?
            else {
                continue;
            };
            let Some(output_id) = function.output_collection_id else {
                continue;
            };
            let output = self
                .sysdb
                .clone()
                .get_collection_with_segments(None, output_id)
                .await
                .map_err(|e| e.boxed())?;
            let first = window
                .logs
                .iter()
                .map(|(record, _)| record.log_offset as u64)
                .min()
                .unwrap_or(window.end);
            // As the worker: a function that has not run on the records before the window runs as its backfill.
            let backfill = function.completion_offset + 1 < first;
            let (records, end) = if backfill {
                // The records and the log position they cover, at one moment, so that a record applied meanwhile is
                // counted once: here, or by its own window.
                let (current, end) = self.current_records(&window.input).await?;
                let records = self
                    .execute_backfill(&window.input, current, &*executor)
                    .await?;
                (records, end.max(window.end))
            } else {
                let reader = SqliteOutputRecords {
                    sqlite_db: self.sqlite_db.clone(),
                    output: output.clone(),
                };
                let records = self
                    .execute_window(&window, &*executor, Some(&reader))
                    .await?;
                (records, window.end)
            };
            self.push_output(&output, records).await?;
            // As the worker applies its output to the output collection in the same compaction, the output is applied
            // to its segments from its log now, not at a later read.
            if let Some(backfill) = &self.backfill {
                backfill
                    .send(
                        BackfillMessage {
                            collection_id: output_id,
                        },
                        None,
                    )
                    .await
                    .map_err(|e| e.boxed())?;
                // The next run of the function reads this output: it waits for its apply.
                self.wait_for_apply(&output).await?;
            }
            self.sysdb
                .clone()
                .flush_compaction_and_attached_function(
                    Vec::new(),
                    AttachedFunctionUpdateInfo {
                        attached_function_id: function.id,
                        completion_offset: end,
                    },
                )
                .await
                .map_err(|e| e.boxed())?;
        }
        Ok(())
    }

    async fn current_records(
        &self,
        input: &CollectionAndSegments,
    ) -> Result<(Vec<ProjectionRecord>, u64), Box<dyn ChromaError>> {
        let reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let mut snapshot = reader.snapshot().await.map_err(|e| e.boxed())?;
        let result = reader
            .get_at(&mut snapshot, records_plan(input, None))
            .await
            .map_err(|e| e.boxed())?;
        let end = snapshot
            .max_seq_id(&input.metadata_segment.id)
            .await
            .map_err(|e| e.boxed())?;
        snapshot.close().await.map_err(|e| e.boxed())?;
        Ok((result.result.records, end))
    }

    // The output collection has applied its log up to its tail, within OUTPUT_APPLY_WAIT.
    async fn wait_for_apply(
        &self,
        output: &CollectionAndSegments,
    ) -> Result<(), Box<dyn ChromaError>> {
        let database = DatabaseName::new(output.collection.database.clone()).ok_or_else(|| {
            Box::new(chroma_error::TonicError(tonic::Status::internal(
                "invalid database name of an output collection",
            ))) as Box<dyn ChromaError>
        })?;
        let tail = self
            .log
            .clone()
            .scout_logs(
                &output.collection.tenant,
                database,
                output.collection.collection_id,
                0,
            )
            .await?
            .saturating_sub(1);
        let reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let deadline = tokio::time::Instant::now() + OUTPUT_APPLY_WAIT;
        while reader
            .current_max_seq_id(&output.metadata_segment.id)
            .await
            .map_err(|e| e.boxed())?
            < tail
        {
            if tokio::time::Instant::now() >= deadline {
                return Err(Box::new(chroma_error::TonicError(
                    tonic::Status::deadline_exceeded(
                        "the output of an attached function was not applied",
                    ),
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    // The records of the window materialized against their state before it, in a record segment in memory, then
    // hydrated, as the worker materializes the log of a compaction against its record segment.
    async fn execute_window(
        &self,
        window: &FunctionWindow,
        executor: &dyn super::AttachedFunctionExecutor,
        output_reader: Option<&dyn OutputRecords>,
    ) -> Result<Vec<OperationRecord>, Box<dyn ChromaError>> {
        let provider = BlockfileProvider::new_memory();
        let segment =
            in_memory_record_segment(&provider, &window.input, before_as_adds(&window.before))
                .await?;
        let shard = SegmentShard::try_from((&segment, 0)).map_err(|e| e.boxed())?;
        let reader = match RecordSegmentReaderShard::from_segment(&shard, &provider, None).await {
            Ok(reader) => Some(reader),
            Err(_) if window.before.is_empty() => None,
            Err(e) => return Err(e.boxed()),
        };
        let materialized = materialize_logs(
            &reader,
            window.logs.clone(),
            Some(Arc::new(AtomicU32::new(window.before.len() as u32 + 1))),
            &Default::default(),
        )
        .await
        .map_err(|e| e.boxed())?;
        let mut hydrated = Vec::new();
        for record in materialized.iter() {
            hydrated.push(
                record
                    .hydrate(reader.as_ref())
                    .await
                    .map_err(|e| e.boxed())?,
            );
        }
        let output = executor
            .execute(
                vec![batch(&window.input, window.end, hydrated)],
                output_reader,
            )
            .await?;
        Ok(output
            .iter()
            .map(|(record, _)| record.record.clone())
            .collect())
    }

    // As the backfill of the worker: every current record of the input collection, as new, with no output read.
    async fn execute_backfill(
        &self,
        input: &CollectionAndSegments,
        current: Vec<ProjectionRecord>,
        executor: &dyn super::AttachedFunctionExecutor,
    ) -> Result<Vec<OperationRecord>, Box<dyn ChromaError>> {
        let logs = Chunk::new(Arc::from(before_as_adds(&current)));
        let offset_ids = (1..=current.len() as u32).collect::<Vec<_>>();
        let materialized = materialize_logs_for_rebuild(logs, offset_ids)
            .await
            .map_err(|e| e.boxed())?;
        let mut hydrated = Vec::new();
        for record in materialized.iter() {
            hydrated.push(record.hydrate(None).await.map_err(|e| e.boxed())?);
        }
        let output = executor
            .execute(vec![batch(input, 0, hydrated)], None)
            .await?;
        Ok(output
            .iter()
            .map(|(record, _)| record.record.clone())
            .collect())
    }

    // The output into the log of the output collection; a write refused for backpressure waits as it asks, and tries
    // again, so that no output is lost and the completion offset moves only after it.
    async fn push_output(
        &self,
        output: &CollectionAndSegments,
        records: Vec<OperationRecord>,
    ) -> Result<(), Box<dyn ChromaError>> {
        if records.is_empty() {
            return Ok(());
        }
        // As a write through the frontend: the SQLite log reads back the vector of a record only with its encoding
        // (sqlite_log.rs, the read of the vector), which the executors leave out, as the worker applies their output to
        // the segments without the log.
        let records = records
            .into_iter()
            .map(|mut record| {
                record.encoding = record.embedding.as_ref().map(|_| ScalarEncoding::FLOAT32);
                record
            })
            .collect::<Vec<_>>();
        let database = DatabaseName::new(output.collection.database.clone()).ok_or_else(|| {
            Box::new(chroma_error::TonicError(tonic::Status::internal(
                "invalid database name of an output collection",
            ))) as Box<dyn ChromaError>
        })?;
        loop {
            match self
                .log
                .clone()
                .push_logs(
                    &output.collection.tenant,
                    database.clone(),
                    output.collection.collection_id,
                    records.clone(),
                    None,
                    None,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(PushLogsError::BackoffCompaction { retry_after }) => {
                    tokio::time::sleep(Duration::from_secs(retry_after.unwrap_or(1).max(1))).await;
                }
                Err(PushLogsError::Backoff) => tokio::time::sleep(Duration::from_secs(1)).await,
                Err(e) => return Err(e.boxed()),
            }
        }
    }
}

fn batch<'me, 'q>(
    input: &CollectionAndSegments,
    pulled_log_offset: u64,
    hydrated: Vec<chroma_segment::types::HydratedMaterializedLogRecord<'me, 'q>>,
) -> HydratedInputBatch<'me, 'q> {
    HydratedInputBatch {
        input_collection_id: input.collection.collection_id,
        input_collection_name: input.collection.name.clone(),
        tenant_id: input.collection.tenant.clone(),
        database_id: input.collection.database_id.to_string(),
        database_name: Some(input.collection.database.clone()),
        pulled_log_offset,
        records: Chunk::new(Arc::from(hydrated)),
    }
}

// Records of SQLite as the adds of a log, to write them into a record segment.
fn before_as_adds(records: &[ProjectionRecord]) -> Vec<LogRecord> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| LogRecord {
            log_offset: index as i64 + 1,
            record: OperationRecord {
                id: record.id.clone(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: record.metadata.as_ref().map(|metadata| {
                    metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), UpdateMetadataValue::from(value.clone())))
                        .collect::<HashMap<_, _>>()
                }),
                document: record.document.clone(),
                operation: Operation::Add,
            },
        })
        .collect()
}

// A record segment in memory with the records given, as the compaction of the worker writes one.
async fn in_memory_record_segment(
    provider: &BlockfileProvider,
    input: &CollectionAndSegments,
    adds: Vec<LogRecord>,
) -> Result<Segment, Box<dyn ChromaError>> {
    let mut segment = Segment {
        id: SegmentUuid::new(),
        r#type: SegmentType::BlockfileRecord,
        scope: SegmentScope::RECORD,
        collection: input.collection.collection_id,
        metadata: None,
        file_path: HashMap::new(),
    };
    if adds.is_empty() {
        return Ok(segment);
    }
    let materialized = materialize_logs(
        &None,
        Chunk::new(Arc::from(adds)),
        Some(Arc::new(AtomicU32::new(1))),
        &Default::default(),
    )
    .await
    .map_err(|e| e.boxed())?;
    let shard = SegmentShard::try_from((&segment, 0)).map_err(|e| e.boxed())?;
    let writer = RecordSegmentWriterShard::from_segment(
        &input.collection.tenant,
        &input.collection.database_id,
        &shard,
        provider,
        None,
        None,
    )
    .await
    .map_err(|e| e.boxed())?;
    writer
        .apply_materialized_log_chunk(&None, &materialized)
        .await
        .map_err(|e| e.boxed())?;
    segment.file_path = writer.commit().await?.flush().await?;
    Ok(segment)
}
