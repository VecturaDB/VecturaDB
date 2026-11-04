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
use backon::{BackoffBuilder, ExponentialBuilder};
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
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
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
    /// The log offset of the last record of the collection applied before the window: the log of the single node
    /// numbers the records of every collection in one sequence, so the window of a collection starts after it, not
    /// after the record before its first.
    pub applied_before: u64,
    /// A run of the asynchronous functions of the collection, up to `end`, where the others are of the synchronous ones.
    pub asynchronous: bool,
    /// Whether the run counts one more version of the input collection (run): the window of an apply of an HNSW
    /// collection, or of its functions behind; a SPANN collection counts its flushes.
    pub counts_version: bool,
}

/// The input collections of the ready synchronous functions behind the records applied to them, with the log offset of
/// the last record applied, as the metadata segment of the collection records it.
pub async fn behind(
    sqlite_db: &SqliteDb,
) -> Result<Vec<(CollectionUuid, u64)>, Box<dyn ChromaError>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT attached_functions.input_collection_id, MAX(CAST(max_seq_id.seq_id AS INTEGER)) \
         FROM attached_functions \
         JOIN functions ON functions.id = attached_functions.function_id \
         JOIN segments ON segments.collection = attached_functions.input_collection_id AND segments.scope = 'METADATA' \
         JOIN max_seq_id ON max_seq_id.segment_id = segments.id \
         WHERE attached_functions.is_deleted = FALSE AND attached_functions.is_ready = TRUE \
         AND functions.is_async = FALSE AND attached_functions.completion_offset < CAST(max_seq_id.seq_id AS INTEGER) \
         AND attached_functions.failure_count < $1 \
         GROUP BY attached_functions.input_collection_id",
    )
    .bind(MAX_FAILURE_COUNT)
    .fetch_all(sqlite_db.get_conn())
    .await
    .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)?;
    rows.into_iter()
        .map(|(id, applied)| {
            CollectionUuid::from_str(&id)
                .map(|id| (id, applied.max(0) as u64))
                .map_err(|e| {
                    Box::new(chroma_error::TonicError(tonic::Status::internal(
                        e.to_string(),
                    ))) as Box<dyn ChromaError>
                })
        })
        .collect()
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
    /// The storage of the single node, which count_to_file_async writes to: its data folder, or S3 when configured.
    pub storage: Option<chroma_storage::Storage>,
    /// The next try of the synchronous functions of the collections whose last run failed.
    pub sync_retries: SyncRetries,
    /// The first wait after a failed run of synchronous functions: the timer of the compaction manager.
    pub sync_retry_delay: Duration,
}

/// The collections whose synchronous functions failed their last run, with the time of their next try.
pub type SyncRetries = Arc<Mutex<HashMap<CollectionUuid, tokio::time::Instant>>>;

// The wait after the given failures in a row of the synchronous functions of a collection (Federico, 04/11: an
// exponential backoff with jitter and a maximum, then as distributed Chroma): the first is the timer, each doubles, with
// a jitter of up to as much again. At MAX_FAILURE_COUNT failures the functions stop, as distributed Chroma stops
// compacting a collection after max_failure_count failures (scheduler.rs). The longest wait is 5 minutes, which a timer
// of the default 10 s does not reach before the stop. The failures are those of the function in SQLite, which a run that
// succeeds resets.
fn sync_retry_wait(first: Duration, failures: i32) -> Option<Duration> {
    ExponentialBuilder::default()
        .with_min_delay(first)
        .with_max_delay(Duration::from_secs(300))
        .with_max_times(MAX_FAILURE_COUNT as usize)
        .with_jitter()
        .build()
        .nth(failures.max(1) as usize - 1)
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

    /// Whether the synchronous functions of a collection whose last run failed can be tried again.
    fn sync_retry_due(&self, collection_id: CollectionUuid) -> bool {
        self.sync_retries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&collection_id)
            .is_none_or(|next| *next <= tokio::time::Instant::now())
    }

    // Whether a synchronous function of the collection is stopped after MAX_FAILURE_COUNT failures: as distributed
    // Chroma stops compacting the collection, an apply counts no version until the function is detached.
    async fn has_stopped_sync_function(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<bool, Box<dyn ChromaError>> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM attached_functions JOIN functions ON functions.id = attached_functions.function_id \
             WHERE attached_functions.input_collection_id = $1 AND attached_functions.is_deleted = FALSE \
             AND functions.is_async = FALSE AND attached_functions.failure_count >= $2)",
        )
        .bind(collection_id.to_string())
        .bind(MAX_FAILURE_COUNT)
        .fetch_one(self.sqlite_db.get_conn())
        .await
        .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)
    }

    // A failed run: one more failure of each ready synchronous function of the collection, as FailAttachedFunction of
    // distributed Chroma, and its next try after the wait for its failures; none at the stop.
    async fn fail_sync_functions(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<(), Box<dyn ChromaError>> {
        let mut most = 0;
        for function in self.ready_sync_functions(collection_id).await? {
            let failures = self
                .sysdb
                .clone()
                .fail_attached_function(chroma_types::chroma_proto::FailAttachedFunctionRequest {
                    attached_function_id: function.id.0.to_string(),
                    collection_id: collection_id.to_string(),
                })
                .await
                .map_err(|e| Box::new(chroma_error::TonicError(e)) as Box<dyn ChromaError>)?;
            most = most.max(failures);
        }
        if most < MAX_FAILURE_COUNT {
            if let Some(wait) = sync_retry_wait(self.sync_retry_delay, most) {
                self.sync_retries
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(collection_id, tokio::time::Instant::now() + wait);
            }
        }
        Ok(())
    }

    // Whether a synchronous function of the collection is attached and not ready yet: the window of its attach waits
    // for it, where one of an asynchronous function does not.
    async fn has_sync_function_not_ready(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<bool, Box<dyn ChromaError>> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM attached_functions JOIN functions ON functions.id = attached_functions.function_id \
             WHERE attached_functions.input_collection_id = $1 AND attached_functions.is_deleted = FALSE \
             AND attached_functions.is_ready = FALSE AND functions.is_async = FALSE)",
        )
        .bind(collection_id.to_string())
        .fetch_one(self.sqlite_db.get_conn())
        .await
        .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)
    }

    /// Runs the ready synchronous functions of the input collection of a window on it.
    pub async fn run(&self, window: FunctionWindow) -> Result<(), Box<dyn ChromaError>> {
        if window.asynchronous {
            return self.run_asynchronous(window).await;
        }
        let input_id = window.input.collection.collection_id;
        // A collection whose functions failed waits for their next try, and one whose function stopped counts nothing:
        // the run of the timer on the function behind covers the records of the window.
        if !self.sync_retry_due(input_id) || self.has_stopped_sync_function(input_id).await? {
            return Ok(());
        }
        let ran = match self.run_synchronous(&window).await {
            Ok(ran) => ran,
            Err(err) => {
                self.fail_sync_functions(input_id).await?;
                return Err(err);
            }
        };
        // As the register of distributed Chroma flushes the input collection with the output of its synchronous
        // functions (FinishAttachedFunctionOperator), one more version of the input once their output is applied: a
        // reader that waits for the version reads the output. A run that fails counts nothing, as a compaction of
        // distributed Chroma that fails; the run of a function behind that then succeeds counts one, as the compaction
        // that covers it. The window of an apply counts its apply also when no function runs on it.
        if window.counts_version && (ran || !window.logs.is_empty()) {
            sqlx::query("UPDATE collections SET version = version + 1 WHERE id = $1")
                .bind(window.input.collection.collection_id.to_string())
                .execute(self.sqlite_db.get_conn())
                .await
                .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)?;
        }
        Ok(())
    }

    // Whether a function ran on the window.
    async fn run_synchronous(&self, window: &FunctionWindow) -> Result<bool, Box<dyn ChromaError>> {
        let input_id = window.input.collection.collection_id;
        let mut functions = self.ready_sync_functions(input_id).await?;
        // The window of an attach waits for its function, which the frontend makes ready right after writing the records
        // of the attach, as the compaction of distributed Chroma comes after the attach.
        let attaches = window
            .logs
            .iter()
            .any(|(record, _)| record.record.operation == Operation::BackfillFn);
        // Whether a function is not ready is read before the ready ones: a function made ready between the two reads is
        // then among the ready ones, where in the other order the window would see it neither ready nor waited for.
        let mut waited = 0;
        while attaches && functions.is_empty() && waited < ATTACH_WAIT_SECONDS {
            let waiting = self.has_sync_function_not_ready(input_id).await?;
            functions = self.ready_sync_functions(input_id).await?;
            if !functions.is_empty() || !waiting {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            waited += 1;
        }
        let mut ran = false;
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
            // As the worker: a function that has not run on every record applied before the window runs as its backfill.
            let backfill = function.completion_offset < window.applied_before;
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
                    .execute_window(window, &*executor, Some(&reader))
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
            ran = true;
        }
        Ok(ran)
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
        // The last record of its own log, which the output has just been written into: the tail that scout_logs
        // gives is the one of the whole SQLite log, of every collection.
        let reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let applied = reader
            .current_max_seq_id(&output.metadata_segment.id)
            .await
            .map_err(|e| e.boxed())?;
        let tail = self
            .log
            .clone()
            .read(
                &output.collection.tenant,
                database,
                output.collection.collection_id,
                applied as i64,
                -1,
                None,
            )
            .await
            .map_err(|e| e.boxed())?
            .iter()
            .map(|record| record.log_offset.max(0) as u64)
            .max()
            .unwrap_or(applied);
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

/// The maximum of failures of an asynchronous function before its work is dead-lettered, as max_failure_count of the
/// consumer of distributed Chroma (rust/worker/src/fn_consumer/config.rs).
pub const MAX_FAILURE_COUNT: i32 = 5;

/// Whether a collection has an asynchronous attached function not deleted and not dead-lettered, ready or not yet: its
/// applies keep the state before them.
pub async fn has_live_async_functions(
    sqlite_db: &SqliteDb,
    collection_id: CollectionUuid,
) -> Result<bool, Box<dyn ChromaError>> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM attached_functions JOIN functions ON functions.id = attached_functions.function_id \
         WHERE attached_functions.input_collection_id = $1 AND attached_functions.is_deleted = FALSE \
         AND functions.is_async = TRUE AND attached_functions.failure_count < $2)",
    )
    .bind(collection_id.to_string())
    .bind(MAX_FAILURE_COUNT)
    .fetch_one(sqlite_db.get_conn())
    .await
    .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)
}

/// The state before an apply of the records it touches, in the transaction of the apply (00017).
pub async fn record_prior(
    conn: &mut sqlx::SqliteConnection,
    collection_id: CollectionUuid,
    logs: &Chunk<LogRecord>,
    before: &[ProjectionRecord],
) -> Result<(), Box<dyn ChromaError>> {
    let sqlx_error = |e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>;
    let Some(window_start) = logs.iter().map(|(record, _)| record.log_offset).min() else {
        return Ok(());
    };
    let collection = collection_id.to_string();
    sqlx::query(
        "INSERT OR IGNORE INTO attached_function_prior_since (input_collection_id, since) VALUES ($1, $2)",
    )
    .bind(&collection)
    .bind(window_start)
    .execute(&mut *conn)
    .await
    .map_err(sqlx_error)?;
    let before = before
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect::<HashMap<_, _>>();
    let mut ids = logs
        .iter()
        .map(|(record, _)| record.record.id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        let record = before.get(id);
        let metadata = record
            .and_then(|record| record.metadata.as_ref())
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| {
                Box::new(chroma_error::TonicError(tonic::Status::internal(
                    e.to_string(),
                ))) as Box<dyn ChromaError>
            })?;
        sqlx::query(
            "INSERT OR REPLACE INTO attached_function_prior \
             (input_collection_id, window_start, id, existed, metadata, document) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&collection)
        .bind(window_start)
        .bind(id)
        .bind(record.is_some())
        .bind(metadata)
        .bind(record.and_then(|record| record.document.clone()))
        .execute(&mut *conn)
        .await
        .map_err(sqlx_error)?;
    }
    Ok(())
}

/// The input collections whose asynchronous functions, ready and not dead-lettered, are behind the records applied to
/// them, with the log offset of the last record applied; and the state kept before the applies of collections that no
/// longer have such a function goes, as their last function was detached or dead-lettered.
pub async fn async_due(
    sqlite_db: &SqliteDb,
) -> Result<Vec<(CollectionUuid, u64)>, Box<dyn ChromaError>> {
    let sqlx_error = |e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>;
    let live = "SELECT attached_functions.input_collection_id FROM attached_functions \
                JOIN functions ON functions.id = attached_functions.function_id \
                WHERE attached_functions.is_deleted = FALSE AND functions.is_async = TRUE \
                AND attached_functions.failure_count < $1";
    for table in ["attached_function_prior", "attached_function_prior_since"] {
        sqlx::query(&format!(
            "DELETE FROM {table} WHERE input_collection_id NOT IN ({live})"
        ))
        .bind(MAX_FAILURE_COUNT)
        .execute(sqlite_db.get_conn())
        .await
        .map_err(sqlx_error)?;
    }
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT attached_functions.input_collection_id, MAX(CAST(max_seq_id.seq_id AS INTEGER)) \
         FROM attached_functions \
         JOIN functions ON functions.id = attached_functions.function_id \
         JOIN segments ON segments.collection = attached_functions.input_collection_id AND segments.scope = 'METADATA' \
         JOIN max_seq_id ON max_seq_id.segment_id = segments.id \
         WHERE attached_functions.is_deleted = FALSE AND attached_functions.is_ready = TRUE \
         AND functions.is_async = TRUE AND attached_functions.failure_count < $1 \
         AND attached_functions.completion_offset < CAST(max_seq_id.seq_id AS INTEGER) \
         GROUP BY attached_functions.input_collection_id",
    )
    .bind(MAX_FAILURE_COUNT)
    .fetch_all(sqlite_db.get_conn())
    .await
    .map_err(sqlx_error)?;
    rows.into_iter()
        .map(|(id, applied)| {
            CollectionUuid::from_str(&id)
                .map(|id| (id, applied.max(0) as u64))
                .map_err(|e| {
                    Box::new(chroma_error::TonicError(tonic::Status::internal(
                        e.to_string(),
                    ))) as Box<dyn ChromaError>
                })
        })
        .collect()
}

impl FunctionRunner {
    // The asynchronous functions of a collection, as the consumer of distributed Chroma runs one item of its queue
    // (rust/worker/src/execution/orchestration/function_execution.rs): the records after the completion offset,
    // materialized against their state at it, or, for a function behind what the collection keeps of it, as its
    // backfill; then TryFinishAsyncAttachedFunctionInvocation, or FailAttachedFunction on an error, which after
    // MAX_FAILURE_COUNT failures dead-letters it. The queue of the single node is the rows of attached_functions
    // (Federico, 04/11), where distributed Chroma keeps a queue of its own.
    async fn run_asynchronous(&self, window: FunctionWindow) -> Result<(), Box<dyn ChromaError>> {
        let input_id = window.input.collection.collection_id;
        let functions = self
            .sysdb
            .clone()
            .get_attached_functions(None, Some(input_id), vec![], true)
            .await
            .map_err(|e| e.boxed())?
            .into_iter()
            .filter(|function| function.is_async && function.failure_count < MAX_FAILURE_COUNT);
        for function in functions {
            if function.completion_offset >= window.end {
                continue;
            }
            match self.run_one_asynchronous(&window, &function).await {
                Ok(()) => {}
                Err(err) => {
                    tracing::warn!(attached_function_id = %function.id, error = %err, "An asynchronous attached function failed");
                    self.sysdb
                        .clone()
                        .fail_attached_function(
                            chroma_types::chroma_proto::FailAttachedFunctionRequest {
                                attached_function_id: function.id.0.to_string(),
                                collection_id: input_id.to_string(),
                            },
                        )
                        .await
                        .map_err(|e| {
                            Box::new(chroma_error::TonicError(e)) as Box<dyn ChromaError>
                        })?;
                }
            }
        }
        Ok(())
    }

    async fn run_one_asynchronous(
        &self,
        window: &FunctionWindow,
        function: &chroma_types::AttachedFunction,
    ) -> Result<(), Box<dyn ChromaError>> {
        let input = &window.input;
        let input_id = input.collection.collection_id;
        let invalid = |e: String| {
            Box::new(chroma_error::TonicError(tonic::Status::invalid_argument(e)))
                as Box<dyn ChromaError>
        };
        let Some(executor) =
            super::async_executor(function, self.storage.clone()).map_err(invalid)?
        else {
            return Err(invalid(format!(
                "the single node does not run the attached function {}",
                function.function_id
            )));
        };
        let database = DatabaseName::new(input.collection.database.clone())
            .ok_or_else(|| invalid("invalid database name of an input collection".to_string()))?;
        let completion_offset = function.completion_offset;
        let logs = self
            .log
            .clone()
            .read(
                &input.collection.tenant,
                database,
                input_id,
                completion_offset as i64,
                -1,
                None,
            )
            .await?
            .into_iter()
            .filter(|record| record.log_offset as u64 <= window.end)
            .collect::<Vec<_>>();
        let since: Option<i64> = sqlx::query_scalar(
            "SELECT since FROM attached_function_prior_since WHERE input_collection_id = $1",
        )
        .bind(input_id.to_string())
        .fetch_optional(self.sqlite_db.get_conn())
        .await
        .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)?;
        let first = logs.first().map(|record| record.log_offset);
        // As distributed Chroma runs a function behind what it keeps of the collection as its backfill: on every current
        // record, as new, with no output read.
        let covered = match (first, since) {
            (None, _) => true,
            (Some(first), Some(since)) => since <= first,
            (Some(_), None) => false,
        };
        let output = match function.output_collection_id {
            Some(output_id) => Some(
                self.sysdb
                    .clone()
                    .get_collection_with_segments(None, output_id)
                    .await
                    .map_err(|e| e.boxed())?,
            ),
            None => None,
        };
        let (records, end) = if !covered {
            let (current, end) = self.current_records(input).await?;
            let records = self.execute_backfill(input, current, &*executor).await?;
            (records, end.max(window.end))
        } else if logs.is_empty() {
            (Vec::new(), window.end)
        } else {
            let logs = Chunk::new(Arc::from(logs));
            let before = self.prior(input_id, completion_offset).await?;
            let run = FunctionWindow {
                input: input.clone(),
                logs,
                before,
                end: window.end,
                applied_before: completion_offset,
                asynchronous: true,
                counts_version: false,
            };
            let reader = output.as_ref().map(|output| SqliteOutputRecords {
                sqlite_db: self.sqlite_db.clone(),
                output: output.clone(),
            });
            let records = self
                .execute_window(
                    &run,
                    &*executor,
                    reader.as_ref().map(|r| r as &dyn OutputRecords),
                )
                .await?;
            (records, window.end)
        };
        if let Some(output) = &output {
            if !records.is_empty() {
                self.push_output(output, records).await?;
                if let Some(backfill) = &self.backfill {
                    backfill
                        .send(
                            BackfillMessage {
                                collection_id: output.collection.collection_id,
                            },
                            None,
                        )
                        .await
                        .map_err(|e| e.boxed())?;
                    self.wait_for_apply(output).await?;
                }
            }
        }
        self.sysdb
            .clone()
            .try_finish_async_attached_function_invocation(
                chroma_types::chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest {
                    attached_function_id: function.id.0.to_string(),
                    collection_id: input_id.to_string(),
                    new_completion_offset: end,
                },
            )
            .await
            .map_err(|e| Box::new(chroma_error::TonicError(e)) as Box<dyn ChromaError>)?;
        // The state before the applies that every asynchronous function of the collection has run past goes.
        sqlx::query(
            "DELETE FROM attached_function_prior WHERE input_collection_id = $1 AND window_start <= \
             (SELECT MIN(attached_functions.completion_offset) FROM attached_functions \
              JOIN functions ON functions.id = attached_functions.function_id \
              WHERE attached_functions.input_collection_id = $1 AND attached_functions.is_deleted = FALSE \
              AND functions.is_async = TRUE AND attached_functions.failure_count < $2)",
        )
        .bind(input_id.to_string())
        .bind(MAX_FAILURE_COUNT)
        .execute(self.sqlite_db.get_conn())
        .await
        .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)?;
        Ok(())
    }

    // The records as they were at the completion offset, from the state kept before the applies after it: for each id,
    // the state before the first apply after the offset that touched it; those that did not exist then are left out.
    // The first apply of each id comes from one pass on the rows of the collection, then each of its rows from the
    // primary key: a subquery for each row would read the rows of the collection once for each, which takes minutes
    // once an asynchronous function lags behind tens of thousands of records.
    async fn prior(
        &self,
        input_id: CollectionUuid,
        completion_offset: u64,
    ) -> Result<Vec<ProjectionRecord>, Box<dyn ChromaError>> {
        let rows: Vec<(String, bool, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT p.id, p.existed, p.metadata, p.document FROM \
             (SELECT id, MIN(window_start) AS first FROM attached_function_prior \
              WHERE input_collection_id = $1 AND window_start > $2 GROUP BY id) AS m \
             JOIN attached_function_prior AS p \
             ON p.input_collection_id = $1 AND p.window_start = m.first AND p.id = m.id",
        )
        .bind(input_id.to_string())
        .bind(completion_offset as i64)
        .fetch_all(self.sqlite_db.get_conn())
        .await
        .map_err(|e| Box::new(chroma_error::WrappedSqlxError(e)) as Box<dyn ChromaError>)?;
        rows.into_iter()
            .filter(|(_, existed, _, _)| *existed)
            .map(|(id, _, metadata, document)| {
                Ok(ProjectionRecord {
                    id,
                    document,
                    embedding: None,
                    metadata: metadata
                        .map(|metadata| serde_json::from_str::<Metadata>(&metadata))
                        .transpose()
                        .map_err(|e| {
                            Box::new(chroma_error::TonicError(tonic::Status::internal(
                                e.to_string(),
                            ))) as Box<dyn ChromaError>
                        })?,
                })
            })
            .collect()
    }
}
