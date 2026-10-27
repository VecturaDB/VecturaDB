// Changed from the original Chroma file; see the git history for the changes and their authors.
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::time::{Duration, Instant};

use crate::Log;
use async_trait::async_trait;
use chroma_config::registry::{Injectable, Registry};
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::blockfile_record::{
    RecordSegmentReaderOptions, RecordSegmentReaderShard, RecordSegmentReaderShardCreationError,
    RecordSegmentWriterShard,
};
use chroma_segment::local_hnsw::LocalHnswSegmentReaderError;
use chroma_segment::local_segment_manager::{LocalSegmentManager, LocalSegmentManagerError};
use chroma_segment::sqlite_metadata::{
    SqliteMetadataError, SqliteMetadataReader, SqliteMetadataWriter,
};
use chroma_segment::types::materialize_logs;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::Handler;
use chroma_system::{Component, ComponentContext};
use chroma_types::{
    Chunk, Collection, CollectionAndSegments, CollectionUuid, DatabaseName,
    GetCollectionWithSegmentsError, LogRecord, Schema, SchemaError, Segment, SegmentFlushInfo,
    SegmentShard, SegmentType, SegmentUuid,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalCompactionManagerConfig {}

#[derive(Clone)]
pub struct LocalCompactionManager {
    log: Log,
    sqlite_db: SqliteDb,
    hnsw_segment_manager: LocalSegmentManager,
    sysdb: SysDb,
    metrics: LocalCompactionMetrics,
    // Collections with nothing left to purge when last checked. The watermarks that bound a purge move when a
    // backfill applies logs, or when an index loads a newer checkpoint: a new instance, which replays through a
    // backfill before a read or a write goes on. Each backfill forgets its collection, any other purge request
    // skips SQLite. A change missed here only delays a purge, which never decides what a read returns.
    purged: HashSet<CollectionUuid>,
    // The versions that the flushes of SPANN collections replaced since the start, by segment.
    spann_versions: HashMap<SegmentUuid, ReplacedVersions>,
    spann_version_grace: Duration,
}

#[derive(Clone, Debug, Default)]
struct ReplacedVersions {
    // The files of each replaced version, with when it was replaced, oldest first.
    replaced: VecDeque<(Instant, HashMap<String, Vec<String>>)>,
    removed_at: Option<Instant>,
}

/// How long the files of a version of a SPANN collection stay after a flush replaced it, for the reads that opened it:
/// a minute, as the garbage collector of distributed Chroma in rust/worker/chroma_config.yaml
/// (relative_cutoff_time_seconds). Injectable for tests.
#[derive(Clone, Copy, Debug)]
pub struct SpannVersionGrace(pub Duration);

impl Default for SpannVersionGrace {
    fn default() -> Self {
        Self(Duration::from_secs(60))
    }
}

impl Injectable for SpannVersionGrace {}

// Forgetting a collection costs one more check, so the set is emptied instead of growing with every collection
// a long-running server has read.
const MAX_PURGED_COLLECTIONS: usize = 65_536;

/// Counters for expensive local recovery work, with an injectable meter for tests.
#[derive(Clone)]
pub struct LocalCompactionMetrics {
    replay_attempts: opentelemetry::metrics::Counter<u64>,
    checkpoint_inspections: opentelemetry::metrics::Counter<u64>,
}

impl Injectable for LocalCompactionMetrics {}

impl LocalCompactionMetrics {
    /// Register local recovery counters with the supplied meter.
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            replay_attempts: meter
                .u64_counter("local_log_replay_attempts")
                .with_description("Local log replay attempts, including failed attempts")
                .build(),
            checkpoint_inspections: meter
                .u64_counter("local_purge_checkpoint_inspections")
                .with_description("Persisted HNSW checkpoint inspections before log purging")
                .build(),
        }
    }
}

impl Default for LocalCompactionMetrics {
    fn default() -> Self {
        Self::new(&opentelemetry::global::meter("chroma.local_compaction"))
    }
}

impl Injectable for LocalCompactionManager {}

#[async_trait]
impl Configurable<LocalCompactionManagerConfig> for LocalCompactionManager {
    async fn try_from_config(
        _config: &LocalCompactionManagerConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let log = registry.get::<Log>().map_err(|e| e.boxed())?;
        let sqlite_db = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        let hnsw_segment_manager = registry
            .get::<LocalSegmentManager>()
            .map_err(|e| e.boxed())?;
        let sysdb = registry.get::<SysDb>().map_err(|e| e.boxed())?;
        let res = Self {
            log,
            sqlite_db,
            hnsw_segment_manager,
            sysdb,
            metrics: registry.get::<LocalCompactionMetrics>().unwrap_or_default(),
            purged: HashSet::new(),
            spann_versions: HashMap::new(),
            spann_version_grace: registry.get::<SpannVersionGrace>().unwrap_or_default().0,
        };
        registry.register(res.clone());
        Ok(res)
    }
}

#[async_trait]
impl Component for LocalCompactionManager {
    fn get_name() -> &'static str {
        "Local Compaction manager"
    }

    fn queue_size(&self) -> usize {
        // TODO(Sanket): Make this configurable.
        1000
    }

    async fn on_start(&mut self, _: &ComponentContext<Self>) -> () {}
}

impl Debug for LocalCompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCompactionManager").finish()
    }
}

#[derive(Error, Debug)]
pub enum CompactionManagerError {
    #[error("Failed to pull logs from the log store")]
    PullLogsFailure,
    #[error("Failed to apply logs to the metadata segment")]
    MetadataApplyLogsFailed,
    #[error("Failed to get hnsw segment writer")]
    GetHnswWriterFailed,
    #[error("Failed to apply logs to the hnsw segment writer")]
    HnswApplyLogsError,
    #[error("Error getting collection with segments: {0}")]
    GetCollectionWithSegmentsError(#[from] GetCollectionWithSegmentsError),
    #[error("Error reading from metadata segment reader: {0} ")]
    MetadataReaderError(#[from] SqliteMetadataError),
    #[error("Error reading from hnsw segment reader: {0}")]
    HnswReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error constructing hnsw segment reader: {0}")]
    HnswReaderConstructionError(#[from] LocalSegmentManagerError),
    #[error("Error purging logs")]
    PurgeLogsFailure,
    #[error("Cannot purge logs without a usable HNSW checkpoint: {0}")]
    UnsafeHnswCheckpoint(#[source] std::io::Error),
    #[error("Failed to reconcile collection schema: {0}")]
    SchemaReconcileError(#[from] SchemaError),
    #[error("SPANN collections need a persistent data path")]
    SpannUnavailable,
    #[error("Error writing the SPANN index: {0}")]
    SpannWriteError(Box<dyn ChromaError>),
}

impl ChromaError for CompactionManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionManagerError::PullLogsFailure => ErrorCodes::InvalidArgument,
            CompactionManagerError::MetadataApplyLogsFailed => ErrorCodes::Internal,
            CompactionManagerError::GetHnswWriterFailed => ErrorCodes::Internal,
            CompactionManagerError::HnswApplyLogsError => ErrorCodes::Internal,
            CompactionManagerError::GetCollectionWithSegmentsError(e) => e.code(),
            CompactionManagerError::MetadataReaderError(e) => e.code(),
            CompactionManagerError::HnswReaderError(e) => e.code(),
            CompactionManagerError::HnswReaderConstructionError(e) => e.code(),
            CompactionManagerError::PurgeLogsFailure => ErrorCodes::Internal,
            CompactionManagerError::UnsafeHnswCheckpoint(_) => ErrorCodes::FailedPrecondition,
            CompactionManagerError::SpannUnavailable => ErrorCodes::FailedPrecondition,
            CompactionManagerError::SpannWriteError(e) => e.code(),
            CompactionManagerError::SchemaReconcileError(e) => e.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PurgeLogsMessage {
    pub collection_id: CollectionUuid,
}

#[derive(Clone, Debug)]
pub struct BackfillMessage {
    pub collection_id: CollectionUuid,
}

#[async_trait]
impl Handler<BackfillMessage> for LocalCompactionManager {
    type Result = Result<(), CompactionManagerError>;

    async fn handle(
        &mut self,
        message: BackfillMessage,
        _: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        // Applying logs moves the watermarks, even when the backfill fails part way.
        self.purged.remove(&message.collection_id);
        self.metrics.replay_attempts.add(1, &[]);
        let mut collection_and_segments = self
            .sysdb
            .get_collection_with_segments(None, message.collection_id)
            .await?;
        let schema_previously_persisted = collection_and_segments.collection.schema.is_some();
        if !schema_previously_persisted {
            collection_and_segments.collection.schema = Some(
                Schema::try_from(&collection_and_segments.collection.config)
                    .map_err(CompactionManagerError::SchemaReconcileError)?,
            );
        }
        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match collection_and_segments.collection.dimension {
            Some(dim) => dim,
            None => return Ok(()),
        };
        let spann = collection_and_segments.vector_segment.r#type == SegmentType::Spann;
        let hnsw_reader = if spann {
            Err(LocalSegmentManagerError::LocalHnswSegmentReaderError(
                LocalHnswSegmentReaderError::UninitializedSegment,
            ))
        } else {
            self.hnsw_segment_manager
                .get_hnsw_reader(
                    &collection_and_segments.collection,
                    &collection_and_segments.vector_segment,
                    dim as usize,
                )
                .await
        };
        let hnsw_reader = match hnsw_reader {
            Ok(reader) => Some(reader),
            Err(LocalSegmentManagerError::LocalHnswSegmentReaderError(
                LocalHnswSegmentReaderError::UninitializedSegment,
            )) => None,
            Err(err) => return Err(CompactionManagerError::HnswReaderConstructionError(err)),
        };
        let hnsw_max_seq_id = match &hnsw_reader {
            Some(reader) => {
                reader.index.set_replay_complete(false).await;
                reader
                    .index
                    .applied_seq_id()
                    .await
                    .map_err(|_| CompactionManagerError::GetHnswWriterFailed)?
            }
            None => 0,
        };
        // Invalidate replay completion before reading the metadata watermark:
        // a failed query must leave subsequent reads able to retry pending logs.
        let metadata_reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let mt_max_seq_id = metadata_reader
            .current_max_seq_id(&collection_and_segments.metadata_segment.id)
            .await?;
        // A SPANN index is at the log position of its last flush, which flush_compaction records.
        let hnsw_max_seq_id = if spann {
            metadata_reader
                .current_max_seq_id(&collection_and_segments.vector_segment.id)
                .await?
        } else {
            hnsw_max_seq_id
        };
        // Get the logs from log service beyond this offset to backfill.
        let dbname = DatabaseName::new(collection_and_segments.collection.database.clone())
            .ok_or(CompactionManagerError::PullLogsFailure)?;
        let logs = self
            .log
            .read(
                &collection_and_segments.collection.tenant,
                // It is up to the log impl to use or not use this.
                dbname,
                collection_and_segments.collection.collection_id,
                mt_max_seq_id.min(hnsw_max_seq_id) as i64,
                -1,
                None,
            )
            .await
            .map_err(|_| CompactionManagerError::PullLogsFailure)?;
        // Set the visibility of the records to be backfilled in the metadata segment.
        let mut mt_visibility = vec![true; logs.len()];
        let mut hnsw_visibility = vec![true; logs.len()];
        let data_chunk: Chunk<LogRecord> = Chunk::new(logs.into());
        let mut mt_data_chunk = data_chunk.clone();
        let mut hnsw_data_chunk = data_chunk.clone();
        for (data, index) in data_chunk.iter() {
            if data.log_offset <= mt_max_seq_id as i64 {
                mt_visibility[index] = false;
            }
            if data.log_offset <= hnsw_max_seq_id as i64 {
                hnsw_visibility[index] = false;
            }
        }
        mt_data_chunk.set_visibility(mt_visibility);
        hnsw_data_chunk.set_visibility(hnsw_visibility);
        // Apply the records to the metadata writer.
        let metadata_writer = SqliteMetadataWriter::new(self.sqlite_db.clone());
        let mut tx = metadata_writer
            .begin()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        let apply_outcome = metadata_writer
            .apply_logs(
                mt_data_chunk,
                collection_and_segments.metadata_segment.id,
                if schema_previously_persisted {
                    collection_and_segments.collection.schema.clone()
                } else {
                    None
                },
                &mut *tx,
            )
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        if schema_previously_persisted {
            if let Some(updated_schema) = apply_outcome.schema_update {
                metadata_writer
                    .update_collection_schema(
                        collection_and_segments.collection.collection_id,
                        &updated_schema,
                        &mut *tx,
                    )
                    .await
                    .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
            }
        }
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        if spann {
            return self
                .apply_spann(&collection_and_segments, dim as usize, hnsw_data_chunk)
                .await;
        }
        // Next apply it to the hnsw writer.
        let mut hnsw_writer = self
            .hnsw_segment_manager
            .get_hnsw_writer(
                &collection_and_segments.collection,
                &collection_and_segments.vector_segment,
                dim as usize,
            )
            .await
            .map_err(|_| CompactionManagerError::GetHnswWriterFailed)?;
        hnsw_writer
            .apply_log_chunk(hnsw_data_chunk)
            .await
            .map_err(|_| CompactionManagerError::HnswApplyLogsError)?;
        hnsw_writer.index.set_replay_complete(true).await;
        Ok(())
    }
}

#[async_trait]
impl Handler<PurgeLogsMessage> for LocalCompactionManager {
    type Result = Result<(), CompactionManagerError>;

    async fn handle(
        &mut self,
        message: PurgeLogsMessage,
        _: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        // Every read asks for a purge, to retry one that failed: one that succeeded holds until a backfill.
        if self.purged.contains(&message.collection_id) {
            return Ok(());
        }
        self.purge_logs(message.collection_id).await?;
        if self.purged.len() >= MAX_PURGED_COLLECTIONS {
            self.purged.clear();
        }
        self.purged.insert(message.collection_id);
        Ok(())
    }
}

impl LocalCompactionManager {
    // As the compaction of distributed Chroma: the records are materialized against the record segment, applied to it
    // and to the SPANN index, and the new files of both are recorded with the log position they cover in one flush.
    async fn apply_spann(
        &mut self,
        collection_and_segments: &CollectionAndSegments,
        dimensionality: usize,
        logs: Chunk<LogRecord>,
    ) -> Result<(), CompactionManagerError> {
        let Some(log_position) = logs.iter().map(|(log, _)| log.log_offset).max() else {
            return Ok(());
        };
        let write = |err: Box<dyn ChromaError>| CompactionManagerError::SpannWriteError(err);
        let provider = self
            .hnsw_segment_manager
            .spann_provider()
            .ok_or(CompactionManagerError::SpannUnavailable)?;
        let collection = &collection_and_segments.collection;
        let record_shard = SegmentShard::try_from((&collection_and_segments.record_segment, 0))
            .map_err(|err| write(err.boxed()))?;
        let vector_shard = SegmentShard::try_from((&collection_and_segments.vector_segment, 0))
            .map_err(|err| write(err.boxed()))?;
        let record_reader = match RecordSegmentReaderShard::from_segment(
            &record_shard,
            &provider.blockfile_provider,
            None,
        )
        .await
        {
            Ok(reader) => Some(reader),
            Err(err)
                if matches!(
                    *err,
                    RecordSegmentReaderShardCreationError::UninitializedSegment
                ) =>
            {
                None
            }
            Err(err) => return Err(write(err.boxed())),
        };
        let materialized = materialize_logs(
            &record_reader,
            logs,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .map_err(|err| write(err.boxed()))?;
        let record_writer = RecordSegmentWriterShard::from_segment(
            &collection.tenant,
            &collection.database_id,
            &record_shard,
            &provider.blockfile_provider,
            None,
            None,
        )
        .await
        .map_err(|err| write(err.boxed()))?;
        record_writer
            .apply_materialized_log_chunk(&record_reader, &materialized)
            .await
            .map_err(|err| write(err.boxed()))?;
        let mut spann_writer = provider
            .write(collection, &vector_shard, dimensionality, None)
            .await
            .map_err(|err| write(err.boxed()))?;
        spann_writer
            .apply_materialized_log_chunk(&record_reader, &materialized)
            .await
            .map_err(|err| write(err.boxed()))?;
        let record_files = Box::pin(record_writer.commit())
            .await
            .map_err(|err| write(err.boxed()))?
            .flush()
            .await
            .map_err(|err| write(err.boxed()))?;
        // As the commit of a compaction (rust/worker/src/execution/operators/commit_segment_writer.rs).
        spann_writer.garbage_collect().await.map_err(write)?;
        let vector_files = Box::pin(spann_writer.commit())
            .await
            .map_err(write)?
            .flush()
            .await
            .map_err(write)?;
        let database_name = DatabaseName::new(collection.database.clone())
            .ok_or(CompactionManagerError::PullLogsFailure)?;
        self.sysdb
            .flush_compaction(
                collection.tenant.clone(),
                database_name,
                collection.collection_id,
                log_position,
                collection.version,
                vec![
                    SegmentFlushInfo {
                        segment_id: collection_and_segments.record_segment.id,
                        file_paths: record_files.clone(),
                    },
                    SegmentFlushInfo {
                        segment_id: collection_and_segments.vector_segment.id,
                        file_paths: vector_files.clone(),
                    },
                ]
                .into(),
                0,
                0,
                None,
            )
            .await
            .map_err(|err| write(err.boxed()))?;
        for (segment, files) in [
            (&collection_and_segments.record_segment, record_files),
            (&collection_and_segments.vector_segment, vector_files),
        ] {
            if let Err(err) = self
                .remove_replaced_versions(collection, segment, files)
                .await
            {
                tracing::warn!(error = %err, "SPANN flush done; the files of older versions stay for the next flush");
            }
        }
        Ok(())
    }

    // The files of the versions of a SPANN segment that a flush replaced longer than the grace ago, removed, at most
    // once a grace for each segment. The version before the current one stays, as min_versions_to_keep 2 in the garbage
    // collector of distributed Chroma. Flushes run one at a time here, so no file of a flush to come is on disk yet;
    // after a restart, the files of the versions replaced before it go at the first flush.
    async fn remove_replaced_versions(
        &mut self,
        collection: &Collection,
        segment: &Segment,
        current: HashMap<String, Vec<String>>,
    ) -> Result<(), LocalSegmentManagerError> {
        let now = Instant::now();
        let grace = self.spann_version_grace;
        let versions = self.spann_versions.entry(segment.id).or_default();
        if !segment.file_path.is_empty() {
            versions
                .replaced
                .push_back((now, segment.file_path.clone()));
        }
        while versions.replaced.len() > 1
            && versions
                .replaced
                .front()
                .is_some_and(|(replaced_at, _)| now.duration_since(*replaced_at) >= grace)
        {
            versions.replaced.pop_front();
        }
        if versions
            .removed_at
            .is_some_and(|removed_at| now.duration_since(removed_at) < grace)
        {
            return Ok(());
        }
        versions.removed_at = Some(now);
        let kept = std::iter::once(&current)
            .chain(versions.replaced.iter().map(|(_, files)| files))
            .collect::<Vec<_>>();
        self.hnsw_segment_manager
            .remove_unused_spann_files(
                &segment.construct_prefix_path(&collection.tenant, &collection.database_id),
                &kept,
            )
            .await?;
        Ok(())
    }

    async fn purge_logs(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<(), CompactionManagerError> {
        let collection_segments = self
            .sysdb
            .get_collection_with_segments(None, collection_id)
            .await?;
        // Read durable watermarks without loading HNSW, but do not trust a
        // watermark when its checkpoint has since become unusable.
        let max_seq_id = max_purge_seq_id(
            &self.sqlite_db,
            &collection_segments.metadata_segment.id,
            &collection_segments.vector_segment.id,
        )
        .await?;
        if let Log::Sqlite(log) = &mut self.log {
            if !log
                .has_purge_work(collection_id, max_seq_id)
                .await
                .map_err(|_| CompactionManagerError::PurgeLogsFailure)?
            {
                return Ok(());
            }
        }
        if max_seq_id > 0 && collection_segments.vector_segment.r#type != SegmentType::Spann {
            self.metrics.checkpoint_inspections.add(1, &[]);
            match self
                .hnsw_segment_manager
                .validate_persisted_checkpoint(&collection_segments.vector_segment.id, max_seq_id)
                .await
            {
                Ok(()) => {}
                // The checkpoint could not be verified now, as when a network file system reports its attributes
                // late: nothing is purged without a verification, so the purge waits for the next write.
                Err(err) if chroma_segment::local_hnsw::is_checkpoint_changed(&err) => {
                    tracing::warn!(
                        collection_id = %collection_id,
                        error = %err,
                        "Purge of logs postponed: the HNSW checkpoint changed during its inspection"
                    );
                    return Ok(());
                }
                Err(err) => return Err(CompactionManagerError::UnsafeHnswCheckpoint(err)),
            }
        }
        self.log
            .purge_logs(collection_id, max_seq_id)
            .await
            .map_err(|_| CompactionManagerError::PurgeLogsFailure)
    }
}

// Never use the metadata watermark alone: vectors may still need replay.
async fn max_purge_seq_id(
    sqlite: &SqliteDb,
    metadata: &SegmentUuid,
    vector: &SegmentUuid,
) -> Result<u64, CompactionManagerError> {
    let reader = SqliteMetadataReader::new(sqlite.clone());
    Ok(reader
        .current_max_seq_id(metadata)
        .await?
        .min(reader.current_max_seq_id(vector).await?))
}

#[cfg(test)]
#[path = "local_compaction_manager_tests.rs"]
mod regression_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn purge_requires_both_durable_watermarks() {
        let sqlite = chroma_sqlite::db::test_utils::get_new_sqlite_db().await;
        let metadata = SegmentUuid::new();
        let vector = SegmentUuid::new();
        sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, ?)")
            .bind(metadata.to_string())
            .bind(40_i64)
            .execute(sqlite.get_conn())
            .await
            .unwrap();
        assert_eq!(
            max_purge_seq_id(&sqlite, &metadata, &vector).await.unwrap(),
            0
        );
        sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, ?)")
            .bind(vector.to_string())
            .bind(17_i64)
            .execute(sqlite.get_conn())
            .await
            .unwrap();
        assert_eq!(
            max_purge_seq_id(&sqlite, &metadata, &vector).await.unwrap(),
            17
        );
    }
}
