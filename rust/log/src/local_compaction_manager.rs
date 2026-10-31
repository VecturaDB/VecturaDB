// Changed from the original Chroma file; see the git history for the changes and their authors.
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{oneshot, Notify};
use tracing::Instrument;

/// When the SPANN collections of the single node flush the records of their log to their files, with the names and the
/// defaults of distributed Chroma: of its compactor (rust/worker/src/compactor/config.rs) and of its log service, which
/// picks the collections to compact (timeout_us, rust/log-service/src/lib.rs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalCompactionManagerConfig {
    /// How often the flushes run.
    #[serde(default = "LocalCompactionManagerConfig::default_compaction_interval_sec")]
    pub compaction_interval_sec: u64,
    /// The records of the log after its last flush from which a collection flushes.
    #[serde(default = "LocalCompactionManagerConfig::default_min_compaction_size")]
    pub min_compaction_size: usize,
    /// The records of the log that one flush writes at most.
    #[serde(default = "LocalCompactionManagerConfig::default_max_compaction_size")]
    pub max_compaction_size: usize,
    /// How long the oldest record of the log after the last flush waits, at most, before its collection flushes with
    /// fewer records than min_compaction_size.
    #[serde(default = "LocalCompactionManagerConfig::default_timeout_us")]
    pub timeout_us: u64,
}

impl LocalCompactionManagerConfig {
    fn default_compaction_interval_sec() -> u64 {
        10
    }

    fn default_min_compaction_size() -> usize {
        10
    }

    fn default_max_compaction_size() -> usize {
        10_000
    }

    fn default_timeout_us() -> u64 {
        86_400_000_000
    }
}

impl Default for LocalCompactionManagerConfig {
    fn default() -> Self {
        Self {
            compaction_interval_sec: Self::default_compaction_interval_sec(),
            min_compaction_size: Self::default_min_compaction_size(),
            max_compaction_size: Self::default_max_compaction_size(),
            timeout_us: Self::default_timeout_us(),
        }
    }
}

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
    // The versions that the flushes of SPANN collections replaced within the grace, by segment.
    spann_versions: HashMap<SegmentUuid, ReplacedVersions>,
    spann_version_grace: Duration,
    // When SPANN collections flush: see LocalCompactionManagerConfig.
    compaction_interval: Duration,
    min_compaction_size: usize,
    max_compaction_size: usize,
    compaction_timeout: Duration,
    // The flushes of SPANN collections that run out of the actor, with the one-off flushes that wait for them.
    spann_flushes: SpannFlushes,
    spann_flush_stop: Option<SpannFlushStop>,
}

// A request of a one-off flush, answered at the end of its flush.
type FlushRequest = oneshot::Sender<Result<(), CompactionManagerError>>;

// The collections whose flush runs out of the actor, each with the requests of one-off flushes that its end answers, and
// the requests of one-off flushes of those collections asked while it runs, which wait for it to end and then get a
// flush of their own, as the scheduler of distributed Chroma keeps a one-off collection whose compaction is in progress
// for a later round (oneoff_collections in rust/worker/src/compactor/scheduler.rs).
#[derive(Debug, Default)]
struct SpannFlushes {
    running: HashMap<CollectionUuid, Vec<FlushRequest>>,
    queued: HashMap<CollectionUuid, Vec<FlushRequest>>,
}

// The requests belong to the actor that runs: a clone of the manager, as the one the registry keeps, has none.
impl Clone for SpannFlushes {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Default)]
struct ReplacedVersions {
    // The files of each replaced version, with when it was replaced, oldest first.
    replaced: VecDeque<(Instant, HashMap<String, Vec<String>>)>,
    removed_at: Option<Instant>,
}

impl ReplacedVersions {
    // When a flush last touched the segment.
    fn last_flush(&self) -> Option<Instant> {
        self.replaced
            .back()
            .map(|(replaced_at, _)| *replaced_at)
            .max(self.removed_at)
    }
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

/// For tests: the first flush of a SPANN collection out of the actor stops with its log read and before it writes its
/// files, until the test lets it go on, so that the test writes, deletes or resets meanwhile. Only that flush: the later
/// ones go on. Injectable, as SpannVersionGrace; none by default.
#[derive(Clone, Debug)]
pub struct SpannFlushStop {
    armed: Arc<AtomicBool>,
    reached: Arc<Notify>,
    go_on: Arc<Notify>,
}

impl Default for SpannFlushStop {
    fn default() -> Self {
        Self {
            armed: Arc::new(AtomicBool::new(true)),
            reached: Arc::default(),
            go_on: Arc::default(),
        }
    }
}

impl SpannFlushStop {
    /// Waits until a flush stops.
    pub async fn reached(&self) {
        self.reached.notified().await;
    }

    /// Lets the flush that stopped go on.
    pub fn go_on(&self) {
        self.go_on.notify_one();
    }

    async fn stop(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.go_on.notified().await;
        }
    }
}

impl Injectable for SpannFlushStop {}

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
        config: &LocalCompactionManagerConfig,
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
            compaction_interval: Duration::from_secs(config.compaction_interval_sec),
            min_compaction_size: config.min_compaction_size,
            max_compaction_size: config.max_compaction_size,
            compaction_timeout: Duration::from_micros(config.timeout_us),
            spann_flushes: SpannFlushes::default(),
            spann_flush_stop: registry.get::<SpannFlushStop>().ok(),
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

    // As the compaction manager of distributed Chroma, which schedules its first compaction when it starts
    // (rust/worker/src/compactor/compaction_manager.rs).
    async fn on_start(&mut self, ctx: &ComponentContext<Self>) -> () {
        ctx.scheduler.schedule(
            ScheduledCompactMessage {},
            self.compaction_interval,
            ctx,
            || {
                Some(tracing::span!(
                    parent: None,
                    tracing::Level::INFO,
                    "Scheduled compaction"
                ))
            },
        );
    }
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
    #[error("The flush of a SPANN collection panicked")]
    SpannFlushPanicked,
    #[error("The flush of a SPANN collection failed: {0}")]
    SpannFlushFailed(String),
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
            CompactionManagerError::SpannFlushPanicked => ErrorCodes::Internal,
            CompactionManagerError::SpannFlushFailed(_) => ErrorCodes::Internal,
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

/// Every compaction interval, the flushes of the SPANN collections that the log picks, as the scheduled compaction of
/// distributed Chroma (ScheduledCompactMessage, rust/worker/src/compactor/compaction_manager.rs). They run out of the
/// actor; the answer tells when they are all done.
#[derive(Clone, Debug)]
pub struct ScheduledCompactMessage {}

/// The flushes of the SPANN collections named, without the thresholds, as a one-off compaction of distributed Chroma
/// (OneOffCompactMessage, rust/worker/src/compactor/types.rs), which compacts them at its next round even below
/// min_compaction_size. Here they start at once, out of the actor, one after the other; a collection whose flush is in
/// progress gets its own after it, as there a one-off collection whose compaction is in progress waits for a later round
/// (rust/worker/src/compactor/scheduler.rs). The answer has one receiver for each collection, which tells the end of its
/// flush: the tests that ask for them read the files once they are written.
#[derive(Clone, Debug)]
pub struct OneOffCompactMessage {
    pub collection_ids: Vec<CollectionUuid>,
}

/// The end of a flush of a SPANN collection out of the actor: the collection with the files that the flush wrote and
/// registered, none when its log had nothing to flush, or its failure.
#[derive(Debug)]
pub struct SpannFlushed {
    collection_id: CollectionUuid,
    result: Result<Option<(CollectionAndSegments, SpannFlush)>, CompactionManagerError>,
}

/// The end of the flushes of a round of the timer, out of the actor: the next round, and the answer to its message.
#[derive(Debug)]
pub struct SpannRoundDone {
    round: oneshot::Sender<()>,
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
        let spann = collection_and_segments.vector_segment.r#type == SegmentType::Spann;
        if spann {
            // Until the log is applied again, a read of the collection replays it.
            self.hnsw_segment_manager
                .set_spann_applied(message.collection_id, false);
        }
        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match collection_and_segments.collection.dimension {
            Some(dim) => dim,
            None => {
                if spann {
                    self.hnsw_segment_manager
                        .set_spann_applied(message.collection_id, true);
                }
                return Ok(());
            }
        };
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
        // A SPANN collection takes from the log only the records that its metadata lacks: its files take them at a later
        // flush (SpannFlusher::flush), and a read merges those after the last flush with the files.
        let hnsw_max_seq_id = if spann {
            mt_max_seq_id
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
            self.hnsw_segment_manager
                .set_spann_applied(message.collection_id, true);
            return Ok(());
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

#[async_trait]
impl Handler<ScheduledCompactMessage> for LocalCompactionManager {
    type Result = oneshot::Receiver<()>;

    async fn handle(
        &mut self,
        _message: ScheduledCompactMessage,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        let collections = match self.spann_collections_to_compact().await {
            Ok(collections) => collections,
            Err(err) => {
                tracing::warn!(error = %err, "Scheduled compaction of SPANN collections failed; the next one tries again");
                Vec::new()
            }
        };
        // As the scheduler of distributed Chroma: a collection whose flush is in progress waits for a later round.
        let collections = collections
            .into_iter()
            .filter(|collection_id| !self.spann_flushes.running.contains_key(collection_id))
            .collect::<Vec<_>>();
        for collection_id in &collections {
            self.spann_flushes
                .running
                .insert(*collection_id, Vec::new());
        }
        let (round, done) = oneshot::channel();
        self.launch_spann_flushes(collections, Some(round), ctx);
        done
    }
}

#[async_trait]
impl Handler<SpannRoundDone> for LocalCompactionManager {
    type Result = ();

    async fn handle(
        &mut self,
        message: SpannRoundDone,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) {
        // As distributed Chroma: the next compaction once this one is done.
        ctx.scheduler.schedule(
            ScheduledCompactMessage {},
            self.compaction_interval,
            ctx,
            || {
                Some(tracing::span!(
                    parent: None,
                    tracing::Level::INFO,
                    "Scheduled compaction"
                ))
            },
        );
        // The message of the round may come from no one who waits for it, as from the scheduler.
        let _ = message.round.send(());
    }
}

#[async_trait]
impl Handler<OneOffCompactMessage> for LocalCompactionManager {
    type Result = Vec<oneshot::Receiver<Result<(), CompactionManagerError>>>;

    async fn handle(
        &mut self,
        message: OneOffCompactMessage,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        let mut answers = Vec::new();
        let mut launch = Vec::new();
        for collection_id in message.collection_ids {
            let (request, answer) = oneshot::channel();
            answers.push(answer);
            match self.spann_flushes.running.entry(collection_id) {
                // A flush of the collection in progress: this one gets its own after it.
                Entry::Occupied(_) => self
                    .spann_flushes
                    .queued
                    .entry(collection_id)
                    .or_default()
                    .push(request),
                Entry::Vacant(running) => {
                    running.insert(vec![request]);
                    launch.push(collection_id);
                }
            }
        }
        if !launch.is_empty() {
            self.launch_spann_flushes(launch, None, ctx);
        }
        answers
    }
}

#[async_trait]
impl Handler<SpannFlushed> for LocalCompactionManager {
    type Result = ();

    async fn handle(
        &mut self,
        message: SpannFlushed,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) {
        let SpannFlushed {
            collection_id,
            result,
        } = message;
        let result = self.finish_spann_flush(collection_id, result).await;
        if let Err(err) = &result {
            tracing::warn!(
                collection_id = %collection_id,
                error = %err,
                "SPANN flush failed; its records stay in the log for the next round"
            );
        }
        let requests = self
            .spann_flushes
            .running
            .remove(&collection_id)
            .unwrap_or_default();
        answer_flush_requests(requests, result);
        // The one-off flushes asked while this one ran get their own, now that it ended.
        if let Some(queued) = self.spann_flushes.queued.remove(&collection_id) {
            self.spann_flushes.running.insert(collection_id, queued);
            self.launch_spann_flushes(vec![collection_id], None, ctx);
        }
    }
}

// The answer of a flush to each request that waits for it: the first one takes its failure, the others its message.
fn answer_flush_requests(requests: Vec<FlushRequest>, result: Result<(), CompactionManagerError>) {
    let message = result.as_ref().err().map(ToString::to_string);
    let mut result = Some(result);
    for request in requests {
        let answer = result.take().unwrap_or_else(|| {
            message.clone().map_or(Ok(()), |message| {
                Err(CompactionManagerError::SpannFlushFailed(message))
            })
        });
        // A request whose sender stopped waiting has no one to answer.
        let _ = request.send(answer);
    }
}

// The files of a flush of a SPANN collection, and the log position they cover.
#[derive(Debug)]
struct SpannFlush {
    log_position: i64,
    record_files: HashMap<String, Vec<String>>,
    vector_files: HashMap<String, Vec<String>>,
}

impl LocalCompactionManager {
    // The SPANN collections that the log picks for a round, as the scheduler of distributed Chroma picks the collections
    // to compact (rust/worker/src/compactor/compaction_manager.rs).
    async fn spann_collections_to_compact(
        &mut self,
    ) -> Result<Vec<CollectionUuid>, CompactionManagerError> {
        let Log::Sqlite(log) = &mut self.log else {
            return Ok(Vec::new());
        };
        log.get_spann_collections_to_compact(
            self.min_compaction_size as u64,
            self.compaction_timeout,
        )
        .await
        .map_err(|_| CompactionManagerError::PullLogsFailure)
    }

    // What a flush needs, out of the actor.
    fn spann_flusher(&self) -> SpannFlusher {
        SpannFlusher {
            log: self.log.clone(),
            sqlite_db: self.sqlite_db.clone(),
            hnsw_segment_manager: self.hnsw_segment_manager.clone(),
            sysdb: self.sysdb.clone(),
            max_compaction_size: self.max_compaction_size,
            stop: self.spann_flush_stop.clone(),
        }
    }

    // The flushes of the collections given, one after the other, out of the actor, as the compaction manager of
    // distributed Chroma runs its compactions out of its own (compact_awaiter_loop in
    // rust/worker/src/compactor/compaction_manager.rs), so that the writes and the reads do not wait behind them: the end
    // of each comes back to the actor as a SpannFlushed, then the end of a round as a SpannRoundDone. A flush that
    // panics ends with an error, as there. The collections are in spann_flushes.running already.
    fn launch_spann_flushes(
        &self,
        collections: Vec<CollectionUuid>,
        round: Option<oneshot::Sender<()>>,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) {
        let flusher = self.spann_flusher();
        let flushed = ctx.receiver::<SpannFlushed>();
        let done = ctx.receiver::<SpannRoundDone>();
        let flushes = async move {
            for collection_id in collections {
                let result = AssertUnwindSafe(flusher.clone().flush(collection_id))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(CompactionManagerError::SpannFlushPanicked));
                if let Err(err) = flushed
                    .send(
                        SpannFlushed {
                            collection_id,
                            result,
                        },
                        None,
                    )
                    .await
                {
                    tracing::error!(
                        collection_id = %collection_id,
                        error = %err,
                        "The end of a SPANN flush did not reach the compaction manager"
                    );
                }
            }
            if let Some(round) = round {
                if let Err(err) = done.send(SpannRoundDone { round }, None).await {
                    tracing::error!(error = %err, "The end of a round of SPANN flushes did not reach the compaction manager");
                }
            }
        };
        tokio::spawn(flushes.instrument(tracing::info_span!(parent: None, "SPANN flushes")));
    }

    // The end of a flush, in the actor: the flush moves the watermark that bounds a purge of the log, the files of the
    // versions it replaced go once the grace allows it, and the reads of the collection open its new version.
    async fn finish_spann_flush(
        &mut self,
        collection_id: CollectionUuid,
        result: Result<Option<(CollectionAndSegments, SpannFlush)>, CompactionManagerError>,
    ) -> Result<(), CompactionManagerError> {
        let Some((collection_and_segments, flush)) = result? else {
            return Ok(());
        };
        self.purged.remove(&collection_id);
        let mut record_segment = collection_and_segments.record_segment.clone();
        record_segment.file_path = flush.record_files.clone();
        for (segment, files) in [
            (&collection_and_segments.record_segment, flush.record_files),
            (&collection_and_segments.vector_segment, flush.vector_files),
        ] {
            if let Err(err) = self
                .remove_replaced_versions(&collection_and_segments.collection, segment, files)
                .await
            {
                tracing::warn!(error = %err, "SPANN flush done; the files of older versions stay for the next flush");
            }
        }
        self.hnsw_segment_manager
            .spann_flushed(collection_id, record_segment);
        Ok(())
    }
    // The files of the versions of a SPANN segment that a flush replaced longer than the grace ago, removed, at most
    // once a grace for each segment. The version before the current one stays, as min_versions_to_keep 2 in the garbage
    // collector of distributed Chroma. A collection flushes one at a time here, and its next flush starts once the end of
    // this one reached the actor, so no file of a flush to come is on disk yet; after a restart, the files of the
    // versions replaced before it go at the first flush.
    async fn remove_replaced_versions(
        &mut self,
        collection: &Collection,
        segment: &Segment,
        current: HashMap<String, Vec<String>>,
    ) -> Result<(), LocalSegmentManagerError> {
        let now = Instant::now();
        let grace = self.spann_version_grace;
        // A segment that no flush touched for longer than the grace, as one of a deleted collection, has no version left
        // to keep for the reads: its next flush keeps the version before its current one anyway.
        self.spann_versions.retain(|segment_id, versions| {
            *segment_id == segment.id
                || versions
                    .last_flush()
                    .is_some_and(|last_flush| now.duration_since(last_flush) < grace)
        });
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

// What a flush of a SPANN collection needs, out of the actor, as the context of the compactions of distributed Chroma
// (CompactionManagerContext in rust/worker/src/compactor/compaction_manager.rs), which each compaction takes a clone of.
#[derive(Clone)]
struct SpannFlusher {
    log: Log,
    sqlite_db: SqliteDb,
    hnsw_segment_manager: LocalSegmentManager,
    sysdb: SysDb,
    max_compaction_size: usize,
    stop: Option<SpannFlushStop>,
}

impl SpannFlusher {
    // A flush of a SPANN collection: the records of its log after its last flush, up to the last record applied to its
    // metadata and at most max_compaction_size of them, as the fetch of the logs of a compaction of distributed Chroma
    // reads at most max_compaction_size (rust/worker/src/execution/orchestration/log_fetch_orchestrator.rs), written to
    // new files (write_spann), which a transaction records with the log position they cover, as the register of a
    // compaction (rust/worker/src/execution/orchestration/register_orchestrator.rs). The metadata holds those records
    // already: a write applies them to SQLite before it answers.
    async fn flush(
        mut self,
        collection_id: CollectionUuid,
    ) -> Result<Option<(CollectionAndSegments, SpannFlush)>, CompactionManagerError> {
        let mut collection_and_segments = self
            .sysdb
            .get_collection_with_segments(None, collection_id)
            .await?;
        if collection_and_segments.vector_segment.r#type != SegmentType::Spann {
            return Ok(None);
        }
        if collection_and_segments.collection.schema.is_none() {
            collection_and_segments.collection.schema = Some(
                Schema::try_from(&collection_and_segments.collection.config)
                    .map_err(CompactionManagerError::SchemaReconcileError)?,
            );
        }
        let Some(dim) = collection_and_segments.collection.dimension else {
            return Ok(None);
        };
        let metadata_reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let applied = metadata_reader
            .current_max_seq_id(&collection_and_segments.metadata_segment.id)
            .await? as i64;
        let flushed = metadata_reader
            .current_max_seq_id(&collection_and_segments.vector_segment.id)
            .await? as i64;
        if applied <= flushed {
            return Ok(None);
        }
        let dbname = DatabaseName::new(collection_and_segments.collection.database.clone())
            .ok_or(CompactionManagerError::PullLogsFailure)?;
        let mut logs = self
            .log
            .read(
                &collection_and_segments.collection.tenant,
                dbname,
                collection_id,
                flushed,
                self.max_compaction_size as i32,
                None,
            )
            .await
            .map_err(|_| CompactionManagerError::PullLogsFailure)?;
        // A write puts its records in the log, then applies them to the metadata: the records past the metadata, if any,
        // wait for it.
        logs.retain(|log| log.log_offset <= applied);
        if let Some(stop) = &self.stop {
            stop.stop().await;
        }
        let flush = self
            .write_spann(
                &collection_and_segments,
                dim as usize,
                Chunk::new(logs.into()),
            )
            .await;
        let flush = match flush {
            Ok(Some(flush)) => self.register(&collection_and_segments, flush).await,
            other => other,
        };
        if flush.is_err() {
            self.remove_files_of_lost_segments(&collection_and_segments)
                .await;
        }
        Ok(flush?.map(|flush| (collection_and_segments, flush)))
    }

    // The files of a flush, recorded with the log position they cover in one transaction, as the register of a compaction
    // of distributed Chroma (rust/worker/src/execution/orchestration/register_orchestrator.rs). It fails for a collection
    // that the catalog no longer has, as after a delete or a reset during the flush, and for a flush older than the last
    // one of the collection.
    async fn register(
        &self,
        collection_and_segments: &CollectionAndSegments,
        flush: SpannFlush,
    ) -> Result<Option<SpannFlush>, CompactionManagerError> {
        let collection_id = collection_and_segments.collection.collection_id;
        let metadata_writer = SqliteMetadataWriter::new(self.sqlite_db.clone());
        let mut tx = metadata_writer
            .begin()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        chroma_sysdb::sqlite::flush_compaction_in(
            &mut tx,
            collection_id,
            flush.log_position,
            collection_and_segments.collection.version,
            &[
                SegmentFlushInfo {
                    segment_id: collection_and_segments.record_segment.id,
                    file_paths: flush.record_files.clone(),
                },
                SegmentFlushInfo {
                    segment_id: collection_and_segments.vector_segment.id,
                    file_paths: flush.vector_files.clone(),
                },
            ],
            None,
        )
        .await
        .map_err(|err| CompactionManagerError::SpannWriteError(err.boxed()))?;
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        Ok(Some(flush))
    }

    // After a flush that failed, the files of the segments of the collection that the catalog no longer has, as after a
    // delete or a reset during the flush: the files that the flush wrote after the delete removed those of the segment
    // go with them. delete_blockfile_segment_files leaves the files of a segment that the catalog has.
    async fn remove_files_of_lost_segments(&self, collection_and_segments: &CollectionAndSegments) {
        for segment in [
            &collection_and_segments.record_segment,
            &collection_and_segments.vector_segment,
        ] {
            if let Err(err) = self
                .hnsw_segment_manager
                .delete_blockfile_segment_files(
                    collection_and_segments.collection.collection_id,
                    segment.id,
                )
                .await
            {
                tracing::warn!(
                    segment_id = %segment.id,
                    error = %err,
                    "The files of a segment that a failed SPANN flush wrote stay for the sweep of the folders"
                );
            }
        }
    }

    // As the compaction of distributed Chroma: the records are materialized against the record segment, applied to it
    // and to the SPANN index, and the new files of both written; the caller records them with the log position they
    // cover. None when the log has nothing past the last flush.
    async fn write_spann(
        &self,
        collection_and_segments: &CollectionAndSegments,
        dimensionality: usize,
        logs: Chunk<LogRecord>,
    ) -> Result<Option<SpannFlush>, CompactionManagerError> {
        let Some(log_position) = logs.iter().map(|(log, _)| log.log_offset).max() else {
            return Ok(None);
        };
        let write = |err: Box<dyn ChromaError>| CompactionManagerError::SpannWriteError(err);
        let provider = self
            .hnsw_segment_manager
            .compaction_spann_provider()
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
        Ok(Some(SpannFlush {
            log_position,
            record_files,
            vector_files,
        }))
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
