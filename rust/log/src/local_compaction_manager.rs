// Changed from the original Chroma file; see the git history for the changes and their authors.
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use crate::attached_functions::{
    self,
    run::{FunctionRunner, FunctionWindow},
};
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
    GetCollectionWithSegmentsError, LogRecord, MetadataValue, Schema, SchemaError, Segment,
    SegmentFlushInfo, SegmentShard, SegmentType, SegmentUuid, CHROMA_GROUP_CHUNK_SIBLINGS_KEY,
};
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{oneshot, Notify, Semaphore};
use tokio::task::JoinSet;
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
    /// The records of the log that one flush writes at most: 50,000 on the single node, where a flush of the timer that
    /// takes them all is followed by the next at once; the compactor of distributed Chroma takes 10,000
    /// (rust/worker/chroma_config.yaml), and the measures of the single node under a continuous load drained the log too
    /// slowly with them.
    #[serde(default = "LocalCompactionManagerConfig::default_max_compaction_size")]
    pub max_compaction_size: usize,
    /// The records of a partition of a flush, at least: the records of an id, or of a document when the collection groups
    /// the chunks of its documents, stay in one partition (max_partition_size of the compactor of distributed Chroma).
    #[serde(default = "LocalCompactionManagerConfig::default_max_partition_size")]
    pub max_partition_size: usize,
    /// How many collections flush at once (max_concurrent_jobs of the compactor of distributed Chroma); they share the
    /// threads of the flushes.
    #[serde(default = "LocalCompactionManagerConfig::default_max_concurrent_jobs")]
    pub max_concurrent_jobs: usize,
    /// How long the oldest record of the log after the last flush waits, at most, before its collection flushes with
    /// fewer records than min_compaction_size.
    #[serde(default = "LocalCompactionManagerConfig::default_timeout_us")]
    pub timeout_us: u64,
    /// How long a stop waits for the work that runs out of the actor, the SPANN flushes, and then for the connections of
    /// SQLite, before it closes the database anyway: 30 s, the default terminationGracePeriodSeconds of Kubernetes, which
    /// is what bounds the stop of the components of distributed Chroma, as neither Chroma nor distributed Chroma has a
    /// setting of its own.
    #[serde(default = "LocalCompactionManagerConfig::default_shutdown_grace_sec")]
    pub shutdown_grace_sec: u64,
    /// How often the log of a collection that conditional transactions use is purged, as gc_interval_mins (120) of the
    /// garbage collector of distributed Chroma.
    #[serde(default = "LocalCompactionManagerConfig::default_purge_interval_sec")]
    pub purge_interval_sec: u64,
    /// Where the attached functions write their files, count_to_file_async: the storage of distributed Chroma, in its
    /// format (the storage of fn_consumer_service in rust/worker/chroma_config.yaml), for example s3 with a bucket and
    /// its credentials. Without it, the data path, as the SPANN indexes (Federico, 01/11).
    #[serde(default)]
    pub storage: Option<chroma_storage::config::StorageConfig>,
}

impl LocalCompactionManagerConfig {
    fn default_compaction_interval_sec() -> u64 {
        10
    }

    fn default_min_compaction_size() -> usize {
        10
    }

    fn default_max_compaction_size() -> usize {
        50_000
    }

    fn default_max_partition_size() -> usize {
        5_000
    }

    fn default_max_concurrent_jobs() -> usize {
        50
    }

    fn default_timeout_us() -> u64 {
        86_400_000_000
    }

    fn default_shutdown_grace_sec() -> u64 {
        30
    }

    fn default_purge_interval_sec() -> u64 {
        7_200
    }
}

impl Default for LocalCompactionManagerConfig {
    fn default() -> Self {
        Self {
            compaction_interval_sec: Self::default_compaction_interval_sec(),
            min_compaction_size: Self::default_min_compaction_size(),
            max_compaction_size: Self::default_max_compaction_size(),
            max_partition_size: Self::default_max_partition_size(),
            max_concurrent_jobs: Self::default_max_concurrent_jobs(),
            timeout_us: Self::default_timeout_us(),
            shutdown_grace_sec: Self::default_shutdown_grace_sec(),
            purge_interval_sec: Self::default_purge_interval_sec(),
            storage: None,
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
    // The collections with conditional transactions whose purge was asked since the last purge, which runs on a timer as
    // the garbage collector of distributed Chroma: their log holds hours of records, and a purge after every write would
    // read them each time.
    purges: PurgeSchedule,
    // A purge of the timer runs out of the actor, as the flushes of SPANN collections, so that a large
    // DELETE does not hold up the reads and writes that wait for the actor; the next one waits for its end.
    purging: bool,
    // The versions that the flushes of SPANN collections replaced within the grace, by segment.
    spann_versions: HashMap<SegmentUuid, ReplacedVersions>,
    spann_version_grace: Duration,
    // When SPANN collections flush: see LocalCompactionManagerConfig.
    compaction_interval: Duration,
    min_compaction_size: usize,
    max_compaction_size: usize,
    max_partition_size: usize,
    max_concurrent_jobs: usize,
    compaction_timeout: Duration,
    // The flushes of SPANN collections that run out of the actor, with the one-off flushes that wait for them.
    spann_flushes: SpannFlushes,
    spann_flush_stop: Option<SpannFlushStop>,
    // The work out of the actor, the flushes of SPANN collections, which a stop waits for (LocalCompactionManager::drain),
    // shared by the clones of the manager, as the one the registry keeps.
    background: tokio_util::task::TaskTracker,
    shutdown_grace: Duration,
    // The synchronous attached functions: see FunctionRuns.
    function_runs: FunctionRuns,
    // The storage of local_compaction_manager: storage, when set, for the files of the attached functions.
    function_storage: Option<chroma_storage::Storage>,
}

// The purges asked, run together once the interval has passed since the last ones.
#[derive(Debug, Clone)]
struct PurgeSchedule {
    pending: HashSet<CollectionUuid>,
    interval: Duration,
    last: Instant,
}

impl PurgeSchedule {
    fn new(interval: Duration) -> Self {
        Self {
            pending: HashSet::new(),
            interval,
            last: Instant::now(),
        }
    }

    fn request(&mut self, collection_id: CollectionUuid) {
        self.pending.insert(collection_id);
    }

    fn due(&mut self, now: Instant) -> Vec<CollectionUuid> {
        if now.duration_since(self.last) < self.interval {
            return Vec::new();
        }
        self.last = now;
        self.pending.drain().collect()
    }
}

// The runs of the synchronous attached functions out of the actor (attached_functions::run), one queue for each input
// collection, so that the windows of its applies run in their order; and, by collection, whether it has a ready
// synchronous function when last read, so that a write of a collection without functions reads no catalog. An attach
// writes records BackfillFn to the log of its input collection, and the apply that sees them reads the catalog again.
// How long the queue of a collection waits for a window before it goes.
const QUEUE_IDLE: Duration = Duration::from_secs(600);

#[derive(Clone, Default)]
struct FunctionRuns {
    queues: Arc<
        std::sync::Mutex<
            HashMap<CollectionUuid, tokio::sync::mpsc::UnboundedSender<FunctionWindow>>,
        >,
    >,
    has_functions: HashMap<CollectionUuid, bool>,
}

impl FunctionRuns {
    // A stop waits for the run in progress (LocalCompactionManager::drain), not for an idle queue: a window it loses, the
    // next start runs as the backfill of its function (attached_functions::run::behind).
    fn send(
        &self,
        background: &tokio_util::task::TaskTracker,
        runner: &FunctionRunner,
        window: FunctionWindow,
    ) {
        let collection_id = window.input.collection.collection_id;
        let mut queues = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        let queue = queues.entry(collection_id).or_insert_with(|| {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<FunctionWindow>();
            let runner = runner.clone();
            let queues = Arc::clone(&self.queues);
            let background = background.clone();
            tokio::spawn(
                async move {
                    loop {
                        let window = match tokio::time::timeout(QUEUE_IDLE, receiver.recv()).await {
                            Ok(Some(window)) => window,
                            Ok(None) => break,
                            // An idle queue goes, as after a delete of its collection or a detach of its functions:
                            // under the lock that the sends take, so that no window comes to it after, and only
                            // while empty, so that the next queue of the collection runs after it.
                            Err(_) => {
                                let mut queues = queues.lock().unwrap_or_else(|e| e.into_inner());
                                if receiver.is_empty() {
                                    queues.remove(&collection_id);
                                    break;
                                }
                                continue;
                            }
                        };
                        // A window that fails leaves the completion offset behind: the next window of the collection
                        // runs its function as the backfill of distributed Chroma.
                        if let Err(err) = background.track_future(runner.run(window)).await {
                            tracing::warn!(collection_id = %collection_id, error = %err, "An attached function failed");
                        }
                    }
                }
                .instrument(tracing::info_span!(parent: None, "Attached functions")),
            );
            sender
        });
        if queue.send(window).is_err() {
            tracing::error!(collection_id = %collection_id, "The queue of the attached functions is closed");
        }
    }
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
        let function_storage = match &config.storage {
            Some(storage) => {
                Some(chroma_storage::Storage::try_from_config(storage, registry).await?)
            }
            None => None,
        };
        let res = Self {
            log,
            sqlite_db,
            hnsw_segment_manager,
            sysdb,
            metrics: registry.get::<LocalCompactionMetrics>().unwrap_or_default(),
            purged: HashSet::new(),
            purges: PurgeSchedule::new(Duration::from_secs(config.purge_interval_sec)),
            purging: false,
            spann_versions: HashMap::new(),
            spann_version_grace: registry.get::<SpannVersionGrace>().unwrap_or_default().0,
            compaction_interval: Duration::from_secs(config.compaction_interval_sec),
            min_compaction_size: config.min_compaction_size,
            max_compaction_size: config.max_compaction_size,
            max_partition_size: config.max_partition_size,
            max_concurrent_jobs: config.max_concurrent_jobs,
            compaction_timeout: Duration::from_micros(config.timeout_us),
            spann_flushes: SpannFlushes::default(),
            spann_flush_stop: registry.get::<SpannFlushStop>().ok(),
            background: tokio_util::task::TaskTracker::new(),
            shutdown_grace: Duration::from_secs(config.shutdown_grace_sec),
            function_runs: FunctionRuns::default(),
            function_storage,
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

/// The end of the purge of the log of a collection out of the actor.
#[derive(Debug)]
pub struct LogsPurged {
    collection_id: CollectionUuid,
    result: Result<(), CompactionManagerError>,
}

/// The end of the purges of the timer out of the actor.
#[derive(Debug)]
pub struct PurgesDone;

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
        ctx: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        // A backfill that fails leaves the loaded instance not replayed, so that the next read asks for a backfill
        // again; one that succeeds, or runs, leaves it as it is (see below).
        let mut loaded = None;
        let result = self.backfill(message, &mut loaded, ctx).await;
        if result.is_err() {
            if let Some(index) = loaded {
                index.set_replay_complete(false).await;
            }
        }
        result
    }
}

impl LocalCompactionManager {
    async fn backfill(
        &mut self,
        message: BackfillMessage,
        loaded: &mut Option<chroma_segment::local_hnsw::LocalHnswIndex>,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) -> Result<(), CompactionManagerError> {
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
            // The instance stays replayed while a write's backfill runs: a read that needs the records of its snapshot
            // in the index waits for them (applied_seq_id_in_memory), and the others go on, as in Chroma 1.5.9, whose
            // writes did not reset the flag of the collection. A backfill that fails resets it (handle).
            Some(reader) => {
                *loaded = Some(reader.index.clone());
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
        let applies_records = mt_visibility.iter().any(|visible| *visible);
        // The synchronous attached functions run on the records of this apply, materialized against their state before
        // it, read here, in the same call of the actor and right before the apply.
        let function_window = if applies_records {
            let attaches = data_chunk
                .iter()
                .any(|(record, _)| record.record.operation == chroma_types::Operation::BackfillFn);
            let has_functions = match self.function_runs.has_functions.get(&message.collection_id) {
                // The records of an attach come before its function is ready (create_attached_function_with_backfill
                // writes them, then finishes the creation): their window waits for it in its run.
                _ if attaches => {
                    self.function_runs
                        .has_functions
                        .remove(&message.collection_id);
                    true
                }
                Some(has) => *has,
                None => {
                    // Any attached function not deleted, ready or not yet: a write that comes between the records of an
                    // attach and its ready must not mark the collection without functions.
                    let has: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM attached_functions WHERE input_collection_id = $1 AND is_deleted = FALSE)",
                    )
                    .bind(message.collection_id.to_string())
                    .fetch_one(self.sqlite_db.get_conn())
                    .await
                    .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
                    self.function_runs
                        .has_functions
                        .insert(message.collection_id, has);
                    has
                }
            };
            if has_functions {
                let mut window_logs = data_chunk.clone();
                window_logs.set_visibility(mt_visibility.clone());
                let window_logs = Chunk::new(Arc::from(
                    window_logs
                        .iter()
                        .map(|(record, _)| record.clone())
                        .collect::<Vec<_>>(),
                ));
                let before = attached_functions::run::read_before(
                    &self.sqlite_db,
                    &collection_and_segments,
                    &window_logs,
                )
                .await
                .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
                let end = window_logs
                    .iter()
                    .map(|(record, _)| record.log_offset as u64)
                    .max()
                    .unwrap_or_default();
                Some(FunctionWindow {
                    input: collection_and_segments.clone(),
                    logs: window_logs,
                    before,
                    end,
                    applied_before: mt_max_seq_id,
                    asynchronous: false,
                    counts_version: !spann,
                })
            } else {
                None
            }
        } else {
            None
        };
        // An asynchronous function runs later, from its queue, on the records after its completion offset: the state
        // before this apply is kept for it, in the transaction of the apply (attached_functions::run::record_prior).
        let keeps_prior = match &function_window {
            Some(_) => attached_functions::run::has_live_async_functions(
                &self.sqlite_db,
                message.collection_id,
            )
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?,
            None => false,
        };
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
        if let (true, Some(window)) = (keeps_prior, &function_window) {
            attached_functions::run::record_prior(
                &mut tx,
                message.collection_id,
                &window.logs,
                &window.before,
            )
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        }
        // As a compaction of distributed Chroma, an apply of records is one more version of the collection; a SPANN
        // collection counts its flushes instead (flush_compaction_in), and a replay that applies nothing does not count.
        // The apply of a collection with functions counts it at the end of the run of its window, after their output.
        if applies_records && !spann && function_window.is_none() {
            sqlx::query("UPDATE collections SET version = version + 1 WHERE id = $1")
                .bind(message.collection_id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        }
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        if let Some(window) = function_window {
            let mut runner = self.function_runner();
            runner.backfill = Some(ctx.receiver::<BackfillMessage>());
            // The asynchronous functions of the collection run after its synchronous ones, in the same queue, up to the
            // end of this apply; the round of the timer runs them too when they are behind (async_due).
            let asynchronous = keeps_prior.then(|| FunctionWindow {
                input: window.input.clone(),
                logs: Chunk::new(Arc::from(Vec::new())),
                before: Vec::new(),
                end: window.end,
                applied_before: window.end,
                asynchronous: true,
                counts_version: false,
            });
            self.function_runs.send(&self.background, &runner, window);
            if let Some(asynchronous) = asynchronous {
                self.function_runs
                    .send(&self.background, &runner, asynchronous);
            }
        }
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
        // Every write, and every read that backfilled, asks for a purge. A collection without conditional transactions
        // keeps no record once applied: its purge runs now, on the few records since the last one, as in Chroma 1.5.9.
        // The purge of a collection with conditional transactions may remove the records of hours: it waits for the timer.
        let on_the_timer = match &self.log {
            Log::Sqlite(log) => log.used_by_conditional_transactions(message.collection_id),
            _ => false,
        };
        if !on_the_timer {
            return self.purge_requested(message.collection_id).await;
        }
        self.purges.request(message.collection_id);
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
        // The rounds tell the log when a write it refuses may be sent again.
        if let Log::Sqlite(log) = &self.log {
            log.spann_round_started(self.compaction_interval, self.max_compaction_size as u64);
        }
        // As the compaction manager of distributed Chroma, which schedules its next round once the compactions of this
        // one are kicked off, not once they end (rust/worker/src/compactor/compaction_manager.rs): a collection whose
        // flush takes less than the interval flushes again an interval after the start of its flush, and one still
        // flushing is skipped by the next round, below.
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
        // A synchronous function behind the records applied to its input, as after a kill that lost the queue of its
        // windows, runs as its backfill, as the scheduler of distributed Chroma compacts a collection whose functions
        // are behind its log position (stale_attached_function_ids in compact.rs).
        match attached_functions::run::behind(&self.sqlite_db).await {
            Ok(behind) => {
                let mut runner = self.function_runner();
                runner.backfill = Some(ctx.receiver::<BackfillMessage>());
                for (collection_id, applied) in behind {
                    match self
                        .sysdb
                        .get_collection_with_segments(None, collection_id)
                        .await
                    {
                        Ok(input) => self.function_runs.send(
                            &self.background,
                            &runner,
                            FunctionWindow {
                                counts_version: input.vector_segment.r#type != SegmentType::Spann,
                                input,
                                logs: Chunk::new(Arc::from(Vec::new())),
                                before: Vec::new(),
                                end: applied,
                                applied_before: applied,
                                asynchronous: false,
                            },
                        ),
                        Err(err) => {
                            tracing::warn!(collection_id = %collection_id, error = %err, "The input of an attached function behind is not found")
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "The attached functions behind could not be read")
            }
        }
        // The asynchronous functions behind the records applied to their input, which their queue, the rows of
        // attached_functions (Federico, 04/11), keeps: as the consumer of distributed Chroma polls its queue.
        match attached_functions::run::async_due(&self.sqlite_db).await {
            Ok(due) => {
                let mut runner = self.function_runner();
                runner.backfill = Some(ctx.receiver::<BackfillMessage>());
                for (collection_id, applied) in due {
                    match self
                        .sysdb
                        .get_collection_with_segments(None, collection_id)
                        .await
                    {
                        Ok(input) => self.function_runs.send(
                            &self.background,
                            &runner,
                            FunctionWindow {
                                input,
                                logs: Chunk::new(Arc::from(Vec::new())),
                                before: Vec::new(),
                                end: applied,
                                applied_before: applied,
                                asynchronous: true,
                                counts_version: false,
                            },
                        ),
                        Err(err) => {
                            tracing::warn!(collection_id = %collection_id, error = %err, "The input of an asynchronous attached function is not found")
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "The asynchronous attached functions behind could not be read")
            }
        }
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
        if let Log::Sqlite(log) = &self.log {
            log.spann_round_ended();
        }
        // After the round, so that the purges do not lengthen the round that sets the Retry-After of a refused write.
        if !self.purging {
            let due = self
                .purges
                .due(Instant::now())
                .into_iter()
                .filter(|collection_id| !self.purged.contains(collection_id))
                .collect::<Vec<_>>();
            if !due.is_empty() {
                self.purging = true;
                self.launch_purges(due, ctx);
            }
        }
        // The message of the round may come from no one who waits for it, as from the scheduler.
        let _ = message.round.send(());
    }
}

#[async_trait]
impl Handler<LogsPurged> for LocalCompactionManager {
    type Result = ();

    async fn handle(&mut self, message: LogsPurged, _: &ComponentContext<LocalCompactionManager>) {
        match message.result {
            Ok(()) => {
                if self.purged.len() >= MAX_PURGED_COLLECTIONS {
                    self.purged.clear();
                }
                self.purged.insert(message.collection_id);
            }
            Err(err) => tracing::warn!(
                collection_id = %message.collection_id,
                error = %err,
                "Purge of logs failed; the next request tries again"
            ),
        }
    }
}

#[async_trait]
impl Handler<PurgesDone> for LocalCompactionManager {
    type Result = ();

    async fn handle(&mut self, _: PurgesDone, _: &ComponentContext<LocalCompactionManager>) {
        self.purging = false;
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
        let full = matches!(&result, Ok(Some((_, flush))) if flush.full);
        let result = self.finish_spann_flush(collection_id, result).await;
        let flushed = result.is_ok();
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
        // A flush asked by a one-off request does as much as it was asked: only the flushes of the timer go on.
        let of_the_timer = requests.is_empty();
        answer_flush_requests(requests, result);
        // The one-off flushes asked while this one ran get their own, now that it ended.
        if let Some(queued) = self.spann_flushes.queued.remove(&collection_id) {
            self.spann_flushes.running.insert(collection_id, queued);
            self.launch_spann_flushes(vec![collection_id], None, ctx);
        } else if full && flushed && of_the_timer {
            // A flush of the timer that took max_compaction_size records leaves more waiting: the next one starts now,
            // not at the next round, so that a collection written faster than one flush a round takes catches up.
            self.spann_flushes.running.insert(collection_id, Vec::new());
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
    // Whether the flush took max_compaction_size records, so that more may wait for the next one.
    full: bool,
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

    /// How long a stop waits, for the work out of the actor and then for the connections of SQLite.
    pub fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }

    /// The first step of a stop, once the actor has stopped: no new work out of the actor, and the work that runs waited
    /// for within the grace given, so that no flush holds a connection of SQLite when the database closes. True if it
    /// all ended in time.
    pub async fn drain(&self, grace: Duration) -> bool {
        self.background.close();
        tokio::time::timeout(grace, self.background.wait())
            .await
            .is_ok()
    }

    // What a flush needs, out of the actor.
    fn function_runner(&self) -> FunctionRunner {
        FunctionRunner {
            log: self.log.clone(),
            sysdb: self.sysdb.clone(),
            sqlite_db: self.sqlite_db.clone(),
            backfill: None,
            storage: self.function_storage.clone().or_else(|| {
                self.hnsw_segment_manager
                    .spann_provider()
                    .and_then(|provider| provider.blockfile_provider.storage())
                    .map(|storage| (*storage).clone())
            }),
        }
    }

    fn spann_flusher(&self) -> SpannFlusher {
        SpannFlusher {
            log: self.log.clone(),
            sqlite_db: self.sqlite_db.clone(),
            hnsw_segment_manager: self.hnsw_segment_manager.clone(),
            sysdb: self.sysdb.clone(),
            max_compaction_size: self.max_compaction_size,
            max_partition_size: self.max_partition_size,
            stop: self.spann_flush_stop.clone(),
            #[cfg(test)]
            fail_at: None,
        }
    }

    // The purges of the timer on a clone of the manager, out of the actor, as the flushes of SPANN
    // collections; each end comes back as a message, then the end of all of them.
    fn launch_purges(
        &self,
        collections: Vec<CollectionUuid>,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) {
        let mut purger = self.clone();
        let purged = ctx.receiver::<LogsPurged>();
        let done = ctx.receiver::<PurgesDone>();
        let purges = async move {
            for collection_id in collections {
                let result = AssertUnwindSafe(purger.purge_logs(collection_id))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(CompactionManagerError::PurgeLogsFailure));
                if let Err(err) = purged
                    .send(
                        LogsPurged {
                            collection_id,
                            result,
                        },
                        None,
                    )
                    .await
                {
                    tracing::error!(collection_id = %collection_id, error = %err, "The end of a purge did not reach the compaction manager");
                }
            }
            if let Err(err) = done.send(PurgesDone, None).await {
                tracing::error!(error = %err, "The end of the purges did not reach the compaction manager");
            }
        };
        // On the tracker of the flushes, so that a stop waits for a purge within its grace too.
        self.background
            .spawn(purges.instrument(tracing::info_span!(parent: None, "Purges of the log")));
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
        let max_concurrent_jobs = self.max_concurrent_jobs;
        let flushes = async move {
            futures::stream::iter(collections)
                .for_each_concurrent(max_concurrent_jobs, |collection_id| {
                    let flusher = flusher.clone();
                    let flushed = flushed.clone();
                    async move {
                        let result = AssertUnwindSafe(flusher.flush(collection_id))
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
                })
                .await;
            if let Some(round) = round {
                if let Err(err) = done.send(SpannRoundDone { round }, None).await {
                    tracing::error!(error = %err, "The end of a round of SPANN flushes did not reach the compaction manager");
                }
            }
        };
        self.background
            .spawn(flushes.instrument(tracing::info_span!(parent: None, "SPANN flushes")));
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

    // Every read asks for a purge, to retry one that failed: one that succeeded holds until a backfill.
    async fn purge_requested(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<(), CompactionManagerError> {
        if self.purged.contains(&collection_id) {
            return Ok(());
        }
        self.purge_logs(collection_id).await?;
        if self.purged.len() >= MAX_PURGED_COLLECTIONS {
            self.purged.clear();
        }
        self.purged.insert(collection_id);
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
        // As the garbage collector of distributed Chroma keeps the log that its attached functions have not run on
        // (fetch_min_attached_function_completion_offset.rs): no record after the lowest completion offset of the
        // functions of the collection goes, as GetMinCompletionOffsetForCollection of the coordinator reads it. A
        // function that keeps failing holds back the purge of its input collection, as in distributed Chroma.
        let min_completion_offset: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(completion_offset) FROM attached_functions WHERE input_collection_id = $1 AND is_deleted = FALSE",
        )
        .bind(collection_id.to_string())
        .fetch_one(self.sqlite_db.get_conn())
        .await
        .map_err(|_| CompactionManagerError::PurgeLogsFailure)?;
        let purge_seq_id = match min_completion_offset {
            Some(offset) => max_seq_id.min(offset.max(0) as u64 + 1),
            None => max_seq_id,
        };
        if let Log::Sqlite(log) = &mut self.log {
            if !log
                .has_purge_work(collection_id, purge_seq_id)
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
            .purge_logs(collection_id, purge_seq_id)
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
    max_partition_size: usize,
    stop: Option<SpannFlushStop>,
    // For tests: the stage of the flush that fails.
    #[cfg(test)]
    fail_at: Option<FlushStage>,
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
        let full = logs.len() >= self.max_compaction_size;
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
            Ok(Some(flush)) => {
                self.register(&collection_and_segments, SpannFlush { full, ..flush })
                    .await
            }
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
        let record_reader: Option<RecordSegmentReaderShard<'static>> =
            match RecordSegmentReaderShard::from_segment(
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
        let spann_writer = provider
            .write(collection, &vector_shard, dimensionality, None)
            .await
            .map_err(|err| write(err.boxed()))?;
        // As the compaction of distributed Chroma (rust/worker/src/execution/orchestration/log_fetch_orchestrator.rs and
        // apply_logs_orchestrator.rs): the partitions materialized, then each applied to each segment as a task of its own.
        // A failure ends the flush at once: dropping the tasks aborts them. One difference, that Federico chose: the
        // partitions are materialized one after the other, in the order of the log, so that the new records take their
        // offset ids in the order of the log, as a read gives them to the records of the tail; distributed Chroma
        // materializes them at once, and the ids go in the order its tasks run, which can change between two flushes of the
        // same log the order of records of equal score.
        let group_chunks = collection
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(CHROMA_GROUP_CHUNK_SIBLINGS_KEY))
            .is_some_and(|value| matches!(value, MetadataValue::Bool(true)));
        let next_offset_id = Arc::new(AtomicU32::new(
            record_reader
                .as_ref()
                .map_or(1, |reader| reader.get_max_offset_id() + 1),
        ));
        let partitions = partition_records(&logs, self.max_partition_size, group_chunks);
        let mut materializations = JoinSet::new();
        let reader = record_reader.clone();
        spawn_flush_task(&mut materializations, async move {
            let mut materialized = Vec::with_capacity(partitions.len());
            for partition in partitions {
                materialized.push(Arc::new(
                    materialize_logs(
                        &reader,
                        partition,
                        Some(next_offset_id.clone()),
                        &RecordSegmentReaderOptions::default(),
                    )
                    .await
                    .map_err(|err| CompactionManagerError::SpannWriteError(err.boxed()))?,
                ));
            }
            Ok(materialized)
        });
        let materialized = next_flush_task(&mut materializations)
            .await?
            .unwrap_or_default();
        #[cfg(test)]
        let fail_at = self.fail_at;
        let mut tasks = JoinSet::new();
        for materialized in materialized {
            let (reader, writer, logs) = (
                record_reader.clone(),
                record_writer.clone(),
                materialized.clone(),
            );
            spawn_flush_task(&mut tasks, async move {
                #[cfg(test)]
                FlushStage::RecordApply.fail_if(fail_at)?;
                writer
                    .apply_materialized_log_chunk(&reader, &logs)
                    .await
                    .map_err(|err| CompactionManagerError::SpannWriteError(err.boxed()))?;
                Ok(())
            });
            let (reader, writer) = (record_reader.clone(), spann_writer.clone());
            spawn_flush_task(&mut tasks, async move {
                #[cfg(test)]
                FlushStage::SpannApply.fail_if(fail_at)?;
                writer
                    .apply_materialized_log_chunk(&reader, &materialized)
                    .await
                    .map_err(|err| CompactionManagerError::SpannWriteError(err.boxed()))?;
                Ok(())
            });
        }
        while next_flush_task(&mut tasks).await?.is_some() {}
        // Each segment commits and flushes as a task of its own too (apply_logs_orchestrator.rs), with the garbage
        // collection of SPANN in its commit (rust/worker/src/execution/operators/commit_segment_writer.rs).
        let mut commits = JoinSet::new();
        spawn_flush_task(&mut commits, async move {
            #[cfg(test)]
            FlushStage::RecordCommit.fail_if(fail_at)?;
            let files = Box::pin(record_writer.commit())
                .await
                .map_err(|err| CompactionManagerError::SpannWriteError(err.boxed()))?
                .flush()
                .await
                .map_err(|err| CompactionManagerError::SpannWriteError(err.boxed()))?;
            Ok((true, files))
        });
        let mut spann_writer = spann_writer;
        spawn_flush_task(&mut commits, async move {
            #[cfg(test)]
            FlushStage::SpannCommit.fail_if(fail_at)?;
            let write = CompactionManagerError::SpannWriteError;
            spann_writer.garbage_collect().await.map_err(write)?;
            let files = Box::pin(spann_writer.commit())
                .await
                .map_err(write)?
                .flush()
                .await
                .map_err(write)?;
            Ok((false, files))
        });
        let (mut record_files, mut vector_files) = (HashMap::new(), HashMap::new());
        while let Some((record, files)) = next_flush_task(&mut commits).await? {
            if record {
                record_files = files;
            } else {
                vector_files = files;
            }
        }
        Ok(Some(SpannFlush {
            log_position,
            record_files,
            vector_files,
            full: false,
        }))
    }
}

// For tests: a stage of a flush, as its tasks run it, that fails.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq)]
enum FlushStage {
    RecordApply,
    SpannApply,
    RecordCommit,
    SpannCommit,
}

#[cfg(test)]
impl FlushStage {
    fn fail_if(self, fail_at: Option<FlushStage>) -> Result<(), CompactionManagerError> {
        if fail_at == Some(self) {
            return Err(CompactionManagerError::SpannFlushPanicked);
        }
        Ok(())
    }
}

// The threads of the flushes of SPANN collections, of all of them, apart from those that answer the reads: the 4 worker
// threads of the dispatcher of the compactor of distributed Chroma (num_worker_threads in rust/worker/chroma_config.yaml).
const FLUSH_THREADS: usize = 4;

static FLUSH_POOL: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(FLUSH_THREADS)
        .thread_name(FLUSH_THREAD_NAME)
        .thread_stack_size(chroma_system::thread_stack_size_bytes())
        .enable_all()
        .build()
        .expect("The threads of the flushes should start")
});

const FLUSH_THREAD_NAME: &str = "spann-flush";

// A worker of the dispatcher runs one task until it ends: a task that waits for a read of a block keeps its place here too.
static FLUSH_PLACES: Semaphore = Semaphore::const_new(FLUSH_THREADS);

fn spawn_flush_task<T: Send + 'static>(
    tasks: &mut JoinSet<Result<T, CompactionManagerError>>,
    task: impl std::future::Future<Output = Result<T, CompactionManagerError>> + Send + 'static,
) {
    tasks.spawn_on(
        async move {
            let _place = FLUSH_PLACES
                .acquire()
                .await
                .expect("The semaphore is never closed");
            task.await
        },
        FLUSH_POOL.handle(),
    );
}

async fn next_flush_task<T: 'static>(
    tasks: &mut JoinSet<Result<T, CompactionManagerError>>,
) -> Result<Option<T>, CompactionManagerError> {
    match tasks.join_next().await {
        None => Ok(None),
        Some(Ok(result)) => result.map(Some),
        Some(Err(_)) => Err(CompactionManagerError::SpannFlushPanicked),
    }
}

// What keeps records in one partition, as chunk_grouping_key of distributed Chroma
// (rust/worker/src/execution/operators/partition_log.rs): the id, or for a collection that groups the chunks of its
// documents the base of an id `{base}-{digits}`.
fn partition_key(id: &str, group_chunks: bool) -> &str {
    if !group_chunks {
        return id;
    }
    match id.rsplit_once('-') {
        Some((base, suffix))
            if !base.is_empty()
                && !suffix.is_empty()
                && suffix.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            base
        }
        _ => id,
    }
}

// The records in partitions of at least partition_size records, as PartitionOperator::partition of distributed Chroma, with
// one difference that Federico chose: the keys taken in the order of their first record in the log, not in the order of a
// HashMap, so that the same log always gives the same partitions and a failure can be repeated.
fn partition_records(
    records: &Chunk<LogRecord>,
    partition_size: usize,
    group_chunks: bool,
) -> Vec<Chunk<LogRecord>> {
    let mut keys: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut order = Vec::new();
    for (record, index) in records.iter() {
        let key = partition_key(&record.record.id, group_chunks);
        keys.entry(key)
            .or_insert_with(|| {
                order.push(key);
                Vec::new()
            })
            .push(index);
    }
    let mut partitions = Vec::new();
    let mut visibility = vec![false; records.total_len()];
    let mut size = 0;
    for key in order {
        for index in &keys[key] {
            visibility[*index] = true;
        }
        size += keys[key].len();
        if size >= partition_size {
            let mut partition = records.clone();
            partition.set_visibility(std::mem::replace(
                &mut visibility,
                vec![false; records.total_len()],
            ));
            partitions.push(partition);
            size = 0;
        }
    }
    if size > 0 {
        let mut partition = records.clone();
        partition.set_visibility(visibility);
        partitions.push(partition);
    }
    partitions
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

    #[test]
    fn purges_wait_for_their_interval_and_run_once() {
        let start = Instant::now();
        let mut purges = PurgeSchedule {
            pending: HashSet::new(),
            interval: Duration::from_secs(7_200),
            last: start,
        };
        let collection_id = CollectionUuid::new();
        purges.request(collection_id);
        purges.request(collection_id);
        assert!(purges.due(start + Duration::from_secs(7_199)).is_empty());
        assert_eq!(
            purges.due(start + Duration::from_secs(7_200)),
            vec![collection_id]
        );
        // The interval counts from the last purges, and a request after them waits for the next ones.
        purges.request(collection_id);
        assert!(purges.due(start + Duration::from_secs(14_399)).is_empty());
        assert_eq!(
            purges.due(start + Duration::from_secs(14_400)),
            vec![collection_id]
        );
        // The interval of distributed Chroma (gc_interval_mins in rust/garbage_collector/garbage_collector_config.yaml).
        assert_eq!(
            LocalCompactionManagerConfig::default().purge_interval_sec,
            7_200
        );
    }

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
