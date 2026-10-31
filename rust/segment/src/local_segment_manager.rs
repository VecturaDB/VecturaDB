// Changed from the original Chroma file; see the git history for the changes and their authors.
use chroma_blockstore::{
    arrow::provider::{BlockManager, RootManagerError},
    config::BlockfileProviderConfig,
    provider::BlockfileProvider,
    RootManager,
};
use chroma_cache::{
    nop::NopCache, AysncPartitionedMutex, Cache, CacheConfig, CacheError, FoyerCacheConfig,
};
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    config::{HnswProviderConfig, SpannProviderConfig},
    hnsw_provider::{HnswIndexProvider, FILES},
    IndexUuid,
};
use chroma_sqlite::db::SqliteDb;
use chroma_storage::{local::LocalStorage, Storage, StorageError};
use chroma_types::{
    Collection, CollectionUuid, Segment, SegmentUuid, HNSW_PATH, USER_ID_BLOOM_FILTER,
};
use futures::StreamExt;
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io,
    path::Path,
    sync::{Arc, Weak},
};
use thiserror::Error;

use crate::local_hnsw::{
    validate_checkpoint, Inner, LocalHnswIndex, LocalHnswSegmentReader,
    LocalHnswSegmentReaderError, LocalHnswSegmentWriter, LocalHnswSegmentWriterError,
};
use crate::spann_provider::SpannProvider;

fn default_hnsw_index_pool_cache_config() -> CacheConfig {
    CacheConfig::Memory(FoyerCacheConfig {
        capacity: 65536,
        ..Default::default()
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalSegmentManagerConfig {
    // Controls resident indexes, not file handles. Local HNSW opens files only
    // during bounded disk operations, independent of cache capacity.
    #[serde(default = "default_hnsw_index_pool_cache_config")]
    pub hnsw_index_pool_cache_config: CacheConfig,
    pub persist_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LocalSegmentManager {
    hnsw_index_pool: Arc<dyn Cache<IndexUuid, LocalHnswIndex>>,
    // Cache eviction must not allow a second native writer while a caller
    // still holds the first index. Serialize misses and retain weak identities.
    live_indexes:
        AysncPartitionedMutex<IndexUuid, HashMap<IndexUuid, Weak<tokio::sync::RwLock<Inner>>>>,
    #[allow(dead_code)]
    eviction_callback_task_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
    cleanup_task: Option<Arc<CleanupTask>>,
    sqlite: SqliteDb,
    persist_root: Option<String>,
    spann_provider: Option<SpannProvider>,
    // The providers of the local compactor: see compaction_spann_provider.
    compaction_spann_provider: Option<SpannProvider>,
    // The SPANN collections whose log is applied: see spann_applied.
    spann_applied: Arc<std::sync::Mutex<HashSet<CollectionUuid>>>,
    // What runs after each flush of a SPANN collection: see on_spann_flushed.
    spann_flushed: SpannFlushListener,
}

/// What runs after each flush of a SPANN collection, once SQLite records its files, with the collection and its record
/// segment at those files.
type SpannFlushed = Arc<dyn Fn(CollectionUuid, Segment) + Send + Sync>;

#[derive(Clone, Default)]
struct SpannFlushListener(Arc<std::sync::Mutex<Option<SpannFlushed>>>);

impl std::fmt::Debug for SpannFlushListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpannFlushListener")
    }
}

#[derive(Error, Debug)]
#[error("Error reading the defaults of the SPANN providers: {0}")]
struct SpannDefaultsError(serde_json::Error);

impl ChromaError for SpannDefaultsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

// The settings of the SPANN index in the configuration of the worker of distributed Chroma
// (rust/worker/chroma_config.yaml): reads as its query service, without the adaptive number of probes, so a query probes
// the search_nprobe of the collection; writes as its compaction service, with the garbage collection of the posting
// lists and of the HNSW index of the centers.
fn local_spann_config() -> Result<SpannProviderConfig, serde_json::Error> {
    serde_json::from_value(serde_json::json!({
        "adaptive_search_nprobe": false,
        "pl_block_size": 5242880,
        "pl_garbage_collection": {"enabled": true, "policy": {"random_sample": {"sample_size": 0.1}}},
        "hnsw_garbage_collection": {"enabled": true, "policy": "full_rebuild"},
    }))
}

// The blockfiles of the SPANN index, with the settings of the compaction service of the worker
// (rust/worker/chroma_config.yaml): blocks of 8 MiB, and a cache of 1000 of them and of 1000 roots in memory.
fn local_blockfile_config() -> Result<BlockfileProviderConfig, serde_json::Error> {
    serde_json::from_value(serde_json::json!({
        "arrow": {
            "block_manager_config": {
                "max_block_size_bytes": 8388608,
                "block_cache_config": {"lru": {"name": "block_cache", "capacity": 1000}},
                "num_concurrent_block_flushes": 40,
            },
            "sparse_index_manager_config": {
                "sparse_index_cache_config": {"lru": {"name": "sparse_index_cache", "capacity": 1000}},
            },
        },
    }))
}

// The providers of the SPANN index, as the worker of distributed Chroma builds them, on local storage at the data path:
// the blockfiles, and the HNSW index of the centers.
async fn local_spann_provider(
    persist_path: &str,
    registry: &Registry,
) -> Result<SpannProvider, Box<dyn ChromaError>> {
    let storage = Storage::Local(LocalStorage::new(persist_path));
    let defaults = |e| -> Box<dyn ChromaError> { Box::new(SpannDefaultsError(e)) };
    let blockfile_provider = BlockfileProvider::try_from_config(
        &(local_blockfile_config().map_err(defaults)?, storage.clone()),
        registry,
    )
    .await?;
    let mut hnsw_config: HnswProviderConfig = serde_json::from_str("{}").map_err(defaults)?;
    hnsw_config.hnsw_cache_config = CacheConfig::Memory(FoyerCacheConfig {
        capacity: 64,
        ..Default::default()
    });
    let hnsw_provider =
        HnswIndexProvider::try_from_config(&(hnsw_config, storage.clone()), registry).await?;
    let spann_config = local_spann_config().map_err(defaults)?;
    #[cfg(not(feature = "usearch"))]
    let config = (hnsw_provider, blockfile_provider, spann_config);
    #[cfg(feature = "usearch")]
    let config = {
        let usearch_cache =
            chroma_cache::from_config(&spann_config.usearch_provider.cache_config).await?;
        let usearch_provider =
            chroma_index::usearch::USearchIndexProvider::new(storage, usearch_cache);
        (
            hnsw_provider,
            blockfile_provider,
            spann_config,
            usearch_provider,
        )
    };
    SpannProvider::try_from_config(&config, registry).await
}

// The folder of a segment of a SPANN collection, under the prefix of distributed Chroma
// (tenant/{tenant}/database/{database}/collection/{collection}/segment/{segment}), removed with the folders of its
// collection that it leaves empty. The catalog no longer names the tenant or the database of a deleted segment: its
// folder is looked for in the folder of its collection under each of them, never in the folders of other collections.
async fn remove_blockfile_segment(
    root: &Path,
    collection_id: CollectionUuid,
    segment_id: SegmentUuid,
) -> io::Result<()> {
    let folders = |path: std::path::PathBuf| async move { entries(&path, true).await };
    let segment = segment_id.to_string();
    for tenant in folders(root.join("tenant")).await? {
        for database in folders(tenant.join("database")).await? {
            let collection = database.join("collection").join(collection_id.to_string());
            let segments = collection.join("segment");
            match tokio::fs::remove_dir_all(segments.join(&segment)).await {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
            // A folder that still holds something stays.
            for empty in [segments, collection] {
                if tokio::fs::remove_dir(&empty).await.is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}

// The folders, or else the files, in a folder; none in a folder that is not there.
async fn entries(path: &Path, folders: bool) -> io::Result<Vec<std::path::PathBuf>> {
    let mut entries = match tokio::fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let mut found = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() == folders {
            found.push(entry.path());
        }
    }
    Ok(found)
}

/// Test hooks of the cleanup of the SPANN files under a prefix, where it touches the files: the reads of roots
/// (ROOT_READS), the removals of files (FILE_REMOVALS) and the listings of the folders of the HNSW indexes
/// (FOLDER_LISTINGS) are counted, and with a meeting the first two of them wait up to 5 s for each other, which they
/// meet only when they run together.
#[cfg(feature = "test-hooks")]
pub static ROOT_READS: std::sync::Mutex<Vec<FileHook>> = std::sync::Mutex::new(Vec::new());

#[cfg(feature = "test-hooks")]
pub static FILE_REMOVALS: std::sync::Mutex<Vec<FileHook>> = std::sync::Mutex::new(Vec::new());

#[cfg(feature = "test-hooks")]
pub static FOLDER_LISTINGS: std::sync::Mutex<Vec<FileHook>> = std::sync::Mutex::new(Vec::new());

#[cfg(feature = "test-hooks")]
#[derive(Clone, Debug)]
pub struct FileHook {
    pub prefix: String,
    pub count: Arc<std::sync::atomic::AtomicUsize>,
    pub meeting: Option<Arc<Meeting>>,
}

/// The first two operations of a hook: they met when the second came while the first still waited for it.
#[cfg(feature = "test-hooks")]
#[derive(Debug, Default)]
pub struct Meeting {
    // How many came, and whether the first gave up waiting.
    state: std::sync::Mutex<(usize, bool)>,
    second: std::sync::Condvar,
    pub met: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "test-hooks")]
fn file_hook(hooks: &std::sync::Mutex<Vec<FileHook>>, prefix: &str) {
    let hook = hooks
        .lock()
        .unwrap()
        .iter()
        .find(|hook| hook.prefix == prefix)
        .cloned();
    let Some(hook) = hook else {
        return;
    };
    hook.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let Some(meeting) = hook.meeting else {
        return;
    };
    let mut state = meeting.state.lock().unwrap();
    state.0 += 1;
    if state.0 == 1 {
        let (mut state, wait) = meeting
            .second
            .wait_timeout_while(state, std::time::Duration::from_secs(5), |state| {
                state.0 < 2
            })
            .unwrap();
        if wait.timed_out() {
            state.1 = true;
        }
    } else if state.0 == 2 && !state.1 {
        meeting.met.store(true, std::sync::atomic::Ordering::SeqCst);
        meeting.second.notify_all();
    }
}

#[derive(Debug)]
struct CleanupTask(tokio::task::JoinHandle<()>);

impl Drop for CleanupTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Injectable for LocalSegmentManager {}

#[async_trait::async_trait]
impl Configurable<LocalSegmentManagerConfig> for LocalSegmentManager {
    async fn try_from_config(
        config: &LocalSegmentManagerConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn chroma_error::ChromaError>> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_index_pool: Box<dyn Cache<IndexUuid, LocalHnswIndex>> =
            chroma_cache::from_config_with_event_listener(&config.hnsw_index_pool_cache_config, tx)
                .await?;
        let sqldb = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        // TODO(Sanket): Might need tokio runtime to be passed here to spawn the task.
        let handle = tokio::spawn(async move {
            while let Some((_, index)) = rx.recv().await {
                // Close the FD here.
                index.close().await;
            }
        });
        let spann_provider = match &config.persist_path {
            Some(persist_path) => Some(local_spann_provider(persist_path, registry).await?),
            None => None,
        };
        let compaction_spann_provider = match &config.persist_path {
            Some(persist_path) => Some(local_spann_provider(persist_path, registry).await?),
            None => None,
        };
        let mut res = Self {
            hnsw_index_pool: hnsw_index_pool.into(),
            live_indexes: AysncPartitionedMutex::with_parallelism(16, HashMap::new()),
            eviction_callback_task_handle: Some(Arc::new(handle)),
            cleanup_task: None,
            sqlite: sqldb,
            persist_root: config.persist_path.clone(),
            spann_provider,
            compaction_spann_provider,
            spann_applied: Arc::default(),
            spann_flushed: SpannFlushListener::default(),
        };
        if res.persist_root.is_some() {
            let cleaner = res.clone();
            res.cleanup_task = Some(Arc::new(CleanupTask(tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
                loop {
                    interval.tick().await;
                    if let Err(err) = cleaner.cleanup_deleted_indexes().await {
                        tracing::warn!(error = %err, "failed to clean up deleted HNSW indexes");
                    }
                }
            }))));
        }
        registry.register(res.clone());
        Ok(res)
    }
}

#[derive(Error, Debug)]
pub enum LocalSegmentManagerError {
    #[error("Segment has been deleted")]
    Deleted,
    #[error("Error checking segment existence: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[error("Error removing HNSW files: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error creating hnsw segment reader: {0}")]
    LocalHnswSegmentReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error reading hnsw pool cache: {0}")]
    PoolCacheError(#[from] CacheError),
    #[error("Error creating hnsw segment writer: {0}")]
    LocalHnswSegmentWriterError(#[from] LocalHnswSegmentWriterError),
    #[error("Error reading the root of a blockfile: {0}")]
    Root(#[from] RootManagerError),
}

impl ChromaError for LocalSegmentManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Deleted => ErrorCodes::NotFound,
            Self::Sqlite(_) | Self::Io(_) => ErrorCodes::Internal,
            LocalSegmentManagerError::LocalHnswSegmentReaderError(e) => e.code(),
            LocalSegmentManagerError::PoolCacheError(e) => e.code(),
            LocalSegmentManagerError::LocalHnswSegmentWriterError(e) => e.code(),
            LocalSegmentManagerError::Root(e) => e.code(),
        }
    }
}

impl LocalSegmentManager {
    /// Validate the on-disk checkpoint before discarding its replay records.
    ///
    /// Reuses validation across successful saves while file identities and
    /// timestamps match; otherwise inspects native files and the ID map.
    /// Missing files, structural corruption, and checkpoints requiring replay
    /// all prevent purging. New-format checkpoints must cover `watermark`, even
    /// when their file validation is cached.
    /// Callers must serialize this check and the subsequent purge with checkpoint
    /// writes; the local compaction manager does so through its message queue.
    pub async fn validate_persisted_checkpoint(
        &self,
        segment: &SegmentUuid,
        watermark: u64,
    ) -> io::Result<()> {
        let root = self.persist_root.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no persistent HNSW directory")
        })?;
        let path = Path::new(root).join(segment.to_string());
        let id = IndexUuid(segment.0);
        let inner = self
            .live_indexes
            .lock(&id)
            .await
            .get(&id)
            .and_then(Weak::upgrade);
        if let Some(inner) = inner {
            LocalHnswIndex { inner }
                .validate_persisted_checkpoint(&path, watermark)
                .await?;
            return Ok(());
        }
        tokio::task::spawn_blocking(move || validate_checkpoint(&path, watermark).map(|_| ()))
            .await
            .map_err(io::Error::other)?
    }

    pub async fn get_hnsw_reader(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentReader, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        let mut live = self.live_indexes.lock(&index_uuid).await;
        if !self.segment_exists(segment.id).await? {
            return Err(LocalSegmentManagerError::Deleted);
        }
        // A concurrent miss may have filled the cache while this caller waited
        // for the lock. Re-inserting that index would fire the cache's replace
        // event, whose listener closes the files of the index being returned.
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        if let Some(inner) = live.get(&index_uuid).and_then(Weak::upgrade) {
            // Reuse the live in-memory index. Checkpointing opens its files
            // under the write lock, so queued eviction callbacks are harmless.
            let index = LocalHnswIndex { inner };
            index.ensure_usable().await?;
            self.hnsw_index_pool.insert(index_uuid, index.clone()).await;
            return Ok(LocalHnswSegmentReader::from_index(index));
        }
        live.retain(|_, index| index.strong_count() != 0);
        let reader = LocalHnswSegmentReader::from_segment(
            collection,
            segment,
            dimensionality,
            self.persist_root.clone(),
            self.sqlite.clone(),
        )
        .await?;
        live.insert(index_uuid, Arc::downgrade(&reader.index.inner));
        self.hnsw_index_pool
            .insert(index_uuid, reader.index.clone())
            .await;
        Ok(reader)
    }

    /// The providers of SPANN segments, on the data path; none without one.
    pub fn spann_provider(&self) -> Option<&SpannProvider> {
        self.spann_provider.as_ref()
    }

    /// The providers with which the local compactor writes SPANN segments: on the same data path, with caches of their
    /// own, as the compaction service of distributed Chroma runs apart from its query service. A flush forks the HNSW
    /// index of the centers and holds its write lock while it changes it; in the cache of the readers the fork would
    /// sit under the key of the collection, where a reader takes its read lock before it sees that the index is
    /// another one (HnswIndexProvider::get), and waits for the flush.
    pub fn compaction_spann_provider(&self) -> Option<&SpannProvider> {
        self.compaction_spann_provider.as_ref()
    }

    /// Whether the log of a SPANN collection is applied to its metadata, as replay_complete for a local HNSW index: the
    /// compaction clears it when it starts on the log of the collection and sets it once the records are in SQLite. The
    /// files take them at a later flush. A read of a collection whose log is applied has nothing to replay. None is
    /// applied after a start.
    pub fn spann_applied(&self, collection_id: CollectionUuid) -> bool {
        self.spann_applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&collection_id)
    }

    /// Sets whether the log of a SPANN collection is applied; a deleted collection is forgotten so.
    pub fn set_spann_applied(&self, collection_id: CollectionUuid, applied: bool) {
        let mut collections = self
            .spann_applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if applied {
            collections.insert(collection_id);
        } else {
            collections.remove(&collection_id);
        }
    }

    /// Sets what runs after each flush of a SPANN collection, once SQLite records its files, in place of what ran
    /// before: the local executor opens there the record segment of the new version, so that the next read finds it
    /// open.
    pub fn on_spann_flushed(
        &self,
        listener: impl Fn(CollectionUuid, Segment) + Send + Sync + 'static,
    ) {
        *self
            .spann_flushed
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(listener));
    }

    /// Runs what on_spann_flushed set, if anything, after a flush of a SPANN collection that SQLite recorded with its
    /// files, given the record segment at those files.
    pub fn spann_flushed(&self, collection_id: CollectionUuid, record_segment: Segment) {
        let listener = self
            .spann_flushed
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(listener) = listener {
            listener(collection_id, record_segment);
        }
    }

    pub async fn get_hnsw_writer(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<LocalHnswSegmentWriter, LocalSegmentManagerError> {
        let index_uuid = IndexUuid(segment.id.0);
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        let mut live = self.live_indexes.lock(&index_uuid).await;
        if !self.segment_exists(segment.id).await? {
            return Err(LocalSegmentManagerError::Deleted);
        }
        // A concurrent miss may have filled the cache while this caller waited
        // for the lock. Re-inserting that index would fire the cache's replace
        // event, whose listener closes the files of the index being returned.
        if let Some(index) = self.hnsw_index_pool.get(&index_uuid).await? {
            index.ensure_usable().await?;
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        if let Some(inner) = live.get(&index_uuid).and_then(Weak::upgrade) {
            // Reuse the live in-memory index. Checkpointing opens its files
            // under the write lock, so queued eviction callbacks are harmless.
            let index = LocalHnswIndex { inner };
            index.ensure_usable().await?;
            self.hnsw_index_pool.insert(index_uuid, index.clone()).await;
            return Ok(LocalHnswSegmentWriter::from_index(index)?);
        }
        live.retain(|_, index| index.strong_count() != 0);
        let writer = LocalHnswSegmentWriter::from_segment(
            collection,
            segment,
            dimensionality,
            self.persist_root.clone(),
            self.sqlite.clone(),
        )
        .await?;
        live.insert(index_uuid, Arc::downgrade(&writer.index.inner));
        self.hnsw_index_pool
            .insert(index_uuid, writer.index.clone())
            .await;
        Ok(writer)
    }

    async fn segment_exists(&self, segment_id: SegmentUuid) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM segments WHERE id = ?)")
            .bind(segment_id.to_string())
            .fetch_one(self.sqlite.get_conn())
            .await
    }

    /// Remove resources only after the sysdb deletion has committed.
    pub async fn delete_hnsw_index(
        &self,
        segment_id: SegmentUuid,
    ) -> Result<(), LocalSegmentManagerError> {
        if self.delete_hnsw_index_files(segment_id).await? {
            self.forget_deleted_hnsw_indexes(&[segment_id]).await?;
        }
        Ok(())
    }

    /// The files of a deleted index, removed: true when they were queued in index_cleanup, whose row stays until
    /// forget_deleted_hnsw_indexes. The blockfiles of a segment of a SPANN collection go with
    /// delete_blockfile_segment_files.
    pub async fn delete_hnsw_index_files(
        &self,
        segment_id: SegmentUuid,
    ) -> Result<bool, LocalSegmentManagerError> {
        let id = IndexUuid(segment_id.0);
        let live = self.live_indexes.lock(&id).await;
        if self.segment_exists(segment_id).await? {
            return Ok(false);
        }
        let authorized: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM index_cleanup WHERE segment_id = ?)")
                .bind(segment_id.to_string())
                .fetch_one(self.sqlite.get_conn())
                .await?;
        if !authorized {
            return Ok(false);
        }
        let inner = live.get(&id).and_then(Weak::upgrade);
        // The committed catalog deletion prevents a new load of this segment.
        // Keep its weak identity for cancellation/retry, but release the shared
        // partition before waiting for an active writer to finish persisting.
        drop(live);
        if let Some(inner) = inner {
            LocalHnswIndex { inner }.mark_deleted().await;
        }
        self.live_indexes.lock(&id).await.remove(&id);
        self.hnsw_index_pool.remove(&id).await;
        if let Some(root) = &self.persist_root {
            match tokio::fs::remove_dir_all(Path::new(root).join(segment_id.to_string())).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(true)
    }

    /// The blockfiles of a deleted segment of a SPANN collection, removed with the folders of its collection that it
    /// leaves empty; nothing for a segment that the catalog still has. Those that a stop leaves behind go with the sweep
    /// of the folders of segments that the catalog no longer has.
    pub async fn delete_blockfile_segment_files(
        &self,
        collection_id: CollectionUuid,
        segment_id: SegmentUuid,
    ) -> Result<(), LocalSegmentManagerError> {
        let Some(root) = &self.persist_root else {
            return Ok(());
        };
        if self.segment_exists(segment_id).await? {
            return Ok(());
        }
        remove_blockfile_segment(Path::new(root), collection_id, segment_id).await?;
        Ok(())
    }

    /// Takes deleted indexes whose files are gone out of index_cleanup, with one statement per 500.
    pub async fn forget_deleted_hnsw_indexes(
        &self,
        segment_ids: &[SegmentUuid],
    ) -> Result<(), LocalSegmentManagerError> {
        for batch in segment_ids.chunks(500) {
            let (sql, values) = sea_query::Query::delete()
                .from_table(sea_query::Alias::new("index_cleanup"))
                .and_where(
                    sea_query::Expr::col(sea_query::Alias::new("segment_id"))
                        .is_in(batch.iter().map(|segment_id| segment_id.to_string())),
                )
                .build_sqlx(sea_query::SqliteQueryBuilder);
            sqlx::query_with(&sql, values)
                .execute(self.sqlite.get_conn())
                .await?;
        }
        Ok(())
    }

    /// The files under the prefix of a segment of a SPANN collection that none of the versions given references,
    /// removed: blocks, roots and the files of the HNSW indexes of the centers, listed as the garbage collector of
    /// distributed Chroma lists the files of a version (rust/garbage_collector/src/operators/list_files_at_version.rs).
    /// Other files stay. Returns how many went.
    pub async fn remove_unused_spann_files(
        &self,
        prefix: &str,
        versions: &[&HashMap<String, Vec<String>>],
    ) -> Result<usize, LocalSegmentManagerError> {
        let (Some(root), Some(storage)) = (
            &self.persist_root,
            self.spann_provider
                .as_ref()
                .and_then(|provider| provider.blockfile_provider.storage()),
        ) else {
            return Ok(0);
        };
        // No cache of roots, as the garbage collector of distributed Chroma by default (root_cache_config): two
        // cleanups of a segment see versions about a grace apart, so a cache of its own would hold almost no root that
        // a later one reads, and the cache of the reads would lose to them the roots of the collections that are read.
        let roots = RootManager::new((*storage).clone(), Box::new(NopCache));
        let mut live = HashSet::new();
        // The roots of the blockfiles of the versions, each once, read a hundred at a time, as distributed Chroma lists
        // the files of a version (list_files_at_version.rs). The local storage reads a file in one go, with nothing to
        // wait for: each read goes to the blocking threads of tokio, as tokio::fs does, so that the reads run together,
        // as those of the storage of distributed Chroma do.
        let mut sparse_indexes = HashMap::new();
        for files in versions {
            for (file_type, paths) in files.iter() {
                for path in paths {
                    let (path_prefix, id) = Segment::extract_prefix_and_id(path)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    if file_type == HNSW_PATH || file_type == "hnsw_index" {
                        live.extend(FILES.iter().map(|file| {
                            HnswIndexProvider::format_key(path_prefix, &IndexUuid(id), file)
                        }));
                    } else if file_type == USER_ID_BLOOM_FILTER {
                        live.insert(path.clone());
                    } else {
                        live.insert(RootManager::get_storage_key(path_prefix, &id));
                        sparse_indexes.insert(id, path_prefix.to_string());
                    }
                }
            }
        }
        let handle = tokio::runtime::Handle::current();
        let mut block_ids = futures::stream::iter(sparse_indexes)
            .map(|(id, path_prefix)| {
                let (roots, handle) = (roots.clone(), handle.clone());
                #[cfg(feature = "test-hooks")]
                let hook_prefix = prefix.to_string();
                async move {
                    let (blocks, path_prefix) = tokio::task::spawn_blocking(move || {
                        #[cfg(feature = "test-hooks")]
                        file_hook(&ROOT_READS, &hook_prefix);
                        let blocks = handle.block_on(roots.get_all_block_ids(&id, &path_prefix));
                        (blocks, path_prefix)
                    })
                    .await
                    .map_err(io::Error::other)?;
                    match blocks {
                        Ok(blocks) => Ok((blocks, path_prefix)),
                        // As distributed Chroma: a root that is not there went with its version.
                        Err(RootManagerError::StorageGetError(StorageError::NotFound { .. })) => {
                            tracing::debug!(
                                "Sparse index {id} not found in storage. Assuming it was previously deleted."
                            );
                            Ok((Vec::new(), path_prefix))
                        }
                        Err(err) => Err(LocalSegmentManagerError::from(err)),
                    }
                }
            })
            .buffer_unordered(100);
        while let Some(result) = block_ids.next().await {
            let (blocks, path_prefix) = result?;
            live.extend(
                blocks
                    .iter()
                    .map(|block| BlockManager::format_key(&path_prefix, block)),
            );
        }
        let root = Path::new(root);
        let mut files = Vec::new();
        for kind in ["block", "root"] {
            for file in entries(&root.join(format!("{prefix}/{kind}")), false).await? {
                if let Some(name) = file.file_name().and_then(|name| name.to_str()) {
                    files.push(format!("{prefix}/{kind}/{name}"));
                }
            }
        }
        let indexes = entries(&root.join(format!("{prefix}/hnsw")), true).await?;
        // The folders of the HNSW indexes of the centers, one for each index that a flush wrote, listed a hundred at a
        // time as well, each listing on the blocking threads of tokio. The stream owns its paths: one that borrowed
        // them made the future of the compaction, which must be Send, too general for the compiler to prove it.
        let mut listings = futures::stream::iter(indexes.clone())
            .map(|index| {
                let folder = index.clone();
                #[cfg(feature = "test-hooks")]
                let hook_prefix = prefix.to_string();
                async move {
                    let listed = tokio::task::spawn_blocking(move || {
                        #[cfg(feature = "test-hooks")]
                        file_hook(&FOLDER_LISTINGS, &hook_prefix);
                        // As entries(index, false), in one go on a blocking thread: the files of the folder, none if
                        // it is not there.
                        let mut found = Vec::new();
                        let folder = match std::fs::read_dir(&folder) {
                            Ok(folder) => folder,
                            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(found),
                            Err(err) => return Err(err),
                        };
                        for entry in folder {
                            let entry = entry?;
                            if !entry.file_type()?.is_dir() {
                                found.push(entry.path());
                            }
                        }
                        Ok(found)
                    })
                    .await
                    .map_err(io::Error::other)?;
                    Ok::<_, io::Error>((index, listed?))
                }
            })
            .buffer_unordered(100);
        while let Some(listed) = listings.next().await {
            let (index, listed) = listed?;
            let Some(index_name) = index.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            for file in listed {
                if let Some(name) = file.file_name().and_then(|name| name.to_str()) {
                    files.push(format!("{prefix}/hnsw/{index_name}/{name}"));
                }
            }
        }
        drop(listings);
        // The files that no version names, a hundred at a time, as distributed Chroma renames them a hundred at a time
        // or deletes them a thousand in a request (delete_unused_files.rs): each removal goes to the blocking threads
        // of tokio, as tokio::fs::remove_file does, so that the removals run together.
        let unused = files.into_iter().filter(|key| !live.contains(key));
        let mut removals = futures::stream::iter(unused)
            .map(|key| {
                let path = root.join(key);
                #[cfg(feature = "test-hooks")]
                let hook_prefix = prefix.to_string();
                async move {
                    let removal = tokio::task::spawn_blocking(move || {
                        #[cfg(feature = "test-hooks")]
                        file_hook(&FILE_REMOVALS, &hook_prefix);
                        std::fs::remove_file(path)
                    })
                    .await
                    .map_err(io::Error::other)?;
                    match removal {
                        Ok(()) => Ok(true),
                        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
                        Err(err) => Err(err),
                    }
                }
            })
            .buffer_unordered(100);
        let mut removed = 0;
        while let Some(result) = removals.next().await {
            removed += result? as usize;
        }
        // A folder that still holds files stays. The folders go a hundred at a time too.
        futures::stream::iter(indexes)
            .map(|index| async move {
                let _ = tokio::fs::remove_dir(&index).await;
            })
            .buffer_unordered(100)
            .collect::<Vec<_>>()
            .await;
        Ok(removed)
    }

    /// Whether the indexes are files, which the sweep at startup and every hour removes once deleted.
    pub fn persists(&self) -> bool {
        self.persist_root.is_some()
    }

    /// Retry queued index deletions on startup and periodically.
    pub async fn cleanup_deleted_indexes(&self) -> Result<(), LocalSegmentManagerError> {
        let pending: Vec<String> =
            sqlx::query_scalar("SELECT segment_id FROM index_cleanup ORDER BY segment_id")
                .fetch_all(self.sqlite.get_conn())
                .await?;
        let mut first_error = None;
        let mut removed = Vec::new();
        for id in pending {
            let result = match uuid::Uuid::parse_str(&id) {
                Ok(uuid) => self
                    .delete_hnsw_index_files(SegmentUuid(uuid))
                    .await
                    .map(|gone| gone.then_some(SegmentUuid(uuid))),
                Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err).into()),
            };
            match result {
                Ok(Some(segment_id)) => removed.push(segment_id),
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(segment_id = %id, error = %err, "index cleanup failed; will retry");
                    first_error.get_or_insert(err);
                }
            }
        }
        // The rows that collection deletions leave wait for this sweep: one statement per 500 of them.
        let forgotten = self.forget_deleted_hnsw_indexes(&removed).await;
        if let Err(err) = self.remove_orphan_blockfile_segments().await {
            tracing::warn!(error = %err, "cleanup of SPANN files failed; will retry");
            first_error.get_or_insert(err);
        }
        match first_error {
            Some(err) => Err(err),
            None => forgotten,
        }
    }

    // The folders of the segments of SPANN collections that the catalog no longer has, removed: a server that knows
    // nothing of them, Chroma 1.5.9 or a VecturaDB release before SPANN, deletes a SPANN collection and leaves its files.
    // A segment has its row before its first file, so a folder without a row is what a deletion left. The empty folders
    // of a collection go only when the catalog no longer has the collection, which no flush writes to.
    async fn remove_orphan_blockfile_segments(&self) -> Result<(), LocalSegmentManagerError> {
        let Some(root) = &self.persist_root else {
            return Ok(());
        };
        let root = Path::new(root);
        for tenant in entries(&root.join("tenant"), true).await? {
            for database in entries(&tenant.join("database"), true).await? {
                for collection in entries(&database.join("collection"), true).await? {
                    let segments = collection.join("segment");
                    for segment in entries(&segments, true).await? {
                        let Some(id) = segment
                            .file_name()
                            .and_then(|name| name.to_str())
                            .and_then(|name| uuid::Uuid::parse_str(name).ok())
                        else {
                            continue;
                        };
                        if self.segment_exists(SegmentUuid(id)).await? {
                            continue;
                        }
                        match tokio::fs::remove_dir_all(&segment).await {
                            Ok(()) => {}
                            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                            Err(err) => return Err(err.into()),
                        }
                    }
                    let Some(collection_id) = collection
                        .file_name()
                        .and_then(|name| name.to_str())
                        .and_then(|name| uuid::Uuid::parse_str(name).ok())
                    else {
                        continue;
                    };
                    let collection_exists: bool =
                        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collections WHERE id = ?)")
                            .bind(collection_id.to_string())
                            .fetch_one(self.sqlite.get_conn())
                            .await?;
                    if !collection_exists {
                        // A folder that still holds something stays.
                        for empty in [segments, collection] {
                            if tokio::fs::remove_dir(&empty).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn reset(&self) -> Result<(), LocalSegmentManagerError> {
        self.hnsw_index_pool.clear().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_hnsw::inspect_persisted_hnsw_index;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        Chunk, KnnIndex, LogRecord, Operation, OperationRecord, Schema, SegmentScope, SegmentType,
        SegmentUuid,
    };

    // The SPANN provider of a single node reads as the query service of the worker of distributed Chroma and writes as
    // its compaction service, with the settings of rust/worker/chroma_config.yaml.
    #[test]
    fn the_spann_provider_has_the_settings_of_the_worker() {
        use figment::providers::Format;
        #[derive(Deserialize)]
        struct Service {
            spann_provider: serde_json::Value,
            blockfile_provider: serde_json::Value,
        }
        #[derive(Deserialize)]
        struct Worker {
            query_service: Service,
            compaction_service: Service,
        }
        let worker: Worker = figment::Figment::from(figment::providers::Yaml::string(
            include_str!("../../worker/chroma_config.yaml"),
        ))
        .extract()
        .unwrap();
        let normalized = |section: serde_json::Value| {
            serde_json::to_value(serde_json::from_value::<SpannProviderConfig>(section).unwrap())
                .unwrap()
        };
        let query = normalized(worker.query_service.spann_provider);
        let compaction = normalized(worker.compaction_service.spann_provider);
        let local = serde_json::to_value(local_spann_config().unwrap()).unwrap();
        assert_eq!(
            local["adaptive_search_nprobe"],
            query["adaptive_search_nprobe"]
        );
        for setting in [
            "pl_block_size",
            "pl_garbage_collection",
            "hnsw_garbage_collection",
        ] {
            assert_eq!(local[setting], compaction[setting], "{setting}");
        }
        let blockfiles = serde_json::to_value(
            serde_json::from_value::<BlockfileProviderConfig>(
                worker.compaction_service.blockfile_provider,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(local_blockfile_config().unwrap()).unwrap(),
            blockfiles
        );
    }

    // The SPANN index of distributed Chroma on the data path of the single node: written and flushed with the
    // providers of one manager, read back from the files by those of the next one.
    #[tokio::test]
    async fn spann_segments_are_written_on_the_data_path_and_read_after_a_restart() {
        use crate::{
            blockfile_record::RecordSegmentReaderOptions,
            distributed_spann::SpannSegmentReaderShard, types::materialize_logs,
        };
        use chroma_types::{
            InternalCollectionConfiguration, InternalSpannConfiguration, SegmentShard,
            VectorIndexConfiguration,
        };

        let root = tempfile::tempdir().unwrap();
        let config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
            persist_path: Some(root.path().to_str().unwrap().to_string()),
        };
        let open = || async {
            let registry = Registry::new();
            registry.register(get_new_sqlite_db().await);
            LocalSegmentManager::try_from_config(&config, &registry)
                .await
                .unwrap()
        };
        let mut collection = Collection {
            config: InternalCollectionConfiguration {
                vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration::default()),
                embedding_function: None,
            },
            ..Collection::test_collection(3)
        };
        collection.schema = Some(Schema::try_from(&collection.config).unwrap());
        let mut segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Spann,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };

        let first = open().await;
        let writer = first
            .spann_provider()
            .unwrap()
            .write(
                &collection,
                &SegmentShard::try_from((&segment, 0)).unwrap(),
                3,
                None,
            )
            .await
            .unwrap();
        let record = |id: &str, embedding: Vec<f32>, log_offset| LogRecord {
            log_offset,
            record: OperationRecord {
                id: id.to_string(),
                embedding: Some(embedding),
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        };
        let logs = Chunk::new(
            vec![
                record("a", vec![1.0, 2.0, 3.0], 1),
                record("b", vec![4.0, 5.0, 6.0], 2),
            ]
            .into(),
        );
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .unwrap();
        writer
            .apply_materialized_log_chunk(&None, &materialized)
            .await
            .unwrap();
        segment.file_path = writer.commit().await.unwrap().flush().await.unwrap();
        assert_eq!(segment.file_path.len(), 4);
        drop(first);

        let second = open().await;
        let provider = second.spann_provider().unwrap();
        let reader = SpannSegmentReaderShard::from_segment(
            &collection,
            &SegmentShard::try_from((&segment, 0)).unwrap(),
            &provider.blockfile_provider,
            &provider.hnsw_provider,
            3,
            true,
        )
        .await
        .unwrap();
        let posting_lists = reader
            .index_reader
            .posting_lists
            .get_range(.., ..)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(posting_lists.len(), 1);
        assert_eq!(
            posting_lists[0].2.doc_embeddings,
            &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        );
        // No temporary file of the writes is left on the data path.
        let mut left = vec![root.path().to_path_buf()];
        while let Some(dir) = left.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                assert!(
                    !path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with(".tmp"),
                    "{path:?}"
                );
                if path.is_dir() {
                    left.push(path);
                }
            }
        }
    }

    // A flush forks the HNSW index of the centers of its collection with the providers of the compactor, and holds the
    // write lock of the fork while it changes it. A reader that opens the collection meanwhile, with the providers of
    // the readers, answers at once: with one provider for both it would find the fork under the key of the collection,
    // wait on its lock, and only then see that the index is another one. The test holds the lock across the wait for the
    // reader on purpose, as the flush holds it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_opens_a_collection_while_a_flush_holds_its_fork() {
        use crate::{
            blockfile_record::RecordSegmentReaderOptions,
            distributed_spann::SpannSegmentReaderShard, types::materialize_logs,
        };
        use chroma_types::{
            InternalCollectionConfiguration, InternalSpannConfiguration, SegmentShard,
            VectorIndexConfiguration, HNSW_PATH,
        };

        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection {
            config: InternalCollectionConfiguration {
                vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration::default()),
                embedding_function: None,
            },
            ..Collection::test_collection(3)
        };
        collection.schema = Some(Schema::try_from(&collection.config).unwrap());
        let mut segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Spann,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };
        let compactor = manager.compaction_spann_provider().unwrap();
        let writer = compactor
            .write(
                &collection,
                &SegmentShard::try_from((&segment, 0)).unwrap(),
                3,
                None,
            )
            .await
            .unwrap();
        let logs = Chunk::new(
            vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "a".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            }]
            .into(),
        );
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .unwrap();
        writer
            .apply_materialized_log_chunk(&None, &materialized)
            .await
            .unwrap();
        segment.file_path = writer.commit().await.unwrap().flush().await.unwrap();

        // The next flush, as SpannIndexWriter forks the index of the centers, stopped with the lock of the fork held.
        let (prefix, id) =
            Segment::extract_prefix_and_id(&segment.file_path.get(HNSW_PATH).unwrap()[0]).unwrap();
        let config = collection
            .schema
            .as_ref()
            .unwrap()
            .get_internal_spann_config()
            .unwrap();
        let fork = compactor
            .hnsw_provider
            .fork(
                &IndexUuid(id),
                &collection.collection_id,
                3,
                config.space.into(),
                config.ef_search,
                prefix,
            )
            .await
            .unwrap();
        let held = fork.inner.write();

        let readers = manager.spann_provider().unwrap().clone();
        let (reading_collection, reading_segment) = (collection.clone(), segment.clone());
        let reading = tokio::spawn(async move {
            SpannSegmentReaderShard::from_segment(
                &reading_collection,
                &SegmentShard::try_from((&reading_segment, 0)).unwrap(),
                &readers.blockfile_provider,
                &readers.hnsw_provider,
                3,
                true,
            )
            .await
            .map(|_| ())
        });
        let opened = tokio::time::timeout(std::time::Duration::from_secs(5), reading).await;
        drop(held);
        assert!(
            matches!(opened, Ok(Ok(Ok(())))),
            "the reader waited for the flush: {opened:?}"
        );
    }

    #[tokio::test]
    async fn one_sweep_takes_out_the_rows_of_every_index_whose_files_are_gone() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let mut manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        manager.cleanup_task.take().unwrap().0.abort();
        // More rows than one statement takes; half of them with their files still there.
        for n in 1..=600u128 {
            let id = uuid::Uuid::from_u128(n).to_string();
            if n % 2 == 0 {
                tokio::fs::create_dir(root.path().join(&id)).await.unwrap();
            }
            sqlx::query("INSERT INTO index_cleanup (segment_id) VALUES (?)")
                .bind(&id)
                .execute(manager.sqlite.get_conn())
                .await
                .unwrap();
        }
        manager.cleanup_deleted_indexes().await.unwrap();
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM index_cleanup")
            .fetch_one(manager.sqlite.get_conn())
            .await
            .unwrap();
        assert_eq!(pending, 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn cleanup_continues_after_entry_failure() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let mut manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        // Isolate the explicit sweep from the startup task.
        manager.cleanup_task.take().unwrap().0.abort();
        let bad = uuid::Uuid::from_u128(1).to_string();
        let good = uuid::Uuid::from_u128(2).to_string();
        // A regular file reliably fails directory removal, even when run as root.
        tokio::fs::write(root.path().join(&bad), b"blocked")
            .await
            .unwrap();
        tokio::fs::create_dir(root.path().join(&good))
            .await
            .unwrap();
        for id in [&bad, &good] {
            sqlx::query("INSERT INTO index_cleanup (segment_id) VALUES (?)")
                .bind(id)
                .execute(manager.sqlite.get_conn())
                .await
                .unwrap();
        }
        assert!(manager.cleanup_deleted_indexes().await.is_err());
        assert!(!root.path().join(&good).exists());
        let pending: Vec<String> = sqlx::query_scalar("SELECT segment_id FROM index_cleanup")
            .fetch_all(manager.sqlite.get_conn())
            .await
            .unwrap();
        assert_eq!(pending, vec![bad.clone()]);
        tokio::fs::remove_file(root.path().join(&bad))
            .await
            .unwrap();
        manager.cleanup_deleted_indexes().await.unwrap();
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM index_cleanup")
            .fetch_one(manager.sqlite.get_conn())
            .await
            .unwrap();
        assert_eq!(pending, 0);
    }

    #[tokio::test]
    async fn fresh_catalog_preserves_unrecognized_indexes() {
        let root = tempfile::tempdir().unwrap();
        let orphan = root.path().join(SegmentUuid::new().to_string());
        tokio::fs::create_dir(&orphan).await.unwrap();
        tokio::fs::write(orphan.join("header.bin"), b"recoverable")
            .await
            .unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        manager.cleanup_deleted_indexes().await.unwrap();
        assert_eq!(
            tokio::fs::read(orphan.join("header.bin")).await.unwrap(),
            b"recoverable"
        );
    }

    #[tokio::test]
    async fn concurrent_misses_and_eviction_share_one_index() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let mut manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        manager.cleanup_task.take().unwrap().0.abort();
        // Force unrelated indexes to share a partition.
        manager.live_indexes = AysncPartitionedMutex::with_parallelism(1, HashMap::new());
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
            .bind(segment.id.to_string())
            .bind("urn:chroma:segment/vector/hnsw-local-memory")
            .bind("VECTOR")
            .bind(collection.collection_id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            manager.get_hnsw_writer(&collection, &segment, 3),
            manager.get_hnsw_writer(&collection, &segment, 3),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &second.index.inner));
        manager
            .hnsw_index_pool
            .remove(&IndexUuid(segment.id.0))
            .await;
        let reader = manager
            .get_hnsw_reader(&collection, &segment, 3)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &reader.index.inner));
        let index_path = root.path().join(segment.id.to_string());
        assert!(index_path.is_dir());
        let orphan = root.path().join(SegmentUuid::new().to_string());
        tokio::fs::create_dir(&orphan).await.unwrap();
        manager.cleanup_deleted_indexes().await.unwrap();
        assert!(orphan.exists());
        assert!(index_path.is_dir());
        sqlx::query("DELETE FROM segments WHERE id = ?")
            .bind(segment.id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        sqlx::query("INSERT INTO index_cleanup (segment_id) VALUES (?)")
            .bind(segment.id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        let writer_guard = first.index.inner.write().await;
        {
            let deletion = manager.delete_hnsw_index(segment.id);
            tokio::pin!(deletion);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(10), &mut deletion,)
                    .await
                    .is_err()
            );
            // Keep deletion pending while another index misses in the same
            // partition. It must not wait for the first index's writer.
            let other = Segment {
                id: SegmentUuid::new(),
                ..segment.clone()
            };
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(other.id.to_string())
                .bind("urn:chroma:segment/vector/hnsw-local-memory")
                .bind("VECTOR")
                .bind(collection.collection_id.to_string())
                .execute(manager.sqlite.get_conn())
                .await
                .unwrap();
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                manager.get_hnsw_writer(&collection, &other, 3),
            )
            .await
            .expect("unrelated cache miss blocked by deletion")
            .unwrap();
            // Cancel the pending deletion; its identity must remain retryable.
        }
        assert!(manager
            .live_indexes
            .lock(&IndexUuid(segment.id.0))
            .await
            .get(&IndexUuid(segment.id.0))
            .and_then(Weak::upgrade)
            .is_some());
        drop(writer_guard);
        manager.delete_hnsw_index(segment.id).await.unwrap();
        assert!(!index_path.exists());
        assert!(matches!(
            reader.query_embedding(&[], vec![1.0; 3], 1).await,
            Err(LocalHnswSegmentReaderError::Deleted)
        ));
        let mut first = first;
        assert!(matches!(
            first
                .apply_log_chunk(chroma_types::Chunk::new(vec![].into()))
                .await,
            Err(LocalHnswSegmentWriterError::Deleted)
        ));
        assert!(matches!(
            manager.get_hnsw_writer(&collection, &segment, 3).await,
            Err(LocalSegmentManagerError::Deleted)
        ));
    }

    fn add(offset: i64, id: &str) -> Chunk<LogRecord> {
        Chunk::new(
            vec![LogRecord {
                log_offset: offset,
                record: OperationRecord {
                    id: id.to_string(),
                    embedding: Some(vec![offset as f32; 3]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            }]
            .into(),
        )
    }

    #[tokio::test]
    async fn checkpoint_validation_requires_persistent_storage() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        for persist_path in [None, Some(root.path().to_str().unwrap().to_string())] {
            let manager = LocalSegmentManager::try_from_config(
                &LocalSegmentManagerConfig {
                    hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                    persist_path,
                },
                &registry,
            )
            .await
            .unwrap();
            assert_eq!(
                manager
                    .validate_persisted_checkpoint(&SegmentUuid::new(), 0)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::NotFound
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_validation_requires_id_map_and_accepts_deleted_vectors() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        registry.register(get_new_sqlite_db().await);
        let manager = LocalSegmentManager::try_from_config(
            &LocalSegmentManagerConfig {
                hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
                persist_path: Some(root.path().to_str().unwrap().to_string()),
            },
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
            .bind(segment.id.to_string())
            .bind("urn:chroma:segment/vector/hnsw-local-persisted")
            .bind("VECTOR")
            .bind(collection.collection_id.to_string())
            .execute(manager.sqlite.get_conn())
            .await
            .unwrap();
        let mut writer = manager
            .get_hnsw_writer(&collection, &segment, 3)
            .await
            .unwrap();
        // Native initialization creates structurally valid empty files, but no
        // ID map has been published. This is not a checkpoint authorizing purge.
        let folder = root.path().join(segment.id.to_string());
        assert!(
            !inspect_persisted_hnsw_index(&folder)
                .unwrap()
                .recovery_required
        );
        assert_eq!(
            manager
                .validate_persisted_checkpoint(&segment.id, 0)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        writer.index.set_sync_threshold(1).await;
        writer.apply_log_chunk(add(1, "a")).await.unwrap();
        manager
            .validate_persisted_checkpoint(&segment.id, 0)
            .await
            .unwrap();
        writer
            .apply_log_chunk(Chunk::new(
                vec![LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "a".into(),
                        embedding: None,
                        encoding: None,
                        document: None,
                        metadata: None,
                        operation: Operation::Delete,
                    },
                }]
                .into(),
            ))
            .await
            .unwrap();
        // Tombstoned native slots with no live vectors are still a usable checkpoint.
        manager
            .validate_persisted_checkpoint(&segment.id, 0)
            .await
            .unwrap();
        writer.index.close().await;
    }

    // Exceeds the Windows CRT's 512-stream limit if any of creation, save,
    // reader load, or writer load leaves four streams open per cached index.
    // Use a subprocess on Unix so lowering the limit cannot affect other tests.
    #[tokio::test]
    async fn cached_indexes_do_not_exhaust_file_handles() {
        #[cfg(unix)]
        if std::env::var_os("CHROMA_HNSW_FD_LIMIT_TEST_CHILD").is_none() {
            let output = std::process::Command::new("sh")
                .args(["-c", "ulimit -n 512 && exec \"$@\"", "hnsw-file-limit-test"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "local_segment_manager::tests::cached_indexes_do_not_exhaust_file_handles",
                    "--nocapture",
                ])
                .env("CHROMA_HNSW_FD_LIMIT_TEST_CHILD", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "limited-file subprocess failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let sqlite = get_new_sqlite_db().await;
        let config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
            persist_path: Some(root.path().to_str().unwrap().to_owned()),
        };
        let registry = Registry::new();
        registry.register(sqlite.clone());
        let manager = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        let mut segments = Vec::new();
        for _ in 0..160 {
            let mut collection = Collection::test_collection(3);
            collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
            let segment = Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::HnswLocalPersisted,
                scope: SegmentScope::VECTOR,
                collection: collection.collection_id,
                metadata: None,
                file_path: Default::default(),
            };
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment.id.to_string())
                .bind("urn:chroma:segment/vector/hnsw-local-persisted")
                .bind("VECTOR")
                .bind(collection.collection_id.to_string())
                .execute(sqlite.get_conn())
                .await
                .unwrap();
            let mut writer = manager
                .get_hnsw_writer(&collection, &segment, 3)
                .await
                .unwrap();
            writer.index.set_sync_threshold(1).await;
            writer.apply_log_chunk(add(1, "a")).await.unwrap();
            segments.push((collection, segment));
        }
        // Keep the original cache alive while loading and retaining readers.
        let readers = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        for (collection, segment) in &segments {
            let reader = readers
                .get_hnsw_reader(collection, segment, 3)
                .await
                .unwrap();
            assert_eq!(reader.index.applied_state().await, (1, 1));
        }
        drop(readers);
        drop(manager);
        let writers = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        for (collection, segment) in &segments {
            let mut writer = writers
                .get_hnsw_writer(collection, segment, 3)
                .await
                .unwrap();
            writer.index.set_sync_threshold(1).await;
            writer.apply_log_chunk(add(2, "b")).await.unwrap();
            assert_eq!(writer.index.applied_state().await, (2, 2));
        }
        // Reopen the checkpoints to verify that closing streams after saves
        // preserved the new vectors and replay watermark.
        let fresh = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        for (collection, segment) in &segments {
            let reader = fresh.get_hnsw_reader(collection, segment, 3).await.unwrap();
            assert_eq!(reader.index.applied_state().await, (2, 2));
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&"b".to_string())
                    .await
                    .unwrap(),
                vec![2.0; 3]
            );
        }
    }

    #[tokio::test]
    async fn shared_index_persists_after_replace_and_eviction() {
        let root = tempfile::tempdir().unwrap();
        let sqlite = get_new_sqlite_db().await;
        let config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: default_hnsw_index_pool_cache_config(),
            persist_path: Some(root.path().to_str().unwrap().to_string()),
        };
        let registry = Registry::new();
        registry.register(sqlite.clone());
        let manager = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
            .bind(segment.id.to_string())
            .bind("urn:chroma:segment/vector/hnsw-local-persisted")
            .bind("VECTOR")
            .bind(collection.collection_id.to_string())
            .execute(sqlite.get_conn())
            .await
            .unwrap();

        // Two concurrent misses share one index, and the loser must not
        // re-insert it into the cache.
        let (first, second) = tokio::join!(
            manager.get_hnsw_writer(&collection, &segment, 3),
            manager.get_hnsw_writer(&collection, &segment, 3),
        );
        let mut first = first.unwrap();
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &second.index.inner));
        first.index.set_sync_threshold(1).await;
        first.apply_log_chunk(add(1, "a")).await.unwrap();

        // Evict while a caller still holds the index. The listener closes its
        // files; the next writer reuses the same index and must still persist.
        manager
            .hnsw_index_pool
            .remove(&IndexUuid(segment.id.0))
            .await;
        let mut reused = manager
            .get_hnsw_writer(&collection, &segment, 3)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.index.inner, &reused.index.inner));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        reused.apply_log_chunk(add(2, "b")).await.unwrap();
        drop((first, second, reused));

        // A fresh manager loads both records from disk.
        let registry = Registry::new();
        registry.register(sqlite);
        let fresh = LocalSegmentManager::try_from_config(&config, &registry)
            .await
            .unwrap();
        let reader = fresh
            .get_hnsw_reader(&collection, &segment, 3)
            .await
            .unwrap();
        assert_eq!(reader.index.applied_state().await, (2, 2));
    }
}
