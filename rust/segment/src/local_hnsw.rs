// Changed from the original Chroma file; see the git history for the changes and their authors.
mod persistence;
pub(crate) use persistence::validate_checkpoint;
use persistence::{validate_checkpoint_seq_id, CheckpointFingerprint, VerifiedCheckpoint};
#[cfg(test)]
mod regression;
#[cfg(feature = "test-hooks")]
pub use persistence::TOUCH_AFTER_INSPECTION;
pub use persistence::{
    inspect_persisted_hnsw_index, inspect_persisted_hnsw_index_for_config_repair,
    is_checkpoint_changed, CheckpointChanged, PersistedHnswIndex,
};

use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    mem::size_of,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chroma_cache::Weighted;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{HnswIndex, HnswIndexConfig, IndexConfig};
use chroma_sqlite::{db::SqliteDb, table::MaxSeqId};
use chroma_types::{
    operator::RecordMeasure, Chunk, Collection, HnswParametersFromSegmentError, LogRecord,
    Operation, Segment, SegmentUuid,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sea_query::{Expr, Query, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_pickle::{DeOptions, SerOptions};
use sqlx::Row;
use thiserror::Error;

pub const METADATA_FILE: &str = "index_metadata.pickle";
pub const HNSW_HEADER_FILE: &str = "header.bin";
pub const HNSW_INDEX_FILES: [&str; 4] = chroma_index::hnsw_provider::FILES;
const HNSW_PERSISTENCE_VERSION: i32 = 1;

// The shortest time between two saves of an index by the same instance: a save syncs the files of the index, and a
// save every sync_threshold records kept the index waiting for the disk most of the time under a steady load. The
// records after the last save are in the log of SQLite, whose position for the segment moves only after a save, and
// the next load applies them again, as after any stop between two saves. The first save of an instance waits for the
// threshold only, as in Chroma 1.5.9: the files of an index never saved are not read at load.
const SAVE_INTERVAL: Duration = Duration::from_secs(30);

// Native HNSW uses four C-runtime streams per persistent index (and another
// four temporarily while loading). Windows defaults to 512 streams per process.
// Budget disk operations across all local managers, leaving room for other I/O;
// cached indexes keep their vectors in memory and hold no idle file streams.
static HNSW_FILE_OPERATIONS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(32);

async fn acquire_hnsw_files() -> tokio::sync::SemaphorePermit<'static> {
    HNSW_FILE_OPERATIONS
        .acquire()
        .await
        .expect("HNSW file semaphore is never closed")
}

// Must be held under the index write lock. Close on every exit, including a
// failed native save, before the file-operation permit is released.
struct HnswFiles<'a>(&'a HnswIndex);

impl Drop for HnswFiles<'_> {
    fn drop(&mut self) {
        self.0.close_fd();
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct LocalHnswSegmentReader {
    pub index: LocalHnswIndex,
}

#[derive(Error, Debug)]
pub enum LocalHnswSegmentReaderError {
    #[error("Segment has been deleted")]
    Deleted,
    #[error("Error opening pickle file: {0}")]
    PickleFileOpenError(#[from] std::io::Error),
    #[error("Error deserializing pickle file: {0}")]
    PickleFileDeserializeError(#[from] serde_pickle::Error),
    #[error("Error loading hnsw index")]
    HnswIndexLoadError,
    #[error("Nothing found on disk")]
    UninitializedSegment,
    #[error("Collection is missing HNSW configuration")]
    MissingHnswConfiguration,
    #[error("Could not parse HNSW configuration: {0}")]
    InvalidHnswConfiguration(#[from] HnswParametersFromSegmentError),
    #[error("Error serializing path to string")]
    PersistPathError,
    #[error("Error finding id")]
    IdNotFound,
    #[error("Error getting embedding")]
    GetEmbeddingError,
    #[error("Error querying knn")]
    QueryError,
    #[error("Persisted HNSW dimensionality {actual} does not match collection dimensionality {expected}")]
    DimensionalityMismatch { expected: usize, actual: usize },
    #[error("Error reading from sqlite: {0}")]
    SqliteError(#[from] sqlx::error::Error),
    #[error("Error building max sequence id migration query")]
    QueryBuilderError(#[from] sea_query::error::Error),
}

impl ChromaError for LocalHnswSegmentReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentReaderError::Deleted => ErrorCodes::NotFound,
            LocalHnswSegmentReaderError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::MissingHnswConfiguration => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::InvalidHnswConfiguration(err) => err.code(),
            LocalHnswSegmentReaderError::PersistPathError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::IdNotFound => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::GetEmbeddingError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::QueryError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::DimensionalityMismatch { .. } => ErrorCodes::DataLoss,
            LocalHnswSegmentReaderError::SqliteError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::QueryBuilderError(_) => ErrorCodes::Internal,
        }
    }
}

async fn get_current_seq_id(
    segment: &Segment,
    sql_db: &SqliteDb,
) -> Result<u64, sqlx::error::Error> {
    let (query, values) = Query::select()
        .column(MaxSeqId::SeqId)
        .from(MaxSeqId::Table)
        .and_where(Expr::col(MaxSeqId::SegmentId).eq(segment.id.to_string()))
        .build_sqlx(SqliteQueryBuilder);
    let row = sqlx::query_with(&query, values)
        .fetch_optional(sql_db.get_conn())
        .await?;
    let seq_id = row
        .map(|row| row.try_get::<u64, _>(0))
        .transpose()?
        .unwrap_or_default();
    Ok(seq_id)
}

// Only call after validating and loading the matching native files. A new pickle
// can be ahead of SQLite if publication was interrupted; replay must start after
// that pickle's operations. Legacy offsets only initialize an absent watermark.
// The journal of a save cut by a stop, before anything reads the files of the index in index_folder: applied when it is
// the last save of the index, and discarded when the files were saved after it by a program that writes no journal, as
// Chroma 1.5.9 opening the same folder. The journal carries the ID map of its save, with the log position it publishes;
// VecturaDB moves the position of the segment in SQLite only after that save, so its own journal always reaches the
// position in SQLite. A journal that stops before it is older than the files: applying it would put back records that
// the log already replayed past.
async fn apply_journals_newer_than_data(
    index_folder: &Path,
    segment: &Segment,
    sql_db: &SqliteDb,
) -> Result<(), Box<dyn ChromaError>> {
    let journals = HnswIndex::persist_journals(index_folder)?;
    if journals.is_empty() {
        return Ok(());
    }
    let newest = journals
        .iter()
        .filter(|journal| journal.complete)
        .filter_map(|journal| {
            journal
                .whole_files
                .iter()
                .find(|(name, _)| name == METADATA_FILE)
                .and_then(|(_, bytes)| {
                    serde_pickle::from_slice::<IdMap>(bytes, DeOptions::new()).ok()
                })
                .and_then(|id_map| id_map.checkpoint_seq_id.or(id_map.max_seq_id))
        })
        .max();
    let current = get_current_seq_id(segment, sql_db)
        .await
        .map_err(|e| LocalHnswSegmentWriterError::from(e).boxed())?;
    match newest {
        Some(newest) if newest < current => {
            tracing::warn!(
                folder = %index_folder.display(),
                journals = newest,
                log = current,
                "Journals of saves older than the files of the index, saved after them without a journal: discarded"
            );
            HnswIndex::discard_persist_journals(index_folder)
        }
        _ => HnswIndex::apply_persist_journal(index_folder).map(|_| ()),
    }
}

async fn restore_checkpoint_seq_id(
    segment: &Segment,
    sql_db: &SqliteDb,
    id_map: &IdMap,
) -> Result<u64, sqlx::Error> {
    if let Some(offset) = id_map.checkpoint_seq_id {
        let current = get_current_seq_id(segment, sql_db).await?;
        validate_checkpoint_seq_id(Some(offset), current)
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        if current == offset {
            return Ok(current);
        }
    }
    if let Some(offset) = id_map.checkpoint_seq_id.or(id_map.max_seq_id) {
        let offset = i64::try_from(offset).map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        let query = if id_map.checkpoint_seq_id.is_some() {
            "INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, ?) \
             ON CONFLICT(segment_id) DO UPDATE SET seq_id = excluded.seq_id \
             WHERE max_seq_id.seq_id < excluded.seq_id"
        } else {
            "INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, ?) \
             ON CONFLICT(segment_id) DO NOTHING"
        };
        sqlx::query(query)
            .bind(segment.id.to_string())
            .bind(offset)
            .execute(sql_db.get_conn())
            .await?;
    }
    get_current_seq_id(segment, sql_db).await
}

pub use chroma_index::parse_persisted_hnsw_dim;

async fn persisted_hnsw_dim(index_folder: &Path) -> Result<usize, std::io::Error> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(index_folder.join(HNSW_HEADER_FILE)).await?;
    let mut header = [0; size_of::<i32>() + 6 * size_of::<usize>()];
    file.read_exact(&mut header).await?;
    parse_persisted_hnsw_dim(&header).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid persisted HNSW header",
        )
    })
}

impl LocalHnswSegmentReader {
    pub fn from_index(hnsw_index: LocalHnswIndex) -> Self {
        Self { index: hnsw_index }
    }

    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        persist_root: Option<String>,
        sql_db: SqliteDb,
    ) -> Result<Self, LocalHnswSegmentReaderError> {
        let hnsw_configuration = collection
            .schema
            .as_ref()
            .map(|schema| schema.get_internal_hnsw_config_with_legacy_fallback(segment))
            .transpose()?
            .flatten()
            .ok_or(LocalHnswSegmentReaderError::MissingHnswConfiguration)?;

        match persist_root {
            Some(path_str) => {
                let _files = acquire_hnsw_files().await;
                let path = Path::new(&path_str);
                let index_folder = path.join(segment.id.to_string());
                // A save cut by a stop leaves its journal: its bytes go back to the files before anything reads them, unless
                // the files were saved after it (apply_journals_newer_than_data).
                if index_folder.exists() {
                    apply_journals_newer_than_data(&index_folder, segment, &sql_db)
                        .await
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                }
                if !index_folder.join(METADATA_FILE).is_file()
                    && get_current_seq_id(segment, &sql_db).await? > 0
                {
                    return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
                }
                if !index_folder.exists() {
                    // Return uninitialized reader.
                    return Err(LocalHnswSegmentReaderError::UninitializedSegment);
                }
                let index_folder_str = match index_folder.to_str() {
                    Some(path) => path,
                    None => return Err(LocalHnswSegmentReaderError::PersistPathError),
                };
                let pickle_file_path = path.join(segment.id.to_string()).join(METADATA_FILE);
                if pickle_file_path.exists() {
                    let file = tokio::fs::File::open(pickle_file_path)
                        .await?
                        .into_std()
                        .await;
                    let mut id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
                    if let Some(actual) = id_map.dimensionality {
                        if actual != dimensionality {
                            return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                                expected: dimensionality,
                                actual,
                            });
                        }
                    }
                    let actual = persisted_hnsw_dim(&index_folder)
                        .await
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                    if actual != dimensionality {
                        return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                            expected: dimensionality,
                            actual,
                        });
                    }
                    id_map.dimensionality = Some(dimensionality);
                    let loaded_before = CheckpointFingerprint::read(&index_folder).ok();
                    let inspection = persistence::validate_files(&index_folder, Some(&id_map))
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                    // Load hnsw index.
                    let index_config = IndexConfig::new(
                        dimensionality as i32,
                        hnsw_configuration.space.clone().into(),
                    );
                    let index = HnswIndex::load(
                        index_folder_str,
                        &index_config,
                        hnsw_configuration.ef_search,
                        chroma_index::IndexUuid(segment.id.0),
                    )
                    .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                    index.close_fd();
                    let verified_checkpoint =
                        loaded_verified(loaded_before, &index_folder, &inspection);

                    reconcile_checkpoint(&index, &mut id_map, &inspection)
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;

                    let current_seq_id =
                        restore_checkpoint_seq_id(segment, &sql_db, &id_map).await?;

                    // TODO(Sanket): Set allow reset appropriately.
                    return Ok(Self {
                        index: LocalHnswIndex {
                            inner: Arc::new(IndexShared::new(Inner {
                                index,
                                id_map,
                                index_init: true,
                                deleted: false,
                                replay_complete: false,
                                recovery_checkpoint_pending: inspection.recovery_required,
                                verified_checkpoint,
                                failed: false,
                                deleted_on_load: inspection
                                    .mapped_deleted_labels
                                    .iter()
                                    .copied()
                                    .collect(),
                                #[cfg(test)]
                                mutation_budget: None,
                                #[cfg(test)]
                                fail_resize: false,
                                allow_reset: false,
                                num_elements_since_last_persist: 0,
                                last_persist: None,
                                save_interval: SAVE_INTERVAL,
                                last_seen_seq_id: current_seq_id,
                                sync_threshold: hnsw_configuration.sync_threshold,
                                persist_path: Some(index_folder_str.to_string()),
                                sqlite: sql_db,
                            })),
                        },
                    });
                }
                // No pickle and no position in SQLite: no save was published, and the files there are those that the
                // creation of the index wrote with no sync, empty or cut after a power cut. The log has every record, so
                // they are not read, as in Chroma 1.5.9.
                // Return uninitialized reader.
                Err(LocalHnswSegmentReaderError::UninitializedSegment)
            }
            None => {
                let index_config = IndexConfig::new(
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                );
                let hnsw_config = HnswIndexConfig::new_ephemeral(
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
                );

                // TODO(Sanket): HnswIndex init is not thread safe. We should not call it from multiple threads
                let index = HnswIndex::init(
                    &index_config,
                    Some(&hnsw_config),
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;

                Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(IndexShared::new(Inner {
                            index,
                            id_map: IdMap::new(dimensionality),
                            index_init: true,
                            deleted: false,
                            replay_complete: false,
                            recovery_checkpoint_pending: false,
                            verified_checkpoint: None,
                            failed: false,
                            deleted_on_load: HashSet::new(),
                            #[cfg(test)]
                            mutation_budget: None,
                            #[cfg(test)]
                            fail_resize: false,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            last_persist: None,
                            save_interval: SAVE_INTERVAL,
                            last_seen_seq_id: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: None,
                            sqlite: sql_db,
                        })),
                    },
                })
            }
        }
    }

    pub async fn get_embedding_by_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<Vec<f32>, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        if guard.failed {
            return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
        }
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        if let Some(actual) = guard.id_map.dimensionality {
            let expected = guard.index.dimensionality() as usize;
            if actual != expected {
                return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                    expected,
                    actual,
                });
            }
        }
        guard
            .index
            .get(offset_id as usize)
            .map_err(|_| LocalHnswSegmentReaderError::GetEmbeddingError)?
            .ok_or(LocalHnswSegmentReaderError::GetEmbeddingError)
    }

    pub async fn current_max_seq_id(
        &self,
        segment_id: &SegmentUuid,
    ) -> Result<u64, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        if guard.failed {
            return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
        }
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        let (sql, values) = Query::select()
            .column(MaxSeqId::SeqId)
            .from(MaxSeqId::Table)
            .and_where(Expr::col(MaxSeqId::SegmentId).eq(segment_id.to_string()))
            .build_sqlx(SqliteQueryBuilder);
        let row_opt = sqlx::query_with(&sql, values)
            .fetch_optional(guard.sqlite.get_conn())
            .await?;
        Ok(row_opt
            .map(|row| row.try_get::<u64, _>(0))
            .transpose()?
            .unwrap_or_default())
    }

    pub async fn get_embedding_by_user_id(
        &self,
        user_id: &String,
    ) -> Result<Vec<f32>, LocalHnswSegmentReaderError> {
        let offset_id = self.get_offset_id_by_user_id(user_id).await?;
        self.get_embedding_by_offset_id(offset_id).await
    }

    pub async fn get_offset_id_by_user_id(
        &self,
        user_id: &String,
    ) -> Result<u32, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        if guard.failed {
            return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
        }
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        guard
            .id_map
            .id_to_label
            .get(user_id)
            .cloned()
            .ok_or(LocalHnswSegmentReaderError::IdNotFound)
    }

    pub async fn get_user_id_by_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<String, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        if guard.failed {
            return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
        }
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        guard
            .id_map
            .label_to_id
            .get(&offset_id)
            .cloned()
            .ok_or(LocalHnswSegmentReaderError::IdNotFound)
    }

    pub async fn query_embedding(
        &self,
        allowed_offset_ids: &[u32],
        embedding: Vec<f32>,
        k: u32,
    ) -> Result<Vec<RecordMeasure>, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        if guard.failed {
            return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
        }
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        if let Some(actual) = guard.id_map.dimensionality {
            let expected = guard.index.dimensionality() as usize;
            if actual != expected {
                return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                    expected,
                    actual,
                });
            }
        }
        if embedding.len() != guard.index.dimensionality() as usize {
            return Err(LocalHnswSegmentReaderError::QueryError);
        }
        let len_with_deleted = guard.index.len_with_deleted();
        let actual_len = guard.index.len();

        // Neither search path needs to run when no results can be returned.
        if k == 0 || actual_len == 0 {
            return Ok(Vec::new());
        }

        let delete_percentage = (len_with_deleted - actual_len) as f32 / len_with_deleted as f32;

        // If the index is small and the delete percentage is high, its quite likely that the index is
        // degraded, so we brute force the search
        // Otherwise search the index normally
        if delete_percentage > 0.2 && actual_len < 100 {
            match guard.index.get_all_ids() {
                Ok((valid_ids, _deleted_ids)) => {
                    let mut max_heap = BinaryHeap::new();
                    let allowed_ids_as_set = allowed_offset_ids
                        .iter()
                        .collect::<std::collections::HashSet<_>>();
                    for curr_id in valid_ids.iter() {
                        if !allowed_ids_as_set.is_empty()
                            && !allowed_ids_as_set.contains(&(*curr_id as u32))
                        {
                            continue;
                        }
                        let curr_embedding = guard.index.get(*curr_id);
                        match curr_embedding {
                            Ok(Some(curr_embedding)) => {
                                let curr_embedding = match guard.index.distance_function {
                                    chroma_distance::DistanceFunction::Cosine => {
                                        chroma_distance::normalize(&curr_embedding)
                                    }
                                    _ => curr_embedding,
                                };
                                let curr_distance = guard
                                    .index
                                    .distance_function
                                    .distance(curr_embedding.as_slice(), embedding.as_slice());
                                if max_heap.len() < k as usize {
                                    max_heap.push(RecordMeasure {
                                        offset_id: *curr_id as u32,
                                        measure: curr_distance,
                                    });
                                } else {
                                    // SAFETY(hammadb): We are sure that the heap has at least one element
                                    // because we insert until we have k elements.
                                    let top = max_heap.peek().unwrap();
                                    if top.measure > curr_distance {
                                        max_heap.pop();
                                        max_heap.push(RecordMeasure {
                                            offset_id: *curr_id as u32,
                                            measure: curr_distance,
                                        });
                                    }
                                }
                            }
                            _ => {
                                return Err(LocalHnswSegmentReaderError::QueryError);
                            }
                        }
                    }
                    Ok(max_heap.into_sorted_vec())
                }
                Err(_) => Err(LocalHnswSegmentReaderError::QueryError),
            }
        } else {
            let allowed_ids = allowed_offset_ids
                .iter()
                .map(|oid| *oid as usize)
                .collect::<Vec<_>>();
            let (offset_ids, distances) = guard
                .index
                .query(&embedding, k as usize, allowed_ids.as_slice(), &[])
                .map_err(|_| LocalHnswSegmentReaderError::QueryError)?;

            Ok(offset_ids
                .into_iter()
                .zip(distances)
                .map(|(offset_id, measure)| RecordMeasure {
                    offset_id: offset_id as u32,
                    measure,
                })
                .collect())
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
struct IdMap {
    dimensionality: Option<usize>,
    total_elements_added: u32,
    /// The max_seq_id field is deprecated in favor of the sqlite table
    #[serde(default)]
    max_seq_id: Option<u64>,
    /// Applied offset published atomically with this ID map after syncing native files.
    /// Absent in older pickles, whose SQLite/legacy watermark remains authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_seq_id: Option<u64>,
    id_to_label: HashMap<String, u32>,
    label_to_id: HashMap<u32, String>,
    id_to_seq_id: HashMap<String, u32>,
}

impl IdMap {
    fn new(dimensionality: usize) -> Self {
        Self {
            dimensionality: Some(dimensionality),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistedHnswLabelMismatch {
    LabelMapsToDifferentId {
        id: String,
        label: u32,
        reverse_id: String,
    },
    MissingReverseLabel {
        id: String,
        label: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedHnswMetadata {
    pub dimensionality: Option<usize>,
    pub total_elements_added: u32,
    pub legacy_max_seq_id: Option<u64>,
    /// Applied offset published with the ID map by current writers.
    pub checkpoint_seq_id: Option<u64>,
    pub id_to_label_count: usize,
    pub label_to_id_count: usize,
    pub first_label_mismatch: Option<PersistedHnswLabelMismatch>,
}

impl From<IdMap> for PersistedHnswMetadata {
    fn from(id_map: IdMap) -> Self {
        let first_label_mismatch =
            id_map
                .id_to_label
                .iter()
                .find_map(|(id, label)| match id_map.label_to_id.get(label) {
                    Some(reverse_id) if reverse_id == id => None,
                    Some(reverse_id) => Some(PersistedHnswLabelMismatch::LabelMapsToDifferentId {
                        id: id.clone(),
                        label: *label,
                        reverse_id: reverse_id.clone(),
                    }),
                    None => Some(PersistedHnswLabelMismatch::MissingReverseLabel {
                        id: id.clone(),
                        label: *label,
                    }),
                });

        Self {
            dimensionality: id_map.dimensionality,
            total_elements_added: id_map.total_elements_added,
            legacy_max_seq_id: id_map.max_seq_id,
            checkpoint_seq_id: id_map.checkpoint_seq_id,
            id_to_label_count: id_map.id_to_label.len(),
            label_to_id_count: id_map.label_to_id.len(),
            first_label_mismatch,
        }
    }
}

#[derive(Error, Debug)]
pub enum PersistedHnswMetadataError {
    #[error("Error opening persisted HNSW metadata: {0}")]
    Open(#[from] std::io::Error),
    #[error("Error deserializing persisted HNSW metadata: {0}")]
    Deserialize(#[from] serde_pickle::Error),
}

pub fn inspect_persisted_hnsw_metadata(
    metadata_path: &Path,
) -> Result<PersistedHnswMetadata, PersistedHnswMetadataError> {
    let file = std::fs::File::open(metadata_path)?;
    let id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
    Ok(id_map.into())
}

#[allow(dead_code)]
pub struct Inner {
    index: HnswIndex,
    // Loaded from pickle file.
    id_map: IdMap,
    index_init: bool,
    deleted: bool,
    replay_complete: bool,
    // Recovery must publish a consistent checkpoint even below the sync threshold.
    // Keep this set until both the files and SQLite watermark are durable.
    recovery_checkpoint_pending: bool,
    verified_checkpoint: Option<VerifiedCheckpoint>,
    /// A failed native mutation may have partially modified the shared graph.
    failed: bool,
    // Tombstones already present in native files ahead of the pickle.
    deleted_on_load: HashSet<u32>,
    #[cfg(test)]
    mutation_budget: Option<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    fail_resize: bool,
    allow_reset: bool,
    num_elements_since_last_persist: u64,
    // When this instance last saved its index; None before its first save.
    last_persist: Option<tokio::time::Instant>,
    // SAVE_INTERVAL, shorter in tests.
    save_interval: Duration,
    last_seen_seq_id: u64,
    sync_threshold: usize,
    persist_path: Option<String>,
    sqlite: SqliteDb,
}

impl Inner {
    fn needs_checkpoint(&self) -> bool {
        self.persist_path.is_some()
            && (self.recovery_checkpoint_pending
                || (self.num_elements_since_last_persist >= self.sync_threshold as u64
                    && self
                        .last_persist
                        .is_none_or(|last| last.elapsed() >= self.save_interval)))
    }
}

/// An index with what a read checks before it uses it, readable without its lock, while a backfill holds the write lock
/// for its apply and its save: the position in the log of the last record the index holds in memory, which a read that
/// needs the records of its snapshot of SQLite compares with it, and three flags that copy the fields of Inner of the same
/// name, written with them under the write lock. Chroma 1.5.9's count and get checked a flag of the collection and read
/// SQLite, without waiting for the index.
pub struct IndexShared {
    lock: tokio::sync::RwLock<Inner>,
    applied_seq_id: AtomicU64,
    deleted: AtomicBool,
    failed: AtomicBool,
    replay_complete: AtomicBool,
}

impl IndexShared {
    // The state of a loaded index, set before the index is shared.
    fn new(inner: Inner) -> Self {
        Self {
            applied_seq_id: AtomicU64::new(inner.last_seen_seq_id),
            deleted: AtomicBool::new(inner.deleted),
            failed: AtomicBool::new(inner.failed),
            replay_complete: AtomicBool::new(inner.replay_complete),
            lock: tokio::sync::RwLock::new(inner),
        }
    }
}

impl std::ops::Deref for IndexShared {
    type Target = tokio::sync::RwLock<Inner>;

    fn deref(&self) -> &Self::Target {
        &self.lock
    }
}

#[derive(Clone)]
pub struct LocalHnswIndex {
    pub(crate) inner: Arc<IndexShared>,
}

impl LocalHnswIndex {
    /// The position in the log of the last record that this instance holds in memory, read without the lock: every
    /// record up to it is in the index. Stored by apply_log_chunk once the index holds its chunk.
    pub fn applied_seq_id_in_memory(&self) -> u64 {
        self.inner.applied_seq_id.load(Ordering::Acquire)
    }

    /// Last successfully applied offset in this instance, independent of durability.
    pub async fn applied_seq_id(&self) -> Result<u64, LocalHnswSegmentWriterError> {
        let guard = self.inner.read().await;
        if guard.deleted {
            return Err(LocalHnswSegmentWriterError::Deleted);
        }
        if guard.failed {
            return Err(LocalHnswSegmentWriterError::HnswIndexLoadError);
        }
        Ok(guard.last_seen_seq_id)
    }

    /// Returns true when a full inspection was needed. Saves invalidate the
    /// cached fingerprint before touching files and refresh it only on success.
    pub(crate) async fn validate_persisted_checkpoint(
        &self,
        path: &Path,
        watermark: u64,
    ) -> std::io::Result<bool> {
        let mut guard = self.inner.write().await;
        let before = CheckpointFingerprint::read(path)?;
        if let Some(verified) = &guard.verified_checkpoint {
            if verified.fingerprint == before {
                validate_checkpoint_seq_id(verified.seq_id, watermark)?;
                return Ok(false);
            }
        }
        guard.verified_checkpoint = None;
        let path = path.to_owned();
        let inspection =
            tokio::task::spawn_blocking(move || persistence::validate_checkpoint(&path, watermark))
                .await
                .map_err(std::io::Error::other)??;
        if before != inspection.fingerprint {
            return Err(persistence::checkpoint_changed());
        }
        guard.verified_checkpoint = Some(inspection);
        Ok(true)
    }

    /// Whether this loaded instance has successfully replayed its log tail. Read without the lock, as every read asks it.
    pub async fn replay_complete(&self) -> bool {
        self.inner.replay_complete.load(Ordering::Acquire)
    }

    /// Update shared replay state. Newly loaded instances start incomplete. Under the write lock, as the field.
    pub async fn set_replay_complete(&self, complete: bool) {
        let mut guard = self.inner.write().await;
        guard.replay_complete = complete;
        self.inner.replay_complete.store(complete, Ordering::Release);
    }

    pub async fn close(&self) {
        self.inner.write().await.index.close_fd();
    }
    pub(crate) async fn mark_deleted(&self) {
        let mut guard = self.inner.write().await;
        guard.deleted = true;
        self.inner.deleted.store(true, Ordering::Release);
        guard.index.close_fd();
    }

    /// Failed native mutations invalidate every handle to this shared index. Read without the lock, as every read asks it
    /// first.
    pub async fn ensure_usable(&self) -> Result<(), LocalHnswSegmentWriterError> {
        if self.inner.deleted.load(Ordering::Acquire) {
            return Err(LocalHnswSegmentWriterError::Deleted);
        }
        if self.inner.failed.load(Ordering::Acquire) {
            return Err(LocalHnswSegmentWriterError::HnswIndexLoadError);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn set_sync_threshold(&self, sync_threshold: usize) {
        self.inner.write().await.sync_threshold = sync_threshold;
    }

    /// The shortest time between two saves of this instance, SAVE_INTERVAL otherwise: for tests that save more than
    /// once.
    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn set_save_interval(&self, save_interval: Duration) {
        self.inner.write().await.save_interval = save_interval;
    }

    /// The last applied log offset and the number of vectors in the index.
    #[cfg(test)]
    pub(crate) async fn applied_state(&self) -> (u64, usize) {
        let guard = self.inner.read().await;
        (guard.last_seen_seq_id, guard.index.len())
    }
}

impl Weighted for LocalHnswIndex {
    fn weight(&self) -> usize {
        1
    }
}

#[allow(dead_code)]
pub struct LocalHnswSegmentWriter {
    pub index: LocalHnswIndex,
}

#[derive(Error, Debug)]
pub enum LocalHnswSegmentWriterError {
    #[error("Segment has been deleted")]
    Deleted,
    #[error("Error creating hnsw config object")]
    HnswConfigError(#[from] Box<chroma_index::HnswIndexConfigError>),
    #[error("Error opening pickle file")]
    PickleFileOpenError(#[from] std::io::Error),
    #[error("Error deserializing pickle file")]
    PickleFileDeserializeError(#[from] serde_pickle::Error),
    #[error("Error loading hnsw index")]
    HnswIndexLoadError,
    #[error("Nothing found on disk")]
    UninitializedSegment,
    #[error("Collection is missing HNSW configuration")]
    MissingHnswConfiguration,
    #[error("Could not parse HNSW configuration: {0}")]
    InvalidHnswConfiguration(#[from] HnswParametersFromSegmentError),
    #[error("Error creating hnsw index")]
    HnswIndexInitError,
    #[error("Error persisting hnsw index")]
    HnswIndexPersistError,
    #[error("Error applying log chunk")]
    EmbeddingNotFound,
    #[error(
        "Embedding dimensionality {actual} does not match collection dimensionality {expected}"
    )]
    DimensionalityMismatch { expected: usize, actual: usize },
    #[error("Error applying log chunk")]
    HnwsIndexAddError,
    #[error("Error applying log chunk")]
    HnswIndexResizeError,
    #[error("HNSW label space exhausted")]
    LabelExhausted,
    #[error("Error applying log chunk")]
    HnswIndexDeleteError,
    #[error("Error converting persistant path to string")]
    PersistPathError,
    #[error("Error updating max sequence id")]
    QueryBuilderError(#[from] sea_query::error::Error),
    #[error("Error updating max sequence id")]
    MaxSeqIdUpdateError(#[from] sqlx::error::Error),
}

impl ChromaError for LocalHnswSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentWriterError::HnswConfigError(e) => e.code(),
            LocalHnswSegmentWriterError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::MissingHnswConfiguration => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::InvalidHnswConfiguration(err) => err.code(),
            LocalHnswSegmentWriterError::Deleted => ErrorCodes::NotFound,
            LocalHnswSegmentWriterError::HnswIndexInitError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexPersistError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::EmbeddingNotFound => ErrorCodes::InvalidArgument,
            LocalHnswSegmentWriterError::DimensionalityMismatch { .. } => {
                ErrorCodes::InvalidArgument
            }
            LocalHnswSegmentWriterError::HnwsIndexAddError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexResizeError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::LabelExhausted => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexDeleteError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::PersistPathError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::QueryBuilderError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::MaxSeqIdUpdateError(_) => ErrorCodes::Internal,
        }
    }
}

fn validate_embedding_dim(
    embedding: &[f32],
    expected: usize,
) -> Result<(), LocalHnswSegmentWriterError> {
    if embedding.len() != expected {
        return Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
            expected,
            actual: embedding.len(),
        });
    }
    Ok(())
}

impl LocalHnswSegmentWriter {
    pub fn from_index(hnsw_index: LocalHnswIndex) -> Result<Self, LocalHnswSegmentWriterError> {
        Ok(Self { index: hnsw_index })
    }

    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        persist_root: Option<String>,
        sql_db: SqliteDb,
    ) -> Result<Self, LocalHnswSegmentWriterError> {
        let hnsw_configuration = collection
            .schema
            .as_ref()
            .map(|schema| schema.get_internal_hnsw_config_with_legacy_fallback(segment))
            .transpose()?
            .flatten()
            .ok_or(LocalHnswSegmentWriterError::MissingHnswConfiguration)?;

        match persist_root {
            Some(path_str) => {
                let _files = acquire_hnsw_files().await;
                let path = Path::new(&path_str);
                let index_folder = path.join(segment.id.to_string());
                // A save cut by a stop leaves its journal: its bytes go back to the files before anything reads them, unless
                // the files were saved after it (apply_journals_newer_than_data).
                if index_folder.exists() {
                    apply_journals_newer_than_data(&index_folder, segment, &sql_db)
                        .await
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                }
                if !index_folder.join(METADATA_FILE).is_file()
                    && get_current_seq_id(segment, &sql_db).await? > 0
                {
                    return Err(LocalHnswSegmentWriterError::HnswIndexLoadError);
                }
                if !index_folder.exists() {
                    tokio::fs::create_dir_all(&index_folder).await?;
                }
                let index_folder_str = match index_folder.to_str() {
                    Some(path) => path,
                    None => return Err(LocalHnswSegmentWriterError::PersistPathError),
                };
                let pickle_file_path = path.join(segment.id.to_string()).join(METADATA_FILE);
                if pickle_file_path.exists() {
                    let file = tokio::fs::File::open(pickle_file_path)
                        .await?
                        .into_std()
                        .await;
                    let mut id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
                    if let Some(actual) = id_map.dimensionality {
                        if actual != dimensionality {
                            return Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
                                expected: dimensionality,
                                actual,
                            });
                        }
                    }
                    let actual = persisted_hnsw_dim(&index_folder)
                        .await
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                    if actual != dimensionality {
                        return Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
                            expected: dimensionality,
                            actual,
                        });
                    }
                    id_map.dimensionality = Some(dimensionality);
                    let loaded_before = CheckpointFingerprint::read(&index_folder).ok();
                    let inspection = persistence::validate_files(&index_folder, Some(&id_map))
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                    // Load hnsw index.
                    let index_config = IndexConfig::new(
                        dimensionality as i32,
                        hnsw_configuration.space.clone().into(),
                    );
                    let index = HnswIndex::load(
                        index_folder_str,
                        &index_config,
                        hnsw_configuration.ef_search,
                        chroma_index::IndexUuid(segment.id.0),
                    )
                    .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                    index.close_fd();
                    let verified_checkpoint =
                        loaded_verified(loaded_before, &index_folder, &inspection);

                    reconcile_checkpoint(&index, &mut id_map, &inspection)
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;

                    let current_seq_id =
                        restore_checkpoint_seq_id(segment, &sql_db, &id_map).await?;

                    // TODO(Sanket): Set allow reset appropriately.
                    return Ok(Self {
                        index: LocalHnswIndex {
                            inner: Arc::new(IndexShared::new(Inner {
                                index,
                                id_map,
                                index_init: true,
                                deleted: false,
                                replay_complete: false,
                                recovery_checkpoint_pending: inspection.recovery_required,
                                verified_checkpoint,
                                failed: false,
                                deleted_on_load: inspection
                                    .mapped_deleted_labels
                                    .iter()
                                    .copied()
                                    .collect(),
                                #[cfg(test)]
                                mutation_budget: None,
                                #[cfg(test)]
                                fail_resize: false,
                                allow_reset: false,
                                num_elements_since_last_persist: 0,
                                last_persist: None,
                                save_interval: SAVE_INTERVAL,
                                last_seen_seq_id: current_seq_id,
                                sync_threshold: hnsw_configuration.sync_threshold,
                                persist_path: Some(index_folder_str.to_string()),
                                sqlite: sql_db,
                            })),
                        },
                    });
                }
                // With no committed watermark or pickle, replay starts at zero, and the index is written again from it.
                // The files there are not read: no save was published, and those that the creation of the index wrote
                // with no sync come back empty or cut after a power cut, as in Chroma 1.5.9.
                // Initialize index.
                let index_config = IndexConfig::new(
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                );
                let hnsw_config = HnswIndexConfig::new_persistent(
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
                    &index_folder,
                )?;

                // TODO(Sanket): HnswIndex init is not thread safe. We should not call it from multiple threads
                let index = HnswIndex::init(
                    &index_config,
                    Some(&hnsw_config),
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexInitError)?;
                index.close_fd();
                // Return uninitialized reader.
                Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(IndexShared::new(Inner {
                            index,
                            id_map: IdMap::new(dimensionality),
                            index_init: true,
                            deleted: false,
                            replay_complete: false,
                            recovery_checkpoint_pending: false,
                            verified_checkpoint: None,
                            failed: false,
                            deleted_on_load: HashSet::new(),
                            #[cfg(test)]
                            mutation_budget: None,
                            #[cfg(test)]
                            fail_resize: false,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            last_persist: None,
                            save_interval: SAVE_INTERVAL,
                            last_seen_seq_id: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: Some(index_folder_str.to_string()),
                            sqlite: sql_db,
                        })),
                    },
                })
            }
            None => {
                let index_config = IndexConfig::new(
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                );
                let hnsw_config = HnswIndexConfig::new_ephemeral(
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
                );

                // TODO(Sanket): HnswIndex init is not thread safe. We should not call it from multiple threads
                let index = HnswIndex::init(
                    &index_config,
                    Some(&hnsw_config),
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexInitError)?;
                Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(IndexShared::new(Inner {
                            index,
                            id_map: IdMap::new(dimensionality),
                            index_init: true,
                            deleted: false,
                            replay_complete: false,
                            recovery_checkpoint_pending: false,
                            verified_checkpoint: None,
                            failed: false,
                            deleted_on_load: HashSet::new(),
                            #[cfg(test)]
                            mutation_budget: None,
                            #[cfg(test)]
                            fail_resize: false,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            last_persist: None,
                            save_interval: SAVE_INTERVAL,
                            last_seen_seq_id: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: None,
                            sqlite: sql_db,
                        })),
                    },
                })
            }
        }
    }

    // Returns the updated log seq id.
    #[allow(dead_code)]
    pub async fn apply_log_chunk(
        &mut self,
        log_chunk: Chunk<LogRecord>,
    ) -> Result<u32, LocalHnswSegmentWriterError> {
        let mut guard = self.index.inner.write().await;
        if guard.deleted {
            return Err(LocalHnswSegmentWriterError::Deleted);
        }
        if guard.failed {
            return Err(LocalHnswSegmentWriterError::HnswIndexLoadError);
        }
        let mut next_label = guard
            .id_map
            .total_elements_added
            .checked_add(1)
            .ok_or(LocalHnswSegmentWriterError::LabelExhausted)?;

        if log_chunk.is_empty() && !guard.needs_checkpoint() {
            return Ok(next_label);
        }
        // Validate the entire batch before changing either the ID map or HNSW.
        // Track only IDs touched by this batch, including add/delete transitions.
        let expected_dim = guard.index.dimensionality() as usize;
        let mut present = HashMap::new();
        for (log, _) in log_chunk.iter() {
            if log.log_offset <= guard.last_seen_seq_id as i64 {
                continue;
            }
            let exists = present
                .entry(log.record.id.as_str())
                .or_insert_with(|| guard.id_map.id_to_label.contains_key(&log.record.id));
            match log.record.operation {
                Operation::Add if !*exists => {
                    let embedding = log
                        .record
                        .embedding
                        .as_ref()
                        .ok_or(LocalHnswSegmentWriterError::EmbeddingNotFound)?;
                    validate_embedding_dim(embedding, expected_dim)?;
                    *exists = true;
                }
                Operation::Upsert => {
                    let embedding = log
                        .record
                        .embedding
                        .as_ref()
                        .ok_or(LocalHnswSegmentWriterError::EmbeddingNotFound)?;
                    validate_embedding_dim(embedding, expected_dim)?;
                    *exists = true;
                }
                Operation::Update if *exists => {
                    if let Some(embedding) = &log.record.embedding {
                        validate_embedding_dim(embedding, expected_dim)?;
                    }
                }
                Operation::Delete => *exists = false,
                _ => {}
            }
        }
        let mut max_seq_id = guard.last_seen_seq_id;
        // In order to insert into hnsw index in parallel, we need to collect all the embeddings
        enum Mutation<'a> {
            Set(&'a [f32]),
            Delete,
        }
        let mut hnsw_batch: HashMap<u32, Vec<Mutation<'_>>> =
            HashMap::with_capacity(log_chunk.len());
        let mut staged: HashMap<String, Option<u32>> = HashMap::new();
        let mut applied = 0;
        let mut new_labels = 0usize;
        for (log, _) in log_chunk.iter() {
            if log.log_offset <= guard.last_seen_seq_id as i64 {
                continue;
            }
            applied += 1;
            max_seq_id = max_seq_id.max(log.log_offset as u64);
            let id = &log.record.id;
            let current = staged
                .entry(id.clone())
                .or_insert_with(|| guard.id_map.id_to_label.get(id).copied());
            let set_vector = match log.record.operation {
                Operation::Add => current.is_none(),
                Operation::Upsert => true,
                Operation::Update => current.is_some() && log.record.embedding.is_some(),
                Operation::Delete | Operation::BackfillFn => false,
            };
            if matches!(log.record.operation, Operation::Delete) {
                if let Some(label) = current.take() {
                    hnsw_batch.entry(label).or_default().push(Mutation::Delete);
                }
            } else if set_vector {
                let label = match *current {
                    Some(label) => label,
                    None => {
                        let label = next_label;
                        next_label = next_label
                            .checked_add(1)
                            .ok_or(LocalHnswSegmentWriterError::LabelExhausted)?;
                        new_labels += 1;
                        *current = Some(label);
                        label
                    }
                };
                hnsw_batch.entry(label).or_default().push(Mutation::Set(
                    log.record
                        .embedding
                        .as_ref()
                        .ok_or(LocalHnswSegmentWriterError::EmbeddingNotFound)?,
                ));
            }
        }

        // Add to hnsw index in parallel using rayon.
        // Resize the index if needed
        let index_len = guard.index.len_with_deleted();
        let index_capacity = guard.index.capacity();
        let needed = index_len
            .checked_add(new_labels)
            .ok_or(LocalHnswSegmentWriterError::HnswIndexResizeError)?;
        if needed > index_capacity {
            let needed_capacity = needed
                .checked_next_power_of_two()
                .ok_or(LocalHnswSegmentWriterError::HnswIndexResizeError)?;
            #[cfg(test)]
            if guard.fail_resize {
                return Err(LocalHnswSegmentWriterError::HnswIndexResizeError);
            }
            if guard.index.resize(needed_capacity).is_err() {
                // A native allocation failure may occur after some buffers changed.
                guard.failed = true;
                self.index.inner.failed.store(true, Ordering::Release);
                return Err(LocalHnswSegmentWriterError::HnswIndexResizeError);
            }
        }
        let index_for_pool = &guard.index;

        let touched_labels = hnsw_batch.keys().copied().collect::<Vec<_>>();
        let result = hnsw_batch
            .into_par_iter()
            .map(
                |(label, mutations)| -> Result<(), LocalHnswSegmentWriterError> {
                    let mut already_deleted = guard.deleted_on_load.contains(&label);
                    for mutation in mutations {
                        #[cfg(test)]
                        if let Some(budget) = &guard.mutation_budget {
                            // Keep compatibility with Rust 1.92, which does not provide try_update.
                            #[allow(deprecated)]
                            if budget
                                .fetch_update(
                                    std::sync::atomic::Ordering::SeqCst,
                                    std::sync::atomic::Ordering::SeqCst,
                                    |n| n.checked_sub(1),
                                )
                                .is_err()
                            {
                                return Err(LocalHnswSegmentWriterError::HnwsIndexAddError);
                            }
                        }
                        match mutation {
                            Mutation::Set(embedding) => {
                                index_for_pool
                                    .add(label as usize, embedding)
                                    .map_err(|_| LocalHnswSegmentWriterError::HnwsIndexAddError)?;
                                already_deleted = false;
                            }
                            Mutation::Delete => {
                                if !already_deleted {
                                    index_for_pool.delete(label as usize).map_err(|_| {
                                        LocalHnswSegmentWriterError::HnswIndexDeleteError
                                    })?;
                                    already_deleted = true;
                                }
                            }
                        }
                    }
                    Ok(())
                },
            )
            .find_any(|result| result.is_err())
            .unwrap_or(Ok(()));
        if let Err(err) = result {
            guard.failed = true;
            self.index.inner.failed.store(true, Ordering::Release);
            return Err(err);
        }
        for label in touched_labels {
            guard.deleted_on_load.remove(&label);
        }
        for (id, label) in staged {
            if let Some(old) = guard.id_map.id_to_label.remove(&id) {
                guard.id_map.label_to_id.remove(&old);
            }
            if let Some(label) = label {
                guard.id_map.id_to_label.insert(id.clone(), label);
                guard.id_map.label_to_id.insert(label, id);
            }
        }
        guard.num_elements_since_last_persist += applied;
        // Native mutations succeeded. A later checkpoint error must not replay
        // already applied records into this live instance.
        guard.last_seen_seq_id = max_seq_id;
        // The index holds the chunk in memory: a read that waits for these records may go on.
        self.index.inner.applied_seq_id.store(max_seq_id, Ordering::Release);
        guard.id_map.total_elements_added = next_label - 1;
        if guard.needs_checkpoint() {
            guard = persist(guard).await?;
            let id = guard.index.id.to_string().into();
            let max_id = max_seq_id.into();
            // Persist max_seq_id to sqlite.
            let (query, values) = Query::insert()
                .into_table(MaxSeqId::Table)
                .replace()
                .columns([MaxSeqId::SegmentId, MaxSeqId::SeqId])
                .values([id, max_id])?
                .build_sqlx(SqliteQueryBuilder);
            let _ = sqlx::query_with(&query, values)
                .execute(guard.sqlite.get_conn())
                .await?;
            guard.num_elements_since_last_persist = 0;
            guard.recovery_checkpoint_pending = false;
            guard.last_persist = Some(tokio::time::Instant::now());
        }

        guard.last_seen_seq_id = max_seq_id;

        Ok(next_label)
    }
}

// The load of an index inspects its files as fully as a purge does (validate_files, Strict); when the files
// did not change between that inspection and the end of the load, the purge takes it as verified, instead of inspecting
// again under the lock of the index at its first purge.
fn loaded_verified(
    before: Option<CheckpointFingerprint>,
    path: &Path,
    inspection: &PersistedHnswIndex,
) -> Option<VerifiedCheckpoint> {
    let before = before?;
    #[cfg(feature = "test-hooks")]
    persistence::touch_after_inspection(path).ok()?;
    if inspection.recovery_required || CheckpointFingerprint::read(path).ok()? != before {
        return None;
    }
    Some(VerifiedCheckpoint {
        fingerprint: before,
        seq_id: inspection.checkpoint_seq_id,
    })
}

fn reconcile_checkpoint(
    index: &HnswIndex,
    id_map: &mut IdMap,
    inspection: &PersistedHnswIndex,
) -> Result<(), Box<dyn ChromaError>> {
    for &label in &inspection.unmapped_active_labels {
        index.delete(label as usize)?;
    }
    id_map.total_elements_added = id_map.total_elements_added.max(inspection.max_label);
    Ok(())
}

async fn persist(
    mut guard: tokio::sync::RwLockWriteGuard<'_, Inner>,
) -> Result<tokio::sync::RwLockWriteGuard<'_, Inner>, LocalHnswSegmentWriterError> {
    if guard.failed {
        return Err(LocalHnswSegmentWriterError::HnswIndexLoadError);
    }
    if let Some(path) = guard.persist_path.clone() {
        guard.id_map.checkpoint_seq_id = Some(guard.last_seen_seq_id);
        // Retain validation only across our own successful writes. Unexpected
        // file changes and any failed save require another complete inspection.
        let verified = guard.verified_checkpoint.take().is_some_and(|previous| {
            CheckpointFingerprint::read(Path::new(&path))
                .is_ok_and(|current| current == previous.fingerprint)
        });
        let path = path.as_str();
        // The ID map goes in the journal of the save of the native files, as the compaction of distributed Chroma
        // publishes its files whole or not at all: a kill between the files and the map left the files of one save
        // with the map of the save before, ids on labels the files had deleted and labels no id had.
        let mut id_map = Vec::new();
        serde_pickle::to_writer(&mut id_map, &guard.id_map, SerOptions::new())?;
        let _permit = acquire_hnsw_files().await;
        {
            // Queries and mutations use memory. Only checkpointing needs open
            // streams, serialized with eviction by the index write lock.
            let files = HnswFiles(&guard.index);
            files
                .0
                .open_fd()
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexPersistError)?;
            files
                .0
                .save_with_journal_file(METADATA_FILE, &id_map)
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexPersistError)?;
        }
        // No sync of the folder: a save syncs nothing, as in Chroma 1.5.9 (the fork of hnswlib, persistDirtyWithJournal).
        if verified {
            guard.verified_checkpoint = Some(VerifiedCheckpoint {
                fingerprint: CheckpointFingerprint::read(Path::new(path))?,
                seq_id: guard.id_map.checkpoint_seq_id,
            });
        }
    }
    Ok(guard)
}

#[cfg(test)]
mod tests {
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        Chunk, Collection, KnnIndex, LogRecord, Operation, OperationRecord, Schema, Segment,
        SegmentScope, SegmentType, SegmentUuid,
    };
    use serde_pickle::DeOptions;

    // A data path of the given length under root, in directories of at most 100 characters.
    #[cfg(windows)]
    fn data_path_of(root: &std::path::Path, length: usize) -> String {
        let root = root.to_str().expect("utf-8 path");
        let needed = length - root.len();
        let count = needed.div_ceil(101);
        let chars = needed - count;
        let mut path = root.to_string();
        for i in 0..count {
            path.push('\\');
            path.push_str(&"d".repeat(chars / count + usize::from(i < chars % count)));
        }
        assert_eq!(path.len(), length);
        path
    }

    // Persists one record under a data path of the given length, then reads it back from the files.
    #[cfg(windows)]
    async fn persist_and_read_back(length: usize) -> Result<Vec<f32>, String> {
        let sqlite = get_new_sqlite_db().await;
        let root = tempfile::tempdir().expect("persist dir");
        let persist_path = data_path_of(root.path(), length);
        std::fs::create_dir_all(&persist_path).expect("data path");
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        let mut writer = LocalHnswSegmentWriter::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path.clone()),
            sqlite.clone(),
        )
        .await
        .map_err(|err| err.to_string())?;
        writer
            .apply_log_chunk(Chunk::new(
                vec![LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "id-1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                }]
                .into(),
            ))
            .await
            .map_err(|err| err.to_string())?;
        {
            let guard = writer.index.inner.write().await;
            drop(persist(guard).await.map_err(|err| err.to_string())?);
        }
        writer.index.close().await;
        drop(writer);
        let reader = LocalHnswSegmentReader::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path),
            sqlite,
        )
        .await
        .map_err(|err| err.to_string())?;
        reader
            .get_embedding_by_user_id(&"id-1".to_string())
            .await
            .map_err(|err| err.to_string())
    }

    // As Chroma 1.5.9: on Windows hnswlib opens <segment id>\data_level0.bin without long paths, so a data path
    // reaches 206 characters, and the ID map, 6 characters longer, must reach them too.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_data_path_of_206_characters_keeps_the_index_on_windows() {
        assert_eq!(persist_and_read_back(206).await, Ok(vec![1.0, 2.0, 3.0]));
    }

    // Past 206 characters hnswlib writes nothing; Chroma 1.5.9 then lost the index in silence, here the write fails.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_longer_data_path_fails_the_write_on_windows() {
        assert!(persist_and_read_back(207).await.is_err());
    }

    // hnswlib throws for a label it does not have while it holds the locks of the label, as when the SPANN index asks
    // for a center that a split deleted (is_head_deleted). C++ compiled for MSVC without /EHsc does not release them on
    // the exception, and the next operation of the thread on the index fails with "resource deadlock would occur".
    #[cfg(windows)]
    #[test]
    fn a_get_of_a_deleted_label_leaves_the_index_usable_on_windows() {
        let index = HnswIndex::init(
            &IndexConfig::new(3, DistanceFunction::Euclidean),
            Some(&HnswIndexConfig::new_ephemeral(16, 100, 100)),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .expect("hnsw init");
        index.add(1, &[1.0, 2.0, 3.0]).expect("hnsw add");
        index.add(2, &[4.0, 5.0, 6.0]).expect("hnsw add");
        index.delete(1).expect("hnsw delete");
        assert!(index.get(1).is_err());
        assert_eq!(index.get_all_ids().expect("hnsw ids").0, vec![2]);
    }

    #[tokio::test]
    async fn reader_migrates_legacy_max_seq_id() {
        let sqlite = get_new_sqlite_db().await;
        let persist_dir = tempfile::tempdir().expect("persist dir");
        let persist_path = persist_dir.path().to_str().expect("utf-8 path").to_string();

        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));

        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };

        let mut writer = LocalHnswSegmentWriter::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path.clone()),
            sqlite.clone(),
        )
        .await
        .expect("writer");

        writer
            .apply_log_chunk(Chunk::new(
                vec![LogRecord {
                    log_offset: 42,
                    record: OperationRecord {
                        id: "id-1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                }]
                .into(),
            ))
            .await
            .expect("apply log");

        {
            let mut guard = writer.index.inner.write().await;
            guard.id_map.max_seq_id = Some(42);
            drop(persist(guard).await.expect("persist"));
        }
        writer.index.close().await;
        drop(writer);

        let metadata_path = persist_dir
            .path()
            .join(vector_segment.id.to_string())
            .join(METADATA_FILE);
        let file = tokio::fs::File::open(metadata_path)
            .await
            .expect("metadata file")
            .into_std()
            .await;
        let id_map: IdMap =
            serde_pickle::from_reader(file, DeOptions::new()).expect("legacy id map");
        assert_eq!(id_map.max_seq_id, Some(42));
        assert_eq!(
            get_current_seq_id(&vector_segment, &sqlite).await.unwrap(),
            0
        );

        // Legacy pickles may omit dimensionality; the binary header must still
        // prevent loading vectors into a differently sized native index.
        let mut legacy_map = id_map;
        legacy_map.dimensionality = None;
        legacy_map.checkpoint_seq_id = None;
        let metadata_path = persist_dir
            .path()
            .join(vector_segment.id.to_string())
            .join(METADATA_FILE);
        let mut file = std::fs::File::create(metadata_path).unwrap();
        serde_pickle::to_writer(&mut file, &legacy_map, SerOptions::new()).unwrap();
        drop(file);
        assert!(matches!(
            LocalHnswSegmentReader::from_segment(
                &collection,
                &vector_segment,
                2,
                Some(persist_path.clone()),
                sqlite.clone(),
            )
            .await,
            Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                expected: 2,
                actual: 3
            })
        ));
        assert!(matches!(
            LocalHnswSegmentWriter::from_segment(
                &collection,
                &vector_segment,
                4,
                Some(persist_path.clone()),
                sqlite.clone(),
            )
            .await,
            Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
                expected: 4,
                actual: 3
            })
        ));

        let reader = LocalHnswSegmentReader::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path),
            sqlite,
        )
        .await
        .expect("reader");

        assert_eq!(
            reader.current_max_seq_id(&vector_segment.id).await.unwrap(),
            42
        );
        assert_eq!(reader.index.inner.read().await.last_seen_seq_id, 42);
        assert!(reader
            .query_embedding(&[], vec![1.0, 2.0], 1)
            .await
            .is_err());
        assert!(reader.query_embedding(&[], vec![1.0; 4], 1).await.is_err());
    }
    use super::*;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_distance::DistanceFunction;
    use chroma_index::IndexUuid;
    use chroma_sqlite::config::SqliteDBConfig;

    fn add_record(id: &str, embedding: Vec<f32>) -> OperationRecord {
        OperationRecord {
            id: id.to_string(),
            embedding: Some(embedding),
            encoding: None,
            metadata: None,
            document: None,
            operation: Operation::Add,
        }
    }

    fn push_i32(buf: &mut Vec<u8>, value: i32) {
        buf.extend_from_slice(&value.to_ne_bytes());
    }

    fn push_usize(buf: &mut Vec<u8>, value: usize) {
        buf.extend_from_slice(&value.to_ne_bytes());
    }

    fn header_for_dim(dim: usize) -> Vec<u8> {
        let offset_data = 68;
        let label_offset = offset_data + dim * size_of::<f32>();
        let size_data_per_element = label_offset + size_of::<usize>();
        let mut header = Vec::new();
        push_i32(&mut header, HNSW_PERSISTENCE_VERSION);
        push_usize(&mut header, 0);
        push_usize(&mut header, 100);
        push_usize(&mut header, 1);
        push_usize(&mut header, size_data_per_element);
        push_usize(&mut header, label_offset);
        push_usize(&mut header, offset_data);
        header
    }

    #[test]
    fn persisted_hnsw_header_reports_dimensionality() {
        assert_eq!(parse_persisted_hnsw_dim(&header_for_dim(8)), Some(8));
        assert_eq!(parse_persisted_hnsw_dim(&header_for_dim(768)), Some(768));
    }

    #[test]
    fn persisted_hnsw_header_matches_hnswlib_layout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let index_config = IndexConfig::new(8, DistanceFunction::Euclidean);
        let hnsw_config =
            HnswIndexConfig::new_persistent(16, 100, 100, dir.path()).expect("hnsw config");
        let index = HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .expect("hnsw init");
        index.add(0, &[0.0; 8]).expect("hnsw add");
        index.save().expect("hnsw save");
        index.close_fd();

        let header = std::fs::read(dir.path().join(HNSW_HEADER_FILE)).expect("header");
        assert_eq!(parse_persisted_hnsw_dim(&header), Some(8));
    }

    #[test]
    fn persisted_hnsw_header_rejects_invalid_layout() {
        let mut header = header_for_dim(8);
        header[0..size_of::<i32>()].copy_from_slice(&2i32.to_ne_bytes());
        assert_eq!(parse_persisted_hnsw_dim(&header), None);

        let mut header = header_for_dim(8);
        let label_offset_offset = size_of::<i32>() + 4 * size_of::<usize>();
        header[label_offset_offset..label_offset_offset + size_of::<usize>()]
            .copy_from_slice(&69usize.to_ne_bytes());
        assert_eq!(parse_persisted_hnsw_dim(&header), None);
    }

    #[test]
    fn persisted_hnsw_metadata_summarizes_legacy_watermark_and_mismatches() {
        let mut id_map = IdMap::new(3);
        id_map.max_seq_id = Some(42);
        id_map.total_elements_added = 1;
        id_map.id_to_label.insert("a".to_string(), 7);
        id_map.label_to_id.insert(7, "b".to_string());

        let metadata = PersistedHnswMetadata::from(id_map);

        assert_eq!(metadata.dimensionality, Some(3));
        assert_eq!(metadata.legacy_max_seq_id, Some(42));
        assert_eq!(metadata.id_to_label_count, 1);
        assert_eq!(metadata.label_to_id_count, 1);
        assert_eq!(
            metadata.first_label_mismatch,
            Some(PersistedHnswLabelMismatch::LabelMapsToDifferentId {
                id: "a".to_string(),
                label: 7,
                reverse_id: "b".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn apply_log_chunk_rejects_bad_dim_without_id_map_side_effects() {
        let sqlite = SqliteDb::try_from_config(&SqliteDBConfig::default(), &Registry::new())
            .await
            .expect("sqlite");
        let index_config = IndexConfig::new(2, DistanceFunction::Euclidean);
        let hnsw_config = HnswIndexConfig::new_ephemeral(16, 100, 100);
        let index = HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .expect("hnsw init");
        let mut writer = LocalHnswSegmentWriter {
            index: LocalHnswIndex {
                inner: Arc::new(IndexShared::new(Inner {
                    index,
                    id_map: IdMap::new(2),
                    index_init: true,
                    deleted: false,
                    replay_complete: false,
                    recovery_checkpoint_pending: false,
                    verified_checkpoint: None,
                    failed: false,
                    deleted_on_load: HashSet::new(),
                    #[cfg(test)]
                    mutation_budget: None,
                    #[cfg(test)]
                    fail_resize: false,
                    allow_reset: false,
                    num_elements_since_last_persist: 0,
                    last_persist: None,
                    save_interval: SAVE_INTERVAL,
                    last_seen_seq_id: 0,
                    sync_threshold: 1000,
                    persist_path: None,
                    sqlite,
                })),
            },
        };
        let chunk = Chunk::new(
            vec![
                LogRecord {
                    log_offset: 1,
                    record: add_record("valid", vec![1.0, 2.0]),
                },
                LogRecord {
                    log_offset: 2,
                    record: add_record("invalid", vec![1.0, 2.0, 3.0]),
                },
            ]
            .into(),
        );

        let err = writer
            .apply_log_chunk(chunk)
            .await
            .expect_err("bad dimension should fail");
        assert!(matches!(
            err,
            LocalHnswSegmentWriterError::DimensionalityMismatch {
                expected: 2,
                actual: 3,
            }
        ));
        let guard = writer.index.inner.read().await;
        assert!(guard.id_map.id_to_label.is_empty());
        assert!(guard.id_map.label_to_id.is_empty());
        assert_eq!(guard.id_map.total_elements_added, 0);
        assert_eq!(guard.num_elements_since_last_persist, 0);
        assert_eq!(guard.last_seen_seq_id, 0);
    }

    // An index loaded from its files counts as verified for the purges, as its load inspected the files as fully as a
    // purge does: the first purge after a start does not inspect them again under the lock of the index, whether a writer
    // or a reader loaded it. Files that change after the load are inspected again. A checkpoint that needs a replay of the
    // log is not verified by its load: the purge inspects it, and refuses.
    #[tokio::test]
    async fn a_loaded_checkpoint_is_verified_for_the_first_purge() {
        let sqlite = get_new_sqlite_db().await;
        let persist_dir = tempfile::tempdir().expect("persist dir");
        let persist_path = persist_dir.path().to_str().expect("utf-8 path").to_string();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        let writer = || {
            super::LocalHnswSegmentWriter::from_segment(
                &collection,
                &vector_segment,
                3,
                Some(persist_path.clone()),
                sqlite.clone(),
            )
        };
        let reader = || {
            LocalHnswSegmentReader::from_segment(
                &collection,
                &vector_segment,
                3,
                Some(persist_path.clone()),
                sqlite.clone(),
            )
        };
        let chunk = |log_offset: i64, id: &str| {
            Chunk::new(
                vec![LogRecord {
                    log_offset,
                    record: OperationRecord {
                        id: id.to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                }]
                .into(),
            )
        };
        let mut first = writer().await.expect("writer");
        first
            .apply_log_chunk(chunk(1, "id-1"))
            .await
            .expect("apply log");
        drop(
            super::persist(first.index.inner.write().await)
                .await
                .expect("persist"),
        );
        first.index.close().await;
        drop(first);
        let path = std::path::Path::new(&persist_path).join(vector_segment.id.to_string());
        let loaded = writer().await.expect("load");
        assert!(!loaded
            .index
            .validate_persisted_checkpoint(&path, 1)
            .await
            .expect("checkpoint"));
        std::fs::File::options()
            .append(true)
            .open(path.join(super::METADATA_FILE))
            .expect("metadata file")
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(10))
            .expect("touch");
        assert!(loaded
            .index
            .validate_persisted_checkpoint(&path, 1)
            .await
            .expect("checkpoint"));
        loaded.index.close().await;
        drop(loaded);
        let loaded = reader().await.expect("load");
        assert!(!loaded
            .index
            .validate_persisted_checkpoint(&path, 1)
            .await
            .expect("checkpoint"));
        loaded.index.close().await;
        drop(loaded);

        // The native files of a save with the ID map of the save before, as a kill between the two would leave them.
        let pickle = path.join(super::METADATA_FILE);
        let old_map = std::fs::read(&pickle).expect("metadata file");
        let mut second = writer().await.expect("load");
        second
            .apply_log_chunk(chunk(2, "id-2"))
            .await
            .expect("apply log");
        drop(
            super::persist(second.index.inner.write().await)
                .await
                .expect("persist"),
        );
        second.index.close().await;
        drop(second);
        std::fs::write(&pickle, old_map).expect("metadata file");
        assert!(
            super::inspect_persisted_hnsw_index(&path)
                .expect("inspection")
                .recovery_required
        );
        let loaded = writer().await.expect("load");
        assert!(loaded
            .index
            .validate_persisted_checkpoint(&path, 1)
            .await
            .is_err());
    }

    // A save every sync_threshold records kept the index waiting for the disk under a steady load: after its first save,
    // an instance saves again only once SAVE_INTERVAL has passed since it, however many records come in between, which
    // stay in memory. The position of the segment in SQLite, which a save moves, tells the saves apart.
    #[tokio::test]
    async fn saves_after_the_first_wait_for_the_save_interval() {
        let sqlite = get_new_sqlite_db().await;
        let persist_dir = tempfile::tempdir().expect("persist dir");
        let persist_path = persist_dir.path().to_str().expect("utf-8 path").to_string();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };
        let mut writer = super::LocalHnswSegmentWriter::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path.clone()),
            sqlite.clone(),
        )
        .await
        .expect("writer");
        writer.index.set_sync_threshold(2).await;
        // The records first and first + 1, as two records of the log.
        let chunk = |first: i64| {
            Chunk::new(
                (first..first + 2)
                    .map(|log_offset| LogRecord {
                        log_offset,
                        record: OperationRecord {
                            id: format!("id-{log_offset}"),
                            embedding: Some(vec![log_offset as f32, 2.0, 3.0]),
                            encoding: None,
                            metadata: None,
                            document: None,
                            operation: Operation::Add,
                        },
                    })
                    .collect::<Vec<_>>()
                    .into(),
            )
        };
        let saved = || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT CAST(seq_id AS INTEGER) FROM max_seq_id WHERE segment_id = ?",
            )
            .bind(vector_segment.id.to_string())
            .fetch_optional(sqlite.get_conn())
            .await
            .expect("max_seq_id")
        };

        writer.apply_log_chunk(chunk(1)).await.expect("apply log");
        assert_eq!(saved().await, Some(2), "the first save, at the threshold");
        for first in [3, 5, 7] {
            writer
                .apply_log_chunk(chunk(first))
                .await
                .expect("apply log");
            assert_eq!(
                saved().await,
                Some(2),
                "no save within the interval, at the records {first} and {}",
                first + 1
            );
        }
        assert_eq!(writer.index.applied_state().await, (8, 8));

        writer.index.set_save_interval(super::Duration::ZERO).await;
        writer.apply_log_chunk(chunk(9)).await.expect("apply log");
        assert_eq!(
            saved().await,
            Some(10),
            "a save once the interval has passed"
        );
        writer.index.close().await;
    }
}
