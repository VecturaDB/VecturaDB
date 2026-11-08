// Changed from the original Chroma file; see the git history for the changes and their authors.
use crate::{
    config::SqliteLogConfig, BackfillMessage, CollectionInfo, CompactionManagerError,
    LocalCompactionManager, PurgeLogsMessage,
};
use async_trait::async_trait;
use chroma_api_types::StaleReadError;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes, WrappedSqlxError};
use chroma_sqlite::{db::SqliteDb, helpers::get_embeddings_queue_topic_name};
use chroma_system::{ChannelError, ComponentHandle, RequestError};
use chroma_types::chroma_proto::PushLogsCondition;
use chroma_types::{
    CollectionUuid, LogRecord, Operation, OperationRecord, ResetError, ResetResponse,
    ScalarEncoding, ScalarEncodingConversionError, SegmentType, UpdateMetadata,
    UpdateMetadataValue,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::{QueryBuilder, Row, SqliteConnection};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqlitePullLogsError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Failed to parse embedding encoding")]
    InvalidEncoding(#[from] ScalarEncodingConversionError),
    #[error("Failed to parse embedding: {0}")]
    InvalidEmbedding(bytemuck::PodCastError),
    #[error("Failed to parse metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error("Method {0} is not implemented")]
    NotImplemented(String),
}

impl ChromaError for SqlitePullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePullLogsError::QueryError(err) => err.code(),
            SqlitePullLogsError::InvalidEncoding(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::InvalidEmbedding(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::InvalidMetadata(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::NotImplemented(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqlitePushLogsError {
    #[error("Error in compaction: {0}")]
    CompactionError(#[from] CompactionManagerError),
    #[error("Error setting compactor handle")]
    CompactorHandleSetError,
    #[error("Error setting max batch size")]
    MaxBatchSizeSetError,
    #[error("Failed to serialize metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error("Error sending message to compactor")]
    MessageSendingError(#[from] RequestError),
    #[error("Error sending purge log msg to compactor")]
    PurgeLogSendingFailure(#[from] ChannelError),
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Error getting max batch size: {0}")]
    GetMaxBatchSizeError(#[from] SqliteGetMaxBatchSizeError),
    #[error("Unimplemented: {0}")]
    Unimplemented(String),
    #[error("Sparse metadata must have valid indices and finite weights")]
    InvalidSparseMetadata,
    // The answer of the log service of distributed Chroma to a write to a collection whose log is too full; the log
    // turns it into PushLogsError::BackoffCompaction, which the log service gets from it, with the seconds after which
    // the flushes should have taken the log under the limit, which distributed Chroma does not give.
    #[error("log needs compaction; too full")]
    BackoffCompaction { retry_after: u64 },
    // A conditional write that the log refuses, with the code and the message of the log service of distributed
    // Chroma (rust/log-service/src/lib.rs: validate_conditional_log_bounds and the scan of the committed log).
    #[error("conditional write conflict")]
    ConditionalWriteConflict,
    #[error("{1}")]
    ConditionalWriteRefused(ErrorCodes, &'static str),
    // A commit that the log can no longer check, stale as a read at its token, so that a client starts it again.
    #[error("{0}")]
    StaleRead(#[from] StaleReadError),
    // The task of the commit and the backfill of a write that did not end, as a panic in it.
    #[error("The write to the log was interrupted: {0}")]
    Interrupted(String),
}

impl ChromaError for SqlitePushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePushLogsError::CompactionError(e) => e.code(),
            SqlitePushLogsError::CompactorHandleSetError => ErrorCodes::FailedPrecondition,
            SqlitePushLogsError::MaxBatchSizeSetError => ErrorCodes::FailedPrecondition,
            SqlitePushLogsError::InvalidMetadata(_) => ErrorCodes::Internal,
            SqlitePushLogsError::MessageSendingError(e) => e.code(),
            SqlitePushLogsError::QueryError(err) => err.code(),
            SqlitePushLogsError::PurgeLogSendingFailure(e) => e.code(),
            SqlitePushLogsError::GetMaxBatchSizeError(e) => e.code(),
            SqlitePushLogsError::Unimplemented(_) => ErrorCodes::Unimplemented,
            SqlitePushLogsError::InvalidSparseMetadata => ErrorCodes::InvalidArgument,
            SqlitePushLogsError::BackoffCompaction { .. } => ErrorCodes::ResourceExhausted,
            SqlitePushLogsError::ConditionalWriteConflict => ErrorCodes::Aborted,
            SqlitePushLogsError::ConditionalWriteRefused(code, _) => *code,
            SqlitePushLogsError::StaleRead(err) => err.code(),
            SqlitePushLogsError::Interrupted(_) => ErrorCodes::Internal,
        }
    }
}

const DEFAULT_VAR_OPT: u32 = 32766;
const PRAGMA_MAX_VAR_OPT: &str = "MAX_VARIABLE_NUMBER";
const VARIABLE_PER_RECORD: u32 = 6;
const DEFAULT_MAX_BATCH_SIZE: u32 = 999;

#[derive(Error, Debug)]
pub enum SqliteGetCollectionsWithNewDataError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Invalid collection ID: {0}")]
    InvalidCollectionId(#[from] uuid::Error),
}

impl ChromaError for SqliteGetCollectionsWithNewDataError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteGetCollectionsWithNewDataError::QueryError(err) => err.code(),
            SqliteGetCollectionsWithNewDataError::InvalidCollectionId(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteUpdateCollectionLogOffsetError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
}

impl ChromaError for SqliteUpdateCollectionLogOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteUpdateCollectionLogOffsetError::QueryError(err) => err.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteGetLegacyEmbeddingsQueueConfigError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Failed to parse legacy config: {0}")]
    InvalidConfig(#[from] serde_json::Error),
}

impl ChromaError for SqliteGetLegacyEmbeddingsQueueConfigError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteGetLegacyEmbeddingsQueueConfigError::QueryError(err) => err.code(),
            SqliteGetLegacyEmbeddingsQueueConfigError::InvalidConfig(_) => ErrorCodes::Internal,
        }
    }
}

pub fn legacy_embeddings_queue_config_default_kind() -> String {
    "EmbeddingsQueueConfigurationInternal".to_owned()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LegacyEmbeddingsQueueConfig {
    pub automatically_purge: bool,
    #[serde(
        default = "legacy_embeddings_queue_config_default_kind",
        rename = "_type"
    )]
    pub kind: String,
}

#[derive(Clone, Debug)]
pub struct SqliteLog {
    db: SqliteDb,
    tenant_id: String,
    topic_namespace: String,
    compactor_handle: OnceLock<ComponentHandle<LocalCompactionManager>>,
    max_batch_size: OnceLock<u32>,
    num_records_before_backpressure: u64,
    // See SqliteLogConfig::purge_cutoff_time_seconds.
    purge_cutoff_time_seconds: u64,
    // The collections that conditional transactions have used since this process started, shared by every copy of the log.
    conditional_uses: Arc<Mutex<HashMap<CollectionUuid, ConditionalUse>>>,
    // The SPANN collections whose log was too full at the last round of the compactor, shared by every copy of the log,
    // as the set of the collections under backpressure of the log service of distributed Chroma.
    backpressure: Arc<Mutex<Arc<HashSet<CollectionUuid>>>>,
    // The rounds of the local compactor, for the seconds after which a refused write may be sent again.
    compaction_rounds: Arc<Mutex<Option<CompactionRounds>>>,
}

// The use of a collection by conditional transactions: the log keeps its records for the cutoff from the first use until the
// cutoff after the last one, for the commits to check them.
#[derive(Clone, Copy, Debug)]
struct ConditionalUse {
    last: Instant,
    // The watermark at the first use: below it, records may have been purged at once, so a window that starts there may
    // have lost records. No read of this use takes a token below it: it comes from a read before the use, as before a
    // restart of the process.
    kept_from: i64,
}

// The rounds of the local compactor: the next one starts compaction_interval after the end of the one before, as in
// distributed Chroma, and flushes at most records_per_flush records of a collection.
#[derive(Clone, Copy, Debug)]
struct CompactionRounds {
    // The start of the last round.
    started: Instant,
    // The end of the last round that ended, and how long it took.
    ended: Option<Instant>,
    length: Duration,
    compaction_interval: Duration,
    records_per_flush: u64,
}

impl CompactionRounds {
    // The time until the end of the flushes that take the given records of a log after its last flush under the limit:
    // the end of the round that runs, or of the next one, then one interval and one round for each further flush.
    fn until_under(&self, records: u64, limit: u64) -> Duration {
        let flushes = (records + 1)
            .saturating_sub(limit)
            .div_ceil(self.records_per_flush.max(1));
        let first = match self.ended {
            Some(ended) if ended >= self.started => {
                self.compaction_interval.saturating_sub(ended.elapsed()) + self.length
            }
            _ => self.length.saturating_sub(self.started.elapsed()),
        };
        first
            + (self.compaction_interval + self.length)
                .saturating_mul(u32::try_from(flushes.saturating_sub(1)).unwrap_or(u32::MAX))
    }
}

impl SqliteLog {
    pub fn new(db: SqliteDb, tenant_id: String, topic_namespace: String) -> Self {
        Self {
            db,
            tenant_id,
            topic_namespace,
            compactor_handle: OnceLock::new(),
            max_batch_size: OnceLock::new(),
            num_records_before_backpressure:
                SqliteLogConfig::default_num_records_before_backpressure(),
            purge_cutoff_time_seconds: SqliteLogConfig::default_purge_cutoff_time_seconds(),
            conditional_uses: Arc::default(),
            backpressure: Arc::default(),
            compaction_rounds: Arc::default(),
        }
    }

    /// The start of a round of the local compactor, which runs compaction_interval after the end of the one before and
    /// flushes at most records_per_flush records of a collection.
    pub fn spann_round_started(&self, compaction_interval: Duration, records_per_flush: u64) {
        let mut rounds = self.compaction_rounds.lock().unwrap();
        let (ended, length) = match *rounds {
            Some(last) => (last.ended, last.length),
            None => (None, Duration::ZERO),
        };
        *rounds = Some(CompactionRounds {
            started: Instant::now(),
            ended,
            length,
            compaction_interval,
            records_per_flush,
        });
    }

    /// The end of a round of the local compactor.
    pub fn spann_round_ended(&self) {
        if let Some(rounds) = self.compaction_rounds.lock().unwrap().as_mut() {
            let now = Instant::now();
            rounds.length = now.duration_since(rounds.started);
            rounds.ended = Some(now);
        }
    }

    pub fn with_num_records_before_backpressure(mut self, records: u64) -> Self {
        self.num_records_before_backpressure = records;
        self
    }

    pub fn with_purge_cutoff_time_seconds(mut self, seconds: u64) -> Self {
        self.purge_cutoff_time_seconds = seconds;
        self
    }

    // The bound of SQLite on created_at, in UTC as CURRENT_TIMESTAMP writes it, of the records the purge may remove: the
    // records of a collection in use by conditional transactions stay for the cutoff, the others go once applied, as in
    // Chroma 1.5.9.
    fn purge_cutoff(&self, collection_id: CollectionUuid) -> String {
        let in_use = self
            .conditional_uses
            .lock()
            .unwrap()
            .get(&collection_id)
            .is_some_and(|used| used.last.elapsed().as_secs() < self.purge_cutoff_time_seconds);
        format!(
            "-{} seconds",
            if in_use {
                self.purge_cutoff_time_seconds
            } else {
                0
            }
        )
    }

    /// A conditional transaction reads or writes a collection, before it does: the log keeps the records of the collection
    /// for the cutoff from now until the cutoff after the last use. After a use that ended, this is a first use again.
    pub async fn note_conditional_use(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<(), WrappedSqlxError> {
        let now = Instant::now();
        let cutoff = Duration::from_secs(self.purge_cutoff_time_seconds);
        {
            let mut uses = self.conditional_uses.lock().unwrap();
            match uses.get_mut(&collection_id) {
                Some(used) if used.last.elapsed() < cutoff => {
                    used.last = now;
                    return Ok(());
                }
                // Until the watermark is read, a commit is refused and starts again.
                _ => {
                    uses.insert(
                        collection_id,
                        ConditionalUse {
                            last: now,
                            kept_from: i64::MAX,
                        },
                    );
                }
            }
        }
        // After the use is noted: a purge that starts from now on keeps the records, and one that started before removes
        // only records below the watermark it read, which is at most this one.
        let kept_from = self.log_position(collection_id).await;
        let mut uses = self.conditional_uses.lock().unwrap();
        match kept_from {
            Ok(kept_from) => {
                if let Some(used) = uses.get_mut(&collection_id) {
                    used.kept_from = kept_from;
                }
                Ok(())
            }
            Err(err) => {
                uses.remove(&collection_id);
                Err(err)
            }
        }
    }

    /// Whether conditional transactions have used the collection since this process started: its purges wait for their
    /// timer, as they may remove the records of hours.
    pub fn used_by_conditional_transactions(&self, collection_id: CollectionUuid) -> bool {
        self.conditional_uses
            .lock()
            .unwrap()
            .contains_key(&collection_id)
    }

    fn set_backpressure(&self, to_pressure: HashSet<CollectionUuid>) {
        *self.backpressure.lock().unwrap() = Arc::new(to_pressure);
    }

    // As check_for_backpressure of the log service of distributed Chroma: a write to a collection that the last round
    // found too full counts its log again, and is refused only if the log is still too full. The set stays as the round
    // left it. The refusal says after how many seconds, at least 1, the flushes should have taken the log under the
    // limit, from the rounds of the compactor.
    async fn check_for_backpressure(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<(), SqlitePushLogsError> {
        let backpressure = Arc::clone(&self.backpressure.lock().unwrap());
        if !backpressure.contains(&collection_id) {
            return Ok(());
        }
        let records: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM segments
            INNER JOIN embeddings_queue   ON embeddings_queue.topic = CONCAT('persistent://', ?, '/', ?, '/', segments.collection)
            LEFT JOIN max_seq_id          ON max_seq_id.segment_id  = segments.id
            WHERE segments.type = ?
            AND segments.collection = ?
            AND embeddings_queue.seq_id > COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)
            "#,
        )
        .bind(&self.tenant_id)
        .bind(&self.topic_namespace)
        .bind(String::from(SegmentType::Spann))
        .bind(collection_id.0.to_string())
        .fetch_one(self.db.get_conn())
        .await
        .map_err(WrappedSqlxError)?;
        if records as u64 >= self.num_records_before_backpressure {
            let until_under = match *self.compaction_rounds.lock().unwrap() {
                Some(rounds) => {
                    rounds.until_under(records as u64, self.num_records_before_backpressure)
                }
                None => Duration::ZERO,
            };
            return Err(SqlitePushLogsError::BackoffCompaction {
                retry_after: (until_under.as_secs_f64().ceil() as u64).max(1),
            });
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub fn init_compactor_handle(
        &self,
        compactor_handle: ComponentHandle<LocalCompactionManager>,
    ) -> Result<(), SqlitePushLogsError> {
        self.compactor_handle
            .set(compactor_handle)
            .map_err(|_| SqlitePushLogsError::CompactorHandleSetError)
    }

    #[allow(clippy::result_large_err)]
    pub fn init_max_batch_size(&self, max_batch_size: u32) -> Result<(), SqlitePushLogsError> {
        self.max_batch_size
            .set(max_batch_size)
            .map_err(|_| SqlitePushLogsError::MaxBatchSizeSetError)
    }

    // The offset of the next record of the log, as the scout of the log service gives the tail of a collection: the
    // seq_id of SQLite is the offset of the single node, and it grows over the whole table, so the tail is its next one.
    pub(super) async fn scout_logs(
        &mut self,
        _collection_id: CollectionUuid,
        _starting_offset: i64,
    ) -> Result<u64, SqlitePullLogsError> {
        let max: Option<i64> = sqlx::query_scalar("SELECT MAX(seq_id) FROM embeddings_queue")
            .fetch_one(self.db.get_conn())
            .await
            .map_err(WrappedSqlxError)?;
        Ok(max.unwrap_or(0) as u64 + 1)
    }

    // The seq_id up to which the records of a collection are applied to both its segments, as max_purge_seq_id: the
    // compactor may purge the log below it, and a read sees every record up to it. It is what the log_position of a
    // collection is to distributed Chroma, which the SQLite sysdb does not keep.
    async fn applied_up_to(
        conn: &mut SqliteConnection,
        collection_id: CollectionUuid,
    ) -> Result<i64, WrappedSqlxError> {
        sqlx::query_scalar(
            r#"
            SELECT COALESCE(MIN(COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)), 0)
            FROM segments
            LEFT JOIN max_seq_id ON max_seq_id.segment_id = segments.id
            WHERE segments.collection = ?
            "#,
        )
        .bind(collection_id.0.to_string())
        .fetch_one(conn)
        .await
        .map_err(WrappedSqlxError)
    }

    /// The log position of a collection for the stale reads of the conditional transactions: see applied_up_to.
    pub async fn log_position(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<i64, WrappedSqlxError> {
        let mut conn = self
            .db
            .get_conn()
            .acquire()
            .await
            .map_err(WrappedSqlxError)?;
        Self::applied_up_to(&mut conn, collection_id).await
    }

    // A use of the collection keeps its records for the cutoff, from the watermark at its start: below that, or below
    // the watermark with a record older than the cutoff before the window, the window may have lost records. seq_id
    // grows with time, so every record after the one before the window, of any collection, is as new as it. That
    // record is one lookup in the primary key, where the first record within the cutoff would be a walk over those a
    // purge has yet to remove, in the write lock.
    async fn may_be_purged(
        &self,
        conn: &mut SqliteConnection,
        collection_id: CollectionUuid,
        observed: i64,
    ) -> Result<bool, WrappedSqlxError> {
        let kept_from = self
            .conditional_uses
            .lock()
            .unwrap()
            .get(&collection_id)
            .map_or(i64::MAX, |used| used.kept_from);
        Ok(observed < kept_from
            || (observed < Self::applied_up_to(&mut *conn, collection_id).await?
                && sqlx::query_scalar::<_, bool>(
                    "SELECT COALESCE((SELECT created_at <= datetime('now', ?) FROM embeddings_queue \
                     WHERE seq_id <= ? ORDER BY seq_id DESC LIMIT 1), 1)",
                )
                .bind(format!("-{} seconds", self.purge_cutoff_time_seconds))
                .bind(observed)
                .fetch_one(&mut *conn)
                .await
                .map_err(WrappedSqlxError)?))
    }

    // Whether a later read of a transaction, on segments that applied the log up to `applied`, answers what its records held
    // at its token. The segments hold only the current state, so it does when the log shows that no record at or after the
    // token, up to what they applied, touched them. A token past what they applied, or a window the log may have lost, cannot
    // be checked and does not stand.
    pub async fn read_stands_at_token(
        &mut self,
        collection_id: CollectionUuid,
        token: i64,
        applied: i64,
        read_ids: &HashSet<&str>,
    ) -> Result<bool, WrappedSqlxError> {
        if token > applied + 1 {
            return Ok(false);
        }
        let purged = self
            .get_legacy_embeddings_queue_config()
            .await
            .map_or(true, |config| config.automatically_purge);
        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);
        let mut tx = self.db.get_conn().begin().await.map_err(WrappedSqlxError)?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM embeddings_queue WHERE topic = ? AND seq_id >= ? AND seq_id <= ?",
        )
        .bind(topic)
        .bind(token)
        .bind(applied)
        .fetch_all(&mut *tx)
        .await
        .map_err(WrappedSqlxError)?;
        if purged && self.may_be_purged(&mut tx, collection_id, token).await? {
            return Ok(false);
        }
        Ok(!ids.iter().any(|id| read_ids.contains(id.as_str())))
    }

    // As validate_conditional_log_bounds and the scan of the committed log of the log service of distributed Chroma, in
    // the transaction of the write: the window goes from the observed offset to the tail; the readable lower bound is
    // the watermark up to which the compactor may have purged the log of the collection (the lower of the watermarks of
    // its segments, as max_purge_seq_id), or 0 when the log is never purged; a record in the window with an id read or
    // written is a conflict.
    async fn check_condition(
        &self,
        tx: &mut SqliteConnection,
        topic: &str,
        collection_id: CollectionUuid,
        condition: &PushLogsCondition,
        write_ids: &HashSet<String>,
        purged: bool,
    ) -> Result<(), SqlitePushLogsError> {
        let refused = SqlitePushLogsError::ConditionalWriteRefused;
        if condition.observed_log_offset < 0 {
            return Err(refused(
                ErrorCodes::InvalidArgument,
                "observed_log_offset must be non-negative",
            ));
        }
        if condition.read_ids.iter().any(String::is_empty) {
            return Err(refused(
                ErrorCodes::InvalidArgument,
                "read_ids must not contain empty ids",
            ));
        }
        if write_ids.contains("") {
            return Err(refused(
                ErrorCodes::InvalidArgument,
                "records must not contain empty ids",
            ));
        }
        let observed = condition.observed_log_offset;
        let max: Option<i64> = sqlx::query_scalar("SELECT MAX(seq_id) FROM embeddings_queue")
            .fetch_one(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;
        if observed > max.unwrap_or(0) + 1 {
            return Err(refused(
                ErrorCodes::InvalidArgument,
                "observed_log_offset is beyond the current log tail",
            ));
        }
        if purged
            && self
                .may_be_purged(&mut *tx, collection_id, observed)
                .await?
        {
            return Err(StaleReadError::version_purged(
                observed as u64,
                "observed_log_offset is before the readable log lower bound",
            )
            .into());
        }
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT id FROM embeddings_queue WHERE topic = ? AND seq_id >= ?")
                .bind(topic)
                .bind(observed)
                .fetch_all(&mut *tx)
                .await
                .map_err(WrappedSqlxError)?;
        if ids
            .iter()
            .any(|id| write_ids.contains(id) || condition.read_ids.contains(id))
        {
            return Err(SqlitePushLogsError::ConditionalWriteConflict);
        }
        Ok(())
    }

    pub(super) async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp_ns: Option<i64>,
    ) -> Result<Vec<LogRecord>, SqlitePullLogsError> {
        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        let end_timestamp_ns = end_timestamp_ns.unwrap_or(i64::MAX);

        let mut logs;
        if batch_size < 0 {
            logs = sqlx::query(
                r#"
            SELECT
              seq_id,
              id,
              operation,
              vector,
              encoding,
              metadata
            FROM embeddings_queue
            WHERE topic = ?
            AND seq_id > ?
            AND CAST(strftime('%s', created_at) AS INTEGER) <= (? / 1000000000)
            ORDER BY seq_id ASC
            "#,
            )
            .bind(topic)
            .bind(offset)
            .bind(end_timestamp_ns)
            .fetch(self.db.get_conn());
        } else {
            logs = sqlx::query(
                r#"
            SELECT
              seq_id,
              id,
              operation,
              vector,
              encoding,
              metadata
            FROM embeddings_queue
            WHERE topic = ?
            AND seq_id > ?
            AND CAST(strftime('%s', created_at) AS INTEGER) <= (? / 1000000000)
            ORDER BY seq_id ASC
            LIMIT ?
            "#,
            )
            .bind(topic)
            .bind(offset)
            .bind(end_timestamp_ns)
            .bind(batch_size)
            .fetch(self.db.get_conn());
        }

        let mut records = Vec::new();
        while let Some(row) = logs.try_next().await.map_err(WrappedSqlxError)? {
            records.push(log_record(&row)?);
        }

        Ok(records)
    }

    /// The records of the log of a collection after the position `after` and up to `up_to`, read on the connection given:
    /// a read of the log in the snapshot of another read, which sees the log at the same moment as the rest of SQLite.
    /// The first record read is the one after `after`, as the fetch of the log of distributed Chroma reads from the log
    /// position of a collection plus one (rust/worker/src/execution/operators/fetch_log.rs). With a limit, at most that
    /// many records, the first ones, as the fetch of distributed Chroma with its maximum_fetch_count: the positions of
    /// the log of the single node are shared by every collection, so the limit counts the records of the collection.
    pub async fn read_tail(
        &self,
        connection: &mut SqliteConnection,
        collection_id: CollectionUuid,
        after: i64,
        up_to: i64,
        limit: Option<u32>,
    ) -> Result<Vec<LogRecord>, SqlitePullLogsError> {
        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);
        // A negative LIMIT is no limit in SQLite.
        let mut logs = sqlx::query(
            r#"
            SELECT
              seq_id,
              id,
              operation,
              vector,
              encoding,
              metadata
            FROM embeddings_queue
            WHERE topic = ?
            AND seq_id > ?
            AND seq_id <= ?
            ORDER BY seq_id ASC
            LIMIT ?
            "#,
        )
        .bind(topic)
        .bind(after)
        .bind(up_to)
        .bind(limit.map_or(-1, i64::from))
        .fetch(&mut *connection);
        let mut records = Vec::new();
        while let Some(row) = logs.try_next().await.map_err(WrappedSqlxError)? {
            records.push(log_record(&row)?);
        }
        Ok(records)
    }

    /// The number of records in the log of a collection after the position `after`, counted on the connection given, as
    /// read_tail reads them, up to the last one written.
    pub async fn count_tail(
        &self,
        connection: &mut SqliteConnection,
        collection_id: CollectionUuid,
        after: i64,
    ) -> Result<u64, SqlitePullLogsError> {
        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM embeddings_queue WHERE topic = ? AND seq_id > ?",
        )
        .bind(topic)
        .bind(after)
        .fetch_one(&mut *connection)
        .await
        .map_err(WrappedSqlxError)?;
        Ok(count as u64)
    }

    // A write without a condition, as the tests of the log write.
    #[cfg(test)]
    pub(super) async fn push_logs(
        &mut self,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<(), SqlitePushLogsError> {
        self.push_logs_with_condition(collection_id, records, None)
            .await
            .map(|_| ())
    }

    // A write to the log; with a condition, the conditional write of distributed Chroma, which answers the position of its
    // first record.
    pub(super) async fn push_logs_with_condition(
        &mut self,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
        condition: Option<&PushLogsCondition>,
    ) -> Result<Option<i64>, SqlitePushLogsError> {
        if records.is_empty() {
            // As the log service of distributed Chroma: a commit of a delete with a filter has no records.
            if condition.is_some() {
                return Err(SqlitePushLogsError::ConditionalWriteRefused(
                    ErrorCodes::InvalidArgument,
                    "Too few records",
                ));
            }
            return Ok(None);
        }
        let write_ids: HashSet<String> = records.iter().map(|record| record.id.clone()).collect();
        self.check_for_backpressure(collection_id).await?;

        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        let records_and_serialized_metadatas = records
            .into_iter()
            .map(|mut record| {
                let mut empty_metadata = UpdateMetadata::new();

                let metadata = record.metadata.as_mut().unwrap_or(&mut empty_metadata);
                for value in metadata.values() {
                    if let UpdateMetadataValue::SparseVector(vector) = value {
                        if vector.validate().is_err()
                            || vector.values.iter().any(|value| !value.is_finite())
                        {
                            return Err(SqlitePushLogsError::InvalidSparseMetadata);
                        }
                    }
                }

                if let Some(ref document) = record.document {
                    metadata.insert(
                        "chroma:document".to_string(),
                        UpdateMetadataValue::Str(document.clone()),
                    );
                }

                let serialized = serde_json::to_string(&metadata)?;
                Ok::<_, SqlitePushLogsError>((record, serialized))
            })
            .collect::<Result<Vec<(OperationRecord, String)>, SqlitePushLogsError>>()?;

        let max_batch_size = self
            .max_batch_size
            .get()
            .copied()
            .unwrap_or(DEFAULT_MAX_BATCH_SIZE) as usize;
        // A conditional write takes the write lock of SQLite before its check, so that no write comes between the check
        // and its own, as the admission of the log service of distributed Chroma.
        // Whether the log is purged, read before the transaction: a database in memory has one connection, which the
        // transaction holds.
        if condition.is_some() {
            self.note_conditional_use(collection_id)
                .await
                .map_err(SqlitePushLogsError::from)?;
        }
        let purged = match condition {
            Some(_) => self
                .get_legacy_embeddings_queue_config()
                .await
                .map_or(true, |config| config.automatically_purge),
            None => false,
        };
        let mut tx = if condition.is_some() {
            self.db.get_conn().begin_with("BEGIN IMMEDIATE").await
        } else {
            self.db.get_conn().begin().await
        }
        .map_err(WrappedSqlxError)?;
        if let Some(condition) = condition {
            self.check_condition(
                &mut tx,
                &topic,
                collection_id,
                condition,
                &write_ids,
                purged,
            )
            .await?;
        }
        let mut first_inserted_record_offset = None;
        for batch in records_and_serialized_metadatas.chunks(max_batch_size) {
            let mut query_builder = QueryBuilder::new(
                "INSERT INTO embeddings_queue (topic, id, operation, vector, encoding, metadata) ",
            );
            query_builder.push_values(batch, |mut builder, (record, serialized_metadata)| {
                builder.push_bind(&topic);
                builder.push_bind(&record.id);
                builder.push_bind(operation_to_code(record.operation));
                builder.push_bind::<Option<Vec<u8>>>(
                    record
                        .embedding
                        .as_ref()
                        .map(|e| bytemuck::cast_slice(e.as_slice()).to_vec()),
                );
                builder.push_bind(record.encoding.as_ref().map(String::from));
                builder.push_bind(serialized_metadata);
            });
            if condition.is_some() {
                query_builder.push(" RETURNING seq_id");
                let seq_ids = query_builder
                    .build_query_scalar::<i64>()
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(WrappedSqlxError)?;
                first_inserted_record_offset =
                    first_inserted_record_offset.or(seq_ids.into_iter().min());
            } else {
                let query = query_builder.build();
                query.execute(&mut *tx).await.map_err(WrappedSqlxError)?;
            }
        }
        // The commit and the backfill of the write run in a task of their own. A client that goes away drops the future of
        // its request: a write committed to the log stayed there unapplied, seen by no read until another write of the
        // collection or a restart. As in distributed Chroma, a write that reaches the log is seen by the reads.
        let handle = self.compactor_handle.get().cloned();
        tokio::spawn(async move {
            tx.commit().await.map_err(WrappedSqlxError)?;
            #[cfg(feature = "test-hooks")]
            hooks::after_commit(collection_id).await;
            if let Some(handle) = handle {
                let backfill_message = BackfillMessage { collection_id };
                let backfill_result = handle.request(backfill_message, None).await;
                let purge_log_msg = PurgeLogsMessage { collection_id };
                let purge_result = handle
                    .clone()
                    .request(purge_log_msg, None)
                    .await
                    .map_err(SqlitePushLogsError::from)
                    .and_then(|result| result.map_err(SqlitePushLogsError::from));
                // Preserve purge failures even when the backfill error takes precedence.
                if let Err(err) = &purge_result {
                    tracing::error!(
                        collection_id = %collection_id,
                        error = %err,
                        "Failed to purge logs after backfill attempt"
                    );
                }
                backfill_result??;
                purge_result?;
            }
            Ok::<_, SqlitePushLogsError>(())
        })
        .await
        .map_err(|err| SqlitePushLogsError::Interrupted(err.to_string()))??;

        Ok(first_inserted_record_offset)
    }

    pub(super) async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, SqliteGetCollectionsWithNewDataError> {
        let mut results = sqlx::query(
            r#"
            SELECT
                collections.id AS collection_id,
                MIN(COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)) AS first_log_offset,
                CAST(strftime('%s', MIN(created_at)) AS INTEGER) * 1000000000 AS first_log_ts
            FROM collections
            INNER JOIN segments           ON segments.collection    = collections.id
            INNER JOIN embeddings_queue   ON embeddings_queue.topic = CONCAT('persistent://', ?, '/', ?, '/', collections.id)
            LEFT JOIN max_seq_id          ON max_seq_id.segment_id  = segments.id
            WHERE embeddings_queue.seq_id > COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)
            GROUP BY
                collections.id
            HAVING
                COUNT(*) > ?
            ORDER BY first_log_ts ASC
            "#,
        )
        .bind(&self.tenant_id)
        .bind(&self.topic_namespace)
        .bind(min_compaction_size as i64) // (SQLite doesn't support u64)
        .fetch(self.db.get_conn());

        let mut infos = Vec::new();
        while let Some(row) = results.try_next().await.map_err(WrappedSqlxError)? {
            infos.push(CollectionInfo {
                collection_id: CollectionUuid::from_str(row.get::<&str, _>("collection_id"))?,
                topology_name: None,
                first_log_offset: row.get("first_log_offset"),
                first_log_ts: row.get("first_log_ts"),
            });
        }

        Ok(infos)
    }

    /// The SPANN collections to flush, oldest first, as the log service of distributed Chroma picks the collections to
    /// compact (rust/log-service/src/lib.rs, should_compact_at): those whose log holds at least min_compaction_size
    /// records after the last flush of their SPANN segment, or whose oldest such record has waited at least the timeout.
    /// The third rule of the log service, a log marked dirty again reinsert_threshold times, has no counterpart here:
    /// it counts how often wal3 marks the log of a collection dirty again, which this log does not record, and since
    /// every write carries at least one record, its default of 10 marks means at least 10 records anyway.
    /// From the same counts, as the log service sets its backpressure from the summaries of the logs it compacts, the
    /// collections whose log holds at least num_records_before_backpressure records become those under backpressure.
    pub async fn get_spann_collections_to_compact(
        &mut self,
        min_compaction_size: u64,
        timeout: Duration,
    ) -> Result<Vec<CollectionUuid>, SqliteGetCollectionsWithNewDataError> {
        let mut results = sqlx::query(
            r#"
            SELECT
                segments.collection AS collection_id,
                COUNT(*) >= ? OR MIN(embeddings_queue.created_at) <= datetime('now', ?) AS to_compact,
                COUNT(*) >= ? AS too_full
            FROM segments
            INNER JOIN embeddings_queue   ON embeddings_queue.topic = CONCAT('persistent://', ?, '/', ?, '/', segments.collection)
            LEFT JOIN max_seq_id          ON max_seq_id.segment_id  = segments.id
            WHERE segments.type = ?
            AND embeddings_queue.seq_id > COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)
            GROUP BY
                segments.collection
            HAVING
                to_compact
                OR too_full
            ORDER BY MIN(embeddings_queue.created_at) ASC
            "#,
        )
        .bind(min_compaction_size as i64) // (SQLite doesn't support u64)
        .bind(format!("-{} seconds", timeout.as_secs()))
        .bind(self.num_records_before_backpressure as i64)
        .bind(&self.tenant_id)
        .bind(&self.topic_namespace)
        .bind(String::from(SegmentType::Spann))
        .fetch(self.db.get_conn());

        let mut collections = Vec::new();
        let mut too_full = HashSet::new();
        while let Some(row) = results.try_next().await.map_err(WrappedSqlxError)? {
            let collection_id = CollectionUuid::from_str(row.get::<&str, _>("collection_id"))?;
            if row.get::<bool, _>("too_full") {
                too_full.insert(collection_id);
            }
            if row.get::<bool, _>("to_compact") {
                collections.push(collection_id);
            }
        }
        self.set_backpressure(too_full);

        Ok(collections)
    }

    pub async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), SqliteUpdateCollectionLogOffsetError> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO max_seq_id (seq_id, segment_id)
            SELECT ?, id
            FROM segments
            WHERE
                collection = ?
        "#,
        )
        .bind(new_offset)
        .bind(collection_id.0.to_string())
        .execute(self.db.get_conn())
        .await
        .map_err(WrappedSqlxError)?;

        Ok(())
    }

    // Use the same topic and strict boundary as purge_logs. Checking actual
    // rows also handles retries, disabled purging, and database resets.
    pub(crate) async fn has_purge_work(
        &mut self,
        collection_id: CollectionUuid,
        seq_id: u64,
    ) -> Result<bool, SqlitePurgeLogsError> {
        if !self
            .get_legacy_embeddings_queue_config()
            .await?
            .automatically_purge
        {
            return Ok(false);
        }
        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM embeddings_queue WHERE topic = ? AND seq_id < ? AND created_at <= datetime('now', ?))",
        )
        .bind(topic)
        .bind(seq_id as i64)
        .bind(self.purge_cutoff(collection_id))
        .fetch_one(self.db.get_conn())
        .await
        .map_err(|err| SqlitePurgeLogsError::from(WrappedSqlxError(err)))
    }

    pub async fn purge_logs(
        &mut self,
        collection_id: CollectionUuid,
        seq_id: u64,
    ) -> Result<(), SqlitePurgeLogsError> {
        let legacy_config = self.get_legacy_embeddings_queue_config().await?;
        // Skip purge if not enabled
        if !legacy_config.automatically_purge {
            return Ok(());
        }

        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        // The records of a collection in use by conditional transactions stay for purge_cutoff_time_seconds.
        sqlx::query(
            "DELETE FROM embeddings_queue WHERE topic = ? AND seq_id < ? AND created_at <= datetime('now', ?)",
        )
        .bind(topic)
        .bind(seq_id as i64)
        .bind(self.purge_cutoff(collection_id))
            .execute(self.db.get_conn())
            .await
            .map_err(WrappedSqlxError)?;

        Ok(())
    }

    pub async fn get_max_batch_size(&self) -> Result<u32, SqliteGetMaxBatchSizeError> {
        let opt_strs = sqlx::query("PRAGMA compile_options")
            .fetch_all(self.db.get_conn())
            .await
            .map_err(WrappedSqlxError)?
            .into_iter()
            .map(|row| row.try_get::<String, _>(0))
            .collect::<Result<Vec<_>, _>>()?;
        let max_variable_number = opt_strs
            .into_iter()
            .filter_map(|opt_str| {
                let mut opt_val = opt_str.split("=");
                if let Some(PRAGMA_MAX_VAR_OPT) = opt_val.next() {
                    opt_val.next().and_then(|val_str| val_str.parse().ok())
                } else {
                    None
                }
            })
            .fold(DEFAULT_VAR_OPT, |_, opt| opt);
        Ok(max_variable_number / VARIABLE_PER_RECORD)
    }

    pub async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        // Populate default config
        self.get_legacy_embeddings_queue_config()
            .await
            .map_err(|e| e.boxed())?;
        Ok(ResetResponse {})
    }

    async fn get_legacy_embeddings_queue_config(
        &mut self,
    ) -> Result<LegacyEmbeddingsQueueConfig, SqliteGetLegacyEmbeddingsQueueConfigError> {
        let mut tx = self.db.get_conn().begin().await.map_err(WrappedSqlxError)?;

        let row = sqlx::query("SELECT * FROM embeddings_queue_config")
            .fetch_optional(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;

        if let Some(row) = row {
            let value: String = row.get("config_json_str");
            let config: LegacyEmbeddingsQueueConfig = serde_json::from_str(&value)?;
            return Ok(config);
        }

        // Insert default
        let log_size = sqlx::query("SELECT COUNT(*) from embeddings_queue")
            .fetch_one(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;
        let log_size = log_size.get::<i64, _>(0);

        let default_config = LegacyEmbeddingsQueueConfig {
            automatically_purge: log_size == 0,
            kind: legacy_embeddings_queue_config_default_kind(),
        };
        let value = serde_json::to_string(&default_config)?;
        sqlx::query("INSERT INTO embeddings_queue_config (config_json_str) VALUES (?)")
            .bind(value)
            .execute(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;
        tx.commit().await.map_err(WrappedSqlxError)?;

        Ok(default_config)
    }

    pub async fn update_legacy_embeddings_queue_config(
        &self,
        config: LegacyEmbeddingsQueueConfig,
    ) -> Result<LegacyEmbeddingsQueueConfig, SqliteGetLegacyEmbeddingsQueueConfigError> {
        let mut tx = self.db.get_conn().begin().await.map_err(WrappedSqlxError)?;
        let value = serde_json::to_string(&config)?;
        sqlx::query(
            "INSERT OR REPLACE INTO embeddings_queue_config (id, config_json_str) VALUES (1, ?)",
        )
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(WrappedSqlxError)?;
        tx.commit().await.map_err(WrappedSqlxError)?;
        Ok(config)
    }
}

#[derive(Error, Debug)]
pub enum SqlitePurgeLogsError {
    #[error("Could not get legacy embedding queue config: {0}")]
    GetLegacyConfigError(#[from] SqliteGetLegacyEmbeddingsQueueConfigError),
    #[error("Delete query error: {0}")]
    DeleteQueryError(#[from] WrappedSqlxError),
}

impl ChromaError for SqlitePurgeLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePurgeLogsError::GetLegacyConfigError(err) => err.code(),
            SqlitePurgeLogsError::DeleteQueryError(err) => err.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteGetMaxBatchSizeError {
    #[error("Error getting compile time options from sqlite: {0}")]
    PragmaQueryError(#[from] WrappedSqlxError),
    #[error("Error parsing row from sqlx: {0}")]
    RowParsingError(#[from] sqlx::Error),
}

impl ChromaError for SqliteGetMaxBatchSizeError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteGetMaxBatchSizeError::PragmaQueryError(err) => err.code(),
            SqliteGetMaxBatchSizeError::RowParsingError(_) => ErrorCodes::Internal,
        }
    }
}

// A record of the log from its row of embeddings_queue.
fn log_record(row: &SqliteRow) -> Result<LogRecord, SqlitePullLogsError> {
    let log_offset: i64 = row.get("seq_id");
    let id: String = row.get("id");
    let embedding_bytes = row.get::<Option<&[u8]>, _>("vector");
    let encoding = row
        .get::<Option<&str>, _>("encoding")
        .map(ScalarEncoding::try_from)
        .transpose()?;
    let metadata_str = row.get::<Option<&str>, _>("metadata");

    // Parse embedding
    let embedding = embedding_bytes
        .map(
            |embedding_bytes| -> Result<Option<_>, SqlitePullLogsError> {
                match encoding {
                    Some(ScalarEncoding::FLOAT32) => {
                        let slice: &[f32] = bytemuck::try_cast_slice(embedding_bytes)
                            .map_err(SqlitePullLogsError::InvalidEmbedding)?;
                        Ok(Some(slice.to_vec()))
                    }
                    Some(ScalarEncoding::INT32) => {
                        unimplemented!()
                    }
                    None => Ok(None),
                }
            },
        )
        .transpose()?
        .flatten();

    // Parse metadata
    let parsed_metadata_and_document: Option<(UpdateMetadata, Option<String>)> = metadata_str
        .map(|metadata_str| {
            let mut parsed: UpdateMetadata = serde_json::from_str(metadata_str)?;

            let document = match parsed.remove("chroma:document") {
                Some(UpdateMetadataValue::Str(document)) => Some(document),
                None => None,
                _ => panic!("Document not found in metadata"),
            };

            Ok::<_, SqlitePullLogsError>((parsed, document))
        })
        .transpose()?;
    let document = parsed_metadata_and_document
        .as_ref()
        .and_then(|(_, document)| document.clone());
    let metadata = parsed_metadata_and_document.map(|(metadata, _)| metadata);

    let operation = operation_from_code(row.get("operation"));

    Ok(LogRecord {
        log_offset,
        record: OperationRecord {
            id,
            embedding,
            encoding,
            metadata,
            document,
            operation,
        },
    })
}

fn operation_from_code(code: u32) -> Operation {
    // chromadb/db/mixins/embeddings_queue.py
    match code {
        0 => Operation::Add,
        1 => Operation::Update,
        2 => Operation::Upsert,
        3 => Operation::Delete,
        4 => Operation::BackfillFn,
        _ => panic!("Invalid operation code"),
    }
}

fn operation_to_code(operation: Operation) -> u32 {
    match operation {
        Operation::Add => 0,
        Operation::Update => 1,
        Operation::Upsert => 2,
        Operation::Delete => 3,
        Operation::BackfillFn => 4,
    }
}

#[async_trait]
impl Configurable<SqliteLogConfig> for SqliteLog {
    async fn try_from_config(
        config: &SqliteLogConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sqlite_db = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        let mut log = Self::new(
            sqlite_db,
            config.tenant_id.clone(),
            config.topic_namespace.clone(),
        )
        .with_num_records_before_backpressure(config.num_records_before_backpressure)
        .with_purge_cutoff_time_seconds(config.purge_cutoff_time_seconds);

        // This populates the legacy config if not present (when upgrading from an old version)
        log.get_legacy_embeddings_queue_config()
            .await
            .map_err(|e| e.boxed())?;

        Ok(log)
    }
}

/// A test hook of the writes to the log: a write of the collection given waits after its commit, before its backfill, so
/// that a test drops its request there, as a client that goes away.
#[cfg(feature = "test-hooks")]
pub mod hooks {
    use chroma_types::CollectionUuid;
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    /// The wait of a write: `reached` after its commit, which then goes on at `go`.
    #[derive(Debug, Default)]
    pub struct Hold {
        pub reached: Notify,
        pub go: Notify,
    }

    static HOLDS: Mutex<Vec<(CollectionUuid, Arc<Hold>)>> = Mutex::new(Vec::new());

    /// A hold of the next write of the collection, after its commit to the log.
    pub fn hold_after_commit(collection_id: CollectionUuid) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        HOLDS.lock().unwrap().push((collection_id, hold.clone()));
        hold
    }

    pub(crate) async fn after_commit(collection_id: CollectionUuid) {
        let hold = {
            let mut holds = HOLDS.lock().unwrap();
            let Some(position) = holds.iter().position(|(id, _)| *id == collection_id) else {
                return;
            };
            holds.remove(position).1
        };
        hold.reached.notify_one();
        hold.go.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_sqlite::{config::SqliteDBConfig, db::test_utils::new_test_db_persist_path};
    use chroma_types::{are_update_metadatas_close_to_equal, CollectionUuid};
    use proptest::prelude::*;

    use tokio::runtime::Runtime;

    async fn setup_sqlite_log() -> SqliteLog {
        let db = SqliteDb::try_from_config(
            &SqliteDBConfig {
                url: new_test_db_persist_path(),
                migration_mode: chroma_sqlite::config::MigrationMode::Apply,
                hash_type: chroma_sqlite::config::MigrationHash::SHA256,
                ..Default::default()
            },
            &Registry::new(),
        )
        .await
        .unwrap();

        SqliteLog::new(db, "default".to_string(), "default".to_string())
    }

    fn conditional_record(id: &str) -> OperationRecord {
        OperationRecord {
            id: id.into(),
            embedding: Some(vec![1.0, 2.0]),
            encoding: Some(ScalarEncoding::FLOAT32),
            metadata: None,
            document: None,
            operation: Operation::Upsert,
        }
    }

    fn condition(observed_log_offset: i64, read_ids: &[&str]) -> PushLogsCondition {
        PushLogsCondition {
            observed_log_offset,
            read_ids: read_ids.iter().map(|id| id.to_string()).collect(),
        }
    }

    async fn push_conditional(
        log: &mut SqliteLog,
        collection: CollectionUuid,
        ids: &[&str],
        condition: &PushLogsCondition,
    ) -> Result<Option<i64>, SqlitePushLogsError> {
        log.push_logs_with_condition(
            collection,
            ids.iter().map(|id| conditional_record(id)).collect(),
            Some(condition),
        )
        .await
    }

    // As the log service of distributed Chroma: a conditional write is refused with a conflict when a record written
    // after the offset it observed has an id it read or writes, and accepted otherwise; the scout gives the offset of
    // the next record.
    #[tokio::test]
    async fn a_conditional_write_conflicts_with_a_record_written_after_its_offset() {
        let mut log = setup_sqlite_log().await;
        let collection = CollectionUuid::new();
        log.push_logs(collection, vec![conditional_record("a")])
            .await
            .unwrap();
        let observed = log.scout_logs(collection, 0).await.unwrap() as i64;
        assert_eq!(observed, 2);
        log.push_logs(collection, vec![conditional_record("b")])
            .await
            .unwrap();
        assert_eq!(log.scout_logs(collection, 0).await.unwrap(), 3);
        // Written after the offset: b, read or written.
        assert!(matches!(
            push_conditional(&mut log, collection, &["c"], &condition(observed, &["b"])).await,
            Err(SqlitePushLogsError::ConditionalWriteConflict)
        ));
        assert!(matches!(
            push_conditional(&mut log, collection, &["b"], &condition(observed, &[])).await,
            Err(SqlitePushLogsError::ConditionalWriteConflict)
        ));
        // a was written before the offset; a record of another collection does not count.
        log.push_logs(CollectionUuid::new(), vec![conditional_record("d")])
            .await
            .unwrap();
        push_conditional(&mut log, collection, &["d"], &condition(observed, &["a"]))
            .await
            .unwrap();
        // The refused writes wrote nothing: b, then d.
        assert_eq!(log.scout_logs(collection, 0).await.unwrap(), 5);
    }

    // As the log service of distributed Chroma, a conditional write of no records is refused, as it would change nothing that
    // the client asked; a write without a condition of no records has nothing to do.
    #[tokio::test]
    async fn a_conditional_write_of_no_records_is_refused() {
        let mut log = setup_sqlite_log().await;
        let collection = CollectionUuid::new();
        assert!(matches!(
            push_conditional(&mut log, collection, &[], &condition(1, &[])).await,
            Err(SqlitePushLogsError::ConditionalWriteRefused(
                ErrorCodes::InvalidArgument,
                "Too few records"
            ))
        ));
        log.push_logs(collection, Vec::new()).await.unwrap();
    }

    // As the log service of distributed Chroma, a conditional write answers the position of its first record, whatever the
    // batches it takes.
    #[tokio::test]
    async fn a_conditional_write_answers_the_position_of_its_first_record() {
        let mut log = setup_sqlite_log().await;
        log.init_max_batch_size(2).unwrap();
        let collection = CollectionUuid::new();
        log.push_logs(collection, vec![conditional_record("a")])
            .await
            .unwrap();
        let observed = log.scout_logs(collection, 0).await.unwrap() as i64;
        assert_eq!(
            push_conditional(
                &mut log,
                collection,
                &["b", "c", "d"],
                &condition(observed, &["a"])
            )
            .await
            .unwrap(),
            Some(observed)
        );
        assert_eq!(
            log.scout_logs(collection, 0).await.unwrap() as i64,
            observed + 3
        );
    }

    // A later read stands when the log shows that no record at or after its token, up to what the segments applied, touched
    // what it read. A token past what they applied, or a window the log may have lost, does not stand.
    #[tokio::test]
    async fn a_later_read_stands_when_the_log_shows_its_records_did_not_change() {
        let mut log = setup_sqlite_log()
            .await
            .with_purge_cutoff_time_seconds(3_600);
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let collection = CollectionUuid::new();
        log.note_conditional_use(collection).await.unwrap();
        log.push_logs(
            collection,
            vec![
                conditional_record("a"),
                conditional_record("b"),
                conditional_record("c"),
            ],
        )
        .await
        .unwrap();
        let cases: &[(i64, i64, &[&str], bool)] = &[
            (2, 3, &["a"], true),
            (2, 3, &["a", "b"], false),
            (2, 3, &["c"], false),
            // A record past what the segments applied does not count; a token past them cannot be read.
            (2, 2, &["c"], true),
            (3, 2, &["c"], true),
            (4, 2, &["a"], false),
        ];
        for (token, applied, ids, stands) in cases {
            let read_ids = ids.iter().copied().collect();
            assert_eq!(
                log.read_stands_at_token(collection, *token, *applied, &read_ids)
                    .await
                    .unwrap(),
                *stands,
                "token {token}, applied {applied}, ids {ids:?}"
            );
        }
        // Below the watermark, with a record older than the cutoff at the start of the window, the log may have lost records.
        let read_ids = ["a"].into_iter().collect();
        sqlx::query(
            "UPDATE embeddings_queue SET created_at = datetime('now', '-7200 seconds') WHERE seq_id = 2",
        )
        .execute(log.db.get_conn())
        .await
        .unwrap();
        assert!(log
            .read_stands_at_token(collection, 2, 3, &read_ids)
            .await
            .unwrap());
        applied_up_to_for_the_tests(&log, collection, 3).await;
        assert!(!log
            .read_stands_at_token(collection, 2, 3, &read_ids)
            .await
            .unwrap());
    }

    // The bounds of the log service of distributed Chroma: an offset that is negative or beyond the tail, or empty ids,
    // are invalid; an offset below what the compactor applied, so that the log may be purged, cannot be checked and is
    // refused, not passed.
    #[tokio::test]
    async fn a_conditional_write_is_refused_outside_the_readable_log() {
        let mut log = setup_sqlite_log().await.with_purge_cutoff_time_seconds(0);
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let collection = CollectionUuid::new();
        log.push_logs(
            collection,
            vec![conditional_record("a"), conditional_record("b")],
        )
        .await
        .unwrap();
        let refused = |result: Result<Option<i64>, SqlitePushLogsError>| match result {
            Err(SqlitePushLogsError::ConditionalWriteRefused(code, message)) => (code, message),
            other => panic!("not refused: {other:?}"),
        };
        assert_eq!(
            refused(push_conditional(&mut log, collection, &["c"], &condition(-1, &[])).await),
            (
                ErrorCodes::InvalidArgument,
                "observed_log_offset must be non-negative"
            )
        );
        assert_eq!(
            refused(push_conditional(&mut log, collection, &["c"], &condition(4, &[])).await),
            (
                ErrorCodes::InvalidArgument,
                "observed_log_offset is beyond the current log tail"
            )
        );
        assert_eq!(
            refused(push_conditional(&mut log, collection, &["c"], &condition(3, &[""])).await),
            (
                ErrorCodes::InvalidArgument,
                "read_ids must not contain empty ids"
            )
        );
        assert_eq!(
            refused(push_conditional(&mut log, collection, &[""], &condition(3, &[])).await),
            (
                ErrorCodes::InvalidArgument,
                "records must not contain empty ids"
            )
        );
        // Both segments of the collection applied up to 2.
        for (segment, scope) in [("m", "METADATA"), ("v", "VECTOR")] {
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment)
                .bind(segment)
                .bind(scope)
                .bind(collection.0.to_string())
                .execute(log.db.get_conn())
                .await
                .unwrap();
            sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, 2)")
                .bind(segment)
                .execute(log.db.get_conn())
                .await
                .unwrap();
        }
        assert_eq!(log.log_position(collection).await.unwrap(), 2);
        assert!(matches!(
            push_conditional(&mut log, collection, &["c"], &condition(1, &[])).await,
            Err(SqlitePushLogsError::StaleRead(_))
        ));
        push_conditional(&mut log, collection, &["c"], &condition(2, &[]))
            .await
            .unwrap();
    }

    // The records applied stay in the log for purge_cutoff_time_seconds, as distributed Chroma keeps its log for the
    // versions of relative_cutoff_time_seconds: a conditional write that observed an offset below the watermark of the
    // compactor is still checked against them, and after the purge, with a cutoff of 0, it is refused.
    #[tokio::test]
    async fn the_log_applied_stays_for_the_cutoff_and_conditional_writes_check_it() {
        // The cutoff of distributed Chroma (relative_cutoff_time_seconds in rust/garbage_collector/garbage_collector_config.yaml).
        assert_eq!(SqliteLogConfig::default().purge_cutoff_time_seconds, 43_200);
        let mut log = setup_sqlite_log().await;
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let collection = CollectionUuid::new();
        log.note_conditional_use(collection).await.unwrap();
        log.push_logs(collection, vec![conditional_record("a")])
            .await
            .unwrap();
        let observed = log.scout_logs(collection, 0).await.unwrap() as i64;
        log.push_logs(
            collection,
            vec![conditional_record("b"), conditional_record("c")],
        )
        .await
        .unwrap();
        // Both segments applied every record: the watermark is past the observed offset.
        for (segment, scope) in [("m", "METADATA"), ("v", "VECTOR")] {
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment)
                .bind(segment)
                .bind(scope)
                .bind(collection.0.to_string())
                .execute(log.db.get_conn())
                .await
                .unwrap();
            sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, 3)")
                .bind(segment)
                .execute(log.db.get_conn())
                .await
                .unwrap();
        }
        let rows = |log: &SqliteLog| {
            let pool = log.db.get_conn().clone();
            async move {
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM embeddings_queue")
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        assert!(!log.has_purge_work(collection, 3).await.unwrap());
        log.purge_logs(collection, 3).await.unwrap();
        assert_eq!(rows(&log).await, 3);
        assert!(matches!(
            push_conditional(&mut log, collection, &["d"], &condition(observed, &["b"])).await,
            Err(SqlitePushLogsError::ConditionalWriteConflict)
        ));
        push_conditional(&mut log, collection, &["d"], &condition(observed, &["a"]))
            .await
            .unwrap();
        let mut log = log.with_purge_cutoff_time_seconds(0);
        assert!(log.has_purge_work(collection, 3).await.unwrap());
        log.purge_logs(collection, 3).await.unwrap();
        assert_eq!(rows(&log).await, 2);
        assert!(matches!(
            push_conditional(&mut log, collection, &["e"], &condition(observed, &[])).await,
            Err(SqlitePushLogsError::StaleRead(_))
        ));
    }

    // Below the watermark, a window is whole only when the record before it was written within the cutoff, since the purge
    // removes the records older than the cutoff, the oldest first: as the readable log lower bound of distributed Chroma.
    #[tokio::test]
    async fn a_conditional_write_below_the_watermark_needs_its_window_kept() {
        let mut log = setup_sqlite_log()
            .await
            .with_purge_cutoff_time_seconds(3_600);
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let collection = CollectionUuid::new();
        log.note_conditional_use(collection).await.unwrap();
        log.push_logs(
            collection,
            vec![
                conditional_record("a"),
                conditional_record("b"),
                conditional_record("c"),
            ],
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE embeddings_queue SET created_at = datetime('now', '-7200 seconds') WHERE seq_id = 2",
        )
        .execute(log.db.get_conn())
        .await
        .unwrap();
        sqlx::query("DELETE FROM embeddings_queue WHERE seq_id = 1")
            .execute(log.db.get_conn())
            .await
            .unwrap();
        for (segment, scope) in [("m", "METADATA"), ("v", "VECTOR")] {
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment)
                .bind(segment)
                .bind(scope)
                .bind(collection.0.to_string())
                .execute(log.db.get_conn())
                .await
                .unwrap();
            sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, 4)")
                .bind(segment)
                .execute(log.db.get_conn())
                .await
                .unwrap();
        }
        // Record 3 is within the cutoff: the window from it is whole.
        push_conditional(&mut log, collection, &["d"], &condition(3, &[]))
            .await
            .unwrap();
        // Record 2 is older than the cutoff, and record 1 is gone, with nothing before it: the window may have lost records.
        for observed in [2, 1] {
            assert!(
                matches!(
                    push_conditional(&mut log, collection, &["e"], &condition(observed, &[])).await,
                    Err(SqlitePushLogsError::StaleRead(_))
                ),
                "offset {observed}"
            );
        }
    }

    async fn applied_up_to_for_the_tests(log: &SqliteLog, collection: CollectionUuid, seq_id: i64) {
        for (segment, scope) in [("m", "METADATA"), ("v", "VECTOR")] {
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment)
                .bind(segment)
                .bind(scope)
                .bind(collection.0.to_string())
                .execute(log.db.get_conn())
                .await
                .unwrap();
            sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, ?)")
                .bind(segment)
                .bind(seq_id)
                .execute(log.db.get_conn())
                .await
                .unwrap();
        }
    }

    async fn rows_of_the_tests(log: &SqliteLog) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue")
            .fetch_one(log.db.get_conn())
            .await
            .unwrap()
    }

    // The position of a collection is the lower of the watermarks of its segments, as max_purge_seq_id, and a segment with
    // no watermark has applied nothing.
    #[tokio::test]
    async fn the_position_of_a_collection_is_the_lower_watermark() {
        let log = setup_sqlite_log().await;
        let collection = CollectionUuid::new();
        for (segment, scope) in [("m", "METADATA"), ("v", "VECTOR")] {
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(segment)
                .bind(segment)
                .bind(scope)
                .bind(collection.0.to_string())
                .execute(log.db.get_conn())
                .await
                .unwrap();
        }
        let watermark = |segment: &'static str, seq_id: i64| {
            let pool = log.db.get_conn().clone();
            async move {
                sqlx::query("INSERT OR REPLACE INTO max_seq_id (segment_id, seq_id) VALUES (?, ?)")
                    .bind(segment)
                    .bind(seq_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        };
        watermark("m", 4).await;
        assert_eq!(log.log_position(collection).await.unwrap(), 0);
        watermark("v", 2).await;
        assert_eq!(log.log_position(collection).await.unwrap(), 2);
    }

    // A conditional write takes the write lock before its check: a write that holds the lock and commits meanwhile falls in
    // the check, and the conditional write conflicts with it, as the admission of the log service of distributed Chroma.
    #[tokio::test]
    async fn a_conditional_write_checks_with_the_write_lock() {
        let mut log = setup_sqlite_log().await;
        let collection = CollectionUuid::new();
        log.push_logs(collection, vec![conditional_record("a")])
            .await
            .unwrap();
        let observed = log.scout_logs(collection, 0).await.unwrap() as i64;
        let mut writer = log.db.get_conn().acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *writer)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO embeddings_queue (operation, topic, id, vector, encoding, metadata) \
             SELECT operation, topic, id, vector, encoding, metadata FROM embeddings_queue WHERE id = 'a'",
        )
        .execute(&mut *writer)
        .await
        .unwrap();
        let mut waiting = log.clone();
        let write = tokio::spawn(async move {
            push_conditional(
                &mut waiting,
                collection,
                &["b"],
                &condition(observed, &["a"]),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
        assert!(matches!(
            write.await.unwrap(),
            Err(SqlitePushLogsError::ConditionalWriteConflict)
        ));
    }

    // The log keeps the records applied only of a collection that conditional transactions use; the others purge them at
    // once, as Chroma 1.5.9.
    #[tokio::test]
    async fn the_log_keeps_the_records_only_of_the_collections_in_use() {
        let mut log = setup_sqlite_log().await;
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let (in_use, other) = (CollectionUuid::new(), CollectionUuid::new());
        log.note_conditional_use(in_use).await.unwrap();
        assert!(log.used_by_conditional_transactions(in_use));
        assert!(!log.used_by_conditional_transactions(other));
        log.push_logs(in_use, vec![conditional_record("a")])
            .await
            .unwrap();
        log.push_logs(other, vec![conditional_record("b")])
            .await
            .unwrap();
        assert!(!log.has_purge_work(in_use, 3).await.unwrap());
        log.purge_logs(in_use, 3).await.unwrap();
        assert_eq!(rows_of_the_tests(&log).await, 2);
        assert!(log.has_purge_work(other, 3).await.unwrap());
        log.purge_logs(other, 3).await.unwrap();
        assert_eq!(rows_of_the_tests(&log).await, 1);
    }

    // Records may have been purged at once before the first use of a collection, as by a process before a restart: a commit
    // whose window starts below the watermark of that first use is refused, and the client starts again.
    #[tokio::test]
    async fn a_commit_older_than_the_first_use_of_the_collection_is_refused() {
        let mut log = setup_sqlite_log().await;
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let collection = CollectionUuid::new();
        log.push_logs(
            collection,
            vec![
                conditional_record("a"),
                conditional_record("b"),
                conditional_record("c"),
            ],
        )
        .await
        .unwrap();
        applied_up_to_for_the_tests(&log, collection, 3).await;
        assert!(matches!(
            push_conditional(&mut log, collection, &["d"], &condition(2, &[])).await,
            Err(SqlitePushLogsError::StaleRead(_))
        ));
        // From the watermark on, nothing was purged.
        push_conditional(&mut log, collection, &["d"], &condition(3, &[]))
            .await
            .unwrap();
    }

    // A use ends a cutoff after its last read or write: the purges go back to at once, and the next use is a first use again.
    #[tokio::test]
    async fn a_use_that_ended_is_a_first_use_again() {
        let mut log = setup_sqlite_log().await.with_purge_cutoff_time_seconds(1);
        // As a server that starts on an empty log, which purges it.
        log.get_legacy_embeddings_queue_config().await.unwrap();
        let collection = CollectionUuid::new();
        log.note_conditional_use(collection).await.unwrap();
        log.push_logs(
            collection,
            vec![conditional_record("a"), conditional_record("b")],
        )
        .await
        .unwrap();
        applied_up_to_for_the_tests(&log, collection, 3).await;
        // Written within the cutoff, in use: they stay, and a window from them is whole.
        log.purge_logs(collection, 3).await.unwrap();
        assert_eq!(rows_of_the_tests(&log).await, 2);
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        // The use ended: the purge takes them, and it stays used for the timer of its purges.
        log.purge_logs(collection, 3).await.unwrap();
        assert_eq!(rows_of_the_tests(&log).await, 0);
        assert!(log.used_by_conditional_transactions(collection));
        // A record of another collection, written just now at the offset, says nothing of the window after it.
        let other = CollectionUuid::new();
        log.push_logs(other, vec![conditional_record("z")])
            .await
            .unwrap();
        assert!(matches!(
            push_conditional(&mut log, collection, &["c"], &condition(2, &[])).await,
            Err(SqlitePushLogsError::StaleRead(_))
        ));
    }

    // A database in memory has one connection, which the transaction of a conditional write holds: the write reads
    // nothing else through the pool, and answers at once, not after the timeout of the pool (30 s).
    #[tokio::test]
    async fn a_conditional_write_on_a_database_in_memory_answers_at_once() {
        let db = SqliteDb::try_from_config(
            &SqliteDBConfig {
                url: None,
                migration_mode: chroma_sqlite::config::MigrationMode::Apply,
                hash_type: chroma_sqlite::config::MigrationHash::SHA256,
                ..Default::default()
            },
            &Registry::new(),
        )
        .await
        .unwrap();
        let mut log = SqliteLog::new(db, "default".to_string(), "default".to_string());
        let collection = CollectionUuid::new();
        let observed = log.scout_logs(collection, 0).await.unwrap() as i64;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            push_conditional(&mut log, collection, &["a"], &condition(observed, &["a"])),
        )
        .await
        .expect("a conditional write on a database in memory waited for the pool")
        .unwrap();
    }

    #[tokio::test]
    async fn test_sparse_invalid_batch_is_rejected_before_log_insertion() {
        let mut log = setup_sqlite_log().await;
        let collection = CollectionUuid::new();
        for operation in [Operation::Add, Operation::Update, Operation::Upsert] {
            for weight in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                let record = |id: &str, metadata| OperationRecord {
                    id: id.into(),
                    embedding: Some(vec![1.0, 2.0]),
                    encoding: Some(ScalarEncoding::FLOAT32),
                    metadata,
                    document: None,
                    operation,
                };
                let sparse = chroma_types::SparseVector::new(vec![1], vec![weight]).unwrap();
                let error = log
                    .push_logs(
                        collection,
                        vec![
                            record("ordinary", None),
                            record(
                                "invalid",
                                Some(UpdateMetadata::from([(
                                    "sparse".into(),
                                    UpdateMetadataValue::SparseVector(sparse),
                                )])),
                            ),
                        ],
                    )
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), ErrorCodes::InvalidArgument);
                assert!(log.read(collection, 0, 100, None).await.unwrap().is_empty());
            }
        }
        log.push_logs(
            collection,
            vec![OperationRecord {
                id: "valid".into(),
                embedding: Some(vec![1.0, 2.0]),
                encoding: Some(ScalarEncoding::FLOAT32),
                metadata: Some(UpdateMetadata::from([(
                    "sparse".into(),
                    UpdateMetadataValue::SparseVector(
                        chroma_types::SparseVector::new(vec![1], vec![-2.0]).unwrap(),
                    ),
                )])),
                document: None,
                operation: Operation::Add,
            }],
        )
        .await
        .unwrap();
        let accepted = log.read(collection, 0, 100, None).await.unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].record.id, "valid");
    }

    #[tokio::test]
    async fn test_log_offset() {
        let mut log = setup_sqlite_log().await;

        let collection_id = CollectionUuid::new();

        // TODO: remove this when there's a sysdb implementation in Rust
        sqlx::query(
            r#"
            INSERT INTO segments (id, type, scope, collection) VALUES ('foo', 'foo', 'foo', ?);
            INSERT INTO collections (id, name, database_id) VALUES (?, 'foo', 0);
        "#,
        )
        .bind(collection_id.0.to_string())
        .bind(collection_id.0.to_string())
        .execute(log.db.get_conn())
        .await
        .unwrap();

        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 0);

        // Push a log
        let operations = vec![OperationRecord {
            id: "id".to_string(),
            embedding: Some(vec![1.0, 2.0, 3.0]),
            encoding: Some(ScalarEncoding::FLOAT32),
            metadata: None,
            document: None,
            operation: Operation::Add,
        }];
        log.push_logs(collection_id, operations).await.unwrap();

        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 1);

        let collections_with_data = log.get_collections_with_new_data(1).await.unwrap();
        assert_eq!(collections_with_data.len(), 0);

        // Update log offset
        log.update_collection_log_offset(collection_id, 0)
            .await
            .unwrap();
        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 1);

        log.update_collection_log_offset(collection_id, 1)
            .await
            .unwrap();
        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 0);
    }

    #[tokio::test]
    async fn test_float_metadata_roundtrip() {
        let mut log = setup_sqlite_log().await;
        let collection_id = CollectionUuid::new();
        // This value loses one bit with serde_json's default float parser,
        // which causes equality filters to miss the stored record.
        let value = -1004.1783447265625;
        let metadata =
            UpdateMetadata::from([("value".to_string(), UpdateMetadataValue::Float(value))]);
        log.push_logs(
            collection_id,
            vec![OperationRecord {
                id: "id".to_string(),
                embedding: Some(vec![1.0, 2.0, 3.0]),
                encoding: Some(ScalarEncoding::FLOAT32),
                metadata: Some(metadata.clone()),
                document: None,
                operation: Operation::Add,
            }],
        )
        .await
        .unwrap();

        let records = log.read(collection_id, 0, 1, None).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.metadata.as_ref(), Some(&metadata));
    }

    proptest! {
        #[test]
         fn test_push_pull_logs(
            read_offset in 0usize..=100,
            batch_size in 0usize..=100,
            operations in proptest::collection::vec(any::<OperationRecord>(), 0..100)
        ) {
            let runtime = Runtime::new().unwrap();

            runtime.block_on(async {
                let mut log = setup_sqlite_log().await;

                let collection_id = CollectionUuid::new();
                log.push_logs(collection_id, operations.clone()).await.unwrap();

                let read_logs = log.read(collection_id, read_offset as i64, batch_size as i32, None)
                    .await
                    .unwrap();

                let expected_length = batch_size.min(operations.len().saturating_sub(read_offset));

                assert_eq!(read_logs.len(), expected_length);

                for i in 0..expected_length {
                    let operation = &operations[i + read_offset];
                    let log = &read_logs[i];

                    let expected_metadata = operation.metadata.clone().unwrap_or_default();
                    let received_metadata = log.record.metadata.clone().unwrap();

                    assert!(log.record.id == operation.id);
                    assert!(log.record.embedding == operation.embedding);
                    assert!(log.record.encoding == operation.encoding);
                    assert!(
                        are_update_metadatas_close_to_equal(
                            &received_metadata,
                            &expected_metadata
                        ),
                        "{:?} != {:?}",
                        received_metadata,
                        expected_metadata
                    );
                    assert!(log.record.document == operation.document);
                    assert!(log.record.operation == operation.operation);
                }
            });
        }
    }

    // Records to add, one for each number.
    fn adds(numbers: std::ops::Range<usize>) -> Vec<OperationRecord> {
        numbers
            .map(|i| OperationRecord {
                id: format!("r{i}"),
                embedding: Some(vec![i as f32, 1.0]),
                encoding: Some(ScalarEncoding::FLOAT32),
                metadata: None,
                document: None,
                operation: Operation::Add,
            })
            .collect()
    }

    // The tail of a collection starts after the position given and ends at the last one given, both positions of the log
    // shared by every collection; with a limit, it takes that many records of the collection, the first ones. Its count
    // goes to the last record written. The records of the other collections stay out.
    #[tokio::test]
    async fn the_tail_of_a_collection_is_between_its_positions() {
        let mut log = setup_sqlite_log().await;
        let collection = CollectionUuid::new();
        let other = CollectionUuid::new();
        // Positions 1 to 3 for the collection, 4 and 5 for the other, 6 and 7 for the collection.
        log.push_logs(collection, adds(0..3)).await.unwrap();
        log.push_logs(other, adds(0..2)).await.unwrap();
        log.push_logs(collection, adds(3..5)).await.unwrap();
        let mut connection = log.db.get_conn().acquire().await.unwrap();
        for (up_to, limit, expected) in [
            (6, None, vec![(3, "r2"), (6, "r3")]),
            (7, Some(2), vec![(3, "r2"), (6, "r3")]),
            (7, Some(5), vec![(3, "r2"), (6, "r3"), (7, "r4")]),
            (7, Some(0), vec![]),
        ] {
            let tail = log
                .read_tail(&mut connection, collection, 2, up_to, limit)
                .await
                .unwrap()
                .into_iter()
                .map(|record| (record.log_offset, record.record.id))
                .collect::<Vec<_>>();
            let expected = expected
                .into_iter()
                .map(|(offset, id)| (offset, id.to_string()))
                .collect::<Vec<_>>();
            assert_eq!(tail, expected, "up to {up_to}, limit {limit:?}");
        }
        assert_eq!(
            log.count_tail(&mut connection, collection, 2)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            log.count_tail(&mut connection, collection, 7)
                .await
                .unwrap(),
            0
        );
        assert_eq!(log.count_tail(&mut connection, other, 0).await.unwrap(), 2);
    }

    // A SPANN collection is to flush with at least min_compaction_size records after its last flush, or with the oldest of
    // them waiting at least the timeout, as should_compact_at of the log service of distributed Chroma: the records up to
    // its last flush do not count, nor do the collections of the other types of index.
    #[tokio::test]
    async fn the_spann_collections_to_compact_are_over_a_threshold() {
        let mut log = setup_sqlite_log().await;
        let day = Duration::from_secs(86_400);
        let spann = CollectionUuid::new();
        let hnsw = CollectionUuid::new();
        let spann_segment = format!("{}-vector", spann.0);
        sqlx::query(
            "INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, 'VECTOR', ?), (?, ?, 'VECTOR', ?)",
        )
        .bind(&spann_segment)
        .bind(String::from(SegmentType::Spann))
        .bind(spann.0.to_string())
        .bind(format!("{}-vector", hnsw.0))
        .bind(String::from(SegmentType::HnswLocalPersisted))
        .bind(hnsw.0.to_string())
        .execute(log.db.get_conn())
        .await
        .unwrap();
        log.push_logs(spann, adds(0..3)).await.unwrap();
        log.push_logs(hnsw, adds(0..5)).await.unwrap();
        assert_eq!(
            log.get_spann_collections_to_compact(3, day).await.unwrap(),
            vec![spann]
        );
        assert!(log
            .get_spann_collections_to_compact(4, day)
            .await
            .unwrap()
            .is_empty());
        // A flush up to the second record leaves the third after it.
        sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES (?, 2)")
            .bind(&spann_segment)
            .execute(log.db.get_conn())
            .await
            .unwrap();
        assert!(log
            .get_spann_collections_to_compact(2, day)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            log.get_spann_collections_to_compact(1, day).await.unwrap(),
            vec![spann]
        );
        // The third record waited a day.
        sqlx::query(
            "UPDATE embeddings_queue SET created_at = datetime('now', '-1 day', '-1 minute')",
        )
        .execute(log.db.get_conn())
        .await
        .unwrap();
        assert_eq!(
            log.get_spann_collections_to_compact(10, day).await.unwrap(),
            vec![spann]
        );
        assert!(log
            .get_spann_collections_to_compact(10, day * 2)
            .await
            .unwrap()
            .is_empty());
    }
}
