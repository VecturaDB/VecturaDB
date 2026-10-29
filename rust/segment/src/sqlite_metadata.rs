// Changed from the original Chroma file; see the git history for the changes and their authors.
use std::{
    collections::{BTreeMap, HashMap},
    num::TryFromIntError,
};

use crate::local_hnsw::{LocalHnswSegmentReader, LocalHnswSegmentReaderError};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sqlite::{
    db::SqliteDb,
    helpers::{
        delete_array_metadata, delete_array_metadata_keys, delete_metadata, update_metadata,
        upsert_array_metadata,
    },
    table::{
        Collections, EmbeddingFulltextSearch, EmbeddingMetadata, EmbeddingMetadataArray,
        Embeddings, MaxSeqId,
    },
};
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, Limit, Projection, ProjectionOutput, ProjectionRecord, Scan,
    },
    plan::{Count, Get},
    BooleanOperator, Chunk, CollectionUuid, CompositeExpression, ContainsOperator,
    DocumentExpression, DocumentOperator, LogRecord, Metadata, MetadataComparison,
    MetadataExpression, MetadataSetValue, MetadataValue, MetadataValueConversionError, Operation,
    OperationRecord, PrimitiveOperator, Schema, SegmentUuid, SetOperator, UpdateMetadata,
    UpdateMetadataValue, Where, CHROMA_DOCUMENT_KEY,
};
use sea_query::{
    Alias, DeleteStatement, Expr, ExprTrait, Func, InsertStatement, LikeExpr, OnConflict, Query,
    SimpleExpr, SqliteQueryBuilder, UpdateStatement,
};
use sea_query_binder::SqlxBinder;
use sqlx::{Row, Sqlite, Transaction};
use thiserror::Error;

const SUBQ_ALIAS: &str = "filter_limit_subq";

#[derive(Debug, Error)]
pub enum SqliteMetadataError {
    #[error("Invalid log offset: {0}")]
    LogOffset(#[from] TryFromIntError),
    #[error("Invalid metadata value: {0}")]
    MetadataValue(#[from] MetadataValueConversionError),
    #[error("Could not update metadata table: {0}")]
    UpdateMetadata(#[from] chroma_sqlite::helpers::MetadataError),
    #[error(transparent)]
    SeaQuery(#[from] sea_query::error::Error),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("Error reading the local HNSW index: {0}")]
    LocalHnsw(Box<LocalHnswSegmentReaderError>),
    #[error("Error reading phantom record: {0}")]
    PhantomRecord(u32),
    #[error("Error reading the dense vectors: {0}")]
    DenseVectors(Box<dyn ChromaError>),
    #[error("No embedding for the record {0}")]
    MissingEmbedding(String),
    #[error("Could not encode or decode sparse metadata: {0}")]
    SparseMetadataJson(serde_json::Error),
    #[error("Could not read the files of a segment: {0}")]
    SegmentFiles(serde_json::Error),
    #[error("Could not serialize schema: {0}")]
    SerializeSchema(#[from] serde_json::Error),
}

impl ChromaError for SqliteMetadataError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::LocalHnsw(err) => err.code(),
            Self::DenseVectors(err) => err.code(),
            _ => ErrorCodes::Internal,
        }
    }
}

/// The dense vectors of a collection for the search: its local HNSW index, or on a single node the SPANN index of a
/// collection that has one.
#[async_trait::async_trait]
pub trait DenseVectorReader: Send + Sync {
    /// The `limit` nearest records of the query, among the allowed user ids or all the records without them: by offset
    /// in the index, user id and distance.
    async fn nearest(
        &self,
        collection: &chroma_types::CollectionAndSegments,
        query: Vec<f32>,
        allowed_user_ids: Option<&[String]>,
        limit: u32,
    ) -> Result<Vec<(u32, String, f32)>, SqliteMetadataError>;

    /// The embedding of a record, by user id.
    async fn embedding(&self, user_id: &str) -> Result<Vec<f32>, SqliteMetadataError>;
}

/// What SQLite answers for one payload of a search of a SPANN collection on the single node
/// ([`SqliteMetadataReader::spann_search_sqlite_at`]), by user id.
#[derive(Clone, Debug, Default)]
pub struct SpannSearchSqlite {
    /// When the payload has a filter and a dense KNN leaf, the user ids of the records that pass the filter, in storage
    /// order: the dense leaves look only among them.
    pub allowed_user_ids: Option<Vec<String>>,
    /// One entry for each KNN leaf of the rank expression, in its DFS order: for a sparse leaf its candidates by user
    /// id and measure, in the order of sparse_knn; None for a dense leaf.
    pub sparse: Vec<Option<Vec<(String, f32)>>>,
    /// Without a rank expression, the page of the records that pass the filter, in storage order.
    pub unranked: Option<Vec<String>>,
}

/// A dense KNN leaf on the local HNSW index (rust/worker/src/execution/operators/knn_hnsw.rs).
#[async_trait::async_trait]
impl DenseVectorReader for LocalHnswSegmentReader {
    async fn nearest(
        &self,
        collection: &chroma_types::CollectionAndSegments,
        query: Vec<f32>,
        allowed_user_ids: Option<&[String]>,
        limit: u32,
    ) -> Result<Vec<(u32, String, f32)>, SqliteMetadataError> {
        // The HNSW index knows the records by label.
        let mut allowed_labels = Vec::new();
        for user_id in allowed_user_ids.unwrap_or_default() {
            allowed_labels.push(
                self.get_offset_id_by_user_id(user_id)
                    .await
                    .map_err(|err| SqliteMetadataError::LocalHnsw(Box::new(err)))?,
            );
        }
        let space = collection
            .collection
            .schema
            .as_ref()
            .map(|schema| {
                schema.get_internal_hnsw_config_with_legacy_fallback(&collection.vector_segment)
            })
            .transpose()
            .map_err(|err| {
                SqliteMetadataError::LocalHnsw(Box::new(
                    LocalHnswSegmentReaderError::InvalidHnswConfiguration(err),
                ))
            })?
            .flatten()
            .ok_or(SqliteMetadataError::LocalHnsw(Box::new(
                LocalHnswSegmentReaderError::MissingHnswConfiguration,
            )))?
            .space;
        let query = if let chroma_types::Space::Cosine = space {
            chroma_distance::normalize(&query)
        } else {
            query
        };
        let neighbors = self
            .query_embedding(&allowed_labels, query, limit)
            .await
            .map_err(|err| SqliteMetadataError::LocalHnsw(Box::new(err)))?;
        let mut nearest = Vec::with_capacity(neighbors.len());
        for neighbor in neighbors {
            let user_id = self
                .get_user_id_by_offset_id(neighbor.offset_id)
                .await
                .map_err(|err| SqliteMetadataError::LocalHnsw(Box::new(err)))?;
            nearest.push((neighbor.offset_id, user_id, neighbor.measure));
        }
        Ok(nearest)
    }

    async fn embedding(&self, user_id: &str) -> Result<Vec<f32>, SqliteMetadataError> {
        self.get_embedding_by_user_id(&user_id.to_string())
            .await
            .map_err(|err| SqliteMetadataError::LocalHnsw(Box::new(err)))
    }
}

/// A read of the metadata at one moment: see SqliteMetadataReader::snapshot. The single node keeps the rollback journal
/// of SQLite, which sqlx does not change, so the read transaction holds its shared lock until it ends, and the commit of
/// a writer waits for it. A writer that waits keeps new readers out: a second connection that reads while a snapshot is
/// open waits for the writer that waits for the snapshot, up to the busy timeout of 1000 s (rust/sqlite/src/config.rs).
/// Every read inside a snapshot goes through it.
pub struct MetadataSnapshot {
    tx: Transaction<'static, Sqlite>,
}

impl MetadataSnapshot {
    /// The end of the snapshot, after which writers commit.
    pub async fn close(self) -> Result<(), SqliteMetadataError> {
        self.tx.commit().await?;
        Ok(())
    }

    /// The files of the segments of a collection, by segment id, as its last flush recorded them.
    pub async fn segment_files(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<HashMap<String, HashMap<String, Vec<String>>>, SqliteMetadataError> {
        let rows = sqlx::query("SELECT id, file_paths FROM segments WHERE collection = ?")
            .bind(collection_id.to_string())
            .fetch_all(&mut *self.tx)
            .await?;
        let mut files = HashMap::with_capacity(rows.len());
        for row in rows {
            let file_paths: String = row.try_get(1)?;
            files.insert(
                row.try_get(0)?,
                serde_json::from_str(&file_paths).map_err(SqliteMetadataError::SegmentFiles)?,
            );
        }
        Ok(files)
    }
}

pub struct SqliteMetadataWriter {
    pub db: SqliteDb,
}

pub struct ApplyLogsOutcome {
    pub schema_update: Option<Schema>,
}

impl SqliteMetadataWriter {
    pub fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    fn add_record_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(Embeddings::Table)
            .columns([
                Embeddings::SegmentId,
                Embeddings::EmbeddingId,
                Embeddings::SeqId,
            ])
            .values([segment_id.to_string().into(), user_id.into(), seq_id.into()])?
            .on_conflict(
                OnConflict::columns([Embeddings::SegmentId, Embeddings::EmbeddingId])
                    .do_nothing()
                    .to_owned(),
            )
            .returning_col(Embeddings::Id)
            .to_owned())
    }

    async fn add_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (add_rec_stmt, values) =
            Self::add_record_stmt(segment_id, seq_id, user_id)?.build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&add_rec_stmt, values)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get(0))
            .transpose()?)
    }

    fn update_record_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> UpdateStatement {
        Query::update()
            .table(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(segment_id.to_string())
                    .and(Expr::col(Embeddings::EmbeddingId).eq(user_id)),
            )
            .value(Embeddings::SeqId, seq_id)
            .returning_col(Embeddings::Id)
            .to_owned()
    }

    async fn update_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (update_rec_stmt, values) =
            Self::update_record_stmt(segment_id, seq_id, user_id).build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&update_rec_stmt, values)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get(0))
            .transpose()?)
    }

    fn upsert_record_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Self::add_record_stmt(segment_id, seq_id, user_id)?
            .on_conflict(
                OnConflict::columns([Embeddings::SegmentId, Embeddings::EmbeddingId])
                    .update_columns([Embeddings::SeqId])
                    .to_owned(),
            )
            .to_owned())
    }

    async fn upsert_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<u32, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (upsert_rec_stmt, values) =
            Self::upsert_record_stmt(segment_id, seq_id, user_id)?.build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&upsert_rec_stmt, values)
            .fetch_one(&mut *tx)
            .await?
            .try_get(0)?)
    }

    fn delete_record_stmt(segment_id: SegmentUuid, user_id: String) -> DeleteStatement {
        Query::delete()
            .from_table(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(segment_id.to_string())
                    .and(Expr::col(Embeddings::EmbeddingId).eq(user_id)),
            )
            .returning_col(Embeddings::Id)
            .to_owned()
    }

    async fn delete_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (delete_rec_stmt, values) =
            Self::delete_record_stmt(segment_id, user_id).build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&delete_rec_stmt, values)
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.try_get(0))
            .transpose()?)
    }

    fn add_document_stmt(
        id: u32,
        document: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(EmbeddingFulltextSearch::Table)
            .columns([
                EmbeddingFulltextSearch::Rowid,
                EmbeddingFulltextSearch::StringValue,
            ])
            .values([id.into(), document.into()])?
            .to_owned())
    }

    async fn add_document<C>(
        tx: &mut C,
        id: u32,
        document: String,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (add_doc_stmt, values) =
            Self::add_document_stmt(id, document)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&add_doc_stmt, values)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    fn delete_document_stmt(id: u32) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingFulltextSearch::Table)
            .and_where(Expr::col(EmbeddingFulltextSearch::Rowid).eq(id))
            .to_owned()
    }

    async fn delete_document<C>(tx: &mut C, id: u32) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (delete_doc_stmt, values) =
            Self::delete_document_stmt(id).build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&delete_doc_stmt, values)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    fn upsert_max_seq_id_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(MaxSeqId::Table)
            .columns([MaxSeqId::SegmentId, MaxSeqId::SeqId])
            .values([segment_id.to_string().into(), seq_id.into()])?
            .on_conflict(
                OnConflict::column(MaxSeqId::SegmentId)
                    .update_column(MaxSeqId::SeqId)
                    .to_owned(),
            )
            .to_owned())
    }

    async fn upsert_max_seq_id<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (upsert_max_seq_id_stmt, values) =
            Self::upsert_max_seq_id_stmt(segment_id, seq_id)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&upsert_max_seq_id_stmt, values)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, SqliteMetadataError> {
        Ok(self.db.get_conn().begin().await?)
    }

    pub async fn update_collection_schema<C>(
        &self,
        collection_id: CollectionUuid,
        schema: &Schema,
        tx: &mut C,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let schema_str = serde_json::to_string(schema)?;
        let (sql, values) = Query::update()
            .table(Collections::Table)
            .value(Collections::SchemaStr, schema_str)
            .and_where(
                Expr::col((Collections::Table, Collections::Id)).eq(collection_id.to_string()),
            )
            .build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&sql, values).execute(&mut *tx).await?;
        Ok(())
    }

    fn ensure_schema_for_update_value(
        schema: &mut Option<Schema>,
        key: &str,
        value: &UpdateMetadataValue,
    ) -> bool {
        match value {
            UpdateMetadataValue::None => false,
            _ => {
                if let Some(schema_mut) = schema.as_mut() {
                    if let Ok(metadata_value) = MetadataValue::try_from(value) {
                        return schema_mut
                            .ensure_key_from_metadata(key, metadata_value.value_type());
                    }
                }
                false
            }
        }
    }

    /// Split `UpdateMetadata` into scalar values (for `embedding_metadata`)
    /// and a resolved `Metadata` map containing only array values (for
    /// `embedding_metadata_array`).  Keys set to `None` are collected
    /// separately so they can be deleted from both tables.
    fn split_metadata(
        meta: UpdateMetadata,
    ) -> (
        UpdateMetadata, // scalar (+ None) entries for the original table
        Metadata,       // array entries only
        Vec<String>,    // keys set to None (need deletion from array table too)
    ) {
        let mut scalars: UpdateMetadata = HashMap::new();
        let mut arrays: Metadata = HashMap::new();
        let mut deleted_keys: Vec<String> = Vec::new();
        for (key, value) in meta {
            match &value {
                UpdateMetadataValue::BoolArray(_)
                | UpdateMetadataValue::IntArray(_)
                | UpdateMetadataValue::FloatArray(_)
                | UpdateMetadataValue::StringArray(_) => {
                    // Safe: we just matched array variants.
                    if let Ok(mv) = MetadataValue::try_from(&value) {
                        arrays.insert(key, mv);
                    }
                }
                UpdateMetadataValue::None => {
                    // Delete from both tables.
                    deleted_keys.push(key.clone());
                    scalars.insert(key, value);
                }
                _ => {
                    scalars.insert(key, value);
                }
            }
        }
        (scalars, arrays, deleted_keys)
    }

    async fn write_metadata<C>(
        tx: &mut C,
        offset_id: u32,
        meta: UpdateMetadata,
        segment_id: SegmentUuid,
        sparse_presence: &mut Option<bool>,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        // Preserve the full sparse value (including tokens and empty presence)
        // in the same transaction as record identity and the replay watermark.
        let mut meta = meta;
        let mut sparse = Vec::new();
        for (key, value) in &meta {
            if let UpdateMetadataValue::SparseVector(vector) = value {
                vector.validate()?;
                if vector.values.iter().any(|value| !value.is_finite()) {
                    return Err(MetadataValueConversionError::InvalidValue.into());
                }
                sparse.push((
                    key.clone(),
                    serde_json::to_string(vector)
                        .map_err(SqliteMetadataError::SparseMetadataJson)?,
                ));
            }
        }
        // Resolve absence once per apply_logs call, after add/update/upsert has
        // acquired the write lock. An earlier read in a deferred transaction
        // would introduce a read-to-write lock upgrade race.
        if !meta.is_empty() && sparse_presence.is_none() {
            *sparse_presence = Some(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM embedding_metadata_sparse AS sparse JOIN embeddings ON embeddings.id = sparse.id WHERE embeddings.segment_id = ?)",
            ).bind(segment_id.to_string()).fetch_one(&mut *tx).await?);
        }
        // A proven-absent segment needs no sparse DELETE per ordinary record.
        // Once true, remain conservative through later removals in this chunk.
        if sparse_presence.unwrap_or(false) {
            // Every explicitly supplied key replaces/removes its prior sparse form.
            // Omitted keys are untouched. Foreign keys are disabled in local SQLite.
            let keys: Vec<_> = meta.keys().collect();
            // Bound parameters independently of record metadata width.
            for keys in keys.chunks(500) {
                let mut query = sqlx::QueryBuilder::<Sqlite>::new(
                    "DELETE FROM embedding_metadata_sparse WHERE id = ",
                );
                query.push_bind(offset_id).push(" AND key IN (");
                let mut values = query.separated(", ");
                for key in keys {
                    values.push_bind(*key);
                }
                values.push_unseparated(")");
                query.build().execute(&mut *tx).await?;
            }
        }
        if !sparse.is_empty() {
            *sparse_presence = Some(true);
        }
        for (key, value) in sparse {
            sqlx::query("INSERT INTO embedding_metadata_sparse (id, key, value) VALUES (?, ?, ?)")
                .bind(offset_id)
                .bind(&key)
                .bind(value)
                .execute(&mut *tx)
                .await?;
            // None clears the old scalar and array representations of this key.
            meta.insert(key, UpdateMetadataValue::None);
        }
        let (mut scalars, arrays, deleted_keys) = Self::split_metadata(meta);

        // Keys becoming arrays must be removed from the scalar table.
        for key in arrays.keys() {
            scalars.insert(key.clone(), UpdateMetadataValue::None);
        }

        // Keys becoming scalars (or explicitly deleted) must be removed from the array table.
        let mut array_keys_to_delete = deleted_keys;
        for (key, val) in &scalars {
            if !matches!(val, UpdateMetadataValue::None) {
                array_keys_to_delete.push(key.clone());
            }
        }

        if !scalars.is_empty() {
            update_metadata::<EmbeddingMetadata, _, _>(tx, offset_id, scalars).await?;
        }

        if !array_keys_to_delete.is_empty() {
            delete_array_metadata_keys(tx, offset_id, &array_keys_to_delete).await?;
        }

        if !arrays.is_empty() {
            upsert_array_metadata(tx, offset_id, &arrays).await?;
        }

        Ok(())
    }

    pub async fn apply_logs<C>(
        &self,
        logs: Chunk<LogRecord>,
        segment_id: SegmentUuid,
        schema: Option<Schema>,
        tx: &mut C,
    ) -> Result<ApplyLogsOutcome, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        if logs.is_empty() {
            return Ok(ApplyLogsOutcome {
                schema_update: None,
            });
        }
        let mut schema = schema;
        let mut schema_modified = false;
        let mut sparse_presence = None;
        let mut max_seq_id = u64::MIN;
        for (
            LogRecord {
                log_offset,
                record:
                    OperationRecord {
                        id,
                        metadata,
                        document,
                        operation,
                        ..
                    },
            },
            _,
        ) in logs.iter()
        {
            let log_offset_unsigned = (*log_offset).try_into()?;
            max_seq_id = max_seq_id.max(log_offset_unsigned);
            let mut metadata_owned = metadata.clone();
            if let Some(doc) = document {
                let mut doc_embedded_meta = metadata_owned.unwrap_or_default();
                doc_embedded_meta.insert(
                    CHROMA_DOCUMENT_KEY.to_string(),
                    UpdateMetadataValue::Str(doc.clone()),
                );
                metadata_owned = Some(doc_embedded_meta);
            }
            match operation {
                Operation::BackfillFn => {
                    continue;
                }
                Operation::Add => {
                    if let Some(offset_id) =
                        Self::add_record(tx, segment_id, log_offset_unsigned, id.clone()).await?
                    {
                        if let Some(meta) = metadata_owned {
                            for (key, value) in meta.iter() {
                                if Self::ensure_schema_for_update_value(&mut schema, key, value) {
                                    schema_modified = true;
                                }
                            }
                            Self::write_metadata(
                                tx,
                                offset_id,
                                meta,
                                segment_id,
                                &mut sparse_presence,
                            )
                            .await?;
                        }

                        if let Some(doc) = document {
                            Self::add_document(tx, offset_id, doc.clone()).await?;
                        }
                    }
                }
                Operation::Update => {
                    if let Some(offset_id) =
                        Self::update_record(tx, segment_id, log_offset_unsigned, id.clone()).await?
                    {
                        if let Some(meta) = metadata_owned {
                            for (key, value) in meta.iter() {
                                if Self::ensure_schema_for_update_value(&mut schema, key, value) {
                                    schema_modified = true;
                                }
                            }
                            Self::write_metadata(
                                tx,
                                offset_id,
                                meta,
                                segment_id,
                                &mut sparse_presence,
                            )
                            .await?;
                        }

                        if let Some(doc) = document {
                            Self::delete_document(tx, offset_id).await?;
                            Self::add_document(tx, offset_id, doc.clone()).await?;
                        }
                    }
                }
                Operation::Upsert => {
                    let offset_id =
                        Self::upsert_record(tx, segment_id, log_offset_unsigned, id.clone())
                            .await?;

                    if let Some(meta) = metadata_owned {
                        for (key, value) in meta.iter() {
                            if Self::ensure_schema_for_update_value(&mut schema, key, value) {
                                schema_modified = true;
                            }
                        }
                        Self::write_metadata(tx, offset_id, meta, segment_id, &mut sparse_presence)
                            .await?;
                    }

                    if let Some(doc) = document {
                        Self::delete_document(tx, offset_id).await?;
                        Self::add_document(tx, offset_id, doc.clone()).await?;
                    }
                }
                Operation::Delete => {
                    if let Some(offset_id) = Self::delete_record(tx, segment_id, id.clone()).await?
                    {
                        delete_metadata::<EmbeddingMetadata, _, _>(tx, offset_id).await?;
                        delete_array_metadata(tx, offset_id).await?;
                        sqlx::query("DELETE FROM embedding_metadata_sparse WHERE id = ?")
                            .bind(offset_id)
                            .execute(&mut *tx)
                            .await?;
                        Self::delete_document(tx, offset_id).await?;
                    }
                }
            }
        }

        Self::upsert_max_seq_id(tx, segment_id, max_seq_id).await?;

        Ok(ApplyLogsOutcome {
            schema_update: if schema_modified { schema } else { None },
        })
    }
}

/// Whether the sparse index of a key weights queries by IDF (BM25): the setting of the key, else the default, as the
/// sparse KNN of distributed Chroma reads it (rust/worker/src/execution/orchestration/sparse_knn.rs:220-240).
pub fn sparse_bm25_enabled(schema: &Schema, key: &str) -> bool {
    schema
        .keys
        .get(key)
        .and_then(|types| types.sparse_vector.as_ref())
        .and_then(|value| value.sparse_vector_index.as_ref())
        .and_then(|index| index.config.bm25)
        .unwrap_or_else(|| {
            schema
                .defaults
                .sparse_vector
                .as_ref()
                .and_then(|value| value.sparse_vector_index.as_ref())
                .and_then(|index| index.config.bm25)
                .unwrap_or(false)
        })
}

trait IntoSqliteExpr {
    /// Evaluate to a binary integer (0/1) indicating boolean value
    /// We cannot directly use a boolean value because `Any` and `All` aggregation does not exist
    /// We need to use `Min` and `Max` as a workaround
    /// In SQLite, boolean can be implicitly treated as binary integer
    fn eval(&self) -> SimpleExpr;
}

// Search reserves #id for the record identifier; ordinary Get metadata evaluation
// must retain its existing meaning for literal metadata keys.
fn search_filter(filter: &Where) -> SimpleExpr {
    match filter {
        Where::Composite(composite) => {
            let and = matches!(composite.operator, BooleanOperator::And);
            composite
                .children
                .iter()
                .fold(Expr::val(and).into(), |expression, child| {
                    if and {
                        Expr::expr(expression).and(search_filter(child))
                    } else {
                        Expr::expr(expression).or(search_filter(child))
                    }
                })
        }
        Where::Metadata(metadata) if metadata.key == "#id" => {
            let id = Expr::col((Embeddings::Table, Embeddings::EmbeddingId));
            match &metadata.comparison {
                MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str(value),
                ) => id.eq(value.clone()),
                MetadataComparison::Primitive(
                    PrimitiveOperator::NotEqual,
                    MetadataValue::Str(value),
                ) => id.ne(value.clone()),
                MetadataComparison::Set(SetOperator::In, MetadataSetValue::Str(values)) => {
                    id.is_in(values.clone())
                }
                MetadataComparison::Set(SetOperator::NotIn, MetadataSetValue::Str(values)) => {
                    id.is_not_in(values.clone())
                }
                // The executor rejects unsupported ID comparisons before replay.
                _ => Expr::val(false).into(),
            }
        }
        _ => filter.eval(),
    }
}

impl IntoSqliteExpr for Where {
    fn eval(&self) -> SimpleExpr {
        match self {
            Where::Composite(expr) => expr.eval(),
            Where::Document(expr) => expr.eval(),
            Where::Metadata(expr) => {
                // Local chroma is mixing the usage of int and float
                match &expr.comparison {
                    MetadataComparison::Set(op, MetadataSetValue::Int(is)) => {
                        let alt = MetadataExpression {
                            key: expr.key.clone(),
                            comparison: MetadataComparison::Set(
                                op.clone(),
                                MetadataSetValue::Float(
                                    is.iter().cloned().map(|i| i as f64).collect(),
                                ),
                            ),
                        };
                        match op {
                            SetOperator::In => expr.eval().or(alt.eval()),
                            SetOperator::NotIn => expr.eval().and(alt.eval()),
                        }
                    }
                    MetadataComparison::Set(op, MetadataSetValue::Float(fs)) => {
                        let alt = MetadataExpression {
                            key: expr.key.clone(),
                            comparison: MetadataComparison::Set(
                                op.clone(),
                                MetadataSetValue::Int(
                                    fs.iter().cloned().map(|f| f as i64).collect(),
                                ),
                            ),
                        };
                        match op {
                            SetOperator::In => expr.eval().or(alt.eval()),
                            SetOperator::NotIn => expr.eval().and(alt.eval()),
                        }
                    }
                    // since the metadata expr eval handles the union in case of int and float, we can just pass through
                    _ => expr.eval(),
                }
            }
        }
    }
}

// this function creates a union subquery for int and float queries
// this utilizes index on int and float columns separately and combines results after
// for better performance

// then on Where::eval(), directly eval the subquery instead of using the OR logic
fn create_union_subquery_for_int_float_ops(
    key: String,
    op: PrimitiveOperator,
    val: MetadataValue,
) -> sea_query::SelectStatement {
    let key_col = Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::Key));
    let key_cond = key_col.clone().eq(key).and(key_col.is_not_null());

    let mut subq1 = Query::select()
        .column(EmbeddingMetadata::Id)
        .from(EmbeddingMetadata::Table)
        .and_where(key_cond.clone())
        .to_owned();

    let mut subq2 = Query::select()
        .column(EmbeddingMetadata::Id)
        .from(EmbeddingMetadata::Table)
        .and_where(key_cond)
        .to_owned();

    // if val is int or float, create two variables, i and f and based on which one convert it to the other type
    let (i, f) = match val {
        MetadataValue::Int(i) => (i, i as f64),
        MetadataValue::Float(f) => (f as i64, f),
        // if val is not int or float, return the subquery as is, no union necessary
        _ => return subq1,
    };

    let int_col = Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::IntValue));
    let float_col = Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::FloatValue));

    match op {
        PrimitiveOperator::Equal => {
            subq1.and_where(int_col.eq(i));
            subq2.and_where(float_col.eq(f));
        }
        PrimitiveOperator::NotEqual => {
            subq1.and_where(int_col.eq(i));
            subq2.and_where(float_col.eq(f));
        }
        PrimitiveOperator::GreaterThan => {
            subq1.and_where(int_col.gt(i));
            subq2.and_where(float_col.gt(f));
        }
        PrimitiveOperator::GreaterThanOrEqual => {
            subq1.and_where(int_col.gte(i));
            subq2.and_where(float_col.gte(f));
        }
        PrimitiveOperator::LessThan => {
            subq1.and_where(int_col.lt(i));
            subq2.and_where(float_col.lt(f));
        }
        PrimitiveOperator::LessThanOrEqual => {
            subq1.and_where(int_col.lte(i));
            subq2.and_where(float_col.lte(f));
        }
    }

    subq1.union(sea_query::UnionType::Distinct, subq2);

    subq1
}

impl IntoSqliteExpr for CompositeExpression {
    fn eval(&self) -> SimpleExpr {
        match self.operator {
            BooleanOperator::And => {
                let mut expr = SimpleExpr::Value(sea_query::Value::Bool(Some(true)));
                for child in &self.children {
                    expr = Expr::expr(expr).and(child.eval());
                }
                expr
            }
            BooleanOperator::Or => {
                let mut expr = SimpleExpr::Value(sea_query::Value::Bool(Some(false)));
                for child in &self.children {
                    expr = Expr::expr(expr).or(child.eval());
                }
                expr
            }
        }
    }
}

impl IntoSqliteExpr for DocumentExpression {
    fn eval(&self) -> SimpleExpr {
        let subq = Query::select()
            .column(EmbeddingFulltextSearch::Rowid)
            .from(EmbeddingFulltextSearch::Table)
            .and_where(match self.operator {
                DocumentOperator::Contains | DocumentOperator::NotContains => {
                    Expr::col(EmbeddingFulltextSearch::StringValue).like(
                        LikeExpr::new(format!(
                            "%{}%",
                            self.pattern
                                .replace("\\", "\\\\") // escape user-provided backslashes
                                .replace("%", "\\%") // escape % characters
                                .replace("_", "\\_") // escape _ characters
                        ))
                        .escape('\\'),
                    )
                }
                DocumentOperator::Regex | DocumentOperator::NotRegex => Expr::cust_with_exprs(
                    "? REGEXP ?",
                    [
                        Expr::col(EmbeddingFulltextSearch::StringValue).into(),
                        Expr::value(&self.pattern),
                    ],
                ),
            })
            .to_owned();
        match self.operator {
            DocumentOperator::Contains | DocumentOperator::Regex => {
                Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
            }
            DocumentOperator::NotContains | DocumentOperator::NotRegex => {
                Expr::col((Embeddings::Table, Embeddings::Id)).not_in_subquery(subq)
            }
        }
    }
}

impl IntoSqliteExpr for MetadataExpression {
    fn eval(&self) -> SimpleExpr {
        let key_col = Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::Key));
        let key_cond = key_col
            .clone()
            .eq(self.key.to_string())
            .and(key_col.is_not_null());
        match &self.comparison {
            MetadataComparison::Primitive(op, val) => {
                let (col, sval) = match val {
                    MetadataValue::Bool(b) => (EmbeddingMetadata::BoolValue, Expr::val(*b)),
                    MetadataValue::Int(i) => (EmbeddingMetadata::IntValue, Expr::val(*i)),
                    MetadataValue::Float(f) => (EmbeddingMetadata::FloatValue, Expr::val(*f)),
                    MetadataValue::Str(s) => (EmbeddingMetadata::StringValue, Expr::val(s)),
                    MetadataValue::SparseVector(_) => {
                        unimplemented!("Comparision with sparse vector is not allowed")
                    }
                    MetadataValue::BoolArray(_)
                    | MetadataValue::IntArray(_)
                    | MetadataValue::FloatArray(_)
                    | MetadataValue::StringArray(_) => {
                        unimplemented!(
                            "Primitive comparison with array metadata values is not supported"
                        )
                    }
                };
                let scol = Expr::col((EmbeddingMetadata::Table, col));
                let mut subq = Query::select()
                    .column(EmbeddingMetadata::Id)
                    .from(EmbeddingMetadata::Table)
                    .and_where(key_cond.clone())
                    .to_owned();

                match op {
                    PrimitiveOperator::Equal => {
                        if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                            subq = create_union_subquery_for_int_float_ops(
                                self.key.clone(),
                                op.clone(),
                                val.clone(),
                            );
                        } else {
                            subq.and_where(scol.eq(sval));
                        }
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                    PrimitiveOperator::NotEqual => {
                        if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                            subq = create_union_subquery_for_int_float_ops(
                                self.key.clone(),
                                op.clone(),
                                val.clone(),
                            );
                        } else {
                            subq.and_where(scol.eq(sval));
                        }
                        Expr::col((Embeddings::Table, Embeddings::Id)).not_in_subquery(subq)
                    }
                    PrimitiveOperator::GreaterThan => {
                        if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                            subq = create_union_subquery_for_int_float_ops(
                                self.key.clone(),
                                op.clone(),
                                val.clone(),
                            );
                        } else {
                            subq.and_where(scol.gt(sval));
                        }
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                    PrimitiveOperator::GreaterThanOrEqual => {
                        if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                            subq = create_union_subquery_for_int_float_ops(
                                self.key.clone(),
                                op.clone(),
                                val.clone(),
                            );
                        } else {
                            subq.and_where(scol.gte(sval));
                        }
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                    PrimitiveOperator::LessThan => {
                        if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                            subq = create_union_subquery_for_int_float_ops(
                                self.key.clone(),
                                op.clone(),
                                val.clone(),
                            );
                        } else {
                            subq.and_where(scol.lt(sval));
                        }
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                    PrimitiveOperator::LessThanOrEqual => {
                        if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                            subq = create_union_subquery_for_int_float_ops(
                                self.key.clone(),
                                op.clone(),
                                val.clone(),
                            );
                        } else {
                            subq.and_where(scol.lte(sval));
                        }
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                }
            }
            MetadataComparison::Set(op, vals) => {
                let (col, svals) = match vals {
                    MetadataSetValue::Bool(bs) => (
                        EmbeddingMetadata::BoolValue,
                        bs.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                    MetadataSetValue::Int(is) => (
                        EmbeddingMetadata::IntValue,
                        is.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                    MetadataSetValue::Float(fs) => (
                        EmbeddingMetadata::FloatValue,
                        fs.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                    MetadataSetValue::Str(ss) => (
                        EmbeddingMetadata::StringValue,
                        ss.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                };
                let scol = Expr::col((EmbeddingMetadata::Table, col));
                let subq = Query::select()
                    .column(EmbeddingMetadata::Id)
                    .from(EmbeddingMetadata::Table)
                    .and_where(key_cond.clone())
                    .and_where(scol.is_in(svals))
                    .to_owned();

                match op {
                    SetOperator::In => {
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                    SetOperator::NotIn => {
                        Expr::col((Embeddings::Table, Embeddings::Id)).not_in_subquery(subq)
                    }
                }
            }
            MetadataComparison::ArrayContains(contains_op, val) => {
                // Array contains: arrays are stored exploded in the separate
                // `embedding_metadata_array` table (one row per element).
                // A contains check is an equality lookup on the scalar value.
                let (col, sval) = match val {
                    MetadataValue::Bool(b) => (EmbeddingMetadataArray::BoolValue, Expr::val(*b)),
                    MetadataValue::Int(i) => (EmbeddingMetadataArray::IntValue, Expr::val(*i)),
                    MetadataValue::Float(f) => (EmbeddingMetadataArray::FloatValue, Expr::val(*f)),
                    MetadataValue::Str(s) => (EmbeddingMetadataArray::StringValue, Expr::val(s)),
                    _ => unreachable!("ArrayContains only supports scalar metadata values"),
                };
                let arr_key_col =
                    Expr::col((EmbeddingMetadataArray::Table, EmbeddingMetadataArray::Key));
                let arr_key_cond = arr_key_col
                    .clone()
                    .eq(self.key.to_string())
                    .and(arr_key_col.is_not_null());
                let scol = Expr::col((EmbeddingMetadataArray::Table, col));
                let subq = Query::select()
                    .column(EmbeddingMetadataArray::Id)
                    .from(EmbeddingMetadataArray::Table)
                    .and_where(arr_key_cond)
                    .and_where(scol.eq(sval))
                    .to_owned();

                match contains_op {
                    ContainsOperator::Contains => {
                        Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(subq)
                    }
                    ContainsOperator::NotContains => {
                        Expr::col((Embeddings::Table, Embeddings::Id)).not_in_subquery(subq)
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteMetadataReader {
    pub db: SqliteDb,
}

impl SqliteMetadataReader {
    pub fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    /// Accumulate an exploded array element into the metadata map.
    ///
    /// Column layout: 0=id, 1=key, 2=string_value, 3=int_value, 4=float_value, 5=bool_value
    fn accumulate_array_value(
        metadata: &mut HashMap<String, MetadataValue>,
        key: &str,
        row: &sqlx::sqlite::SqliteRow,
    ) -> Result<(), SqliteMetadataError> {
        if let Ok(Some(s)) = row.try_get::<Option<String>, _>(2) {
            match metadata.get_mut(key) {
                Some(MetadataValue::StringArray(arr)) => arr.push(s),
                _ => {
                    metadata.insert(key.to_string(), MetadataValue::StringArray(vec![s]));
                }
            }
        } else if let Ok(Some(i)) = row.try_get::<Option<i64>, _>(3) {
            match metadata.get_mut(key) {
                Some(MetadataValue::IntArray(arr)) => arr.push(i),
                _ => {
                    metadata.insert(key.to_string(), MetadataValue::IntArray(vec![i]));
                }
            }
        } else if let Ok(Some(f)) = row.try_get::<Option<f64>, _>(4) {
            match metadata.get_mut(key) {
                Some(MetadataValue::FloatArray(arr)) => arr.push(f),
                _ => {
                    metadata.insert(key.to_string(), MetadataValue::FloatArray(vec![f]));
                }
            }
        } else if let Ok(Some(b)) = row.try_get::<Option<bool>, _>(5) {
            match metadata.get_mut(key) {
                Some(MetadataValue::BoolArray(arr)) => arr.push(b),
                _ => {
                    metadata.insert(key.to_string(), MetadataValue::BoolArray(vec![b]));
                }
            }
        }
        Ok(())
    }

    pub async fn current_max_seq_id(
        &self,
        segment_id: &SegmentUuid,
    ) -> Result<u64, SqliteMetadataError> {
        let (sql, values) = Query::select()
            .column(MaxSeqId::SeqId)
            .from(MaxSeqId::Table)
            .and_where(Expr::col(MaxSeqId::SegmentId).eq(segment_id.to_string()))
            .build_sqlx(SqliteQueryBuilder);
        let row_opt = sqlx::query_with(&sql, values)
            .fetch_optional(self.db.get_conn())
            .await?;
        Ok(row_opt
            .map(|row| row.try_get::<u64, _>(0))
            .transpose()?
            .unwrap_or_default())
    }

    pub async fn count(
        &self,
        Count {
            scan: Scan {
                collection_and_segments,
                ..
            },
            ..
        }: Count,
    ) -> Result<CountResult, SqliteMetadataError> {
        let (sql, values) = Query::select()
            .expr(Func::count(Expr::col(Embeddings::Id)))
            .from(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(collection_and_segments.metadata_segment.id.to_string()),
            )
            .build_sqlx(SqliteQueryBuilder);

        // Count should yield exactly one row with exactly one column
        let count = sqlx::query_with(&sql, values)
            .fetch_one(self.db.get_conn())
            .await?
            .try_get(0)?;
        Ok(CountResult {
            count,
            pulled_log_bytes: 0,
        })
    }

    pub async fn get(&self, plan: Get) -> Result<GetResult, SqliteMetadataError> {
        // Every query of a get in one read transaction, as a search: the get reads one moment of the collection, and its
        // queries, one more for each 500 records for their sparse vectors, take the lock of SQLite once instead of once
        // each.
        let mut tx = self.db.get_conn().begin().await?;
        let result = self.get_with_connection(plan, &mut tx).await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn get_with_connection(
        &self,
        Get {
            scan: Scan {
                collection_and_segments,
                ..
            },
            filter: Filter {
                query_ids,
                where_clause,
            },
            limit: Limit { offset, limit },
            proj: Projection {
                document, metadata, ..
            },
        }: Get,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<GetResult, SqliteMetadataError> {
        let mut filter_limit_query = Query::select();
        filter_limit_query.columns([
            (Embeddings::Table, Embeddings::Id),
            (Embeddings::Table, Embeddings::EmbeddingId),
        ]);
        filter_limit_query.from(Embeddings::Table).and_where(
            Expr::col((Embeddings::Table, Embeddings::SegmentId))
                .eq(collection_and_segments.metadata_segment.id.to_string()),
        );

        if let Some(ids) = &query_ids {
            filter_limit_query
                .cond_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(ids));
        }

        if let Some(whr) = &where_clause {
            filter_limit_query.distinct().cond_where(whr.eval());
        }

        filter_limit_query
            .order_by((Embeddings::Table, Embeddings::Id), sea_query::Order::Asc)
            .offset(offset as u64)
            .limit(limit.unwrap_or(u32::MAX) as u64);

        let alias = Alias::new(SUBQ_ALIAS);
        let mut projection_query = Query::select();
        projection_query
            .columns([
                (alias.clone(), Embeddings::Id),
                (alias.clone(), Embeddings::EmbeddingId),
            ])
            .from_subquery(filter_limit_query.clone(), alias.clone());

        if document || metadata {
            projection_query
                .left_join(
                    EmbeddingMetadata::Table,
                    Expr::col((alias.clone(), Embeddings::Id))
                        .equals((EmbeddingMetadata::Table, EmbeddingMetadata::Id)),
                )
                .columns(
                    [
                        EmbeddingMetadata::Key,
                        EmbeddingMetadata::StringValue,
                        EmbeddingMetadata::IntValue,
                        EmbeddingMetadata::FloatValue,
                        EmbeddingMetadata::BoolValue,
                    ]
                    .map(|c| (EmbeddingMetadata::Table, c)),
                );
        }

        let (sql, values) = projection_query.build_sqlx(SqliteQueryBuilder);

        let rows = sqlx::query_with(&sql, values).fetch_all(&mut *conn).await?;
        #[cfg(test)]
        stop_get(&collection_and_segments.metadata_segment.id).await;

        let mut records = BTreeMap::new();

        for row in &rows {
            let offset_id: u32 = row.try_get(0)?;
            let user_id: String = row.try_get(1)?;
            let record = records.entry(offset_id).or_insert(ProjectionRecord {
                id: user_id,
                document: None,
                embedding: None,
                metadata: (document || metadata).then_some(HashMap::new()),
            });

            if document || metadata {
                if let Ok(key) = row.try_get::<String, _>(2) {
                    if let Some(metadata) = record.metadata.as_mut() {
                        if let Ok(Some(s)) = row.try_get(3) {
                            metadata.insert(key.clone(), MetadataValue::Str(s));
                        } else if let Ok(Some(i)) = row.try_get(4) {
                            metadata.insert(key.clone(), MetadataValue::Int(i));
                        } else if let Ok(Some(f)) = row.try_get(5) {
                            metadata.insert(key.clone(), MetadataValue::Float(f));
                        } else if let Ok(Some(b)) = row.try_get(6) {
                            metadata.insert(key, MetadataValue::Bool(b));
                        }
                    }
                }
            }
        }

        // Fetch array metadata from the separate array table and merge
        // into the records we already have.
        if metadata && !records.is_empty() {
            let offset_ids: Vec<u32> = records.keys().copied().collect();
            let (arr_sql, arr_vals) = Query::select()
                .columns([
                    EmbeddingMetadataArray::Id,
                    EmbeddingMetadataArray::Key,
                    EmbeddingMetadataArray::StringValue,
                    EmbeddingMetadataArray::IntValue,
                    EmbeddingMetadataArray::FloatValue,
                    EmbeddingMetadataArray::BoolValue,
                ])
                .from(EmbeddingMetadataArray::Table)
                .and_where(
                    Expr::col(EmbeddingMetadataArray::Id)
                        .is_in(offset_ids.iter().copied().map(|id| id as i64)),
                )
                .build_sqlx(SqliteQueryBuilder);

            let arr_rows = sqlx::query_with(&arr_sql, arr_vals)
                .fetch_all(&mut *conn)
                .await?;

            for row in arr_rows {
                let offset_id: u32 = row.try_get(0)?;
                let key: String = row.try_get(1)?;
                if let Some(record) = records.get_mut(&offset_id) {
                    if let Some(md) = record.metadata.as_mut() {
                        Self::accumulate_array_value(md, &key, &row)?;
                    }
                }
            }
        }

        if metadata && !records.is_empty() {
            // The sparse vectors of the rows already selected, in batches: evaluating the selection again would
            // double the cost of a Get with a large filter.
            let offset_ids: Vec<u32> = records.keys().copied().collect();
            let mut rows = Vec::new();
            for batch in offset_ids.chunks(500) {
                let (sql, values) = Query::select()
                    .columns([Alias::new("id"), Alias::new("key"), Alias::new("value")])
                    .from(Alias::new("embedding_metadata_sparse"))
                    .and_where(Expr::col(Alias::new("id")).is_in(batch.iter().map(|id| *id as i64)))
                    .build_sqlx(SqliteQueryBuilder);
                rows.extend(sqlx::query_with(&sql, values).fetch_all(&mut *conn).await?);
            }
            for row in rows {
                let id: u32 = row.try_get(0)?;
                let key: String = row.try_get(1)?;
                let value: String = row.try_get(2)?;
                let vector: chroma_types::SparseVector = serde_json::from_str(&value)
                    .map_err(SqliteMetadataError::SparseMetadataJson)?;
                vector.validate()?;
                if vector.values.iter().any(|value| !value.is_finite()) {
                    return Err(MetadataValueConversionError::InvalidValue.into());
                }
                if let Some(record) = records.get_mut(&id) {
                    if let Some(metadata) = record.metadata.as_mut() {
                        metadata.insert(key, MetadataValue::SparseVector(vector));
                    }
                }
            }
        }
        // The rows of the selection go only now, after the last query of the get: every query of the get runs on one
        // connection, and the memory of those rows, freed before the next query, went back to the thread of that
        // connection just before its first large allocation, which then had malloc merge it all (in a cold get, the copy
        // of the text of the query of the arrays, 60 KB for 20000 records). Freed here, the next get takes it again.
        #[cfg(test)]
        ROWS_AFTER_LAST_QUERY
            .lock()
            .unwrap()
            .push((collection_and_segments.metadata_segment.id, rows.len()));
        drop(rows);

        Ok(GetResult {
            pulled_log_bytes: 0,
            result: ProjectionOutput {
                records: records
                    .into_values()
                    .map(|mut rec| {
                        if let Some(mut meta) = rec.metadata.take() {
                            if let Some(MetadataValue::Str(doc)) = meta.remove(CHROMA_DOCUMENT_KEY)
                            {
                                rec.document = Some(doc);
                            }
                            if !meta.is_empty() {
                                rec.metadata = Some(meta)
                            }
                        }
                        rec
                    })
                    .collect(),
            },
        })
    }
    /// The Search of single-node Chroma, run as distributed Chroma runs it once a collection is compacted
    /// (rust/worker/src/server.rs:681-851 and the operators chained by rust/worker/src/execution/orchestration/rank.rs):
    /// the filter, one KNN for each leaf of the rank expression (dense from the local HNSW index, sparse from SQLite),
    /// then rank, group by, limit and select. Every SQLite read of the request shares one snapshot.
    pub async fn search(
        &self,
        collection: chroma_types::CollectionAndSegments,
        vectors: Option<&dyn DenseVectorReader>,
        payloads: Vec<chroma_types::plan::SearchPayload>,
    ) -> Result<chroma_types::operator::SearchResult, SqliteMetadataError> {
        let mut tx = self.db.get_conn().begin().await?;
        // The first read fixes the snapshot for every payload and projection.
        sqlx::query("SELECT seq_id FROM max_seq_id WHERE segment_id = ?")
            .bind(collection.metadata_segment.id.to_string())
            .fetch_optional(&mut *tx)
            .await?;
        let result = self
            .search_with_connection(collection, vectors, payloads, &mut tx)
            .await?;
        tx.commit().await?;
        Ok(result)
    }

    /// A read of the metadata at one moment: a read transaction of SQLite, whose first read fixes what all its reads
    /// see. A flush of a SPANN collection records its files in the transaction of its records, so a snapshot reads one
    /// version of every segment of the collection, as a read of distributed Chroma does.
    pub async fn snapshot(&self) -> Result<MetadataSnapshot, SqliteMetadataError> {
        Ok(MetadataSnapshot {
            tx: self.db.get_conn().begin().await?,
        })
    }

    /// A get at the moment of a snapshot.
    pub async fn get_at(
        &self,
        snapshot: &mut MetadataSnapshot,
        plan: Get,
    ) -> Result<GetResult, SqliteMetadataError> {
        self.get_with_connection(plan, &mut snapshot.tx).await
    }

    /// A search at the moment of a snapshot.
    pub async fn search_at(
        &self,
        snapshot: &mut MetadataSnapshot,
        collection: chroma_types::CollectionAndSegments,
        vectors: Option<&dyn DenseVectorReader>,
        payloads: Vec<chroma_types::plan::SearchPayload>,
    ) -> Result<chroma_types::operator::SearchResult, SqliteMetadataError> {
        self.search_with_connection(collection, vectors, payloads, &mut snapshot.tx)
            .await
    }

    /// The part of a search of a SPANN collection on the single node that SQLite answers, at the moment of a snapshot:
    /// for each payload, in their order, the records that pass its filter, its sparse KNN leaves and, without a rank
    /// expression, its page, by user id, as search_with_connection finds them. The executor closes the snapshot after
    /// it and takes the dense KNN leaves, the rank, the group by and the selection from the files of the flush that the
    /// snapshot reads (rust/frontend/src/executor/local.rs, spann_search): the search keeps no read of SQLite through
    /// its files, as a get, a query and a count.
    pub async fn spann_search_sqlite_at(
        &self,
        snapshot: &mut MetadataSnapshot,
        collection: &chroma_types::CollectionAndSegments,
        payloads: &[chroma_types::plan::SearchPayload],
    ) -> Result<Vec<SpannSearchSqlite>, SqliteMetadataError> {
        use chroma_types::operator::QueryVector;
        let conn: &mut sqlx::SqliteConnection = &mut snapshot.tx;
        let mut parts = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let mut part = SpannSearchSqlite::default();
            match &payload.rank.expr {
                Some(expr) => {
                    let mut dense = false;
                    for knn in expr.knn_queries() {
                        part.sparse.push(match knn.query {
                            QueryVector::Dense(_) => {
                                dense = true;
                                None
                            }
                            QueryVector::Sparse(query) => {
                                let measures = self
                                    .sparse_knn(
                                        conn,
                                        collection,
                                        &payload.filter,
                                        &knn.key.to_string(),
                                        &query,
                                        knn.limit,
                                    )
                                    .await?;
                                Some(self.user_ids_of_rows(conn, collection, measures).await?)
                            }
                        });
                    }
                    // As dense_knn: with a filter, the dense leaves look only among the records that pass it.
                    let filtered =
                        payload.filter.query_ids.is_some() || payload.filter.where_clause.is_some();
                    if dense && filtered {
                        part.allowed_user_ids = Some(
                            self.filtered_records(conn, collection, &payload.filter, None)
                                .await?
                                .into_iter()
                                .map(|(_, user_id)| user_id)
                                .collect(),
                        );
                    }
                }
                None => {
                    part.unranked = Some(
                        self.filtered_records(
                            conn,
                            collection,
                            &payload.filter,
                            Some(&payload.limit),
                        )
                        .await?
                        .into_iter()
                        .map(|(_, user_id)| user_id)
                        .collect(),
                    );
                }
            }
            parts.push(part);
        }
        Ok(parts)
    }

    /// The user ids of the SQLite rows of KNN results, with their measures, in their order.
    async fn user_ids_of_rows(
        &self,
        conn: &mut sqlx::SqliteConnection,
        collection: &chroma_types::CollectionAndSegments,
        measures: Vec<chroma_types::operator::RecordMeasure>,
    ) -> Result<Vec<(String, f32)>, SqliteMetadataError> {
        let mut user_ids = HashMap::with_capacity(measures.len());
        for batch in measures.chunks(500) {
            let (sql, values) = Query::select()
                .columns([
                    (Embeddings::Table, Embeddings::Id),
                    (Embeddings::Table, Embeddings::EmbeddingId),
                ])
                .from(Embeddings::Table)
                .and_where(
                    Expr::col((Embeddings::Table, Embeddings::SegmentId))
                        .eq(collection.metadata_segment.id.to_string()),
                )
                .and_where(
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .is_in(batch.iter().map(|record| record.offset_id as i64)),
                )
                .build_sqlx(SqliteQueryBuilder);
            for row in sqlx::query_with(&sql, values).fetch_all(&mut *conn).await? {
                user_ids.insert(row.try_get::<u32, _>(0)?, row.try_get::<String, _>(1)?);
            }
        }
        measures
            .into_iter()
            .map(|record| {
                user_ids
                    .remove(&record.offset_id)
                    .map(|user_id| (user_id, record.measure))
                    .ok_or(SqliteMetadataError::PhantomRecord(record.offset_id))
            })
            .collect()
    }

    async fn search_with_connection(
        &self,
        collection: chroma_types::CollectionAndSegments,
        vectors: Option<&dyn DenseVectorReader>,
        payloads: Vec<chroma_types::plan::SearchPayload>,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<chroma_types::operator::SearchResult, SqliteMetadataError> {
        use chroma_types::{
            operator::{QueryVector, RecordMeasure, SearchResult},
            ranking::EnrichedRecord,
        };
        let mut results = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let records = match &payload.rank.expr {
                Some(expr) => {
                    // The knn results in the DFS order of the leaves, as the rank evaluation consumes them.
                    let mut knn_results = Vec::new();
                    for knn in expr.knn_queries() {
                        knn_results.push(match knn.query {
                            QueryVector::Dense(query) => {
                                self.dense_knn(
                                    conn,
                                    &collection,
                                    vectors,
                                    &payload.filter,
                                    query,
                                    knn.limit,
                                )
                                .await?
                            }
                            QueryVector::Sparse(query) => {
                                self.sparse_knn(
                                    conn,
                                    &collection,
                                    &payload.filter,
                                    &knn.key.to_string(),
                                    &query,
                                    knn.limit,
                                )
                                .await?
                            }
                        });
                    }
                    let mut ranks = expr.rank(knn_results);
                    if payload.group_by.is_active() {
                        let mut hydrated = self
                            .hydrate(
                                conn,
                                &collection,
                                ranks.iter().map(|record| record.offset_id),
                                false,
                                true,
                            )
                            .await?;
                        let mut enriched_records = Vec::with_capacity(ranks.len());
                        for record in ranks {
                            let metadata = hydrated
                                .remove(&record.offset_id)
                                .ok_or(SqliteMetadataError::PhantomRecord(record.offset_id))?
                                .metadata
                                .unwrap_or_default();
                            enriched_records.push(EnrichedRecord { record, metadata });
                        }
                        ranks = payload.group_by.group_ranked(enriched_records);
                    }
                    ranks
                        .into_iter()
                        .skip(payload.limit.offset as usize)
                        .take(payload.limit.limit.unwrap_or(u32::MAX) as usize)
                        .collect()
                }
                // Without a rank expression the records come in their storage order, and the score is the position.
                None => self
                    .filtered_records(conn, &collection, &payload.filter, Some(&payload.limit))
                    .await?
                    .into_iter()
                    .enumerate()
                    .map(|(rank_position, (offset_id, _))| RecordMeasure {
                        offset_id,
                        measure: rank_position as f32,
                    })
                    .collect(),
            };
            results.push(
                self.select(conn, &collection, vectors, records, &payload.select)
                    .await?,
            );
        }
        Ok(SearchResult {
            results,
            pulled_log_bytes: 0,
        })
    }

    /// The selection of the records that pass the filter of a Search, by their SQLite row, in storage order.
    fn search_selection(
        collection: &chroma_types::CollectionAndSegments,
        filter: &Filter,
    ) -> sea_query::SelectStatement {
        let mut selection = Query::select();
        selection
            .columns([
                (Embeddings::Table, Embeddings::Id),
                (Embeddings::Table, Embeddings::EmbeddingId),
            ])
            .from(Embeddings::Table)
            .and_where(
                Expr::col((Embeddings::Table, Embeddings::SegmentId))
                    .eq(collection.metadata_segment.id.to_string()),
            );
        if let Some(ids) = &filter.query_ids {
            selection.and_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(ids));
        }
        if let Some(where_clause) = &filter.where_clause {
            selection.distinct().cond_where(search_filter(where_clause));
        }
        selection.order_by((Embeddings::Table, Embeddings::Id), sea_query::Order::Asc);
        selection
    }

    /// The SQLite row and the user id of the records that pass the filter, in storage order, optionally paginated.
    async fn filtered_records(
        &self,
        conn: &mut sqlx::SqliteConnection,
        collection: &chroma_types::CollectionAndSegments,
        filter: &Filter,
        limit: Option<&Limit>,
    ) -> Result<Vec<(u32, String)>, SqliteMetadataError> {
        let mut selection = Self::search_selection(collection, filter);
        if let Some(limit) = limit {
            selection
                .offset(limit.offset as u64)
                .limit(limit.limit.unwrap_or(u32::MAX) as u64);
        }
        let (sql, values) = selection.build_sqlx(SqliteQueryBuilder);
        let rows = sqlx::query_with(&sql, values).fetch_all(&mut *conn).await?;
        rows.into_iter()
            .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
            .collect()
    }

    /// A dense KNN leaf: the `limit` nearest records that pass the filter, with their distance, by SQLite row.
    async fn dense_knn(
        &self,
        conn: &mut sqlx::SqliteConnection,
        collection: &chroma_types::CollectionAndSegments,
        vectors: Option<&dyn DenseVectorReader>,
        filter: &Filter,
        query: Vec<f32>,
        limit: u32,
    ) -> Result<Vec<chroma_types::operator::RecordMeasure>, SqliteMetadataError> {
        use chroma_types::operator::RecordMeasure;
        let Some(vectors) = vectors else {
            return Ok(Vec::new());
        };
        // Records by user id: the index knows them by its own offset, the other leaves and the rank by SQLite row.
        let mut rows_by_user_id = HashMap::new();
        let mut allowed_user_ids = None;
        if filter.query_ids.is_some() || filter.where_clause.is_some() {
            let allowed = self
                .filtered_records(conn, collection, filter, None)
                .await?;
            if allowed.is_empty() {
                return Ok(Vec::new());
            }
            let mut user_ids = Vec::with_capacity(allowed.len());
            for (row, user_id) in allowed {
                user_ids.push(user_id.clone());
                rows_by_user_id.insert(user_id, row);
            }
            allowed_user_ids = Some(user_ids);
        }
        let neighbors = vectors
            .nearest(collection, query, allowed_user_ids.as_deref(), limit)
            .await?;
        let user_ids = neighbors
            .iter()
            .map(|(_, user_id, _)| user_id.clone())
            .collect::<Vec<_>>();
        if rows_by_user_id.is_empty() {
            for batch in user_ids.chunks(500) {
                let (sql, values) = Query::select()
                    .columns([
                        (Embeddings::Table, Embeddings::Id),
                        (Embeddings::Table, Embeddings::EmbeddingId),
                    ])
                    .from(Embeddings::Table)
                    .and_where(
                        Expr::col((Embeddings::Table, Embeddings::SegmentId))
                            .eq(collection.metadata_segment.id.to_string()),
                    )
                    .and_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(batch))
                    .build_sqlx(SqliteQueryBuilder);
                for row in sqlx::query_with(&sql, values).fetch_all(&mut *conn).await? {
                    rows_by_user_id.insert(row.try_get::<String, _>(1)?, row.try_get::<u32, _>(0)?);
                }
            }
        }
        let mut records = neighbors
            .into_iter()
            .map(|(offset_id, user_id, measure)| {
                rows_by_user_id
                    .get(&user_id)
                    .map(|row| RecordMeasure {
                        offset_id: *row,
                        measure,
                    })
                    .ok_or(SqliteMetadataError::PhantomRecord(offset_id))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // As the merge of the distributed KNN results: by distance, then by record.
        records.sort_unstable();
        Ok(records)
    }

    /// The query of a BM25 key weighted as distributed Chroma does (rust/worker/src/execution/operators/idf.rs:84-89,
    /// 208-231): every weight times ln_1p((n - n_t + 0.5) / (n_t + 0.5)), in f32, where n is the number of records of
    /// the collection and n_t the number of records whose vector of the key has the dimension, at most n.
    async fn idf_scaled_query(
        conn: &mut sqlx::SqliteConnection,
        segment_id: &str,
        key: &str,
        query: &chroma_types::SparseVector,
    ) -> Result<chroma_types::SparseVector, SqliteMetadataError> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embeddings WHERE segment_id = ?")
            .bind(segment_id)
            .fetch_one(&mut *conn)
            .await?;
        let mut counts: HashMap<u32, i64> = HashMap::new();
        if !query.indices.is_empty() {
            let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
                "SELECT term.dim, COUNT(*) FROM embedding_metadata_sparse_term AS term \
                 JOIN embeddings ON embeddings.id = term.id WHERE embeddings.segment_id = ",
            );
            builder
                .push_bind(segment_id)
                .push(" AND term.key = ")
                .push_bind(key)
                .push(" AND term.dim IN (");
            {
                let mut dims = builder.separated(", ");
                for dim in &query.indices {
                    dims.push_bind(*dim);
                }
                dims.push_unseparated(") GROUP BY term.dim");
            }
            let rows: Vec<(u32, i64)> = builder.build_query_as().fetch_all(&mut *conn).await?;
            counts.extend(rows);
        }
        let n = n as f32;
        let mut scaled = query.clone();
        for (dim, value) in scaled.indices.iter().zip(scaled.values.iter_mut()) {
            let nt = (counts.get(dim).copied().unwrap_or_default() as f32).min(n);
            *value *= ((n - nt + 0.5) / (nt + 0.5)).ln_1p();
        }
        Ok(scaled)
    }

    /// A sparse KNN leaf (rust/worker/src/execution/orchestration/sparse_knn.rs): the `limit` records that pass the
    /// filter with the lowest 1 - q.d, by SQLite row, with the query weighted by IDF on a BM25 key.
    async fn sparse_knn(
        &self,
        conn: &mut sqlx::SqliteConnection,
        collection: &chroma_types::CollectionAndSegments,
        filter: &Filter,
        key: &str,
        query: &chroma_types::SparseVector,
        candidate_limit: u32,
    ) -> Result<Vec<chroma_types::operator::RecordMeasure>, SqliteMetadataError> {
        use chroma_types::operator::RecordMeasure;
        use futures::TryStreamExt;
        use std::collections::BinaryHeap;
        let query = match collection.collection.schema.as_ref() {
            Some(schema) if sparse_bm25_enabled(schema, key) => {
                Self::idf_scaled_query(
                    conn,
                    &collection.metadata_segment.id.to_string(),
                    key,
                    query,
                )
                .await?
            }
            _ => query.clone(),
        };
        // Only the dimensions of the query: a record with none of them is not a candidate.
        let term = Alias::new("term");
        let mut selection = Query::select();
        selection
            .column((Embeddings::Table, Embeddings::Id))
            .column((term.clone(), Alias::new("dim")))
            .column((term.clone(), Alias::new("value")))
            .from(Embeddings::Table)
            .join_as(
                sea_query::JoinType::InnerJoin,
                Alias::new("embedding_metadata_sparse_term"),
                term.clone(),
                Expr::col((Embeddings::Table, Embeddings::Id))
                    .equals((term.clone(), Alias::new("id"))),
            )
            .and_where(
                Expr::col((Embeddings::Table, Embeddings::SegmentId))
                    .eq(collection.metadata_segment.id.to_string()),
            )
            .and_where(Expr::col((term.clone(), Alias::new("key"))).eq(key))
            .and_where(Expr::col((term.clone(), Alias::new("dim"))).is_in(query.indices.clone()))
            .order_by((Embeddings::Table, Embeddings::Id), sea_query::Order::Asc)
            .order_by((term, Alias::new("dim")), sea_query::Order::Asc);
        if let Some(ids) = &filter.query_ids {
            selection.and_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(ids));
        }
        if let Some(where_clause) = &filter.where_clause {
            selection.cond_where(search_filter(where_clause));
        }
        let (sql, values) = selection.build_sqlx(SqliteQueryBuilder);
        let weights: HashMap<u32, f32> = query.iter().collect();
        let mut best: BinaryHeap<RecordMeasure> = BinaryHeap::new();
        let mut offer = |offset_id: u32, dot: f32| {
            // As the sparse index of distributed Chroma once a collection is compacted: only the records that
            // share a dimension with the query are candidates, not the disjoint or empty ones with measure 1.
            let candidate = RecordMeasure {
                offset_id,
                measure: 1.0 - dot,
            };
            if best.len() < candidate_limit as usize {
                best.push(candidate);
            } else if best.peek().is_some_and(|worst| candidate < *worst) {
                best.pop();
                best.push(candidate);
            }
        };
        if candidate_limit > 0 && !query.indices.is_empty() {
            let mut rows = sqlx::query_with(&sql, values).fetch(&mut *conn);
            let mut current: Option<(u32, f32)> = None;
            while let Some(row) = rows.try_next().await? {
                let offset_id: u32 = row.try_get(0)?;
                let dim: u32 = row.try_get(1)?;
                let value = row.try_get::<f64, _>(2)? as f32;
                // Rows come by record, then by dimension: the products add up in the order of the dimensions.
                let product = weights.get(&dim).copied().unwrap_or_default() * value;
                match current.as_mut() {
                    Some((current_offset, dot)) if *current_offset == offset_id => *dot += product,
                    _ => {
                        if let Some((done_offset, dot)) = current.take() {
                            offer(done_offset, dot);
                        }
                        current = Some((offset_id, product));
                    }
                }
            }
            if let Some((done_offset, dot)) = current.take() {
                offer(done_offset, dot);
            }
        }
        Ok(best.into_sorted_vec())
    }

    /// The user id of the given SQLite rows, with their document and metadata when asked.
    async fn hydrate(
        &self,
        conn: &mut sqlx::SqliteConnection,
        collection: &chroma_types::CollectionAndSegments,
        offset_ids: impl Iterator<Item = u32>,
        document: bool,
        metadata: bool,
    ) -> Result<HashMap<u32, ProjectionRecord>, SqliteMetadataError> {
        let offset_ids = offset_ids.collect::<Vec<_>>();
        let mut records = HashMap::with_capacity(offset_ids.len());
        for batch in offset_ids.chunks(500) {
            let (sql, values) = Query::select()
                .columns([
                    (Embeddings::Table, Embeddings::Id),
                    (Embeddings::Table, Embeddings::EmbeddingId),
                ])
                .from(Embeddings::Table)
                .and_where(
                    Expr::col((Embeddings::Table, Embeddings::SegmentId))
                        .eq(collection.metadata_segment.id.to_string()),
                )
                .and_where(
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .is_in(batch.iter().map(|id| *id as i64)),
                )
                .build_sqlx(SqliteQueryBuilder);
            let mut rows_by_user_id = HashMap::with_capacity(batch.len());
            for row in sqlx::query_with(&sql, values).fetch_all(&mut *conn).await? {
                rows_by_user_id.insert(row.try_get::<String, _>(1)?, row.try_get::<u32, _>(0)?);
            }
            if document || metadata {
                let get = Get {
                    scan: Scan {
                        collection_and_segments: collection.clone(),
                        shard_index: 0,
                        num_shards: 1,
                        log_upper_bound_offset: 0,
                    },
                    filter: Filter {
                        query_ids: Some(rows_by_user_id.keys().cloned().collect()),
                        where_clause: None,
                    },
                    limit: Limit {
                        offset: 0,
                        limit: None,
                    },
                    proj: Projection {
                        document,
                        metadata,
                        embedding: false,
                    },
                };
                for mut record in self.get_with_connection(get, conn).await?.result.records {
                    // Without the metadata projection the record still carries the scalar metadata it was read with.
                    if !metadata {
                        record.metadata = None;
                    }
                    if let Some(row) = rows_by_user_id.get(&record.id) {
                        records.insert(*row, record);
                    }
                }
            } else {
                for (id, row) in rows_by_user_id {
                    records.insert(
                        row,
                        ProjectionRecord {
                            id,
                            document: None,
                            embedding: None,
                            metadata: None,
                        },
                    );
                }
            }
        }
        Ok(records)
    }

    /// The selected keys of the records, in their order (rust/worker/src/execution/operators/select.rs).
    async fn select(
        &self,
        conn: &mut sqlx::SqliteConnection,
        collection: &chroma_types::CollectionAndSegments,
        vectors: Option<&dyn DenseVectorReader>,
        records: Vec<chroma_types::operator::RecordMeasure>,
        select: &chroma_types::operator::Select,
    ) -> Result<chroma_types::operator::SearchPayloadResult, SqliteMetadataError> {
        use chroma_types::operator::{Key, SearchPayloadResult, SearchRecord};
        if records.is_empty() {
            return Ok(SearchPayloadResult {
                records: Vec::new(),
            });
        }

        // Determine which keys to select
        let select_document = select.keys.contains(&Key::Document);
        let select_embedding = select.keys.contains(&Key::Embedding);
        let select_score = select.keys.contains(&Key::Score);

        // Check if we need to select any metadata
        let select_all_metadata = select.keys.contains(&Key::Metadata);

        // Collect specific metadata keys to select
        let metadata_fields_to_select = select
            .keys
            .iter()
            .filter_map(|field| {
                if let Key::MetadataField(key) = field {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<std::collections::HashSet<_>>();

        let select_metadata = select_all_metadata || !metadata_fields_to_select.is_empty();

        let mut hydrated = self
            .hydrate(
                conn,
                collection,
                records.iter().map(|record| record.offset_id),
                select_document,
                select_metadata,
            )
            .await?;

        let mut selected = Vec::with_capacity(records.len());
        for record in records {
            let data = hydrated
                .remove(&record.offset_id)
                .ok_or(SqliteMetadataError::PhantomRecord(record.offset_id))?;
            let embedding = match (select_embedding, vectors) {
                (true, Some(vectors)) => Some(vectors.embedding(&data.id).await?),
                (true, None) => {
                    return Err(SqliteMetadataError::LocalHnsw(Box::new(
                        LocalHnswSegmentReaderError::UninitializedSegment,
                    )))
                }
                (false, _) => None,
            };
            let mut full_metadata = data.metadata.unwrap_or_default();
            if !select_all_metadata {
                full_metadata.retain(|key, _| metadata_fields_to_select.contains(key));
            }
            selected.push(SearchRecord {
                id: data.id,
                document: select_document.then_some(data.document).flatten(),
                embedding,
                metadata: if full_metadata.is_empty() {
                    None
                } else {
                    Some(full_metadata)
                },
                score: select_score.then_some(record.measure),
            });
        }
        Ok(SearchPayloadResult { records: selected })
    }
}

/// Test hook: for each get, its metadata segment and how many rows of its selection it still held after its last query.
#[cfg(test)]
static ROWS_AFTER_LAST_QUERY: std::sync::Mutex<Vec<(chroma_types::SegmentUuid, usize)>> =
    std::sync::Mutex::new(Vec::new());

/// Test hook: the gets of the metadata segments that a test stops after their first query, until it lets them go on.
#[cfg(test)]
#[allow(clippy::type_complexity)]
static STOP_GET: std::sync::Mutex<
    Vec<(
        chroma_types::SegmentUuid,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn stop_get(segment: &chroma_types::SegmentUuid) {
    let stop = {
        let mut stops = STOP_GET.lock().unwrap();
        stops
            .iter()
            .position(|(id, _, _)| id == segment)
            .map(|at| stops.remove(at))
    };
    if let Some((_, stopped, go_on)) = stop {
        let _ = stopped.send(());
        let _ = go_on.await;
    }
}

#[cfg(test)]
mod tests {
    use super::{SqliteMetadataReader, SqliteMetadataWriter};
    use crate::test::TestReferenceSegment;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        operator::{Filter, Limit, Projection, Scan},
        plan::{Count, Get, ReadLevel},
        strategies::{any_collection_data_and_where_filter, TestCollectionData},
        Chunk, CollectionAndSegments, ContainsOperator, DocumentOperator, LogRecord,
        MetadataComparison, MetadataExpression, MetadataValue, Operation, OperationRecord,
        PrimitiveOperator, UpdateMetadataValue, Where,
    };
    use proptest::prelude::*;
    use std::collections::HashMap;
    use tokio::runtime::Runtime;

    proptest! {
        #[test]
        fn test_count(
            test_data in any::<TestCollectionData>()
        ) {
            let runtime = Runtime::new().expect("Should be able to start tokio runtime");
            let mut ref_seg = TestReferenceSegment::default();
            let sqlite_seg_writer = SqliteMetadataWriter {
                db: runtime.block_on(get_new_sqlite_db())
            };

            let metadata_seg_id = test_data.collection_and_segments.metadata_segment.id;
            ref_seg.apply_logs(test_data.logs.clone(), metadata_seg_id);
            let mut tx = runtime.block_on(sqlite_seg_writer.begin()).expect("Should be able to start transaction");
            let data: Chunk<LogRecord> = Chunk::new(test_data.logs.clone().into());
            runtime.block_on(sqlite_seg_writer.apply_logs(
                data,
                metadata_seg_id,
                test_data
                    .collection_and_segments
                    .collection
                    .schema
                    .clone(),
                &mut *tx,
            ))
            .expect("Should be able to apply logs");
            runtime.block_on(tx.commit()).expect("Should be able to commit log");

            let sqlite_seg_reader = SqliteMetadataReader {
                db: sqlite_seg_writer.db
            };
            let plan = Count { scan: Scan { collection_and_segments: test_data.collection_and_segments.clone(), shard_index: 0, num_shards: 1, log_upper_bound_offset: 0 }, read_level: ReadLevel::default() };
            let ref_count = ref_seg.count(plan.clone()).expect("Count should not fail").count;
            let sqlite_count = runtime.block_on(sqlite_seg_reader.count(plan)).expect("Count should not fail").count;
            assert_eq!(sqlite_count, ref_count);
        }
    }

    proptest! {
        #[test]
        fn test_get(
            (test_data, where_clause) in any_collection_data_and_where_filter()
        ) {
            let runtime = Runtime::new().expect("Should be able to start tokio runtime");
            let mut ref_seg = TestReferenceSegment::default();
            let sqlite_seg_writer = SqliteMetadataWriter {
                db: runtime.block_on(get_new_sqlite_db())
            };

            let metadata_seg_id = test_data.collection_and_segments.metadata_segment.id;
            ref_seg.apply_logs(test_data.logs.clone(), metadata_seg_id);
            let mut tx = runtime.block_on(sqlite_seg_writer.begin()).expect("Should be able to start transaction");
            let data: Chunk<LogRecord> = Chunk::new(test_data.logs.clone().into());
            runtime.block_on(sqlite_seg_writer.apply_logs(
                data,
                metadata_seg_id,
                test_data
                    .collection_and_segments
                    .collection
                    .schema
                    .clone(),
                &mut *tx,
            ))
            .expect("Should be able to apply logs");
            runtime.block_on(tx.commit()).expect("Should be able to commit log");

            let sqlite_seg_reader = SqliteMetadataReader {
                db: sqlite_seg_writer.db
            };

            let plan = Get {
                scan: Scan {
                    collection_and_segments: test_data.collection_and_segments.clone(),
                    shard_index: 0,
                    num_shards: 1,
                    log_upper_bound_offset: 0,
                },
                filter: Filter {
                    query_ids: None,
                    where_clause: Some(where_clause.clause),
                },
                limit: Limit {
                    offset: 3,
                    limit: Some(6),
                },
                proj: Projection {
                    document: true,
                    embedding: false,
                    metadata: true,
                },
            };
            let ref_get = ref_seg.get(plan.clone()).expect("Get should not fail");
            let sqlite_get = runtime.block_on(sqlite_seg_reader.get(plan)).expect("Get should not fail");
            assert_eq!(sqlite_get, ref_get);
        }
    }

    #[tokio::test]
    async fn test_metadata_key_null_edge_case() {
        let mut ref_seg = TestReferenceSegment::default();
        let sqlite_seg_writer = SqliteMetadataWriter {
            db: get_new_sqlite_db().await,
        };

        let mut logs = Vec::new();
        let mut metadata1 = HashMap::new();
        metadata1.insert(
            "test_key".to_string(),
            UpdateMetadataValue::Str("test_value".to_string()),
        );

        let mut metadata2 = HashMap::new();
        metadata2.insert("test_key".to_string(), UpdateMetadataValue::None);

        logs.push(LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "id1".to_string(),
                metadata: Some(metadata1),
                document: None,
                operation: Operation::Add,
                embedding: None,
                encoding: None,
            },
        });

        logs.push(LogRecord {
            log_offset: 1,
            record: OperationRecord {
                id: "id2".to_string(),
                metadata: Some(metadata2),
                document: None,
                operation: Operation::Add,
                embedding: None,
                encoding: None,
            },
        });

        let collection_and_segments = CollectionAndSegments::test(3);
        let metadata_seg_id = collection_and_segments.metadata_segment.id;

        ref_seg.apply_logs(logs.clone(), metadata_seg_id);
        let mut tx = sqlite_seg_writer
            .begin()
            .await
            .expect("Should be able to start transaction");
        let data: Chunk<LogRecord> = Chunk::new(logs.into());
        sqlite_seg_writer
            .apply_logs(
                data,
                metadata_seg_id,
                collection_and_segments.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .expect("Should be able to apply logs");
        tx.commit().await.expect("Should be able to commit log");

        let sqlite_seg_reader = SqliteMetadataReader {
            db: sqlite_seg_writer.db,
        };

        let where_clause = Where::Metadata(MetadataExpression {
            key: "test_key".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("test_value".to_string()),
            ),
        });

        let plan = Get {
            scan: Scan {
                collection_and_segments: collection_and_segments.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
                where_clause: Some(where_clause),
            },
            limit: Limit {
                offset: 0,
                limit: None,
            },
            proj: Projection {
                document: false,
                embedding: false,
                metadata: true,
            },
        };

        let ref_get = ref_seg.get(plan.clone()).expect("Get should not fail");
        let sqlite_get = sqlite_seg_reader
            .get(plan)
            .await
            .expect("Get should not fail");
        assert_eq!(sqlite_get, ref_get);
        assert_eq!(ref_get.result.records.len(), 1);
        assert_eq!(ref_get.result.records[0].id, "id1");

        // Test not equal, to ensure null metadata is also returned for key
        let where_clause2 = Where::Metadata(MetadataExpression {
            key: "test_key".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::NotEqual,
                MetadataValue::Str("failing_value".to_string()),
            ),
        });

        let plan2 = Get {
            scan: Scan {
                collection_and_segments: collection_and_segments.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
                where_clause: Some(where_clause2),
            },
            limit: Limit {
                offset: 0,
                limit: None,
            },
            proj: Projection {
                document: false,
                embedding: false,
                metadata: true,
            },
        };

        let ref_get2 = ref_seg.get(plan2.clone()).expect("Get should not fail");
        let sqlite_get2 = sqlite_seg_reader
            .get(plan2)
            .await
            .expect("Get should not fail");
        assert_eq!(sqlite_get2, ref_get2);
        assert_eq!(ref_get2.result.records.len(), 2);
        assert_eq!(ref_get2.result.records[0].id, "id1");
        assert_eq!(ref_get2.result.records[1].id, "id2");
    }

    #[tokio::test]
    async fn test_fts_match_no_metadata_match() {
        let mut ref_seg = TestReferenceSegment::default();
        let sqlite_seg_writer = SqliteMetadataWriter {
            db: get_new_sqlite_db().await,
        };

        let mut logs = Vec::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            "test_key".to_string(),
            UpdateMetadataValue::Str("different_value".to_string()),
        );

        logs.push(LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "id1".to_string(),
                metadata: Some(metadata),
                document: Some("test document content".to_string()),
                operation: Operation::Add,
                embedding: None,
                encoding: None,
            },
        });

        let collection_and_segments = CollectionAndSegments::test(3);
        let metadata_seg_id = collection_and_segments.metadata_segment.id;

        ref_seg.apply_logs(logs.clone(), metadata_seg_id);
        let mut tx = sqlite_seg_writer
            .begin()
            .await
            .expect("Should be able to start transaction");
        let data: Chunk<LogRecord> = Chunk::new(logs.into());
        sqlite_seg_writer
            .apply_logs(
                data,
                metadata_seg_id,
                collection_and_segments.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .expect("Should be able to apply logs");
        tx.commit().await.expect("Should be able to commit log");

        let sqlite_seg_reader = SqliteMetadataReader {
            db: sqlite_seg_writer.db,
        };

        let fts_where_clause = Where::Document(chroma_types::DocumentExpression {
            pattern: "test document".to_string(),
            operator: DocumentOperator::Contains,
        });

        let metadata_where_clause = Where::Metadata(MetadataExpression {
            key: "test_key".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("test_value".to_string()),
            ),
        });

        let hybrid_where_clause = Where::Composite(chroma_types::CompositeExpression {
            operator: chroma_types::BooleanOperator::And,
            children: vec![
                Where::Document(chroma_types::DocumentExpression {
                    pattern: "test document".to_string(),
                    operator: DocumentOperator::Contains,
                }),
                Where::Metadata(MetadataExpression {
                    key: "test_key".to_string(),
                    comparison: MetadataComparison::Primitive(
                        PrimitiveOperator::Equal,
                        MetadataValue::Str("test_value".to_string()),
                    ),
                }),
            ],
        });

        let fts_plan = Get {
            scan: Scan {
                collection_and_segments: collection_and_segments.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
                where_clause: Some(fts_where_clause),
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
        };

        let ref_get = ref_seg.get(fts_plan.clone()).expect("Get should not fail");
        let sqlite_get = sqlite_seg_reader
            .get(fts_plan)
            .await
            .expect("Get should not fail");
        assert_eq!(sqlite_get, ref_get);
        assert_eq!(ref_get.result.records.len(), 1);
        assert_eq!(ref_get.result.records[0].id, "id1");

        // Test metadata where clause
        let metadata_plan = Get {
            scan: Scan {
                collection_and_segments: collection_and_segments.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
                where_clause: Some(metadata_where_clause),
            },
            limit: Limit {
                offset: 0,
                limit: None,
            },
            proj: Projection {
                document: false,
                embedding: false,
                metadata: true,
            },
        };

        let ref_get2 = ref_seg
            .get(metadata_plan.clone())
            .expect("Get should not fail");
        let sqlite_get2 = sqlite_seg_reader
            .get(metadata_plan)
            .await
            .expect("Get should not fail");
        assert_eq!(sqlite_get2, ref_get2);
        assert_eq!(ref_get2.result.records.len(), 0);

        let hybrid_plan = Get {
            scan: Scan {
                collection_and_segments: collection_and_segments.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
                where_clause: Some(hybrid_where_clause),
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
        };

        let ref_get = ref_seg
            .get(hybrid_plan.clone())
            .expect("Get should not fail");
        let sqlite_get = sqlite_seg_reader
            .get(hybrid_plan)
            .await
            .expect("Get should not fail");
        assert_eq!(sqlite_get, ref_get);

        // no results bc fts matches but metadata does not
        assert_eq!(ref_get.result.records.len(), 0);
    }

    /// Helper: create logs, write them, and return reader + collection_and_segments.
    async fn setup_with_logs(
        logs: Vec<LogRecord>,
    ) -> (SqliteMetadataReader, CollectionAndSegments) {
        let writer = SqliteMetadataWriter {
            db: get_new_sqlite_db().await,
        };
        let cas = CollectionAndSegments::test(3);
        let seg_id = cas.metadata_segment.id;
        let mut tx = writer.begin().await.expect("begin tx");
        writer
            .apply_logs(
                Chunk::new(logs.into()),
                seg_id,
                cas.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .expect("apply logs");
        tx.commit().await.expect("commit");
        let reader = SqliteMetadataReader { db: writer.db };
        (reader, cas)
    }

    fn make_get_plan(cas: &CollectionAndSegments, where_clause: Option<Where>) -> Get {
        Get {
            scan: Scan {
                collection_and_segments: cas.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
                where_clause,
            },
            limit: Limit {
                offset: 0,
                limit: None,
            },
            proj: Projection {
                document: false,
                embedding: false,
                metadata: true,
            },
        }
    }

    fn exact_payload(query: Vec<(u32, f32)>, k: u32) -> chroma_types::plan::SearchPayload {
        use chroma_types::operator::{Key, QueryVector, Rank, RankExpr, Select};
        chroma_types::plan::SearchPayload {
            rank: Rank {
                expr: Some(RankExpr::Knn {
                    query: QueryVector::Sparse(chroma_types::SparseVector::from_pairs(query)),
                    key: Key::MetadataField("sparse".into()),
                    limit: k,
                    default: None,
                    return_rank: false,
                }),
            },
            select: Select {
                keys: [Key::Metadata, Key::Document, Key::Score]
                    .into_iter()
                    .collect(),
            },
            ..Default::default()
        }
    }

    async fn sparse_terms(db: &chroma_sqlite::db::SqliteDb) -> Vec<(String, u32, f64)> {
        sqlx::query_as(
            "SELECT embeddings.embedding_id, term.dim, term.value FROM embedding_metadata_sparse_term AS term \
             JOIN embeddings ON embeddings.id = term.id ORDER BY embeddings.embedding_id, term.dim",
        )
        .fetch_all(db.get_conn())
        .await
        .unwrap()
    }

    // The term table follows every write of the sparse vectors through its triggers, and the search reads from it.
    #[tokio::test]
    async fn test_sparse_terms_follow_add_update_delete() {
        let vector = |pairs: Vec<(u32, f32)>| {
            Some(UpdateMetadataValue::SparseVector(
                chroma_types::SparseVector::from_pairs(pairs),
            ))
        };
        let (reader, cas) = setup_with_logs(vec![
            sparse_log(1, "a", Operation::Add, vector(vec![(1, 0.5), (7, 0.25)])),
            sparse_log(2, "b", Operation::Add, vector(vec![(2, 1.0)])),
            sparse_log(3, "c", Operation::Add, vector(vec![])),
        ])
        .await;
        assert_eq!(
            sparse_terms(&reader.db).await,
            vec![
                ("a".into(), 1, 0.5),
                ("a".into(), 7, 0.25),
                ("b".into(), 2, 1.0)
            ]
        );
        let writer = SqliteMetadataWriter {
            db: reader.db.clone(),
        };
        let mut tx = writer.begin().await.unwrap();
        writer
            .apply_logs(
                Chunk::new(
                    vec![
                        sparse_log(4, "a", Operation::Update, vector(vec![(7, 2.0)])),
                        sparse_log(5, "b", Operation::Delete, None),
                        sparse_log(6, "c", Operation::Upsert, vector(vec![(7, 0.5)])),
                    ]
                    .into(),
                ),
                cas.metadata_segment.id,
                cas.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            sparse_terms(&reader.db).await,
            vec![("a".into(), 7, 2.0), ("c".into(), 7, 0.5)]
        );
        let result = reader
            .search(cas, None, vec![exact_payload(vec![(7, 1.0)], 10)])
            .await
            .unwrap();
        assert_eq!(
            result.results[0]
                .records
                .iter()
                .map(|r| (r.id.as_str(), r.score.unwrap()))
                .collect::<Vec<_>>(),
            vec![("a", -1.0), ("c", 0.5)]
        );
    }

    fn bm25_schema() -> chroma_types::Schema {
        use chroma_types::{
            KnnIndex, Schema, SparseIndexAlgorithm, SparseVectorIndexConfig, SparseVectorIndexType,
            SparseVectorValueType,
        };
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        schema
            .keys
            .entry("sparse".into())
            .or_default()
            .sparse_vector = Some(SparseVectorValueType {
            sparse_vector_index: Some(SparseVectorIndexType {
                enabled: true,
                config: SparseVectorIndexConfig {
                    bm25: Some(true),
                    embedding_function: None,
                    source_key: None,
                    algorithm: SparseIndexAlgorithm::Wand,
                },
            }),
        });
        schema
    }

    // The BM25 vectors of ChromaBm25EmbeddingFunction (chromadb 1.5.9, k 1.2, b 0.75, avg_doc_length 256) for the
    // four documents of the Cloud comparison (CLOUD-PROVE.md), and of the query "fox".
    fn bm25_documents() -> Vec<(&'static str, Vec<(u32, f32)>)> {
        let doc =
            |indices: &[u32], value: f32| indices.iter().map(|i| (*i, value)).collect::<Vec<_>>();
        vec![
            (
                "fox1",
                doc(
                    &[
                        226376294, 741580288, 771291085, 1312749093, 1621867415, 1913189942,
                    ],
                    1.6652868,
                ),
            ),
            (
                "fox2",
                doc(
                    &[
                        983533787, 1440641117, 1621867415, 1774389278, 1935863057, 2084400053,
                    ],
                    1.6652868,
                ),
            ),
            (
                "ml",
                doc(
                    &[34379837, 1228389567, 1299890214, 1644170059, 2088942923],
                    1.6697302,
                ),
            ),
            (
                "pie",
                doc(
                    &[
                        207604854, 662344706, 1085451005, 1180591805, 1481737067, 1985449994,
                    ],
                    1.6652868,
                ),
            ),
        ]
    }

    async fn bm25_fox(fillers: u32) -> Vec<(String, f32)> {
        let mut logs: Vec<_> = bm25_documents()
            .into_iter()
            .enumerate()
            .map(|(i, (id, pairs))| {
                sparse_log(
                    i as i64 + 1,
                    id,
                    Operation::Add,
                    Some(UpdateMetadataValue::SparseVector(
                        chroma_types::SparseVector::from_pairs(pairs),
                    )),
                )
            })
            .collect();
        // Records without the term, as the 300 fillers of the Cloud test: they change n, not n_t.
        for f in 0..fillers {
            logs.push(sparse_log(
                10 + f as i64,
                &format!("f{f}"),
                Operation::Add,
                Some(UpdateMetadataValue::SparseVector(
                    chroma_types::SparseVector::from_pairs(vec![(f + 1, 1.0)]),
                )),
            ));
        }
        let (reader, mut cas) = setup_with_logs(logs).await;
        cas.collection.schema = Some(bm25_schema());
        let result = reader
            .search(
                cas,
                None,
                vec![exact_payload(vec![(1621867415, 1.6877435)], 10)],
            )
            .await
            .unwrap();
        result.results[0]
            .records
            .iter()
            .map(|r| (r.id.clone(), r.score.unwrap()))
            .collect()
    }

    // BM25 with IDF as on Chroma Cloud once the collection is indexed: only fox1 and fox2, with the score of Cloud.
    #[tokio::test]
    async fn test_sparse_bm25_scores_of_chroma_cloud() {
        // 4 records, n_t 2: the distributed cluster on AKS answered -0.9481 after compaction.
        let four = bm25_fox(0).await;
        assert_eq!(
            four.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["fox1", "fox2"]
        );
        for (_, score) in &four {
            assert!((score - (-0.9481)).abs() < 5e-5, "{score}");
        }
        // 304 records, n_t 2: Chroma Cloud answered -12.50207 (CLOUD-PROVE.md, test B, after indexing).
        let many = bm25_fox(300).await;
        assert_eq!(
            many.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["fox1", "fox2"]
        );
        for (_, score) in &many {
            assert!((score - (-12.50207)).abs() < 5e-6, "{score}");
        }
    }

    // Without bm25 on the key the query is not weighted: 1 - q.d.
    #[tokio::test]
    async fn test_sparse_without_bm25_is_not_weighted() {
        let (reader, cas) = setup_with_logs(
            bm25_documents()
                .into_iter()
                .enumerate()
                .map(|(i, (id, pairs))| {
                    sparse_log(
                        i as i64 + 1,
                        id,
                        Operation::Add,
                        Some(UpdateMetadataValue::SparseVector(
                            chroma_types::SparseVector::from_pairs(pairs),
                        )),
                    )
                })
                .collect(),
        )
        .await;
        let result = reader
            .search(
                cas,
                None,
                vec![exact_payload(vec![(1621867415, 1.6877435)], 10)],
            )
            .await
            .unwrap();
        for record in &result.results[0].records {
            assert_eq!(record.score.unwrap(), 1.0 - 1.6877435_f32 * 1.6652868_f32);
        }
    }

    // The Search beyond one leaf: two leaves fused with their defaults, a group by on the metadata, and no rank.
    #[tokio::test]
    async fn test_search_fuses_leaves_groups_and_keeps_storage_order() {
        use chroma_types::operator::{
            Aggregate, GroupBy, Key, QueryVector, Rank, RankExpr, Select,
        };
        let logs = [
            ("a", vec![(1, 1.0)], "x"),
            ("b", vec![(1, 0.5), (2, 1.0)], "x"),
            ("c", vec![(2, 0.5)], "y"),
            ("d", vec![(9, 1.0)], "y"),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (id, pairs, label))| {
            let mut log = sparse_log(
                i as i64 + 1,
                id,
                Operation::Add,
                Some(UpdateMetadataValue::SparseVector(
                    chroma_types::SparseVector::from_pairs(pairs),
                )),
            );
            log.record
                .metadata
                .as_mut()
                .unwrap()
                .insert("label".into(), UpdateMetadataValue::Str(label.into()));
            log
        })
        .collect();
        let (reader, cas) = setup_with_logs(logs).await;
        let leaf = |dim: u32| RankExpr::Knn {
            query: QueryVector::Sparse(chroma_types::SparseVector::from_pairs(vec![(dim, 1.0)])),
            key: Key::MetadataField("sparse".into()),
            limit: 10,
            default: Some(1.0),
            return_rank: false,
        };
        let score = Select {
            keys: [Key::Score].into_iter().collect(),
        };
        // a 0.0 + 1.0 (default), b 0.5 + 0.0, c 1.0 (default) + 0.5; d is in neither leaf.
        let fused = chroma_types::plan::SearchPayload {
            rank: Rank {
                expr: Some(RankExpr::Summation(vec![leaf(1), leaf(2)])),
            },
            select: score.clone(),
            ..Default::default()
        };
        let mut grouped = fused.clone();
        grouped.group_by = GroupBy {
            keys: vec![Key::MetadataField("label".into())],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };
        let unranked = chroma_types::plan::SearchPayload {
            limit: Limit {
                offset: 1,
                limit: Some(2),
            },
            select: score,
            ..Default::default()
        };
        let result = reader
            .search(cas, None, vec![fused, grouped, unranked])
            .await
            .unwrap();
        let rows = |i: usize| {
            result.results[i]
                .records
                .iter()
                .map(|r| (r.id.as_str(), r.score.unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(rows(0), vec![("b", 0.5), ("a", 1.0), ("c", 1.5)]);
        assert_eq!(rows(1), vec![("b", 0.5), ("c", 1.5)]);
        // Without a rank the records come in storage order, and the score is the position after the offset.
        assert_eq!(rows(2), vec![("b", 0.0), ("c", 1.0)]);
    }

    #[tokio::test]
    async fn test_sparse_exact_search_signed_disjoint_empty_and_pagination() {
        use chroma_types::operator::Key;
        let docs = [
            ("a", vec![(1, -2.0)]),
            ("b", vec![(9, 1.0)]),
            ("c", vec![]),
            ("d", vec![(1, 2.0)]),
            ("e", vec![(1, 2.0)]),
        ];
        let mut logs: Vec<_> = docs
            .into_iter()
            .enumerate()
            .map(|(i, (id, vector))| {
                let mut log = sparse_log(
                    i as i64 + 1,
                    id,
                    Operation::Add,
                    Some(UpdateMetadataValue::SparseVector(
                        chroma_types::SparseVector::from_pairs(vector),
                    )),
                );
                log.record.document = Some(format!("document-{id}"));
                log.record
                    .metadata
                    .as_mut()
                    .unwrap()
                    .insert("label".into(), UpdateMetadataValue::Str(id.into()));
                log
            })
            .collect();
        logs.push(sparse_log(6, "absent", Operation::Add, None));
        let (reader, cas) = setup_with_logs(logs).await;
        let full = exact_payload(vec![(1, 1.0)], 10);
        let mut page = exact_payload(vec![(1, 1.0)], 2);
        page.limit = Limit {
            offset: 1,
            limit: Some(3),
        };
        page.select.keys = [Key::MetadataField("label".into()), Key::Score]
            .into_iter()
            .collect();
        let mut filtered = full.clone();
        filtered.filter.query_ids = Some(vec!["a".into(), "b".into(), "c".into()]);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reader.search(cas.clone(), None, vec![full, page, filtered]),
        )
        .await
        .expect("single-connection Search must not self-wait")
        .unwrap();
        let actual: Vec<_> = result.results[0]
            .records
            .iter()
            .map(|row| (row.id.as_str(), row.score.unwrap()))
            .collect();
        // b shares no dimension with the query and c is empty: neither is a candidate.
        assert_eq!(actual, vec![("d", -1.0), ("e", -1.0), ("a", 3.0)]);
        assert_eq!(
            result.results[0].records[0].document.as_deref(),
            Some("document-d")
        );
        let page = &result.results[1].records;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, "e");
        assert!(page[0].document.is_none());
        assert_eq!(page[0].metadata.as_ref().unwrap().len(), 1);
        assert!(page[0].metadata.as_ref().unwrap().contains_key("label"));
        assert_eq!(
            result.results[2]
                .records
                .iter()
                .map(|r| r.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        for keys in [
            vec![],
            vec![Key::Metadata],
            vec![Key::MetadataField("label".into())],
        ] {
            let mut projection = exact_payload(vec![(1, 1.0)], 1);
            projection.select.keys = keys.iter().cloned().collect();
            let result = reader
                .search(cas.clone(), None, vec![projection])
                .await
                .unwrap();
            let record = &result.results[0].records[0];
            assert_eq!(record.id, "d");
            assert!(record.document.is_none());
            assert!(record.score.is_none());
            assert!(record.embedding.is_none());
            if keys.is_empty() {
                assert!(record.metadata.is_none());
            } else if keys == vec![Key::Metadata] {
                assert!(record.metadata.as_ref().unwrap().contains_key("sparse"));
            } else {
                assert_eq!(record.metadata.as_ref().unwrap().len(), 1);
            }
        }
        let mut scalar_filtered = exact_payload(vec![(1, 1.0)], 10);
        scalar_filtered.filter.where_clause = Some(chroma_types::Where::Metadata(
            chroma_types::MetadataExpression {
                key: "label".into(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    chroma_types::PrimitiveOperator::Equal,
                    chroma_types::MetadataValue::Str("a".into()),
                ),
            },
        ));
        let result = reader
            .search(cas.clone(), None, vec![scalar_filtered])
            .await
            .unwrap();
        assert_eq!(
            result.results[0]
                .records
                .iter()
                .map(|r| r.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        for (wire_filter, expected) in [
            (serde_json::json!({"#id":{"$in":["a","b","c"]}}), vec!["a"]),
            (serde_json::json!({"#id":{"$nin":["a","d","e"]}}), vec![]),
            (serde_json::json!({"#id":{"$eq":"a"}}), vec!["a"]),
            (
                serde_json::json!({"$and":[{"#id":{"$in":["a","d"]}},{"label":{"$eq":"a"}}]}),
                vec!["a"],
            ),
            (
                serde_json::json!({"$or":[{"#id":{"$eq":"a"}},{"label":{"$eq":"d"}}]}),
                vec!["d", "a"],
            ),
            (serde_json::json!({"#id":{"$ne":"a"}}), vec!["d", "e"]),
        ] {
            let mut payload = exact_payload(vec![(1, 1.0)], 10);
            payload.filter = serde_json::from_value(wire_filter).unwrap();
            assert!(
                payload.filter.query_ids.is_none(),
                "exercise public Search wire filter"
            );
            let result = reader
                .search(cas.clone(), None, vec![payload])
                .await
                .unwrap();
            assert_eq!(
                result.results[0]
                    .records
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let negative = reader
            .search(cas.clone(), None, vec![exact_payload(vec![(1, -1.0)], 2)])
            .await
            .unwrap();
        assert_eq!(
            negative.results[0]
                .records
                .iter()
                .map(|r| (r.id.as_str(), r.score.unwrap()))
                .collect::<Vec<_>>(),
            vec![("a", -1.0), ("d", 3.0)]
        );
        assert!(reader
            .search(cas.clone(), None, vec![exact_payload(vec![], 0)])
            .await
            .unwrap()
            .results[0]
            .records
            .is_empty());
        // As distributed Chroma, the accumulated score is not checked: here it overflows.
        let overflow = reader
            .search(cas, None, vec![exact_payload(vec![(1, f32::MAX)], 2)])
            .await
            .unwrap();
        assert_eq!(
            overflow.results[0]
                .records
                .iter()
                .map(|r| (r.id.as_str(), r.score.unwrap()))
                .collect::<Vec<_>>(),
            vec![("d", f32::NEG_INFINITY), ("e", f32::NEG_INFINITY)]
        );
    }

    #[tokio::test]
    async fn test_sparse_exact_search_ephemeral_and_cancelled_connection() {
        use chroma_config::{registry::Registry, Configurable};
        use chroma_sqlite::config::{MigrationHash, MigrationMode, SqliteDBConfig};
        let db = chroma_sqlite::db::SqliteDb::try_from_config(
            &SqliteDBConfig {
                url: None,
                hash_type: MigrationHash::MD5,
                migration_mode: MigrationMode::Apply,
            },
            &Registry::new(),
        )
        .await
        .unwrap();
        let reader = SqliteMetadataReader { db: db.clone() };
        let cas = CollectionAndSegments::test(3);
        let connection = db.get_conn().acquire().await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            reader.search(cas.clone(), None, vec![exact_payload(vec![], 2)])
        )
        .await
        .is_err());
        drop(connection);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reader.search(cas, None, vec![exact_payload(vec![], 2)]),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(result.results[0].records.is_empty());
    }

    #[tokio::test]
    async fn test_sparse_exact_search_keeps_score_and_projection_in_snapshot() {
        let mut initial = sparse_log(
            1,
            "record",
            Operation::Add,
            Some(UpdateMetadataValue::SparseVector(
                chroma_types::SparseVector::from_pairs(vec![(1, 2.0)]),
            )),
        );
        initial.record.document = Some("old".into());
        let (reader, cas) = setup_with_logs(vec![initial]).await;
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(reader.db.get_conn())
            .await
            .unwrap();
        let mut snapshot = reader.db.get_conn().begin().await.unwrap();
        sqlx::query("SELECT seq_id FROM max_seq_id WHERE segment_id = ?")
            .bind(cas.metadata_segment.id.to_string())
            .fetch_optional(&mut *snapshot)
            .await
            .unwrap();
        let writer = SqliteMetadataWriter {
            db: reader.db.clone(),
        };
        let mut update = sparse_log(
            2,
            "record",
            Operation::Update,
            Some(UpdateMetadataValue::SparseVector(
                chroma_types::SparseVector::from_pairs(vec![(1, -3.0)]),
            )),
        );
        update.record.document = Some("new".into());
        let mut tx = writer.begin().await.unwrap();
        writer
            .apply_logs(
                Chunk::new(vec![update].into()),
                cas.metadata_segment.id,
                cas.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let old = reader
            .search_with_connection(
                cas.clone(),
                None,
                vec![exact_payload(vec![(1, 1.0)], 2)],
                &mut snapshot,
            )
            .await
            .unwrap();
        assert_eq!(old.results[0].records[0].score, Some(-1.0));
        assert_eq!(old.results[0].records[0].document.as_deref(), Some("old"));
        snapshot.commit().await.unwrap();
        let new = reader
            .search(cas, None, vec![exact_payload(vec![(1, 1.0)], 2)])
            .await
            .unwrap();
        assert_eq!(new.results[0].records[0].score, Some(4.0));
        assert_eq!(new.results[0].records[0].document.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn test_sparse_presence_guard_tracks_same_chunk_and_committed_transitions() {
        let sparse = || {
            UpdateMetadataValue::SparseVector(chroma_types::SparseVector::from_pairs(vec![(
                1, 2.0,
            )]))
        };
        let (reader, cas) = setup_with_logs(vec![
            sparse_log(1, "a", Operation::Add, Some(UpdateMetadataValue::Int(1))),
            sparse_log(2, "a", Operation::Update, Some(sparse())),
            sparse_log(3, "a", Operation::Update, Some(UpdateMetadataValue::Int(3))),
            sparse_log(4, "b", Operation::Add, Some(sparse())),
            sparse_log(5, "b", Operation::Update, Some(UpdateMetadataValue::None)),
            sparse_log(6, "c", Operation::Add, Some(sparse())),
            sparse_log(7, "c", Operation::Delete, None),
            sparse_log(8, "c", Operation::Add, Some(UpdateMetadataValue::Int(4))),
            sparse_log(9, "a", Operation::Add, Some(sparse())),
            sparse_log(10, "missing", Operation::Update, Some(sparse())),
        ])
        .await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embedding_metadata_sparse")
            .fetch_one(reader.db.get_conn())
            .await
            .unwrap();
        assert_eq!(count, 0);
        let records = reader
            .get(make_get_plan(&cas, None))
            .await
            .unwrap()
            .result
            .records;
        assert_eq!(records.len(), 3);
        assert_eq!(
            records
                .iter()
                .find(|row| row.id == "a")
                .unwrap()
                .metadata
                .as_ref()
                .unwrap()["sparse"],
            MetadataValue::Int(3)
        );
        assert_eq!(
            records
                .iter()
                .find(|row| row.id == "c")
                .unwrap()
                .metadata
                .as_ref()
                .unwrap()["sparse"],
            MetadataValue::Int(4)
        );
        let writer = SqliteMetadataWriter {
            db: reader.db.clone(),
        };
        for (seq, value) in [
            (11, sparse()),
            (12, UpdateMetadataValue::Str("scalar".into())),
        ] {
            let mut tx = writer.begin().await.unwrap();
            writer
                .apply_logs(
                    Chunk::new(vec![sparse_log(seq, "a", Operation::Update, Some(value))].into()),
                    cas.metadata_segment.id,
                    cas.collection.schema.clone(),
                    &mut *tx,
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embedding_metadata_sparse")
            .fetch_one(reader.db.get_conn())
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "a new chunk must discover previously committed sparse values"
        );
    }

    fn sparse_log(
        seq: i64,
        id: &str,
        operation: Operation,
        value: Option<UpdateMetadataValue>,
    ) -> LogRecord {
        LogRecord {
            log_offset: seq,
            record: OperationRecord {
                id: id.to_string(),
                metadata: value.map(|value| HashMap::from([("sparse".to_string(), value)])),
                document: None,
                operation,
                embedding: None,
                encoding: None,
            },
        }
    }

    fn sparse_value(weight: f32) -> UpdateMetadataValue {
        UpdateMetadataValue::SparseVector(
            chroma_types::SparseVector::new_with_tokens(
                vec![1, 4],
                vec![weight, 0.0],
                vec!["token one".to_string(), "λ".to_string()],
            )
            .unwrap(),
        )
    }

    // A Get with metadata reads the sparse vector of every selected row, across batches, and only of those rows.
    #[tokio::test]
    async fn test_get_reads_sparse_vectors_of_every_selected_row() {
        let logs = (0..1200)
            .map(|i| {
                let mut log = sparse_log(
                    i as i64 + 1,
                    &format!("r{i}"),
                    Operation::Add,
                    Some(UpdateMetadataValue::SparseVector(
                        chroma_types::SparseVector::from_pairs(vec![(i as u32, 1.0)]),
                    )),
                );
                log.record
                    .metadata
                    .as_mut()
                    .unwrap()
                    .insert("i".into(), UpdateMetadataValue::Int(i));
                log
            })
            .collect();
        let (reader, cas) = setup_with_logs(logs).await;
        let first_1100 = Where::Metadata(MetadataExpression {
            key: "i".into(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::LessThan,
                MetadataValue::Int(1100),
            ),
        });
        let records = reader
            .get(make_get_plan(&cas, Some(first_1100)))
            .await
            .unwrap()
            .result
            .records;
        assert_eq!(records.len(), 1100);
        for record in records {
            let metadata = record.metadata.unwrap();
            let Some(MetadataValue::Int(i)) = metadata.get("i") else {
                panic!("missing i");
            };
            assert_eq!(
                metadata.get("sparse"),
                Some(&MetadataValue::SparseVector(
                    chroma_types::SparseVector::from_pairs(vec![(*i as u32, 1.0)])
                ))
            );
        }
    }

    // Every query of a get runs in one read transaction: stopped after its first query, the get still holds SQLite, so that
    // a write from another connection cannot commit before the get ends (with no busy timeout it fails at once), and the
    // get answers the records of the moment it started, its sparse vectors included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_get_runs_its_queries_in_one_read_transaction() {
        let mut log = sparse_log(1, "r0", Operation::Add, Some(sparse_value(1.0)));
        log.record
            .metadata
            .as_mut()
            .unwrap()
            .insert("i".into(), UpdateMetadataValue::Int(0));
        let (reader, cas) = setup_with_logs(vec![log]).await;
        let (stopped_tx, stopped) = tokio::sync::oneshot::channel();
        let (go_on, waiting) = tokio::sync::oneshot::channel();
        super::STOP_GET
            .lock()
            .unwrap()
            .push((cas.metadata_segment.id, stopped_tx, waiting));
        let get = tokio::spawn({
            let reader = reader.clone();
            let plan = make_get_plan(&cas, None);
            async move { reader.get(plan).await }
        });
        stopped.await.unwrap();
        let mut writer = reader.db.get_conn().acquire().await.unwrap();
        sqlx::query("PRAGMA busy_timeout = 0")
            .execute(&mut *writer)
            .await
            .unwrap();
        let write = sqlx::query("UPDATE embedding_metadata SET int_value = 1 WHERE key = 'i'")
            .execute(&mut *writer)
            .await;
        let _ = go_on.send(());
        let records = get.await.unwrap().unwrap().result.records;
        assert!(
            write.is_err(),
            "a write committed while the get was stopped: {write:?}"
        );
        let metadata = records[0].metadata.as_ref().unwrap();
        assert_eq!(metadata.get("i"), Some(&MetadataValue::Int(0)));
        assert!(matches!(
            metadata.get("sparse"),
            Some(MetadataValue::SparseVector(_))
        ));
    }

    // A get keeps the rows of its selection until after its last query: its queries run on one connection, and rows freed
    // before the next query had malloc merge their memory at the first large allocation of that query, on the same
    // thread (in a cold get of 20000 records on a folder of Windows mounted in Docker, 17.4 ms inside malloc).
    #[tokio::test]
    async fn test_get_keeps_its_selection_until_its_last_query() {
        let logs = (1..=3)
            .map(|i| sparse_log(i, &format!("r{i}"), Operation::Add, Some(sparse_value(1.0))))
            .collect();
        let (reader, cas) = setup_with_logs(logs).await;
        let result = reader.get(make_get_plan(&cas, None)).await.unwrap();
        assert_eq!(result.result.records.len(), 3);
        let held = super::ROWS_AFTER_LAST_QUERY
            .lock()
            .unwrap()
            .iter()
            .find(|(segment, _)| *segment == cas.metadata_segment.id)
            .map(|(_, rows)| *rows);
        assert!(
            held.unwrap_or(0) >= 3,
            "rows held after the last query: {held:?}"
        );
    }

    #[tokio::test]
    async fn test_sparse_metadata_roundtrip_tokens_and_empty() {
        let empty = UpdateMetadataValue::SparseVector(
            chroma_types::SparseVector::new(vec![], vec![]).unwrap(),
        );
        let (reader, cas) = setup_with_logs(vec![
            sparse_log(1, "tokens", Operation::Add, Some(sparse_value(-2.0))),
            sparse_log(2, "empty", Operation::Add, Some(empty.clone())),
        ])
        .await;
        let result = reader.get(make_get_plan(&cas, None)).await.unwrap();
        assert_eq!(result.result.records.len(), 2);
        assert_eq!(
            result.result.records[0].metadata.as_ref().unwrap()["sparse"],
            MetadataValue::try_from(&sparse_value(-2.0)).unwrap()
        );
        assert_eq!(
            result.result.records[1].metadata.as_ref().unwrap()["sparse"],
            MetadataValue::try_from(&empty).unwrap()
        );
        let mut without_metadata = make_get_plan(&cas, None);
        without_metadata.proj.metadata = false;
        assert!(reader
            .get(without_metadata)
            .await
            .unwrap()
            .result
            .records
            .iter()
            .all(|record| record.metadata.is_none()));
    }

    #[tokio::test]
    async fn test_sparse_metadata_operations_and_transitions() {
        let (reader, cas) = setup_with_logs(vec![sparse_log(
            1,
            "row",
            Operation::Add,
            Some(sparse_value(1.0)),
        )])
        .await;
        let writer = SqliteMetadataWriter::new(reader.db.clone());
        let stages = vec![
            (
                Operation::Add,
                Some(sparse_value(9.0)),
                Some(sparse_value(1.0)),
            ), // duplicate Add unchanged
            (Operation::Update, None, Some(sparse_value(1.0))), // omitted key unchanged
            (
                Operation::Update,
                Some(UpdateMetadataValue::Str("scalar".into())),
                Some(UpdateMetadataValue::Str("scalar".into())),
            ),
            (
                Operation::Update,
                Some(sparse_value(2.0)),
                Some(sparse_value(2.0)),
            ),
            (
                Operation::Upsert,
                Some(UpdateMetadataValue::IntArray(vec![1, 2])),
                Some(UpdateMetadataValue::IntArray(vec![1, 2])),
            ),
            (
                Operation::Upsert,
                Some(sparse_value(3.0)),
                Some(sparse_value(3.0)),
            ),
            (Operation::Update, Some(UpdateMetadataValue::None), None),
            (
                Operation::Upsert,
                Some(sparse_value(4.0)),
                Some(sparse_value(4.0)),
            ),
        ];
        for (i, (op, value, expected)) in stages.into_iter().enumerate() {
            let mut tx = writer.begin().await.unwrap();
            writer
                .apply_logs(
                    Chunk::new(vec![sparse_log(i as i64 + 2, "row", op, value)].into()),
                    cas.metadata_segment.id,
                    None,
                    &mut *tx,
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let records = reader
                .get(make_get_plan(&cas, None))
                .await
                .unwrap()
                .result
                .records;
            let actual = records[0]
                .metadata
                .as_ref()
                .and_then(|meta| meta.get("sparse"))
                .cloned();
            assert_eq!(
                actual,
                expected.map(|value| MetadataValue::try_from(&value).unwrap())
            );
            let sparse_rows: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM embedding_metadata_sparse")
                    .fetch_one(reader.db.get_conn())
                    .await
                    .unwrap();
            assert_eq!(
                sparse_rows,
                i64::from(matches!(actual, Some(MetadataValue::SparseVector(_))))
            );
        }
        let mut tx = writer.begin().await.unwrap();
        writer
            .apply_logs(
                Chunk::new(
                    vec![
                        sparse_log(20, "missing", Operation::Update, Some(sparse_value(5.0))),
                        sparse_log(21, "row", Operation::Delete, None),
                    ]
                    .into(),
                ),
                cas.metadata_segment.id,
                None,
                &mut *tx,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(reader
            .get(make_get_plan(&cas, None))
            .await
            .unwrap()
            .result
            .records
            .is_empty());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embedding_metadata_sparse")
            .fetch_one(reader.db.get_conn())
            .await
            .unwrap();
        assert_eq!(count, 0);
        let mut tx = writer.begin().await.unwrap();
        writer
            .apply_logs(
                Chunk::new(
                    vec![sparse_log(
                        22,
                        "row",
                        Operation::Upsert,
                        Some(sparse_value(6.0)),
                    )]
                    .into(),
                ),
                cas.metadata_segment.id,
                None,
                &mut *tx,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            reader
                .get(make_get_plan(&cas, None))
                .await
                .unwrap()
                .result
                .records[0]
                .metadata
                .as_ref()
                .unwrap()["sparse"],
            MetadataValue::try_from(&sparse_value(6.0)).unwrap()
        );
    }

    #[tokio::test]
    async fn test_sparse_metadata_failure_rolls_back_batch_and_watermark() {
        let writer = SqliteMetadataWriter::new(get_new_sqlite_db().await);
        let cas = CollectionAndSegments::test(3);
        let mut tx = writer.begin().await.unwrap();
        let result = writer
            .apply_logs(
                Chunk::new(
                    vec![
                        sparse_log(1, "valid", Operation::Add, Some(sparse_value(1.0))),
                        sparse_log(2, "invalid", Operation::Add, Some(sparse_value(f32::NAN))),
                    ]
                    .into(),
                ),
                cas.metadata_segment.id,
                None,
                &mut *tx,
            )
            .await;
        assert!(result.is_err());
        tx.rollback().await.unwrap();
        for table in ["embeddings", "embedding_metadata_sparse", "max_seq_id"] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(writer.db.get_conn())
                .await
                .unwrap();
            assert_eq!(count, 0, "rollback left rows in {table}");
        }
    }

    #[tokio::test]
    async fn test_array_metadata_write_and_read() {
        // Write records with array metadata and read them back.
        let mut meta = HashMap::new();
        meta.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["action".to_string(), "comedy".to_string()]),
        );
        meta.insert(
            "scores".to_string(),
            UpdateMetadataValue::IntArray(vec![10, 20, 30]),
        );
        meta.insert(
            "scalar_key".to_string(),
            UpdateMetadataValue::Str("plain".to_string()),
        );

        let logs = vec![LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "id1".to_string(),
                metadata: Some(meta),
                document: None,
                operation: Operation::Add,
                embedding: None,
                encoding: None,
            },
        }];

        let (reader, cas) = setup_with_logs(logs).await;
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);

        let md = result.result.records[0]
            .metadata
            .as_ref()
            .expect("metadata present");

        // Scalar comes back as scalar.
        assert_eq!(
            md.get("scalar_key"),
            Some(&MetadataValue::Str("plain".to_string()))
        );

        // String array round-trips (order may vary within the array).
        match md.get("tags") {
            Some(MetadataValue::StringArray(arr)) => {
                let mut sorted = arr.clone();
                sorted.sort();
                assert_eq!(sorted, vec!["action", "comedy"]);
            }
            other => panic!("Expected StringArray for 'tags', got {:?}", other),
        }

        // Int array round-trips.
        match md.get("scores") {
            Some(MetadataValue::IntArray(arr)) => {
                let mut sorted = arr.clone();
                sorted.sort();
                assert_eq!(sorted, vec![10, 20, 30]);
            }
            other => panic!("Expected IntArray for 'scores', got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_array_contains_query() {
        // Two records: id1 has tags ["action", "comedy"], id2 has tags ["drama"].
        let mut meta1 = HashMap::new();
        meta1.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["action".to_string(), "comedy".to_string()]),
        );
        let mut meta2 = HashMap::new();
        meta2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["drama".to_string()]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id2".to_string(),
                    metadata: Some(meta2),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // $contains "action" → should match only id1
        let contains_action = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("action".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_action));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // $contains "drama" → should match only id2
        let contains_drama = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("drama".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_drama));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");

        // $contains "unknown" → no matches
        let contains_none = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("unknown".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_none));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);
    }

    #[tokio::test]
    async fn test_array_not_contains_query() {
        // id1: tags=["action","comedy"], id2: tags=["drama"]
        let mut meta1 = HashMap::new();
        meta1.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["action".to_string(), "comedy".to_string()]),
        );
        let mut meta2 = HashMap::new();
        meta2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["drama".to_string()]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id2".to_string(),
                    metadata: Some(meta2),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // $not_contains "action" → should exclude id1, returning only id2
        let not_contains_action = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::NotContains,
                MetadataValue::Str("action".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(not_contains_action));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");
    }

    #[tokio::test]
    async fn test_array_int_contains_query() {
        // Test contains on integer arrays.
        let mut meta1 = HashMap::new();
        meta1.insert(
            "scores".to_string(),
            UpdateMetadataValue::IntArray(vec![1, 2, 3]),
        );
        let mut meta2 = HashMap::new();
        meta2.insert(
            "scores".to_string(),
            UpdateMetadataValue::IntArray(vec![4, 5]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id2".to_string(),
                    metadata: Some(meta2),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // $contains 2 → id1 only
        let contains_2 = Where::Metadata(MetadataExpression {
            key: "scores".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Int(2),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_2));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // $contains 5 → id2 only
        let contains_5 = Where::Metadata(MetadataExpression {
            key: "scores".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Int(5),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_5));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");
    }

    #[tokio::test]
    async fn test_array_metadata_update() {
        // Verify that updating an array replaces the old exploded rows.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["old_a".to_string(), "old_b".to_string()]),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["new_x".to_string()]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // After update, $contains "old_a" should NOT match
        let contains_old = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("old_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_old));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);

        // $contains "new_x" should match
        let contains_new = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("new_x".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_new));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // Read back metadata to verify the array was replaced
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        let md = result.result.records[0]
            .metadata
            .as_ref()
            .expect("metadata");
        match md.get("tags") {
            Some(MetadataValue::StringArray(arr)) => {
                assert_eq!(arr, &vec!["new_x".to_string()]);
            }
            other => panic!("Expected StringArray, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_mixed_scalar_and_array_metadata() {
        // Verify that scalar metadata (embedding_metadata table) and
        // array metadata (embedding_metadata_array table) coexist correctly
        // on the same record and are both returned when projecting.
        let mut meta = HashMap::new();
        meta.insert(
            "name".to_string(),
            UpdateMetadataValue::Str("Alice".to_string()),
        );
        meta.insert("score".to_string(), UpdateMetadataValue::Int(42));
        meta.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["admin".to_string(), "user".to_string()]),
        );
        meta.insert(
            "ratings".to_string(),
            UpdateMetadataValue::FloatArray(vec![4.5, 3.2]),
        );

        let logs = vec![LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "id1".to_string(),
                metadata: Some(meta),
                document: Some("hello world".to_string()),
                operation: Operation::Add,
                embedding: None,
                encoding: None,
            },
        }];

        let (reader, cas) = setup_with_logs(logs).await;

        // Read back all metadata (no filter).
        let plan = Get {
            scan: Scan {
                collection_and_segments: cas.clone(),
                shard_index: 0,
                num_shards: 1,
                log_upper_bound_offset: 0,
            },
            filter: Filter {
                query_ids: None,
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
        };
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);

        let rec = &result.result.records[0];
        assert_eq!(rec.document.as_deref(), Some("hello world"));

        let md = rec.metadata.as_ref().expect("metadata");
        // Scalars
        assert_eq!(
            md.get("name"),
            Some(&MetadataValue::Str("Alice".to_string()))
        );
        assert_eq!(md.get("score"), Some(&MetadataValue::Int(42)));
        // Arrays
        match md.get("tags") {
            Some(MetadataValue::StringArray(arr)) => {
                let mut sorted = arr.clone();
                sorted.sort();
                assert_eq!(sorted, vec!["admin", "user"]);
            }
            other => panic!("Expected StringArray for 'tags', got {:?}", other),
        }
        match md.get("ratings") {
            Some(MetadataValue::FloatArray(arr)) => {
                assert_eq!(arr.len(), 2);
                let mut sorted = arr.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                assert!((sorted[0] - 3.2).abs() < 1e-6);
                assert!((sorted[1] - 4.5).abs() < 1e-6);
            }
            other => panic!("Expected FloatArray for 'ratings', got {:?}", other),
        }

        // Now filter by $contains on the array while scalars also present.
        let contains_admin = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("admin".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_admin));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // Filter by scalar while arrays also present.
        let eq_score = Where::Metadata(MetadataExpression {
            key: "score".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Int(42),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_score));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");
    }

    #[tokio::test]
    async fn test_delete_cleans_array_metadata() {
        // Add a record with array metadata, then delete it.
        // Verify the record is gone and the array table is clean
        // (a subsequent add + contains query doesn't see stale data).
        let mut meta = HashMap::new();
        meta.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["stale_a".to_string(), "stale_b".to_string()]),
        );
        meta.insert("score".to_string(), UpdateMetadataValue::Int(99));

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            // Delete the same record.
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // No records should exist.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);

        // A $contains query for the old value should also return nothing,
        // confirming no orphaned rows in the array table.
        let contains_stale = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("stale_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_stale));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);
    }

    #[tokio::test]
    async fn test_float_and_bool_contains_query() {
        // Test $contains on float and bool array types.
        let mut meta1 = HashMap::new();
        meta1.insert(
            "ratings".to_string(),
            UpdateMetadataValue::FloatArray(vec![4.5, 3.2, 1.0]),
        );
        meta1.insert(
            "flags".to_string(),
            UpdateMetadataValue::BoolArray(vec![true, false]),
        );

        let mut meta2 = HashMap::new();
        meta2.insert(
            "ratings".to_string(),
            UpdateMetadataValue::FloatArray(vec![2.0, 5.0]),
        );
        meta2.insert(
            "flags".to_string(),
            UpdateMetadataValue::BoolArray(vec![false]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id2".to_string(),
                    metadata: Some(meta2),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // Float $contains 4.5 → id1 only
        let contains_4_5 = Where::Metadata(MetadataExpression {
            key: "ratings".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Float(4.5),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_4_5));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // Float $contains 5.0 → id2 only
        let contains_5 = Where::Metadata(MetadataExpression {
            key: "ratings".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Float(5.0),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_5));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");

        // Float $not_contains 4.5 → id2 only
        let not_contains_4_5 = Where::Metadata(MetadataExpression {
            key: "ratings".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::NotContains,
                MetadataValue::Float(4.5),
            ),
        });
        let plan = make_get_plan(&cas, Some(not_contains_4_5));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");

        // Bool $contains true → id1 only (id2 has only [false])
        let contains_true = Where::Metadata(MetadataExpression {
            key: "flags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Bool(true),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_true));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // Bool $contains false → both id1 and id2
        let contains_false = Where::Metadata(MetadataExpression {
            key: "flags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Bool(false),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_false));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 2);

        // Bool $not_contains true → id2 only
        let not_contains_true = Where::Metadata(MetadataExpression {
            key: "flags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::NotContains,
                MetadataValue::Bool(true),
            ),
        });
        let plan = make_get_plan(&cas, Some(not_contains_true));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");

        // Read back and verify array types are correct.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 2);

        for rec in &result.result.records {
            let md = rec.metadata.as_ref().expect("metadata");
            match md.get("ratings") {
                Some(MetadataValue::FloatArray(_)) => {}
                other => panic!(
                    "Expected FloatArray for 'ratings' on {}, got {:?}",
                    rec.id, other
                ),
            }
            match md.get("flags") {
                Some(MetadataValue::BoolArray(_)) => {}
                other => panic!(
                    "Expected BoolArray for 'flags' on {}, got {:?}",
                    rec.id, other
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_delete_isolation() {
        // Two records with array metadata. Delete one, verify the other is
        // completely unaffected — both its scalar and array metadata survive.
        let mut meta1 = HashMap::new();
        meta1.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["a".to_string(), "b".to_string()]),
        );
        meta1.insert("x".to_string(), UpdateMetadataValue::Int(1));

        let mut meta2 = HashMap::new();
        meta2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["c".to_string(), "d".to_string()]),
        );
        meta2.insert("x".to_string(), UpdateMetadataValue::Int(2));

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id2".to_string(),
                    metadata: Some(meta2),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            // Delete only id1.
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // Only id2 should remain.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");

        let md = result.result.records[0]
            .metadata
            .as_ref()
            .expect("metadata");
        assert_eq!(md.get("x"), Some(&MetadataValue::Int(2)));
        match md.get("tags") {
            Some(MetadataValue::StringArray(arr)) => {
                let mut sorted = arr.clone();
                sorted.sort();
                assert_eq!(sorted, vec!["c", "d"]);
            }
            other => panic!("Expected StringArray for 'tags', got {:?}", other),
        }

        // $contains "a" (from deleted id1) should return nothing.
        let contains_a = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_a));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);

        // $contains "c" (from surviving id2) should return id2.
        let contains_c = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("c".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_c));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id2");
    }

    #[tokio::test]
    async fn test_partial_metadata_key_delete() {
        // A record has scalar "name", array "tags", and array "scores".
        // Update with tags=None to delete just that key.
        // Verify "name" and "scores" survive, "tags" is gone, and
        // $contains queries reflect the change.
        let mut meta = HashMap::new();
        meta.insert(
            "name".to_string(),
            UpdateMetadataValue::Str("Alice".to_string()),
        );
        meta.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["admin".to_string(), "user".to_string()]),
        );
        meta.insert(
            "scores".to_string(),
            UpdateMetadataValue::IntArray(vec![10, 20]),
        );

        let mut delete_tags = HashMap::new();
        delete_tags.insert("tags".to_string(), UpdateMetadataValue::None);

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            // Update: delete only the "tags" key.
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(delete_tags),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // Read back: "tags" should be gone, "name" and "scores" survive.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);

        let md = result.result.records[0]
            .metadata
            .as_ref()
            .expect("metadata");
        assert_eq!(
            md.get("name"),
            Some(&MetadataValue::Str("Alice".to_string()))
        );
        assert!(md.get("tags").is_none(), "tags should have been deleted");
        match md.get("scores") {
            Some(MetadataValue::IntArray(arr)) => {
                let mut sorted = arr.clone();
                sorted.sort();
                assert_eq!(sorted, vec![10, 20]);
            }
            other => panic!("Expected IntArray for 'scores', got {:?}", other),
        }

        // $contains "admin" should return nothing (tags deleted).
        let contains_admin = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("admin".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_admin));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);

        // $contains 10 on scores should still work.
        let contains_10 = Where::Metadata(MetadataExpression {
            key: "scores".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Int(10),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_10));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");
    }

    #[tokio::test]
    async fn test_metadata_type_change_scalar_to_array() {
        // Add a record with a scalar "tags" field, then update it to an array.
        // The old scalar value must be cleaned from embedding_metadata so that
        // a scalar equality query no longer matches.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("old_scalar".to_string()),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["new_a".to_string(), "new_b".to_string()]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // The old scalar value must NOT be returned by an equality query.
        let eq_old = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("old_scalar".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_old));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Stale scalar value should have been removed"
        );

        // The new array value should be queryable via $contains.
        let contains_new = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("new_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_new));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // Read back metadata and verify it's an array now.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        let md = result.result.records[0]
            .metadata
            .as_ref()
            .expect("metadata");
        match md.get("tags") {
            Some(MetadataValue::StringArray(arr)) => {
                let mut sorted = arr.clone();
                sorted.sort();
                assert_eq!(sorted, vec!["new_a", "new_b"]);
            }
            other => panic!("Expected StringArray, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_metadata_type_change_array_to_scalar() {
        // Add a record with an array "tags" field, then update it to a scalar.
        // The old array rows must be cleaned from embedding_metadata_array so
        // that a $contains query no longer matches.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["old_a".to_string(), "old_b".to_string()]),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("new_scalar".to_string()),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // The old array values must NOT match a $contains query.
        let contains_old = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("old_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_old));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Stale array rows should have been removed"
        );

        // The new scalar value should be queryable via equality.
        let eq_new = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("new_scalar".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_new));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        assert_eq!(result.result.records[0].id, "id1");

        // Read back metadata and verify it's a scalar now.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        let md = result.result.records[0]
            .metadata
            .as_ref()
            .expect("metadata");
        assert_eq!(
            md.get("tags"),
            Some(&MetadataValue::Str("new_scalar".to_string()))
        );
    }

    #[tokio::test]
    async fn test_metadata_type_change_scalar_to_array_two_apply_logs() {
        // Simulate the e2e path: Add is applied in one apply_logs call,
        // then Update is applied in a second apply_logs call.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("old_scalar".to_string()),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["new_a".to_string(), "new_b".to_string()]),
        );

        let writer = SqliteMetadataWriter {
            db: get_new_sqlite_db().await,
        };
        let cas = CollectionAndSegments::test(3);
        let seg_id = cas.metadata_segment.id;

        // First apply_logs: Add
        let mut tx = writer.begin().await.expect("begin tx");
        writer
            .apply_logs(
                Chunk::new(
                    vec![LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: "id1".to_string(),
                            metadata: Some(meta_v1),
                            document: None,
                            operation: Operation::Add,
                            embedding: None,
                            encoding: None,
                        },
                    }]
                    .into(),
                ),
                seg_id,
                cas.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .expect("apply logs (add)");
        tx.commit().await.expect("commit");

        // Second apply_logs: Update (scalar -> array)
        let mut tx = writer.begin().await.expect("begin tx");
        writer
            .apply_logs(
                Chunk::new(
                    vec![LogRecord {
                        log_offset: 1,
                        record: OperationRecord {
                            id: "id1".to_string(),
                            metadata: Some(meta_v2),
                            document: None,
                            operation: Operation::Update,
                            embedding: None,
                            encoding: None,
                        },
                    }]
                    .into(),
                ),
                seg_id,
                cas.collection.schema.clone(),
                &mut *tx,
            )
            .await
            .expect("apply logs (update)");
        tx.commit().await.expect("commit");

        let reader = SqliteMetadataReader { db: writer.db };

        // The old scalar value must NOT match.
        let eq_old = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("old_scalar".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_old));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Stale scalar value should have been removed after type change"
        );

        // The new array value should be queryable.
        let contains_new = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("new_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_new));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
    }

    #[tokio::test]
    async fn test_metadata_type_change_via_upsert() {
        // Upsert with scalar, then upsert again changing to array.
        // Verifies the Upsert code path (not just Update) also cleans up.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("scalar_val".to_string()),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["arr_a".to_string(), "arr_b".to_string()]),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Upsert,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Upsert,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // Old scalar must be gone.
        let eq_old = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("scalar_val".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_old));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0, "Stale scalar from upsert");

        // New array must be queryable.
        let contains_new = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("arr_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_new));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
    }

    #[tokio::test]
    async fn test_metadata_rapid_type_flip() {
        // scalar -> array -> scalar: two type changes in rapid succession.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("original".to_string()),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["mid_a".to_string()]),
        );

        let mut meta_v3 = HashMap::new();
        meta_v3.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("final_scalar".to_string()),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v3),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // Original scalar must be gone.
        let eq_original = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("original".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_original));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Original scalar should be gone"
        );

        // Intermediate array must be gone.
        let contains_mid = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("mid_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_mid));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Intermediate array should be gone"
        );

        // Final scalar must be queryable.
        let eq_final = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("final_scalar".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_final));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);

        // Verify readback.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records[0]
                .metadata
                .as_ref()
                .unwrap()
                .get("tags"),
            Some(&MetadataValue::Str("final_scalar".to_string()))
        );
    }

    #[tokio::test]
    async fn test_metadata_mixed_simultaneous_type_changes() {
        // Single update changes "color" from scalar->array AND "tags" from array->scalar.
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "color".to_string(),
            UpdateMetadataValue::Str("red".to_string()),
        );
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["old_a".to_string(), "old_b".to_string()]),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "color".to_string(),
            UpdateMetadataValue::StringArray(vec!["blue".to_string(), "green".to_string()]),
        );
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("new_scalar".to_string()),
        );

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // "color" old scalar must be gone.
        let eq_red = Where::Metadata(MetadataExpression {
            key: "color".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("red".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_red));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Old color scalar should be gone"
        );

        // "color" new array must be queryable.
        let contains_blue = Where::Metadata(MetadataExpression {
            key: "color".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("blue".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_blue));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);

        // "tags" old array must be gone.
        let contains_old_a = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("old_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_old_a));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(
            result.result.records.len(),
            0,
            "Old tags array should be gone"
        );

        // "tags" new scalar must be queryable.
        let eq_new_scalar = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("new_scalar".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_new_scalar));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
    }

    #[tokio::test]
    async fn test_metadata_delete_after_type_change() {
        // Add with scalar, update to array, then delete the key entirely.
        // Verifies None cleans up from the array table (not just scalar).
        let mut meta_v1 = HashMap::new();
        meta_v1.insert(
            "tags".to_string(),
            UpdateMetadataValue::Str("scalar_val".to_string()),
        );

        let mut meta_v2 = HashMap::new();
        meta_v2.insert(
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec!["arr_a".to_string(), "arr_b".to_string()]),
        );

        let mut meta_v3 = HashMap::new();
        meta_v3.insert("tags".to_string(), UpdateMetadataValue::None);

        let logs = vec![
            LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v1),
                    document: None,
                    operation: Operation::Add,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v2),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "id1".to_string(),
                    metadata: Some(meta_v3),
                    document: None,
                    operation: Operation::Update,
                    embedding: None,
                    encoding: None,
                },
            },
        ];

        let (reader, cas) = setup_with_logs(logs).await;

        // Record still exists but has no "tags" key at all.
        let plan = make_get_plan(&cas, None);
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 1);
        let has_tags = result.result.records[0]
            .metadata
            .as_ref()
            .map(|md| md.contains_key("tags"))
            .unwrap_or(false);
        assert!(
            !has_tags,
            "tags key should be fully deleted, got {:?}",
            result.result.records[0].metadata
        );

        // Neither scalar nor array queries should match.
        let eq_scalar = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("scalar_val".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(eq_scalar));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);

        let contains_arr = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                MetadataValue::Str("arr_a".to_string()),
            ),
        });
        let plan = make_get_plan(&cas, Some(contains_arr));
        let result = reader.get(plan).await.expect("get");
        assert_eq!(result.result.records.len(), 0);
    }
}
