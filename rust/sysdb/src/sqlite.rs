// Changed from the original Chroma file; see the git history for the changes and their authors.
use crate::{DatabaseOrTopology, FlushCompactionError, GetCollectionsOptions, SqliteSysDbConfig};
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes, WrappedSqlxError};
use chroma_sqlite::db::SqliteDb;
use chroma_sqlite::helpers::{delete_metadata, get_embeddings_queue_topic_name, update_metadata};
use chroma_sqlite::table;
use chroma_types::chroma_proto;
use chroma_types::{
    AttachedFunctionUuid, Collection, CollectionAndSegments, CollectionMetadataUpdate,
    CollectionUuid, CountForksError, CreateCollectionError, CreateCollectionResponse,
    CreateDatabaseError, CreateDatabaseResponse, CreateTenantError, CreateTenantResponse, Database,
    DatabaseUuid, DeleteCollectionError, DeleteDatabaseError, DeleteDatabaseResponse,
    FlushCompactionResponse, ForkCollectionError, GetCollectionByCrnError,
    GetCollectionWithSegmentsError, GetCollectionsError, GetDatabaseError, GetSegmentsError,
    GetTenantError, GetTenantResponse, InternalCollectionConfiguration,
    InternalUpdateCollectionConfiguration, ListDatabasesError, Metadata, MetadataValue, ResetError,
    ResetResponse, Schema, SchemaError, Segment, SegmentFlushInfo, SegmentScope, SegmentType,
    SegmentUuid, UpdateCollectionError, UpdateTenantError, UpdateTenantResponse,
    UpdateVectorIndexConfiguration,
};
use thiserror::Error;

#[derive(Error, Debug)]
#[error("MCMR not supported: {0}")]
struct McmrNotSupportedError(String);

impl ChromaError for McmrNotSupportedError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

use futures::TryStreamExt;
use sea_query_binder::SqlxBinder;
use sqlx::error::ErrorKind;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;

//////////////////////// SqliteSysDb ////////////////////////

#[derive(Debug, Clone)]
#[allow(dead_code)]
/// A wrapper around a SqliteDb that accesses the SysDB
/// This is the database that stores metadata about databases, tenants, and collections etc
/// ## Notes
/// - The SqliteSysDb should be "Shareable" - it should be possible to clone it and use it in multiple threads
///   without having divergent state
pub struct SqliteSysDb {
    db: SqliteDb,
    log_topic_namespace: String,
    log_tenant: String,
}

impl SqliteSysDb {
    #[allow(dead_code)]
    pub fn new(db: SqliteDb, log_tenant: String, log_topic_namespace: String) -> Self {
        Self {
            db,
            log_topic_namespace,
            log_tenant,
        }
    }

    ////////////////////////// Database Methods ////////////////////////
    #[allow(dead_code)]
    pub(crate) async fn create_database(
        &self,
        id: uuid::Uuid,
        name: &str,
        tenant: &str,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        sqlx::query("INSERT INTO databases (id, name, tenant_id) VALUES ($1, $2, $3)")
            .bind(id.to_string())
            .bind(name)
            .bind(tenant)
            .execute(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db_err)
                    if db_err.kind() == ErrorKind::UniqueViolation =>
                {
                    CreateDatabaseError::AlreadyExists(name.to_string())
                }
                _ => CreateDatabaseError::Internal(e.into()),
            })?;

        Ok(CreateDatabaseResponse {})
    }

    pub(crate) async fn get_database(
        &self,
        name: &str,
        tenant: &str,
    ) -> Result<Database, GetDatabaseError> {
        sqlx::query("SELECT id, name, tenant_id FROM databases WHERE name = $1 AND tenant_id = $2")
            .bind(name)
            .bind(tenant)
            .fetch_one(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => GetDatabaseError::NotFound(name.to_string()),
                _ => GetDatabaseError::Internal(e.into()),
            })
            .and_then(|row| {
                let id = Uuid::from_str(row.get::<&str, _>(0))
                    .map_err(|e| GetDatabaseError::InvalidID(e.to_string()))?;
                Ok(Database {
                    id,
                    name: row.get(1),
                    tenant: row.get(2),
                })
            })
    }

    pub(crate) async fn get_databases_by_ids(
        &self,
        ids: &[Uuid],
        tenant: &str,
    ) -> Result<Vec<Database>, GetDatabaseError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT id, name, tenant_id FROM databases WHERE tenant_id = ",
        );
        query.push_bind(tenant).push(" AND id IN (");
        let mut values = query.separated(", ");
        for id in ids {
            values.push_bind(id.to_string());
        }
        values.push_unseparated(")");
        let rows = query
            .build()
            .fetch_all(self.db.get_conn())
            .await
            .map_err(|e| GetDatabaseError::Internal(e.into()))?;
        rows.into_iter()
            .map(|row| {
                Ok(Database {
                    id: Uuid::parse_str(row.get::<&str, _>(0))
                        .map_err(|e| GetDatabaseError::InvalidID(e.to_string()))?,
                    name: row.get(1),
                    tenant: row.get(2),
                })
            })
            .collect()
    }

    pub(crate) async fn get_database_by_id(
        &self,
        database_id: Uuid,
        tenant: &str,
    ) -> Result<Database, GetDatabaseError> {
        sqlx::query("SELECT id, name, tenant_id FROM databases WHERE id = $1 AND tenant_id = $2")
            .bind(database_id.to_string())
            .bind(tenant)
            .fetch_one(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => GetDatabaseError::NotFound(database_id.to_string()),
                _ => GetDatabaseError::Internal(e.into()),
            })
            .and_then(|row| {
                let id = Uuid::from_str(row.get::<&str, _>(0))
                    .map_err(|e| GetDatabaseError::InvalidID(e.to_string()))?;
                Ok(Database {
                    id,
                    name: row.get(1),
                    tenant: row.get(2),
                })
            })
    }

    pub(crate) async fn delete_database(
        &self,
        database_name: String,
        tenant: String,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| DeleteDatabaseError::Internal(e.into()))?;

        let collections = self
            .get_collections_with_conn(
                &mut *tx,
                None,
                None,
                Some(tenant.clone()),
                Some(database_name.clone()),
                None,
                0,
            )
            .await
            .map_err(|e| e.boxed())?;

        for collection in collections {
            self.delete_collection_with_conn(
                &mut *tx,
                tenant.clone(),
                database_name.clone(),
                collection.collection_id,
                vec![],
            )
            .await
            .map_err(|e| e.boxed())?;
        }

        let result = sqlx::query("DELETE FROM databases WHERE name = $1 AND tenant_id = $2")
            .bind(&database_name)
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .map_err(|e| DeleteDatabaseError::Internal(e.into()))?;

        if result.rows_affected() == 0 {
            return Err(DeleteDatabaseError::NotFound(database_name));
        }

        tx.commit()
            .await
            .map_err(|e| DeleteDatabaseError::Internal(e.into()))?;

        Ok(DeleteDatabaseResponse {})
    }

    pub(crate) async fn count_databases(
        &self,
        tenant: String,
    ) -> Result<u64, chroma_types::CountDatabasesError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databases WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_one(self.db.get_conn())
            .await
            .map_err(|err| chroma_types::CountDatabasesError(err.into()))?;
        Ok(count as u64)
    }

    pub(crate) async fn list_databases(
        &self,
        tenant_id: String,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Database>, ListDatabasesError> {
        let mut rows = sqlx::query(
            r#"
                SELECT id, name, tenant_id
                FROM databases
                WHERE tenant_id = $1
                ORDER BY name
                LIMIT $2 OFFSET $3
            "#,
        )
        .bind(tenant_id)
        .bind(limit.unwrap_or(u32::MAX))
        .bind(offset)
        .fetch(self.db.get_conn());

        let mut databases = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ListDatabasesError::Internal(e.into()))?
        {
            let id = Uuid::from_str(row.get::<&str, _>(0))
                .map_err(|e| ListDatabasesError::InvalidID(e.to_string()))?;
            databases.push(Database {
                id,
                name: row.get(1),
                tenant: row.get(2),
            });
        }

        Ok(databases)
    }

    ////////////////////////// Tenant Methods ////////////////////////

    pub(crate) async fn create_tenant(
        &self,
        name: String,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        sqlx::query("INSERT INTO tenants (id) VALUES ($1)")
            .bind(&name)
            .execute(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db_err)
                    if db_err.kind() == ErrorKind::UniqueViolation =>
                {
                    CreateTenantError::AlreadyExists(name.clone())
                }
                _ => CreateTenantError::Internal(e.into()),
            })?;

        Ok(CreateTenantResponse {})
    }

    pub(crate) async fn get_tenant(&self, name: &str) -> Result<GetTenantResponse, GetTenantError> {
        sqlx::query("SELECT id, resource_name FROM tenants WHERE id = $1")
            .bind(name)
            .fetch_one(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => GetTenantError::NotFound(name.to_string()),
                _ => GetTenantError::Internal(e.into()),
            })
            .map(|row| GetTenantResponse {
                name: row.get(0),
                resource_name: row.get(1),
            })
    }

    // As SetTenantResourceName of distributed Chroma: a tenant gets its resource name once, and no other tenant has it.
    pub(crate) async fn update_tenant(
        &self,
        tenant_id: String,
        resource_name: String,
    ) -> Result<UpdateTenantResponse, UpdateTenantError> {
        let failed = |message: &str| {
            UpdateTenantError::FailedToSetResourceName(tonic::Status::already_exists(message))
        };
        let updated = sqlx::query(
            "UPDATE tenants SET resource_name = $1 WHERE id = $2 AND resource_name IS NULL",
        )
        .bind(&resource_name)
        .bind(&tenant_id)
        .execute(self.db.get_conn())
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(ref db_err) if db_err.kind() == ErrorKind::UniqueViolation => {
                failed("resource name is used by another tenant")
            }
            _ => UpdateTenantError::Internal(e.into()),
        })?;
        if updated.rows_affected() == 0 {
            let exists = sqlx::query("SELECT 1 FROM tenants WHERE id = $1")
                .bind(&tenant_id)
                .fetch_optional(self.db.get_conn())
                .await
                .map_err(|e| UpdateTenantError::Internal(e.into()))?
                .is_some();
            return Err(if exists {
                failed("tenant resource name is already set")
            } else {
                UpdateTenantError::NotFound(tenant_id)
            });
        }
        Ok(UpdateTenantResponse {})
    }

    ////////////////////////// Collection Methods ////////////////////////

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_collection(
        &self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        name: String,
        segments: Vec<Segment>,
        configuration: Option<InternalCollectionConfiguration>,
        schema: Option<Schema>,
        metadata: Option<Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
    ) -> Result<CreateCollectionResponse, CreateCollectionError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| CreateCollectionError::Internal(e.into()))?;
        self.db
            .begin_immediate(&mut *tx)
            .await
            .map_err(|e| CreateCollectionError::Internal(e.into()))?;

        let mut existing_collections = self
            .get_collections_with_conn(
                &mut *tx,
                None,
                Some(name.clone()),
                Some(tenant.clone()),
                Some(database.clone()),
                None,
                0,
            )
            .await
            .map_err(CreateCollectionError::Get)?;

        if let Some(collection) = existing_collections.pop() {
            if get_or_create {
                return Ok(collection);
            } else {
                return Err(CreateCollectionError::AlreadyExists(name.to_string()));
            }
        }

        // Look up database
        let database_result =
            sqlx::query("SELECT id FROM databases WHERE name = $1 AND tenant_id = $2")
                .bind(&database)
                .bind(&tenant)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| match e {
                    sqlx::Error::RowNotFound => {
                        CreateCollectionError::DatabaseNotFound(database.clone())
                    }
                    _ => CreateCollectionError::Internal(e.into()),
                })?;
        let database_id = database_result.get::<&str, _>(0);
        let database_uuid = DatabaseUuid::from_str(database_id)
            .map_err(|_| CreateCollectionError::DatabaseIdParseError)?;

        let configuration_json_str = match configuration {
            Some(configuration) => serde_json::to_string(&configuration)
                .map_err(CreateCollectionError::Configuration)?,
            None => "{}".to_string(),
        };

        let schema_json = schema
            .as_ref()
            .map(|schema| {
                serde_json::to_string(schema).map_err(|e| {
                    CreateCollectionError::Schema(SchemaError::InvalidSchema {
                        reason: e.to_string(),
                    })
                })
            })
            .transpose()?;

        sqlx::query(
            r#"
            INSERT INTO collections
                (id, name, config_json_str, schema_str, dimension, database_id)
            VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(collection_id.to_string())
        .bind(&name)
        .bind(configuration_json_str.clone())
        .bind(schema_json)
        .bind(dimension)
        .bind(database_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| CreateCollectionError::Internal(e.into()))?;

        if let Some(metadata) = metadata.clone() {
            update_metadata::<table::CollectionMetadata, _, _>(
                &mut *tx,
                collection_id.to_string(),
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect(),
            )
            .await
            .map_err(|e| e.boxed())?;
        }

        for segment in segments {
            self.create_segment_with_tx(&mut *tx, segment)
                .await
                .map_err(|e| CreateCollectionError::Internal(e.boxed()))?;
        }

        tx.commit()
            .await
            .map_err(|e| CreateCollectionError::Internal(e.into()))?;

        Ok(CreateCollectionResponse {
            collection_id,
            name,
            tenant,
            database,
            config: serde_json::from_str(&configuration_json_str)
                .map_err(CreateCollectionError::Configuration)?,
            metadata,
            schema,
            dimension,
            log_position: 0,
            total_records_post_compaction: 0,
            version: 0,
            size_bytes_post_compaction: 0,
            last_compaction_time_secs: 0,
            version_file_path: None,
            root_collection_id: None,
            lineage_file_path: None,
            updated_at: SystemTime::UNIX_EPOCH,
            database_id: database_uuid,
            compaction_failure_count: 0,
        })
    }

    pub(crate) async fn update_collection(
        &self,
        collection_id: CollectionUuid,
        name: Option<String>,
        metadata: Option<CollectionMetadataUpdate>,
        dimension: Option<u32>,
        configuration: Option<InternalUpdateCollectionConfiguration>,
    ) -> Result<(), UpdateCollectionError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| UpdateCollectionError::Internal(e.into()))?;

        if let Some(dimension) = dimension {
            // The first writer initializes dimension; later writers must agree.
            // This statement acquires SQLite's write transaction before reading
            // the winning dimension, including across frontend instances.
            let actual: Option<i64> = sqlx::query_scalar(
                "UPDATE collections SET dimension = COALESCE(dimension, ?)
                 WHERE id = ? RETURNING dimension",
            )
            .bind(i64::from(dimension))
            .bind(collection_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| UpdateCollectionError::Internal(e.into()))?;
            let actual =
                actual.ok_or_else(|| UpdateCollectionError::NotFound(collection_id.to_string()))?;
            if actual != i64::from(dimension) {
                return Err(UpdateCollectionError::DimensionMismatch(
                    actual as u32,
                    dimension,
                ));
            }
        }

        let mut configuration_json_str = None;
        let mut schema_str = None;
        if let Some(configuration) = configuration {
            let collections = self
                .get_collections_with_conn(&mut *tx, Some(collection_id), None, None, None, None, 0)
                .await;
            let collections = collections.unwrap();
            let collection = collections.into_iter().next().unwrap();
            // if schema exists, update schema instead of configuration
            if let Some(mut existing_schema) = collection.schema {
                // As distributed Chroma, which fails the update of the HNSW settings of a SPANN collection.
                if existing_schema.get_internal_spann_config().is_some()
                    && matches!(
                        configuration.vector_index,
                        Some(UpdateVectorIndexConfiguration::Hnsw(_))
                    )
                {
                    return Err(UpdateCollectionError::HnswUpdateOfSpann);
                }
                existing_schema.update(&configuration);
                schema_str = Some(
                    serde_json::to_string(&existing_schema)
                        .map_err(UpdateCollectionError::Schema)?,
                );
            } else {
                let mut existing_configuration = collection.config;
                existing_configuration.update(&configuration);
                configuration_json_str = Some(
                    serde_json::to_string(&existing_configuration)
                        .map_err(UpdateCollectionError::Configuration)?,
                );
            }
        }

        if name.is_some() {
            let mut query = sea_query::Query::update();
            let mut query = query.table(table::Collections::Table).cond_where(
                sea_query::Expr::col((table::Collections::Table, table::Collections::Id))
                    .eq(collection_id.to_string()),
            );

            if let Some(name) = name {
                query = query.value(table::Collections::Name, name.to_string());
            }

            let (sql, values) = query.build_sqlx(sea_query::SqliteQueryBuilder);

            let result = sqlx::query_with(&sql, values)
                .execute(&mut *tx)
                .await
                .map_err(|e| UpdateCollectionError::Internal(e.into()))?;
            if result.rows_affected() == 0 {
                return Err(UpdateCollectionError::NotFound(collection_id.to_string()));
            }
        }

        if let Some(configuration_json_str) = configuration_json_str {
            let mut query = sea_query::Query::update();
            let mut query = query.table(table::Collections::Table).cond_where(
                sea_query::Expr::col((table::Collections::Table, table::Collections::Id))
                    .eq(collection_id.to_string()),
            );
            query = query.value(table::Collections::ConfigJsonStr, configuration_json_str);

            let (sql, values) = query.build_sqlx(sea_query::SqliteQueryBuilder);

            let result = sqlx::query_with(&sql, values)
                .execute(&mut *tx)
                .await
                .map_err(|e| UpdateCollectionError::Internal(e.into()))?;
            if result.rows_affected() == 0 {
                return Err(UpdateCollectionError::NotFound(collection_id.to_string()));
            }
        }

        if let Some(schema_str) = schema_str {
            let mut query = sea_query::Query::update();
            let mut query = query.table(table::Collections::Table).cond_where(
                sea_query::Expr::col((table::Collections::Table, table::Collections::Id))
                    .eq(collection_id.to_string()),
            );
            query = query.value(table::Collections::SchemaStr, schema_str);

            let (sql, values) = query.build_sqlx(sea_query::SqliteQueryBuilder);

            let result = sqlx::query_with(&sql, values)
                .execute(&mut *tx)
                .await
                .map_err(|e| UpdateCollectionError::Internal(e.into()))?;
            if result.rows_affected() == 0 {
                return Err(UpdateCollectionError::NotFound(collection_id.to_string()));
            }
        }
        if let Some(metadata) = metadata {
            delete_metadata::<table::CollectionMetadata, _, _>(&mut *tx, collection_id.to_string())
                .await
                .map_err(|e| e.boxed())?;

            if let CollectionMetadataUpdate::UpdateMetadata(metadata) = metadata {
                update_metadata::<table::CollectionMetadata, _, _>(
                    &mut *tx,
                    collection_id.to_string(),
                    metadata,
                )
                .await
                .map_err(|e| e.boxed())?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| UpdateCollectionError::Internal(e.into()))?;

        Ok(())
    }

    async fn create_segment_with_tx<C>(
        &self,
        conn: &mut C,
        segment: Segment,
    ) -> Result<(), Box<dyn ChromaError>>
    where
        for<'a> &'a mut C: sqlx::Executor<'a, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            r#"
            INSERT INTO segments (id, type, scope, collection, file_paths) VALUES ($1, $2, $3, $4, $5)
        "#,
        )
        .bind(segment.id.to_string())
        .bind(String::from(segment.r#type))
        .bind(String::from(segment.scope))
        .bind(segment.collection.to_string())
        .bind(serde_json::to_string(&segment.file_path).map_err(|e| {
            Box::new(chroma_error::TonicError(tonic::Status::internal(e.to_string())))
                as Box<dyn ChromaError>
        })?)
        .execute(&mut *conn)
        .await
        .map_err(|e| WrappedSqlxError(e).boxed())?;

        if let Some(metadata) = segment.metadata {
            update_metadata::<table::SegmentMetadata, _, _>(
                conn,
                segment.id.to_string(),
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect(),
            )
            .await
            .map_err(|e| e.boxed())?;
        }

        Ok(())
    }

    pub(crate) async fn get_collections(
        &self,
        options: GetCollectionsOptions,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        let GetCollectionsOptions {
            collection_id,
            name,
            tenant,
            database_or_topology,
            limit,
            offset,
            ..
        } = options;

        let database_string = match database_or_topology {
            Some(DatabaseOrTopology::Database(db)) => Some(db.into_string()),
            Some(DatabaseOrTopology::Topology(topology)) => {
                return Err(GetCollectionsError::Internal(Box::new(
                    McmrNotSupportedError(topology.to_string()),
                )));
            }
            None => None,
        };

        self.get_collections_with_conn(
            self.db.get_conn(),
            collection_id,
            name,
            tenant,
            database_string,
            limit,
            offset,
        )
        .await
    }

    // As GetCollectionByResourceName of distributed Chroma: the tenant of the resource name, then the collection by
    // database and name.
    pub(crate) async fn get_collection_by_crn(
        &self,
        tenant_resource_name: String,
        database: String,
        name: String,
    ) -> Result<Collection, GetCollectionByCrnError> {
        let not_found = || {
            GetCollectionByCrnError::NotFound(format!("{tenant_resource_name}:{database}:{name}"))
        };
        let Some(tenant) = sqlx::query("SELECT id FROM tenants WHERE resource_name = $1")
            .bind(&tenant_resource_name)
            .fetch_optional(self.db.get_conn())
            .await
            .map_err(|e| GetCollectionByCrnError::Internal(e.into()))?
            .map(|row| row.get::<String, _>(0))
        else {
            return Err(not_found());
        };
        self.get_collections_with_conn(
            self.db.get_conn(),
            None,
            Some(name.clone()),
            Some(tenant),
            Some(database.clone()),
            Some(1),
            0,
        )
        .await
        .map_err(|e| GetCollectionByCrnError::Internal(e.boxed()))?
        .into_iter()
        .next()
        .ok_or_else(not_found)
    }

    // As FlushCollectionCompaction of distributed Chroma, in one transaction: the files of each segment, and the log
    // position that they cover as the max_seq_id of the segment, which the local compactor reads and never lowers. The
    // single node keeps no version, record count or size of a collection: the version comes back as it went in.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn flush_compaction(
        &self,
        collection_id: CollectionUuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        schema: Option<Schema>,
    ) -> Result<FlushCompactionResponse, FlushCompactionError> {
        let internal = |message: String| {
            FlushCompactionError::FailedToFlushCompaction(tonic::Status::internal(message))
        };
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| internal(e.to_string()))?;
        let response = flush_compaction_in(
            &mut tx,
            collection_id,
            log_position,
            collection_version,
            &segment_flush_info,
            schema,
        )
        .await?;
        tx.commit().await.map_err(|e| internal(e.to_string()))?;
        Ok(response)
    }

    pub(crate) async fn delete_collection(
        &self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        segment_ids: Vec<SegmentUuid>,
    ) -> Result<(), DeleteCollectionError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        let was_found = self
            .delete_collection_with_conn(&mut *tx, tenant, database, collection_id, segment_ids)
            .await
            .map_err(|e| e.boxed())?;
        if !was_found {
            return Err(DeleteCollectionError::NotFound(collection_id.to_string()));
        }

        tx.commit()
            .await
            .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        Ok(())
    }

    pub(crate) async fn get_segments(
        &self,
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: CollectionUuid,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        self.get_segments_with_conn(self.db.get_conn(), collection, id, r#type, scope)
            .await
    }

    pub(crate) async fn get_collection_with_segments(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, GetCollectionWithSegmentsError> {
        let collections = self
            .get_collections_with_conn(
                self.db.get_conn(),
                Some(collection_id),
                None,
                None,
                None,
                None,
                0,
            )
            .await
            .map_err(|e| e.boxed())?;
        let collection = collections
            .first()
            .ok_or(GetCollectionWithSegmentsError::NotFound(
                collection_id.to_string(),
            ))?;

        let segments = self
            .get_segments_with_conn(self.db.get_conn(), collection_id, None, None, None)
            .await?;

        let metadata_segment = segments
            .iter()
            .find(|s| s.scope == SegmentScope::METADATA)
            .ok_or(GetCollectionWithSegmentsError::Field(
                "Missing metadata segment".to_string(),
            ))?;

        let vector_segment = segments
            .iter()
            .find(|s| s.scope == SegmentScope::VECTOR)
            .ok_or(GetCollectionWithSegmentsError::Field(
                "Missing vector segment".to_string(),
            ))?;

        // Single node Chroma has a record segment only where its vector index needs one, as SPANN does.
        let record_segment = segments
            .iter()
            .find(|s| s.scope == SegmentScope::RECORD)
            .unwrap_or(metadata_segment);

        Ok(CollectionAndSegments {
            collection: collection.clone(),
            metadata_segment: metadata_segment.clone(),
            vector_segment: vector_segment.clone(),
            record_segment: record_segment.clone(),
        })
    }

    /// The fork of a collection, as the sysdb of distributed Chroma makes it
    /// (go/pkg/sysdb/coordinator/table_catalog.go:1053-1248): a new collection in the database of the source with its
    /// stored configuration, schema, dimension and metadata, new segments of the same type, scope and metadata, and
    /// one more fork in the lineage of the root. The records are copied by the caller.
    pub(crate) async fn fork_collection(
        &self,
        source_collection_id: CollectionUuid,
        target_collection_id: CollectionUuid,
        target_collection_name: String,
    ) -> Result<CollectionAndSegments, ForkCollectionError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| ForkCollectionError::Internal(e.into()))?;
        self.db
            .begin_immediate(&mut *tx)
            .await
            .map_err(|e| ForkCollectionError::Internal(e.into()))?;

        let source = self
            .get_collections_with_conn(
                &mut *tx,
                Some(source_collection_id),
                None,
                None,
                None,
                None,
                0,
            )
            .await
            .map_err(|e| ForkCollectionError::Internal(e.boxed()))?
            .pop()
            .ok_or_else(|| ForkCollectionError::NotFound(source_collection_id.to_string()))?;
        let existing = self
            .get_collections_with_conn(
                &mut *tx,
                None,
                Some(target_collection_name.clone()),
                Some(source.tenant.clone()),
                Some(source.database.clone()),
                None,
                0,
            )
            .await
            .map_err(|e| ForkCollectionError::Internal(e.boxed()))?;
        if !existing.is_empty() {
            return Err(ForkCollectionError::AlreadyExists(target_collection_name));
        }

        // The configuration, schema and metadata as stored, not as they read back.
        sqlx::query(
            "INSERT INTO collections (id, name, config_json_str, schema_str, dimension, database_id) \
             SELECT $1, $2, config_json_str, schema_str, dimension, database_id FROM collections WHERE id = $3",
        )
        .bind(target_collection_id.to_string())
        .bind(&target_collection_name)
        .bind(source_collection_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| ForkCollectionError::Internal(e.into()))?;
        sqlx::query(
            "INSERT INTO collection_metadata (collection_id, key, str_value, int_value, float_value, bool_value) \
             SELECT $1, key, str_value, int_value, float_value, bool_value FROM collection_metadata \
             WHERE collection_id = $2",
        )
        .bind(target_collection_id.to_string())
        .bind(source_collection_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| ForkCollectionError::Internal(e.into()))?;

        let source_segments = self
            .get_segments_with_conn(&mut *tx, source_collection_id, None, None, None)
            .await
            .map_err(|e| ForkCollectionError::Internal(e.boxed()))?;
        for segment in source_segments {
            let segment_id = SegmentUuid::new();
            // Without the files of the source: on a single node the records are written again into the fork, which
            // keeps files of its own, so the deletion of one collection never takes the files of another.
            sqlx::query(
                "INSERT INTO segments (id, type, scope, collection) \
                 SELECT $1, type, scope, $2 FROM segments WHERE id = $3",
            )
            .bind(segment_id.to_string())
            .bind(target_collection_id.to_string())
            .bind(segment.id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| ForkCollectionError::Internal(e.into()))?;
            sqlx::query(
                "INSERT INTO segment_metadata (segment_id, key, str_value, int_value, float_value, bool_value) \
                 SELECT $1, key, str_value, int_value, float_value, bool_value FROM segment_metadata \
                 WHERE segment_id = $2",
            )
            .bind(segment_id.to_string())
            .bind(segment.id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| ForkCollectionError::Internal(e.into()))?;
        }

        // A fork of a fork belongs to the lineage of the first source.
        let root_collection_id: Option<String> = sqlx::query_scalar(
            "SELECT root_collection_id FROM collection_forks WHERE target_collection_id = $1",
        )
        .bind(source_collection_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ForkCollectionError::Internal(e.into()))?;
        sqlx::query(
            "INSERT INTO collection_forks (target_collection_id, source_collection_id, root_collection_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(target_collection_id.to_string())
        .bind(source_collection_id.to_string())
        .bind(root_collection_id.unwrap_or_else(|| source_collection_id.to_string()))
        .execute(&mut *tx)
        .await
        .map_err(|e| ForkCollectionError::Internal(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| ForkCollectionError::Internal(e.into()))?;

        self.get_collection_with_segments(target_collection_id)
            .await
            .map_err(|e| ForkCollectionError::Internal(e.boxed()))
    }

    /// The number of forks in the lineage of a collection, as distributed Chroma counts them
    /// (go/pkg/sysdb/coordinator/table_catalog.go:1251-1292): the forks of its root and of every collection forked
    /// from it, deleted or not. When the root is deleted the lineage cannot be read.
    pub(crate) async fn count_forks(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<usize, CountForksError> {
        let exists = |id: String| async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM collections WHERE id = $1")
                .bind(id)
                .fetch_one(self.db.get_conn())
                .await
                .map(|count| count > 0)
                .map_err(|e| CountForksError::Internal(e.into()))
        };
        if !exists(collection_id.to_string()).await? {
            return Err(CountForksError::NotFound(collection_id.to_string()));
        }
        let root_collection_id: String = sqlx::query_scalar(
            "SELECT root_collection_id FROM collection_forks WHERE target_collection_id = $1",
        )
        .bind(collection_id.to_string())
        .fetch_optional(self.db.get_conn())
        .await
        .map_err(|e| CountForksError::Internal(e.into()))?
        .unwrap_or_else(|| collection_id.to_string());
        if !exists(root_collection_id.clone()).await? {
            return Err(CountForksError::NotFound(collection_id.to_string()));
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM collection_forks WHERE root_collection_id = $1",
        )
        .bind(root_collection_id)
        .fetch_one(self.db.get_conn())
        .await
        .map_err(|e| CountForksError::Internal(e.into()))?;
        Ok(count as usize)
    }

    pub(crate) async fn reset(&self) -> Result<ResetResponse, ResetError> {
        self.db.reset().await.map_err(|e| e.boxed())?;
        Ok(ResetResponse {})
    }

    // AttachFunction of the coordinator of distributed Chroma (go/pkg/sysdb/coordinator/task.go), with the status that
    // its gRPC server answers (go/pkg/sysdb/grpc/task_service.go): the errors it does not map keep their message with
    // the code Unknown, as gRPC sends them. The transaction takes the write lock of SQLite at its start, so it stands for
    // the locks of the graph that the coordinator takes, and the checks it repeats under them find what they found.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_attached_function(
        &self,
        name: String,
        function_name: String,
        input_collection_id: CollectionUuid,
        output_collection_name: String,
        params: serde_json::Value,
        tenant_id: String,
        database_name: String,
        min_records_for_invocation: u64,
    ) -> Result<(AttachedFunctionUuid, bool), tonic::Status> {
        if name.starts_with("_deleted_") {
            return Err(tonic::Status::unknown(ERR_INVALID_ATTACHED_FUNCTION_NAME));
        }
        // As the gRPC sysdb sends the params: a JSON object, or none, which the coordinator stores as {}.
        let params_json = match params {
            serde_json::Value::Object(map) if !map.is_empty() => {
                serde_json::to_string(&map).map_err(|e| tonic::Status::internal(e.to_string()))?
            }
            serde_json::Value::Object(_) | serde_json::Value::Null => "{}".to_string(),
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "params must be a json object",
                ))
            }
        };
        let input = input_collection_id.to_string();

        let mut tx = self.db.get_conn().begin().await.map_err(internal)?;
        self.db.begin_immediate(&mut *tx).await.map_err(internal)?;

        let function = function_by_name(&mut tx, &function_name)
            .await?
            .ok_or_else(|| tonic::Status::not_found(ERR_FUNCTION_NOT_FOUND))?;

        let existing =
            attached_function_rows(&mut tx, AttachedFunctionFilter::input(&input), false).await?;
        let functions = functions_by_id(&mut tx, &existing).await?;
        let database_id = database_id(&mut tx, &tenant_id, &database_name).await?;
        for attached_function in &existing {
            if attached_function.name == name
                && attached_function.tenant_id == tenant_id
                && attached_function.output_collection_name == output_collection_name
                && attached_function.min_records_for_invocation == min_records_for_invocation as i64
            {
                let existing_function =
                    functions
                        .get(&attached_function.function_id)
                        .ok_or_else(|| {
                            tonic::Status::unknown(format!(
                                "unknown function ID: {}",
                                attached_function.function_id
                            ))
                        })?;
                if existing_function.name == function_name {
                    let database_id = database_id
                        .as_ref()
                        .ok_or_else(|| tonic::Status::unknown(ERR_DATABASE_NOT_FOUND))?;
                    if &attached_function.database_id == database_id {
                        return Ok((
                            AttachedFunctionUuid(attached_function.id),
                            !attached_function.is_ready,
                        ));
                    }
                }
            }
            let existing_function = functions
                .get(&attached_function.function_id)
                .ok_or_else(|| tonic::Status::unknown(ERR_FUNCTION_NOT_FOUND))?;
            if existing_function.is_async == function.is_async {
                return Err(tonic::Status::already_exists(format!(
                    "collection already has an attached function with the same execution mode (pre-lock validation): name={}, function={}, output_collection={}",
                    attached_function.name, existing_function.name, attached_function.output_collection_name
                )));
            }
        }

        let database_id =
            database_id.ok_or_else(|| tonic::Status::unknown(ERR_DATABASE_NOT_FOUND))?;
        let collections = collection_ids_by_name(&mut tx, &tenant_id, &database_name).await?;
        if !collections.values().any(|id| id == &input) {
            return Err(tonic::Status::unknown(ERR_COLLECTION_NOT_FOUND));
        }
        let existing_output = collections.get(&output_collection_name).cloned();

        let graph = AttachedFunctionGraph {
            attached_functions: attached_function_rows(
                &mut tx,
                AttachedFunctionFilter::default(),
                false,
            )
            .await?,
            collections,
        };

        // The input collection can take another upstream edge only from asynchronous functions.
        let upstream = graph.incoming(&input);
        let upstream_functions = functions_by_id(&mut tx, &upstream).await?;
        for attached_function in &upstream {
            let existing_function = upstream_functions
                .get(&attached_function.function_id)
                .ok_or_else(|| tonic::Status::unknown(ERR_FUNCTION_NOT_FOUND))?;
            if !existing_function.is_async {
                return Err(tonic::Status::unknown(
                    ERR_CANNOT_ATTACH_TO_OUTPUT_COLLECTION,
                ));
            }
        }

        let mut output_tail_depth = 0;
        if let Some(output) = &existing_output {
            if graph.reaches(output, &input) {
                return Err(tonic::Status::unknown(
                    ERR_CANNOT_ATTACH_TO_OUTPUT_COLLECTION,
                ));
            }
            output_tail_depth = graph.max_path_length(
                output,
                &mut HashMap::new(),
                &mut HashSet::new(),
                "attached function cycle detected while computing downstream depth",
                &|id| graph.outgoing_collection_ids(id),
            )?;
        }
        let input_depth = graph.max_path_length(
            &input,
            &mut HashMap::new(),
            &mut HashSet::new(),
            "attached function cycle detected while computing depth",
            &|id| graph.incoming_collection_ids(id),
        )?;
        if input_depth + 1 + output_tail_depth > MAX_ATTACHED_FUNCTION_DEPTH {
            return Err(tonic::Status::invalid_argument(format!(
                "attached function depth exceeds maximum of {MAX_ATTACHED_FUNCTION_DEPTH}"
            )));
        }

        let attached_function_id = Uuid::new_v4();
        let inserted = sqlx::query(
            r#"
            INSERT INTO attached_functions
                (id, name, tenant_id, database_id, input_collection_id, output_collection_name, function_id,
                 function_params, min_records_for_invocation, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, strftime('%Y-%m-%d %H:%M:%f', 'now'),
                    strftime('%Y-%m-%d %H:%M:%f', 'now'))
            "#,
        )
        .bind(attached_function_id.to_string())
        .bind(&name)
        .bind(&tenant_id)
        .bind(&database_id)
        .bind(&input)
        .bind(&output_collection_name)
        .bind(function.id.to_string())
        .bind(&params_json)
        .bind(min_records_for_invocation as i64)
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(e)) if e.kind() == ErrorKind::UniqueViolation => {
                return Err(tonic::Status::already_exists(
                    ERR_ATTACHED_FUNCTION_ALREADY_EXISTS,
                ));
            }
            Err(e) => return Err(internal(e)),
        }

        tx.commit().await.map_err(internal)?;
        Ok((AttachedFunctionUuid(attached_function_id), true))
    }

    // FinishCreateAttachedFunction of the coordinator: the output collection, created if none has its name, then the
    // attached function ready with it. Where the coordinator gives the output collection the segments of distributed
    // Chroma, those of the single node (output_segments), as HNSW is the index of both.
    pub(crate) async fn finish_create_attached_function(
        &self,
        attached_function_id: AttachedFunctionUuid,
        output_collection_schema_str: String,
        output_segments: impl FnOnce(CollectionUuid) -> Vec<Segment>,
    ) -> Result<bool, tonic::Status> {
        let id = attached_function_id.0.to_string();
        let mut tx = self.db.get_conn().begin().await.map_err(internal)?;
        self.db.begin_immediate(&mut *tx).await.map_err(internal)?;

        let rows = attached_function_rows(&mut tx, AttachedFunctionFilter::id(&id), false).await?;
        let Some(attached_function) = rows.first() else {
            return Err(tonic::Status::not_found("attached function not found"));
        };
        if let Some(ready) = rows.iter().find(|row| row.is_ready) {
            mark_ready(&mut tx, &id, ready.output_collection_id.as_deref()).await?;
            tx.commit().await.map_err(internal)?;
            return Ok(false);
        }

        let database_name: Option<String> =
            sqlx::query_scalar("SELECT name FROM databases WHERE id = $1")
                .bind(&attached_function.database_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(internal)?;
        let database_name =
            database_name.ok_or_else(|| tonic::Status::internal(ERR_DATABASE_NOT_FOUND))?;

        let collections =
            collection_ids_by_name(&mut tx, &attached_function.tenant_id, &database_name).await?;
        let (output_collection_id, created) = match collections
            .get(&attached_function.output_collection_name)
        {
            Some(output) => (output.clone(), false),
            None => {
                let collection_id = CollectionUuid::new();
                sqlx::query(
                        r#"
                        INSERT INTO collections (id, name, config_json_str, schema_str, dimension, database_id)
                        VALUES ($1, $2, '{}', $3, 1, $4)
                        "#,
                    )
                    .bind(collection_id.to_string())
                    .bind(&attached_function.output_collection_name)
                    .bind(&output_collection_schema_str)
                    .bind(&attached_function.database_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| match e {
                        sqlx::Error::Database(e) if e.kind() == ErrorKind::UniqueViolation => {
                            tonic::Status::already_exists(format!(
                                "output collection '{}' already exists",
                                attached_function.output_collection_name
                            ))
                        }
                        e => internal(e),
                    })?;
                for segment in output_segments(collection_id) {
                    self.create_segment_with_tx(&mut *tx, segment)
                        .await
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;
                }
                (collection_id.to_string(), true)
            }
        };
        mark_ready(&mut tx, &id, Some(&output_collection_id)).await?;

        // At most one ready synchronous function and one ready asynchronous function on a collection.
        let ready = attached_function_rows(
            &mut tx,
            AttachedFunctionFilter::input(&attached_function.input_collection_id),
            true,
        )
        .await?;
        let functions = functions_by_id(&mut tx, &ready).await?;
        let (mut sync, mut r#async) = (0, 0);
        for row in &ready {
            match functions.get(&row.function_id) {
                Some(function) if function.is_async => r#async += 1,
                Some(_) => sync += 1,
                None => return Err(tonic::Status::internal(ERR_FUNCTION_NOT_FOUND)),
            }
        }
        if sync > 1 || r#async > 1 {
            return Err(tonic::Status::internal(
                ERR_ATTACHED_FUNCTION_ALREADY_EXISTS,
            ));
        }

        tx.commit().await.map_err(internal)?;
        Ok(created)
    }

    // AddAttachedFunctionInput of the coordinator, with insertAttachedFunctionForInputCollection: one more input
    // collection of an asynchronous attached function, a row of the same id; the row of the same id already there is
    // the same input, created while it is not ready.
    pub(crate) async fn add_attached_function_input(
        &self,
        attached_function_id: AttachedFunctionUuid,
        input_collection_id: CollectionUuid,
    ) -> Result<(AttachedFunctionUuid, bool), tonic::Status> {
        let id = attached_function_id.0.to_string();
        let input = input_collection_id.to_string();
        let mut tx = self.db.get_conn().begin().await.map_err(internal)?;
        self.db.begin_immediate(&mut *tx).await.map_err(internal)?;

        let rows = attached_function_rows(&mut tx, AttachedFunctionFilter::id(&id), false).await?;
        let Some(base) = rows.first() else {
            return Err(tonic::Status::not_found(ERR_ATTACHED_FUNCTION_NOT_FOUND));
        };
        let functions = functions_by_id(&mut tx, std::slice::from_ref(base)).await?;
        let function = functions
            .get(&base.function_id)
            .ok_or_else(|| tonic::Status::not_found(ERR_FUNCTION_NOT_FOUND))?;
        if !function.is_async {
            return Err(tonic::Status::invalid_argument(
                "multiple input collections are only supported for async attached functions",
            ));
        }
        let database_name: Option<String> =
            sqlx::query_scalar("SELECT name FROM databases WHERE id = $1")
                .bind(&base.database_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(internal)?;
        let database_name =
            database_name.ok_or_else(|| tonic::Status::unknown(ERR_DATABASE_NOT_FOUND))?;

        let existing =
            attached_function_rows(&mut tx, AttachedFunctionFilter::input(&input), false).await?;
        let existing_functions = functions_by_id(&mut tx, &existing).await?;
        for attached_function in &existing {
            if attached_function.id == base.id {
                return Ok((attached_function_id, !attached_function.is_ready));
            }
            let existing_function = existing_functions
                .get(&attached_function.function_id)
                .ok_or_else(|| tonic::Status::not_found(ERR_FUNCTION_NOT_FOUND))?;
            if existing_function.is_async == function.is_async {
                return Err(tonic::Status::already_exists(format!(
                    "collection already has an attached function with the same execution mode (post-lock validation): name={}, function={}, output_collection={}",
                    attached_function.name, existing_function.name, attached_function.output_collection_name
                )));
            }
        }
        let collections = collection_ids_by_name(&mut tx, &base.tenant_id, &database_name).await?;
        if !collections.values().any(|id| id == &input) {
            return Err(tonic::Status::unknown(ERR_COLLECTION_NOT_FOUND));
        }

        sqlx::query(
            r#"
            INSERT INTO attached_functions
                (id, name, tenant_id, database_id, input_collection_id, output_collection_name, function_id,
                 function_params, min_records_for_invocation, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, strftime('%Y-%m-%d %H:%M:%f', 'now'),
                    strftime('%Y-%m-%d %H:%M:%f', 'now'))
            "#,
        )
        .bind(&id)
        .bind(&base.name)
        .bind(&base.tenant_id)
        .bind(&base.database_id)
        .bind(&input)
        .bind(&base.output_collection_name)
        .bind(base.function_id.to_string())
        .bind(&base.function_params)
        .bind(base.min_records_for_invocation)
        .execute(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.kind() == ErrorKind::UniqueViolation => {
                tonic::Status::already_exists(ERR_ATTACHED_FUNCTION_ALREADY_EXISTS)
            }
            e => internal(e),
        })?;
        tx.commit().await.map_err(internal)?;
        Ok((attached_function_id, true))
    }

    // The update of the completion offset of FlushCollectionCompactionsAndAttachedFunction of the coordinator (its
    // Update of the store, go/pkg/sysdb/metastore/db/dao/task.go): the rows of the attached function, of every input, not
    // deleted; none is ErrAttachedFunctionNotFound. The single node flushes no collection with it: the output of a
    // function goes through the log of its output collection. A run that succeeds resets the failures of the function.
    pub(crate) async fn update_attached_function_completion_offset(
        &self,
        attached_function_id: AttachedFunctionUuid,
        completion_offset: u64,
    ) -> Result<(), tonic::Status> {
        let updated = sqlx::query(
            "UPDATE attached_functions SET completion_offset = $1, failure_count = 0, \
             updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = $2 AND is_deleted = FALSE",
        )
        .bind(completion_offset as i64)
        .bind(attached_function_id.0.to_string())
        .execute(self.db.get_conn())
        .await
        .map_err(internal)?;
        if updated.rows_affected() == 0 {
            return Err(tonic::Status::unknown(ERR_ATTACHED_FUNCTION_NOT_FOUND));
        }
        Ok(())
    }

    // TryFinishAsyncAttachedFunctionInvocation of the coordinator: the completion offset of an asynchronous function
    // for one input, moved forward only, with heap_entry_pending set when it is behind the log position of the input,
    // which a collection of the single node answers as the lowest max_seq_id of its segments.
    pub(crate) async fn try_finish_async_attached_function_invocation(
        &self,
        request: chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest,
    ) -> Result<chroma_proto::TryFinishAsyncAttachedFunctionInvocationResponse, tonic::Status> {
        let id = Uuid::parse_str(&request.attached_function_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid attached_function_id: {e}"))
        })?;
        let collection_id = CollectionUuid::from_str(&request.collection_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid collection_id: {e}")))?;
        let new_offset = request.new_completion_offset as i64;
        let id_str = id.to_string();
        let mut tx = self.db.get_conn().begin().await.map_err(internal)?;
        self.db.begin_immediate(&mut *tx).await.map_err(internal)?;
        let rows =
            attached_function_rows(&mut tx, AttachedFunctionFilter::id(&id_str), true).await?;
        if rows.is_empty() {
            return Err(tonic::Status::not_found("attached function not found"));
        }
        let Some(row) = rows
            .iter()
            .find(|row| row.input_collection_id == request.collection_id)
        else {
            return Err(tonic::Status::not_found(
                "attached function not found for collection",
            ));
        };
        let functions = functions_by_id(&mut tx, std::slice::from_ref(row)).await?;
        if !functions.get(&row.function_id).is_some_and(|f| f.is_async) {
            return Err(tonic::Status::invalid_argument(
                "attached function is not async",
            ));
        }
        let log_position: Option<i64> = sqlx::query_scalar(
            "SELECT (SELECT COALESCE(MIN(COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)), 0) FROM segments \
             LEFT JOIN max_seq_id ON max_seq_id.segment_id = segments.id WHERE segments.collection = collections.id) \
             FROM collections WHERE id = $1",
        )
        .bind(collection_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        let Some(witnessed_log_offset) = log_position else {
            return Err(tonic::Status::not_found("collection not found"));
        };
        // UpdateCompletionOffsetAndHeapEntry of the store: forward only, the failure count reset.
        let updated = sqlx::query(
            "UPDATE attached_functions SET completion_offset = $1, heap_entry_pending = ($1 < $2), failure_count = 0, \
             updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') \
             WHERE id = $3 AND input_collection_id = $4 AND is_deleted = FALSE AND completion_offset <= $1",
        )
        .bind(new_offset)
        .bind(witnessed_log_offset)
        .bind(&id_str)
        .bind(&request.collection_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if updated.rows_affected() == 0 {
            return Err(tonic::Status::unknown(
                "completion offset cannot move backwards",
            ));
        }
        tx.commit().await.map_err(internal)?;
        use chroma_proto::try_finish_async_attached_function_invocation_response::Result as FinishResult;
        Ok(
            chroma_proto::TryFinishAsyncAttachedFunctionInvocationResponse {
                result: Some(if new_offset < witnessed_log_offset {
                    FinishResult::NeedsRepair(chroma_proto::FinishAsyncNeedsRepair {
                        current_collection_log_offset: witnessed_log_offset as u64,
                    })
                } else {
                    FinishResult::Success(chroma_proto::FinishAsyncSuccess {
                        updated_completion_offset: request.new_completion_offset,
                    })
                }),
            },
        )
    }

    // FailAttachedFunction (no count given: one more) and SetAttachedFunctionFailureCount (the count given) of the
    // coordinator, with IncrementFailureCount and SetFailureCount of its store: the failure count of an asynchronous
    // function for one input.
    pub(crate) async fn change_attached_function_failure_count(
        &self,
        attached_function_id: &str,
        collection_id: &str,
        failure_count: Option<i32>,
    ) -> Result<i32, tonic::Status> {
        if failure_count.is_some_and(|count| count < 0) {
            return Err(tonic::Status::invalid_argument(
                "failure_count must be non-negative",
            ));
        }
        let id = Uuid::parse_str(attached_function_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid attached_function_id: {e}"))
        })?;
        CollectionUuid::from_str(collection_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid collection_id: {e}")))?;
        let updated: Option<i32> = sqlx::query_scalar(
            "UPDATE attached_functions SET failure_count = COALESCE($1, failure_count + 1), \
             updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') \
             WHERE id = $2 AND input_collection_id = $3 AND is_deleted = FALSE RETURNING failure_count",
        )
        .bind(failure_count)
        .bind(id.to_string())
        .bind(collection_id)
        .fetch_optional(self.db.get_conn())
        .await
        .map_err(internal)?;
        updated.ok_or_else(|| tonic::Status::not_found("attached function not found"))
    }

    // FinalizeAsyncAttachedFunctionRepair of the coordinator, with UpdateHeapEntryPending of its store:
    // heap_entry_pending back to false.
    pub(crate) async fn finalize_async_attached_function_repair(
        &self,
        request: chroma_proto::FinalizeAsyncAttachedFunctionRepairRequest,
    ) -> Result<(), tonic::Status> {
        let id = Uuid::parse_str(&request.attached_function_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid attached_function_id: {e}"))
        })?;
        CollectionUuid::from_str(&request.collection_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid collection_id: {e}")))?;
        let updated = sqlx::query(
            "UPDATE attached_functions SET heap_entry_pending = FALSE, \
             updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') \
             WHERE id = $1 AND input_collection_id = $2 AND is_deleted = FALSE",
        )
        .bind(id.to_string())
        .bind(&request.collection_id)
        .execute(self.db.get_conn())
        .await
        .map_err(internal)?;
        if updated.rows_affected() == 0 {
            return Err(tonic::Status::unknown(ERR_ATTACHED_FUNCTION_NOT_FOUND));
        }
        Ok(())
    }

    // CheckInvocationStatus of the coordinator, with the query of its store: for each item, DONE when the attached
    // function is gone or deleted, or past the offset given and not pending; NEEDS_REPAIR when past it and pending;
    // NOT_DONE otherwise.
    pub(crate) async fn check_invocation_status(
        &self,
        request: chroma_proto::CheckInvocationStatusRequest,
    ) -> Result<chroma_proto::CheckInvocationStatusResponse, tonic::Status> {
        let mut results = Vec::with_capacity(request.items.len());
        for (index, item) in request.items.iter().enumerate() {
            Uuid::parse_str(&item.function_id).map_err(|e| {
                tonic::Status::invalid_argument(format!(
                    "invalid function_id at index {index}: {e}"
                ))
            })?;
            Uuid::parse_str(&item.input_collection_id).map_err(|e| {
                tonic::Status::invalid_argument(format!(
                    "invalid input_collection_id at index {index}: {e}"
                ))
            })?;
            let row: Option<(bool, i64, bool)> = sqlx::query_as(
                "SELECT is_deleted, completion_offset, heap_entry_pending FROM attached_functions \
                 WHERE id = $1 AND input_collection_id = $2",
            )
            .bind(&item.function_id)
            .bind(&item.input_collection_id)
            .fetch_optional(self.db.get_conn())
            .await
            .map_err(internal)?;
            let (status, current) = match row {
                None => (chroma_proto::InvocationStatus::Done, item.completion_offset),
                Some((true, offset, _)) => (chroma_proto::InvocationStatus::Done, offset),
                Some((false, offset, true)) if offset > item.completion_offset => {
                    (chroma_proto::InvocationStatus::NeedsRepair, offset)
                }
                Some((false, offset, false)) if offset > item.completion_offset => {
                    (chroma_proto::InvocationStatus::Done, offset)
                }
                Some((false, offset, _)) => (chroma_proto::InvocationStatus::NotDone, offset),
            };
            results.push(chroma_proto::InvocationStatusResult {
                status: status as i32,
                current_completion_offset: current,
            });
        }
        Ok(chroma_proto::CheckInvocationStatusResponse { results })
    }

    // GetAttachedFunctions of the coordinator, with the attached functions as the gRPC sysdb reads them.
    pub(crate) async fn get_attached_functions(
        &self,
        name: Option<String>,
        input_collection_id: Option<CollectionUuid>,
        ids: Vec<AttachedFunctionUuid>,
        only_ready: bool,
    ) -> Result<Vec<chroma_types::AttachedFunction>, tonic::Status> {
        let ids = ids.iter().map(|id| id.0.to_string()).collect::<Vec<_>>();
        let input = input_collection_id.map(|id| id.to_string());
        let mut conn = self.db.get_conn().acquire().await.map_err(internal)?;
        let rows = attached_function_rows(
            &mut conn,
            AttachedFunctionFilter {
                ids: &ids,
                name: name.as_deref(),
                input: input.as_deref(),
                output: None,
            },
            only_ready,
        )
        .await?;
        let functions = functions_by_id(&mut conn, &rows).await?;
        rows.into_iter()
            .map(|row| {
                let function = functions
                    .get(&row.function_id)
                    .ok_or_else(|| tonic::Status::unknown(ERR_FUNCTION_NOT_FOUND))?;
                row.into_attached_function(function)
            })
            .collect()
    }

    // DetachFunction of the coordinator: the attached function deleted, renamed as it does, and its output collection
    // with it when asked, in one transaction.
    pub(crate) async fn soft_delete_attached_function(
        &self,
        name: String,
        input_collection_id: CollectionUuid,
        delete_output: bool,
    ) -> Result<(), tonic::Status> {
        let input = input_collection_id.to_string();
        let mut tx = self.db.get_conn().begin().await.map_err(internal)?;
        self.db.begin_immediate(&mut *tx).await.map_err(internal)?;
        let rows = attached_function_rows(
            &mut tx,
            AttachedFunctionFilter {
                name: Some(&name),
                input: Some(&input),
                ..Default::default()
            },
            true,
        )
        .await?;
        let Some(attached_function) = rows.first() else {
            return Err(tonic::Status::not_found(ERR_ATTACHED_FUNCTION_NOT_FOUND));
        };
        if delete_output {
            if let Some(output) = attached_function
                .output_collection_id
                .as_deref()
                .filter(|id| !id.is_empty())
            {
                let output = CollectionUuid::from_str(output).map_err(|e| {
                    tonic::Status::invalid_argument(format!("invalid output_collection_id: {e}"))
                })?;
                let database_name: Option<String> =
                    sqlx::query_scalar("SELECT name FROM databases WHERE id = $1")
                        .bind(&attached_function.database_id)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(internal)?;
                // A missing output collection, already deleted, leaves the attached function to delete.
                if let Some(database_name) = database_name {
                    self.delete_collection_with_conn(
                        &mut *tx,
                        attached_function.tenant_id.clone(),
                        database_name,
                        output,
                        Vec::new(),
                    )
                    .await
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                }
            }
        }
        sqlx::query(
            r#"
            UPDATE attached_functions
            SET name = '_deleted_' || name || '_' || id, is_deleted = TRUE,
                updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
            WHERE input_collection_id = $1 AND name = $2 AND is_deleted = FALSE
            "#,
        )
        .bind(&input)
        .bind(&name)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        Ok(())
    }

    // GetFunctions of the coordinator, as the gRPC sysdb lists them.
    pub(crate) async fn get_all_functions(&self) -> Result<Vec<(String, Uuid)>, tonic::Status> {
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT name, id FROM functions")
            .fetch_all(self.db.get_conn())
            .await
            .map_err(internal)?;
        rows.into_iter()
            .map(|(name, id)| {
                Uuid::parse_str(&id)
                    .map(|id| (name, id))
                    .map_err(|e| tonic::Status::internal(e.to_string()))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_collections_with_conn<'a, C>(
        &self,
        conn: C,
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Collection>, GetCollectionsError>
    where
        C: sqlx::Executor<'a, Database = sqlx::Sqlite>,
    {
        let mut collections_query = sea_query::Query::select();
        let collections_query = collections_query
            .from(table::Collections::Table)
            .column((table::Collections::Table, table::Collections::Id))
            .column((table::Collections::Table, table::Collections::Name))
            .column((table::Collections::Table, table::Collections::ConfigJsonStr))
            .column((table::Collections::Table, table::Collections::Dimension))
            .column((table::Collections::Table, table::Collections::DatabaseId))
            .column((table::Collections::Table, table::Collections::SchemaStr))
            .column((table::Collections::Table, table::Collections::Version))
            .inner_join(
                table::Databases::Table,
                sea_query::Expr::col((table::Databases::Table, table::Databases::Id))
                    .equals((table::Collections::Table, table::Collections::DatabaseId)),
            )
            .cond_where(
                sea_query::Cond::all()
                    .add_option(name.map(|name| {
                        sea_query::Expr::col((table::Collections::Table, table::Collections::Name))
                            .eq(name)
                    }))
                    .add_option(database.map(|database| {
                        sea_query::Expr::col((table::Databases::Table, table::Databases::Name))
                            .eq(database)
                    }))
                    .add_option(
                        tenant.map(|tenant| {
                            sea_query::Expr::col(table::Databases::TenantId).eq(tenant)
                        }),
                    )
                    .add_option(collection_id.map(|collection_id| {
                        sea_query::Expr::col((table::Collections::Table, table::Collections::Id))
                            .eq(collection_id.to_string())
                    })),
            )
            .order_by(
                (table::Collections::Table, table::Collections::Id),
                sea_query::Order::Asc,
            )
            .limit(limit.unwrap_or(u32::MAX).into()) // SQLite requires that limit is always set if offset is provided
            .offset(offset.into());

        let (sql, values) = sea_query::Query::select()
            .from_subquery(collections_query.take(), table::Collections::Table)
            .left_join(
                table::CollectionMetadata::Table,
                sea_query::Expr::col((
                    table::CollectionMetadata::Table,
                    table::CollectionMetadata::CollectionId,
                ))
                .equals((table::Collections::Table, table::Collections::Id)),
            )
            .inner_join(
                table::Databases::Table,
                sea_query::Expr::col((table::Databases::Table, table::Databases::Id))
                    .equals((table::Collections::Table, table::Collections::DatabaseId)),
            )
            .column((table::Collections::Table, table::Collections::Id))
            .column((table::Collections::Table, table::Collections::Name))
            .column((table::Collections::Table, table::Collections::ConfigJsonStr))
            .column((table::Collections::Table, table::Collections::Dimension))
            .column((table::Databases::Table, table::Databases::TenantId))
            .column((table::Databases::Table, table::Databases::Name))
            .column((table::Collections::Table, table::Collections::DatabaseId))
            .column((table::Collections::Table, table::Collections::SchemaStr))
            .columns([
                table::CollectionMetadata::Key,
                table::CollectionMetadata::StrValue,
                table::CollectionMetadata::IntValue,
                table::CollectionMetadata::FloatValue,
                table::CollectionMetadata::BoolValue,
            ])
            // As the log position of distributed Chroma, which its compaction moves: the records that every segment of
            // the collection has applied, the lowest max_seq_id of its segments.
            .expr_as(
                sea_query::Expr::cust(
                    "(SELECT COALESCE(MIN(COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)), 0) FROM segments \
                     LEFT JOIN max_seq_id ON max_seq_id.segment_id = segments.id \
                     WHERE segments.collection = collections.id)",
                ),
                sea_query::Alias::new("log_position"),
            )
            // As the version of distributed Chroma, which each flush moves (00016).
            .column((table::Collections::Table, table::Collections::Version))
            .build_sqlx(sea_query::SqliteQueryBuilder);

        let mut rows = sqlx::query_with(&sql, values).fetch(conn);
        let mut rows_by_collection_id: HashMap<CollectionUuid, Vec<SqliteRow>> = HashMap::new();

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| GetCollectionsError::Internal(e.into()))?
        {
            let collection_id = CollectionUuid::from_str(row.get::<&str, _>(0))
                .map_err(GetCollectionsError::CollectionId)?;

            if let Some(entry) = rows_by_collection_id.get_mut(&collection_id) {
                entry.push(row);
            } else {
                rows_by_collection_id.insert(collection_id, vec![row]);
            }
        }

        let mut collections = rows_by_collection_id
            .into_iter()
            .filter_map(|(collection_id, rows)| {
                if rows.is_empty() {
                    // should never happen
                    return None;
                }

                let metadata = self.metadata_from_rows(rows.iter());
                let first_row = rows.first().unwrap();

                let configuration = match first_row.get::<Option<&str>, _>(2) {
                    Some(json_str) => {
                        match serde_json::from_str::<InternalCollectionConfiguration>(json_str)
                            .map_err(GetCollectionsError::Configuration)
                        {
                            Ok(configuration) => configuration,
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    None => InternalCollectionConfiguration::default_hnsw(),
                };
                let schema = match first_row.get::<Option<&str>, _>(7) {
                    Some(json_str) if !json_str.trim().is_empty() && json_str.trim() != "null" => {
                        match serde_json::from_str::<Schema>(json_str)
                            .map_err(GetCollectionsError::Schema)
                        {
                            Ok(schema) => Some(schema),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    None => None,
                    _ => None,
                };
                let database_id = match DatabaseUuid::from_str(first_row.get(6)) {
                    Ok(db_id) => db_id,
                    Err(_) => return Some(Err(GetCollectionsError::DatabaseId)),
                };

                Some(Ok(Collection {
                    collection_id,
                    config: configuration,
                    schema,
                    metadata,
                    total_records_post_compaction: 0,
                    version: first_row.get::<i32, _>("version"),
                    log_position: first_row.get("log_position"),
                    dimension: first_row.get(3),
                    name: first_row.get(1),
                    tenant: first_row.get(4),
                    database: first_row.get(5),
                    size_bytes_post_compaction: 0,
                    last_compaction_time_secs: 0,
                    version_file_path: None,
                    root_collection_id: None,
                    lineage_file_path: None,
                    updated_at: SystemTime::UNIX_EPOCH,
                    database_id,
                    compaction_failure_count: 0,
                }))
            })
            .collect::<Result<Vec<_>, GetCollectionsError>>()?;

        collections.sort_unstable_by_key(|c| c.collection_id);

        Ok(collections)
    }

    async fn get_segments_with_conn<'a, C>(
        &self,
        conn: C,
        collection_id: CollectionUuid,
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
    ) -> Result<Vec<Segment>, GetSegmentsError>
    where
        C: sqlx::Executor<'a, Database = sqlx::Sqlite>,
    {
        let (sql, values) = sea_query::Query::select()
            .from(table::Segments::Table)
            .left_join(
                table::SegmentMetadata::Table,
                sea_query::Expr::col((
                    table::SegmentMetadata::Table,
                    table::SegmentMetadata::SegmentId,
                ))
                .equals((table::Segments::Table, table::Segments::Id)),
            )
            .and_where(
                sea_query::Expr::col((table::Segments::Table, table::Segments::Collection))
                    .eq(collection_id.to_string()),
            )
            .cond_where(
                sea_query::Cond::all()
                    .add_option(id.map(|id| {
                        sea_query::Expr::col((table::Segments::Table, table::Segments::Id))
                            .eq(id.to_string())
                    }))
                    .add_option(r#type.map(|r#type| {
                        sea_query::Expr::col((table::Segments::Table, table::Segments::Type))
                            .eq(r#type)
                    }))
                    .add_option(scope.map(|scope| {
                        sea_query::Expr::col((table::Segments::Table, table::Segments::Scope))
                            .eq(String::from(scope))
                    })),
            )
            .column((table::Segments::Table, table::Segments::Id))
            .column((table::Segments::Table, table::Segments::Type))
            .column((table::Segments::Table, table::Segments::Scope))
            .column((table::Segments::Table, table::Segments::FilePaths))
            .columns([
                table::SegmentMetadata::Key,
                table::SegmentMetadata::StrValue,
                table::SegmentMetadata::IntValue,
                table::SegmentMetadata::FloatValue,
                table::SegmentMetadata::BoolValue,
            ])
            .build_sqlx(sea_query::SqliteQueryBuilder);

        let mut rows = sqlx::query_with(&sql, values).fetch(conn);
        let mut rows_by_segment_id: HashMap<SegmentUuid, Vec<SqliteRow>> = HashMap::new();

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| WrappedSqlxError(e).boxed())?
        {
            let segment_id = SegmentUuid::from_str(row.get::<&str, _>(0))
                .map_err(GetSegmentsError::SegmentConversion)?;

            if let Some(entry) = rows_by_segment_id.get_mut(&segment_id) {
                entry.push(row);
            } else {
                rows_by_segment_id.insert(segment_id, vec![row]);
            }
        }

        let segments = rows_by_segment_id
            .into_iter()
            .filter_map(|(segment_id, rows)| {
                if rows.is_empty() {
                    // should never happen
                    return None;
                }

                let metadata = self.metadata_from_rows(rows.iter());
                let first_row = rows.first().unwrap();

                let segment_type = match SegmentType::try_from(first_row.get::<&str, _>(1))
                    .map_err(GetSegmentsError::SegmentConversion)
                {
                    Ok(segment_type) => segment_type,
                    Err(err) => return Some(Err(err)),
                };

                let segment_scope = match SegmentScope::try_from(first_row.get::<&str, _>(2))
                    .map_err(GetSegmentsError::UnknownScope)
                {
                    Ok(scope) => scope,
                    Err(err) => return Some(Err(err)),
                };

                let file_path = match serde_json::from_str(first_row.get::<&str, _>("file_paths")) {
                    Ok(file_path) => file_path,
                    Err(err) => {
                        return Some(Err(GetSegmentsError::Internal(Box::new(
                            chroma_error::TonicError(tonic::Status::internal(err.to_string())),
                        ))))
                    }
                };

                Some(Ok(Segment {
                    id: segment_id,
                    r#type: segment_type,
                    scope: segment_scope,
                    collection: collection_id,
                    metadata,
                    file_path,
                }))
            })
            .collect::<Result<Vec<_>, GetSegmentsError>>()?;

        Ok(segments)
    }

    /// Returns true if the collection was deleted, false if it was not found
    async fn delete_collection_with_conn<C>(
        &self,
        conn: &mut C,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        _segment_ids: Vec<SegmentUuid>,
    ) -> Result<bool, WrappedSqlxError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        // Delete embedding metadata for the embedding ids matching the segment ids
        sqlx::query(
            r#"
            DELETE FROM embedding_metadata
            WHERE id IN (
                SELECT id FROM embeddings
                WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1)
            )
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Foreign keys are disabled: remove sparse payloads before their records.
        sqlx::query(
            "DELETE FROM embedding_metadata_sparse WHERE id IN (SELECT id FROM embeddings \
             WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1))",
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Delete embeddings fulltext search records
        sqlx::query(
            r#"
            DELETE FROM embedding_fulltext_search
            WHERE rowid IN (
                SELECT id FROM embeddings
                WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1)
            )
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Delete embeddings
        sqlx::query(
            r#"
            DELETE FROM embeddings
            WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1)
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Delete segment metadata
        sqlx::query(
            r#"
            DELETE FROM segment_metadata
            WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1)
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Delete max_seq_id records for segments being deleted
        sqlx::query(
            r#"
            DELETE FROM max_seq_id
            WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1)
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Snapshot actual index IDs in the deletion transaction, not the caller's
        // earlier listing. Metadata segments have no index files to reclaim; the
        // record segment and the index of a SPANN collection have blockfiles.
        sqlx::query(
            "INSERT OR IGNORE INTO index_cleanup (segment_id) \
             SELECT id FROM segments WHERE collection = ? AND type IN \
             ('urn:chroma:segment/vector/hnsw-local-memory', \
              'urn:chroma:segment/vector/hnsw-local-persisted', \
              'urn:chroma:segment/vector/spann', \
              'urn:chroma:segment/record/blockfile')",
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;
        // The folders of those blockfiles are under the folder of the collection: their rows keep it, so that the sweep
        // removes them there, and only what a deletion recorded (00018).
        sqlx::query(
            "INSERT OR IGNORE INTO blockfile_cleanup (segment_id, collection_id) \
             SELECT id, collection FROM segments WHERE collection = ? AND type IN \
             ('urn:chroma:segment/vector/spann', 'urn:chroma:segment/record/blockfile')",
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Delete segments
        let (sql, values) = sea_query::Query::delete()
            .from_table(table::Segments::Table)
            .and_where(
                sea_query::Expr::col((table::Segments::Table, table::Segments::Collection))
                    .eq(collection_id.to_string()),
            )
            .build_sqlx(sea_query::SqliteQueryBuilder);

        sqlx::query_with(&sql, values).execute(&mut *conn).await?;

        // Delete collection metadata
        sqlx::query(
            r#"
            DELETE FROM collection_metadata
            WHERE collection_id = $1
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        // Delete logs
        sqlx::query(
            r#"
            DELETE FROM embeddings_queue
            WHERE topic = $1
            "#,
        )
        .bind(get_embeddings_queue_topic_name(
            &self.log_tenant,
            &self.log_topic_namespace,
            collection_id,
        ))
        .execute(&mut *conn)
        .await?;

        // As SoftDeleteCollection of the coordinator of distributed Chroma: the ready attached functions that read the
        // collection, and those that write into it, deleted with it.
        sqlx::query(
            r#"
            UPDATE attached_functions
            SET name = '_deleted_' || name || '_' || id, is_deleted = TRUE,
                updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
            WHERE (input_collection_id = $1 OR output_collection_id = $1) AND is_ready = TRUE
              AND is_deleted = FALSE
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *conn)
        .await?;

        let deleted_rows = sqlx::query(
            r#"
            DELETE FROM collections
            WHERE id = $1
            AND database_id = (SELECT id FROM databases WHERE name = $2 AND tenant_id = $3)
            RETURNING id
        "#,
        )
        .bind(collection_id.to_string())
        .bind(&database)
        .bind(&tenant)
        .execute(&mut *conn)
        .await?;

        Ok(deleted_rows.rows_affected() > 0)
    }

    // TODO: reuse logic from metadata reader
    fn metadata_from_rows<'row>(
        &self,
        rows: impl Iterator<Item = &'row SqliteRow>,
    ) -> Option<Metadata> {
        let metadata: Metadata = rows
            .filter_map(|row| {
                let key = row.get::<&str, _>("key");

                if let Some(str_value) = row.get::<Option<String>, _>("str_value") {
                    Some((key.to_string(), MetadataValue::Str(str_value)))
                } else if let Some(int_value) = row.get::<Option<i64>, _>("int_value") {
                    Some((key.to_string(), MetadataValue::Int(int_value)))
                } else if let Some(float_value) = row.get::<Option<f64>, _>("float_value") {
                    Some((key.to_string(), MetadataValue::Float(float_value)))
                } else {
                    row.get::<Option<bool>, _>("bool_value")
                        .map(|bool_value| (key.to_string(), MetadataValue::Bool(bool_value)))
                }
            })
            .collect();

        if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        }
    }
}

//////////////////////// Configurable Implementation ////////////////////////

#[async_trait]
impl Configurable<SqliteSysDbConfig> for SqliteSysDb {
    async fn try_from_config(
        config: &SqliteSysDbConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // Assume the registry has a sqlite db
        let db = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        Ok(Self::new(
            db,
            config.log_tenant.clone(),
            config.log_topic_namespace.clone(),
        ))
    }
}

/// FlushCollectionCompaction on a connection of the caller: the new files of each segment and the log position they
/// cover, as the max_seq_id of the segment, which never goes down; a segment of another collection fails the whole
/// flush. A caller can record a flush in the transaction of other writes to the same database, as the compaction of a
/// SPANN collection on a single node does with its records.
pub async fn flush_compaction_in(
    conn: &mut sqlx::SqliteConnection,
    collection_id: CollectionUuid,
    log_position: i64,
    _collection_version: i32,
    segment_flush_info: &[SegmentFlushInfo],
    schema: Option<Schema>,
) -> Result<FlushCompactionResponse, FlushCompactionError> {
    let internal = |message: String| {
        FlushCompactionError::FailedToFlushCompaction(tonic::Status::internal(message))
    };
    let schema_str = schema
        .map(|schema| serde_json::to_string(&schema))
        .transpose()
        .map_err(|e| internal(e.to_string()))?;
    // As a flush of distributed Chroma, one more version of the collection.
    let collection_version: i32 = sqlx::query_scalar(
        "UPDATE collections SET schema_str = COALESCE($1, schema_str), version = version + 1 WHERE id = $2 \
         RETURNING version",
    )
    .bind(schema_str)
    .bind(collection_id.to_string())
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| internal(e.to_string()))?
    .ok_or(FlushCompactionError::CollectionNotFound)?;
    for info in segment_flush_info.iter() {
        // As the version check of distributed Chroma: a flush older than the last one of the segment would put back
        // files without the records after it, so the whole flush fails.
        let current: Option<i64> =
            sqlx::query_scalar("SELECT seq_id FROM max_seq_id WHERE segment_id = $1")
                .bind(info.segment_id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(|e| internal(e.to_string()))?;
        if let Some(current) = current.filter(|current| *current > log_position) {
            return Err(FlushCompactionError::FailedToFlushCompaction(
                tonic::Status::failed_precondition(format!(
                    "Flush is stale: segment {} is at log position {current}, the flush at {log_position}",
                    info.segment_id
                )),
            ));
        }
        let file_paths =
            serde_json::to_string(&info.file_paths).map_err(|e| internal(e.to_string()))?;
        let updated =
            sqlx::query("UPDATE segments SET file_paths = $1 WHERE id = $2 AND collection = $3")
                .bind(file_paths)
                .bind(info.segment_id.to_string())
                .bind(collection_id.to_string())
                .execute(&mut *conn)
                .await
                .map_err(|e| internal(e.to_string()))?
                .rows_affected();
        if updated == 0 {
            return Err(FlushCompactionError::SegmentNotFound);
        }
        sqlx::query(
            "INSERT INTO max_seq_id (segment_id, seq_id) VALUES ($1, $2) \
             ON CONFLICT(segment_id) DO UPDATE SET seq_id = excluded.seq_id \
             WHERE max_seq_id.seq_id < excluded.seq_id",
        )
        .bind(info.segment_id.to_string())
        .bind(log_position)
        .execute(&mut *conn)
        .await
        .map_err(|e| internal(e.to_string()))?;
    }
    let last_compaction_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default();
    Ok(FlushCompactionResponse {
        collection_id,
        collection_version,
        last_compaction_time,
    })
}

// The attached functions of the single node: the errors and the limit of the coordinator of distributed Chroma
// (go/pkg/common/errors.go and go/pkg/sysdb/coordinator/task.go), and its queries of the tables of 00015.
const MAX_ATTACHED_FUNCTION_DEPTH: usize = 5;
const ERR_DATABASE_NOT_FOUND: &str = "database not found";
const ERR_COLLECTION_NOT_FOUND: &str = "collection not found";
const ERR_FUNCTION_NOT_FOUND: &str = "function not found";
const ERR_ATTACHED_FUNCTION_ALREADY_EXISTS: &str =
    "the attached function that was being created already exists for this collection";
const ERR_ATTACHED_FUNCTION_NOT_FOUND: &str = "the requested attached function was not found";
const ERR_INVALID_ATTACHED_FUNCTION_NAME: &str =
    "attached function name cannot start with reserved prefix '_deleted_'";
const ERR_CANNOT_ATTACH_TO_OUTPUT_COLLECTION: &str =
    "cannot attach function to an output collection";

fn internal(e: sqlx::Error) -> tonic::Status {
    tonic::Status::internal(e.to_string())
}

struct FunctionRow {
    id: Uuid,
    name: String,
    is_async: bool,
}

#[derive(Clone)]
struct AttachedFunctionRow {
    id: Uuid,
    name: String,
    tenant_id: String,
    database_id: String,
    input_collection_id: String,
    output_collection_name: String,
    output_collection_id: Option<String>,
    function_id: Uuid,
    function_params: String,
    completion_offset: i64,
    min_records_for_invocation: i64,
    is_ready: bool,
    created_at_micros: i64,
    updated_at_micros: i64,
    failure_count: i32,
}

impl AttachedFunctionRow {
    fn from_row(row: &SqliteRow) -> Result<Self, tonic::Status> {
        let uuid = |column: &str| {
            Uuid::parse_str(row.get::<&str, _>(column))
                .map_err(|e| tonic::Status::internal(format!("invalid {column}: {e}")))
        };
        Ok(Self {
            id: uuid("id")?,
            name: row.get("name"),
            tenant_id: row.get("tenant_id"),
            database_id: row.get("database_id"),
            input_collection_id: row.get("input_collection_id"),
            output_collection_name: row.get("output_collection_name"),
            output_collection_id: row.get("output_collection_id"),
            function_id: uuid("function_id")?,
            function_params: row.get("function_params"),
            completion_offset: row.get("completion_offset"),
            min_records_for_invocation: row.get("min_records_for_invocation"),
            is_ready: row.get("is_ready"),
            created_at_micros: row.get("created_at_micros"),
            updated_at_micros: row.get("updated_at_micros"),
            failure_count: row.get("failure_count"),
        })
    }

    // As attachedFunctionToProto of the coordinator, then attached_function_from_proto of the gRPC sysdb.
    fn into_attached_function(
        self,
        function: &FunctionRow,
    ) -> Result<chroma_types::AttachedFunction, tonic::Status> {
        if self.completion_offset < 0 {
            return Err(tonic::Status::internal(format!(
                "attached function has invalid completion_offset: {}",
                self.completion_offset
            )));
        }
        if !self.is_ready {
            return Err(tonic::Status::internal(
                "serialized attached function is not ready",
            ));
        }
        let input_collection_id = CollectionUuid::from_str(&self.input_collection_id)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let output_collection_id = self
            .output_collection_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .map(CollectionUuid::from_str)
            .transpose()
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let params = (!self.function_params.is_empty())
            .then(|| {
                serde_json::from_str::<serde_json::Value>(&self.function_params)
                    .and_then(|value| serde_json::to_string(&value))
            })
            .transpose()
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let at = |micros: i64| {
            SystemTime::UNIX_EPOCH + std::time::Duration::from_micros(micros.max(0) as u64)
        };
        Ok(chroma_types::AttachedFunction {
            id: AttachedFunctionUuid(self.id),
            name: self.name,
            function_id: function.id,
            input_collection_id,
            output_collection_name: self.output_collection_name,
            output_collection_id,
            params,
            tenant_id: self.tenant_id,
            database_id: self.database_id,
            last_run: None,
            completion_offset: self.completion_offset as u64,
            min_records_for_invocation: self.min_records_for_invocation as u64,
            is_deleted: false,
            is_async: function.is_async,
            failure_count: self.failure_count,
            created_at: at(self.created_at_micros),
            updated_at: at(self.updated_at_micros),
        })
    }
}

// The filters of GetAttachedFunctions of the store of the coordinator (go/pkg/sysdb/metastore/db/dao/task.go): none
// filters on a field left out.
#[derive(Default)]
struct AttachedFunctionFilter<'a> {
    ids: &'a [String],
    name: Option<&'a str>,
    input: Option<&'a str>,
    output: Option<&'a str>,
}

impl<'a> AttachedFunctionFilter<'a> {
    fn input(input: &'a str) -> Self {
        Self {
            input: Some(input),
            ..Default::default()
        }
    }

    fn id(id: &'a String) -> Self {
        Self {
            ids: std::slice::from_ref(id),
            ..Default::default()
        }
    }
}

async fn attached_function_rows(
    conn: &mut sqlx::SqliteConnection,
    filter: AttachedFunctionFilter<'_>,
    only_ready: bool,
) -> Result<Vec<AttachedFunctionRow>, tonic::Status> {
    let mut sql = String::from(
        "SELECT id, name, tenant_id, database_id, input_collection_id, output_collection_name, output_collection_id, \
         function_id, function_params, completion_offset, min_records_for_invocation, is_ready, failure_count, \
         CAST((julianday(created_at) - 2440587.5) * 86400000000 AS INTEGER) AS created_at_micros, \
         CAST((julianday(updated_at) - 2440587.5) * 86400000000 AS INTEGER) AS updated_at_micros \
         FROM attached_functions WHERE is_deleted = FALSE",
    );
    if !filter.ids.is_empty() {
        sql.push_str(&format!(
            " AND id IN ({})",
            vec!["?"; filter.ids.len()].join(", ")
        ));
    }
    for (value, column) in [
        (filter.name, "name"),
        (filter.input, "input_collection_id"),
        (filter.output, "output_collection_id"),
    ] {
        if value.is_some() {
            sql.push_str(&format!(" AND {column} = ?"));
        }
    }
    if only_ready {
        sql.push_str(" AND is_ready = TRUE");
    }
    let mut query = sqlx::query(&sql);
    for id in filter.ids {
        query = query.bind(id);
    }
    for value in [filter.name, filter.input, filter.output]
        .into_iter()
        .flatten()
    {
        query = query.bind(value);
    }
    query
        .fetch_all(&mut *conn)
        .await
        .map_err(internal)?
        .iter()
        .map(AttachedFunctionRow::from_row)
        .collect()
}

fn function_from_row(row: &SqliteRow) -> Result<FunctionRow, tonic::Status> {
    Ok(FunctionRow {
        id: Uuid::parse_str(row.get::<&str, _>("id"))
            .map_err(|e| tonic::Status::internal(e.to_string()))?,
        name: row.get("name"),
        is_async: row.get("is_async"),
    })
}

async fn function_by_name(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
) -> Result<Option<FunctionRow>, tonic::Status> {
    sqlx::query("SELECT id, name, is_async FROM functions WHERE name = $1")
        .bind(name)
        .fetch_optional(&mut *conn)
        .await
        .map_err(internal)?
        .as_ref()
        .map(function_from_row)
        .transpose()
}

async fn functions_by_id(
    conn: &mut sqlx::SqliteConnection,
    attached_functions: &[AttachedFunctionRow],
) -> Result<HashMap<Uuid, FunctionRow>, tonic::Status> {
    let mut functions = HashMap::new();
    for row in sqlx::query("SELECT id, name, is_async FROM functions")
        .fetch_all(&mut *conn)
        .await
        .map_err(internal)?
    {
        let function = function_from_row(&row)?;
        if attached_functions
            .iter()
            .any(|attached_function| attached_function.function_id == function.id)
        {
            functions.insert(function.id, function);
        }
    }
    Ok(functions)
}

async fn database_id(
    conn: &mut sqlx::SqliteConnection,
    tenant: &str,
    database: &str,
) -> Result<Option<String>, tonic::Status> {
    sqlx::query_scalar("SELECT id FROM databases WHERE name = $1 AND tenant_id = $2")
        .bind(database)
        .bind(tenant)
        .fetch_optional(&mut *conn)
        .await
        .map_err(internal)
}

// The collections of a database, by name.
async fn collection_ids_by_name(
    conn: &mut sqlx::SqliteConnection,
    tenant: &str,
    database: &str,
) -> Result<HashMap<String, String>, tonic::Status> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT collections.name, collections.id FROM collections \
         JOIN databases ON collections.database_id = databases.id \
         WHERE databases.name = $1 AND databases.tenant_id = $2",
    )
    .bind(database)
    .bind(tenant)
    .fetch_all(&mut *conn)
    .await
    .map_err(internal)?;
    Ok(rows.into_iter().collect())
}

// Update of the coordinator, as FinishCreateAttachedFunction calls it: every row of the attached function, of every
// input, ready with its output collection; none updated is ErrAttachedFunctionNotFound.
async fn mark_ready(
    conn: &mut sqlx::SqliteConnection,
    id: &str,
    output_collection_id: Option<&str>,
) -> Result<(), tonic::Status> {
    let updated = sqlx::query(
        "UPDATE attached_functions SET output_collection_id = COALESCE($1, output_collection_id), is_ready = TRUE, \
         updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = $2 AND is_deleted = FALSE",
    )
    .bind(output_collection_id)
    .bind(id)
    .execute(&mut *conn)
    .await
    .map_err(internal)?;
    if updated.rows_affected() == 0 {
        return Err(tonic::Status::internal(ERR_ATTACHED_FUNCTION_NOT_FOUND));
    }
    Ok(())
}

// The graph of attachedFunctionGraphState of the coordinator, read whole in the transaction of the attach: its edges
// go from the input collection of an attached function to its output collection, by id, or by name in the database
// of the attach while the output collection is not created yet.
struct AttachedFunctionGraph {
    attached_functions: Vec<AttachedFunctionRow>,
    collections: HashMap<String, String>,
}

impl AttachedFunctionGraph {
    fn incoming(&self, collection_id: &str) -> Vec<AttachedFunctionRow> {
        self.attached_functions
            .iter()
            .filter(|row| row.output_collection_id.as_deref() == Some(collection_id))
            .cloned()
            .collect()
    }

    fn output_collection_id(&self, row: &AttachedFunctionRow) -> Option<String> {
        row.output_collection_id
            .clone()
            .or_else(|| self.collections.get(&row.output_collection_name).cloned())
    }

    fn incoming_collection_ids(&self, collection_id: &str) -> Vec<String> {
        self.attached_functions
            .iter()
            .filter(|row| row.output_collection_id.as_deref() == Some(collection_id))
            .map(|row| row.input_collection_id.clone())
            .collect()
    }

    fn outgoing_collection_ids(&self, collection_id: &str) -> Vec<String> {
        self.attached_functions
            .iter()
            .filter(|row| row.input_collection_id == collection_id)
            .filter_map(|row| self.output_collection_id(row))
            .collect()
    }

    fn max_path_length(
        &self,
        collection_id: &str,
        memo: &mut HashMap<String, usize>,
        visiting: &mut HashSet<String>,
        cycle_message: &str,
        neighbors: &dyn Fn(&str) -> Vec<String>,
    ) -> Result<usize, tonic::Status> {
        if let Some(depth) = memo.get(collection_id) {
            return Ok(*depth);
        }
        if !visiting.insert(collection_id.to_string()) {
            return Err(tonic::Status::failed_precondition(cycle_message));
        }
        let mut max_depth = 0;
        for next in neighbors(collection_id) {
            let depth = self.max_path_length(&next, memo, visiting, cycle_message, neighbors);
            let depth = match depth {
                Ok(depth) => depth,
                Err(e) => {
                    visiting.remove(collection_id);
                    return Err(e);
                }
            };
            max_depth = max_depth.max(depth + 1);
        }
        visiting.remove(collection_id);
        memo.insert(collection_id.to_string(), max_depth);
        Ok(max_depth)
    }

    fn reaches(&self, start: &str, target: &str) -> bool {
        if start == target {
            return true;
        }
        let mut queue = std::collections::VecDeque::from([start.to_string()]);
        let mut visited = HashSet::from([start.to_string()]);
        while let Some(current) = queue.pop_front() {
            for output in self.outgoing_collection_ids(&current) {
                if output == target {
                    return true;
                }
                if visited.insert(output.clone()) {
                    queue.push_back(output);
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        InternalUpdateCollectionConfiguration, KnnIndex, SegmentScope, SegmentType, SegmentUuid,
        UpdateHnswConfiguration, UpdateMetadata, UpdateMetadataValue,
        UpdateVectorIndexConfiguration, VectorIndexConfiguration,
    };

    #[tokio::test]
    async fn test_count_databases() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".into(), "default".into());
        sysdb.create_tenant("count-tenant".into()).await.unwrap();
        assert_eq!(
            sysdb.count_databases("count-tenant".into()).await.unwrap(),
            0
        );
        sysdb
            .create_database(Uuid::new_v4(), "one", "count-tenant")
            .await
            .unwrap();
        sysdb
            .create_database(Uuid::new_v4(), "two", "count-tenant")
            .await
            .unwrap();
        sysdb
            .create_database(Uuid::new_v4(), "other", "default_tenant")
            .await
            .unwrap();
        assert_eq!(
            sysdb.count_databases("count-tenant".into()).await.unwrap(),
            2
        );
        sysdb
            .delete_database("two".into(), "count-tenant".into())
            .await
            .unwrap();
        assert_eq!(
            sysdb.count_databases("count-tenant".into()).await.unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_create_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());
        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        // Second call should fail
        let result = sysdb
            .create_database(uuid::Uuid::new_v4(), "test", "default_tenant")
            .await;

        matches!(result, Err(CreateDatabaseError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_get_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        // Get non-existent database
        let result = sysdb.get_database("test", "default_tenant").await;
        matches!(result, Err(GetDatabaseError::NotFound(_)));

        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        let database = sysdb.get_database("test", "default_tenant").await.unwrap();
        assert_eq!(database.id, db_id);
    }

    #[tokio::test]
    async fn test_get_database_by_id_is_tenant_scoped() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());
        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        let database = sysdb
            .get_database_by_id(db_id, "default_tenant")
            .await
            .unwrap();
        assert_eq!(database.id, db_id);
        assert_eq!(database.name, "test");

        let wrong_tenant = sysdb.get_database_by_id(db_id, "other_tenant").await;
        assert!(matches!(wrong_tenant, Err(GetDatabaseError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db.clone(), "default".to_string(), "default".to_string());

        // Delete non-existent database
        let result = sysdb
            .delete_database("test".to_string(), "default_tenant".to_string())
            .await;
        matches!(result, Err(DeleteDatabaseError::NotFound(_)));

        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        let collection_id = CollectionUuid::new();
        let segments = vec![
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::HnswLocalPersisted,
                scope: SegmentScope::VECTOR,
                collection: collection_id,
                metadata: None,
                file_path: HashMap::new(),
            },
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::Sqlite,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                metadata: None,
                file_path: HashMap::new(),
            },
        ];
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "test".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            sysdb
                .get_segments(None, None, None, collection_id)
                .await
                .unwrap()
                .len(),
            2
        );

        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM index_cleanup")
            .fetch_one(db.get_conn())
            .await
            .unwrap();
        assert_eq!(pending, 0);

        // Delete database
        sysdb
            .delete_database("test".to_string(), "default_tenant".to_string())
            .await
            .unwrap();
        assert!(sysdb
            .get_segments(None, None, None, collection_id)
            .await
            .unwrap()
            .is_empty());
        let mut pending: Vec<String> = sqlx::query_scalar("SELECT segment_id FROM index_cleanup")
            .fetch_all(db.get_conn())
            .await
            .unwrap();
        let mut expected: Vec<String> = segments
            .iter()
            .filter(|s| {
                matches!(
                    s.r#type,
                    SegmentType::HnswLocalMemory | SegmentType::HnswLocalPersisted
                )
            })
            .map(|s| s.id.to_string())
            .collect();
        pending.sort();
        expected.sort();
        assert_eq!(pending, expected);
    }

    #[tokio::test]
    async fn test_list_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        // List default databases
        let databases = sysdb
            .list_databases("default_tenant".to_string(), None, 0)
            .await
            .unwrap();
        assert_eq!(databases.len(), 1);

        // Create database and list again
        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        let databases = sysdb
            .list_databases("default_tenant".to_string(), None, 0)
            .await
            .unwrap();
        assert_eq!(databases.len(), 2);

        // Offset list by 1 and limit to 1 result
        let databases = sysdb
            .list_databases("default_tenant".to_string(), Some(1), 1)
            .await
            .unwrap();
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name, "test");
    }

    #[tokio::test]
    async fn test_create_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        // Create tenant
        sysdb.create_tenant("new_tenant".to_string()).await.unwrap();

        // Second call should fail
        let result = sysdb.create_tenant("new_tenant".to_string()).await;
        matches!(result, Err(CreateTenantError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_get_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        // Get non-existent tenant
        let result = sysdb.get_tenant("test").await;
        matches!(result, Err(GetTenantError::NotFound(_)));

        // Create tenant
        sysdb.create_tenant("new_tenant".to_string()).await.unwrap();

        // Get tenant
        let tenant = sysdb.get_tenant("new_tenant").await.unwrap();
        assert_eq!(tenant.name, "new_tenant");
    }

    #[tokio::test]
    async fn test_update_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        // Create tenant
        sysdb.create_tenant("new_tenant".to_string()).await.unwrap();

        // Get tenant
        let tenant = sysdb.get_tenant("new_tenant").await.unwrap();
        assert_eq!(tenant.name, "new_tenant");
        assert_eq!(tenant.resource_name, None);

        // Update tenant
        sysdb
            .update_tenant("new_tenant".to_string(), "new_resource_name".to_string())
            .await
            .unwrap();

        // Get tenant
        let tenant = sysdb.get_tenant("new_tenant").await.unwrap();
        assert_eq!(tenant.name, "new_tenant");
        assert_eq!(tenant.resource_name, Some("new_resource_name".to_string()));

        // As distributed Chroma, the resource name is set once, and a tenant that is not there is not found.
        let again = sysdb
            .update_tenant("new_tenant".to_string(), "other_resource_name".to_string())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(again, UpdateTenantError::FailedToSetResourceName(_)),
            "{again:?}"
        );
        let tenant = sysdb.get_tenant("new_tenant").await.unwrap();
        assert_eq!(tenant.resource_name, Some("new_resource_name".to_string()));
        let missing = sysdb
            .update_tenant("no_tenant".to_string(), "missing_resource_name".to_string())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(missing, UpdateTenantError::NotFound(_)),
            "{missing:?}"
        );

        // No two tenants have the same resource name.
        sysdb
            .create_tenant("other_tenant".to_string())
            .await
            .unwrap();
        let taken = sysdb
            .update_tenant("other_tenant".to_string(), "new_resource_name".to_string())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(taken, UpdateTenantError::FailedToSetResourceName(_)),
            "{taken:?}"
        );
        let other = sysdb.get_tenant("other_tenant").await.unwrap();
        assert_eq!(other.resource_name, None);
    }

    #[tokio::test]
    async fn test_create_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let mut collection_metadata = Metadata::new();
        collection_metadata.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        collection_metadata.insert("key2".to_string(), MetadataValue::Int(42));
        collection_metadata.insert("key3".to_string(), MetadataValue::Float(42.0));
        collection_metadata.insert("key4".to_string(), MetadataValue::Bool(true));

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        }];
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                Some(collection_metadata.clone()),
                None,
                false,
            )
            .await
            .unwrap();

        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .unwrap();
        let collection = collections.first().unwrap();

        assert_eq!(collection.name, "test_collection");
        assert_eq!(collection.metadata.as_ref().unwrap().len(), 4);
        assert_eq!(collection.metadata, Some(collection_metadata));
    }

    // As the sysdb of distributed Chroma: the files of a segment are kept, a flush replaces them together with the log
    // position they cover, and a fork shares them.
    #[tokio::test]
    async fn test_flush_compaction_keeps_the_files_of_the_segments() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db.clone(), "default".to_string(), "default".to_string());
        let collection_id = CollectionUuid::new();
        let segment = |r#type, scope| Segment {
            id: SegmentUuid::new(),
            r#type,
            scope,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };
        let metadata = segment(SegmentType::Sqlite, SegmentScope::METADATA);
        let record = segment(SegmentType::BlockfileRecord, SegmentScope::RECORD);
        let vector = segment(SegmentType::Spann, SegmentScope::VECTOR);
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "files".to_string(),
                vec![metadata.clone(), record.clone(), vector.clone()],
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        let created = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();
        assert_eq!(created.record_segment.id, record.id);
        assert!(created.vector_segment.file_path.is_empty());
        assert_eq!(created.collection.version, 0);

        let files = |name: &str| {
            HashMap::from([(name.to_string(), vec![format!("{name}/{}", Uuid::new_v4())])])
        };
        let (record_files, vector_files) = (files("id_to_data"), files("posting_list"));
        let flush = |segment_id: SegmentUuid, log_position: i64| {
            sysdb.flush_compaction(
                collection_id,
                log_position,
                0,
                vec![
                    SegmentFlushInfo {
                        segment_id: record.id,
                        file_paths: record_files.clone(),
                    },
                    SegmentFlushInfo {
                        segment_id,
                        file_paths: vector_files.clone(),
                    },
                ]
                .into(),
                None,
            )
        };
        // As a flush of distributed Chroma, one more version of the collection.
        assert_eq!(flush(vector.id, 7).await.unwrap().collection_version, 1);
        let flushed = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();
        assert_eq!(flushed.collection.version, 1);
        assert_eq!(flushed.record_segment.file_path, record_files);
        assert_eq!(flushed.vector_segment.file_path, vector_files);
        let seq_id = |segment_id: SegmentUuid| {
            let db = db.clone();
            async move {
                sqlx::query_scalar::<_, i64>("SELECT seq_id FROM max_seq_id WHERE segment_id = $1")
                    .bind(segment_id.to_string())
                    .fetch_optional(db.get_conn())
                    .await
                    .unwrap()
            }
        };
        assert_eq!(seq_id(record.id).await, Some(7));
        assert_eq!(seq_id(vector.id).await, Some(7));

        // A segment of another collection fails the whole flush.
        let before = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();
        assert!(matches!(
            flush(SegmentUuid::new(), 9).await,
            Err(FlushCompactionError::SegmentNotFound)
        ));
        let after = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();
        assert_eq!(
            after.record_segment.file_path,
            before.record_segment.file_path
        );
        assert_eq!(seq_id(record.id).await, Some(7));
        assert!(matches!(
            sysdb
                .flush_compaction(CollectionUuid::new(), 9, 0, vec![].into(), None)
                .await,
            Err(FlushCompactionError::CollectionNotFound)
        ));

        // A flush older than the last one fails, and leaves the files and the position of the last one.
        flush(vector.id, 9).await.unwrap();
        let stale = sysdb
            .flush_compaction(
                collection_id,
                8,
                0,
                vec![SegmentFlushInfo {
                    segment_id: vector.id,
                    file_paths: files("posting_list"),
                }]
                .into(),
                None,
            )
            .await;
        assert!(
            matches!(stale, Err(FlushCompactionError::FailedToFlushCompaction(_))),
            "{stale:?}"
        );
        let after = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();
        assert_eq!(after.vector_segment.file_path, vector_files);
        assert_eq!(seq_id(vector.id).await, Some(9));

        let fork_id = CollectionUuid::new();
        sysdb
            .fork_collection(collection_id, fork_id, "files_fork".to_string())
            .await
            .unwrap();
        let fork = sysdb.get_collection_with_segments(fork_id).await.unwrap();
        assert_ne!(fork.vector_segment.id, vector.id);
        assert!(fork.record_segment.file_path.is_empty());
        assert!(fork.vector_segment.file_path.is_empty());
    }

    #[tokio::test]
    async fn test_create_collection_fails_for_duplicate_name() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        }];
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.name, "test_collection");

        // Should fail when attempting to create with the same name
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments,
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await;
        matches!(result, Err(CreateCollectionError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_create_collection_get_or_create() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        }];
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.name, "test_collection");

        // Should return existing collection
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                CollectionUuid::new(),
                "test_collection".to_string(),
                vec![],
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(result.collection_id, collection_id);
    }

    #[tokio::test]
    async fn dimension_initialization_is_atomic() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());
        let id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".into(),
                "default_database".into(),
                id,
                "dimension_race".into(),
                vec![],
                Some(InternalCollectionConfiguration::default_hnsw()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        let other = sysdb.clone();
        let (first, second) = tokio::join!(
            sysdb.update_collection(id, None, None, Some(2), None),
            other.update_collection(id, None, None, Some(3), None),
        );
        let winner = match (first, second) {
            (Ok(()), Err(UpdateCollectionError::DimensionMismatch(2, 3))) => 2,
            (Err(UpdateCollectionError::DimensionMismatch(3, 2)), Ok(())) => 3,
            results => panic!("expected exactly one winning dimension: {results:?}"),
        };
        sysdb
            .update_collection(id, None, None, Some(winner), None)
            .await
            .unwrap();
        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(collections[0].dimension, Some(winner as i32));
    }

    #[tokio::test]
    async fn test_update_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                vec![],
                Some(InternalCollectionConfiguration::default_hnsw()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        let mut metadata: UpdateMetadata = HashMap::new();
        metadata.insert(
            "key1".to_string(),
            UpdateMetadataValue::Str("value1".to_string()),
        );

        sysdb
            .update_collection(
                collection_id,
                Some("new_name".to_string()),
                Some(CollectionMetadataUpdate::UpdateMetadata(metadata)),
                Some(1024),
                Some(InternalUpdateCollectionConfiguration {
                    vector_index: Some(UpdateVectorIndexConfiguration::Hnsw(Some(
                        UpdateHnswConfiguration {
                            ef_search: Some(10),
                            num_threads: Some(2),
                            ..Default::default()
                        },
                    ))),
                    embedding_function: None,
                }),
            )
            .await
            .unwrap();

        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .unwrap();
        let collection = collections.first().unwrap();
        assert_eq!(collection.name, "new_name");
        assert_eq!(collection.dimension, Some(1024));
        let metadata = collection.metadata.as_ref().unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata.get("key1").unwrap(),
            &MetadataValue::Str("value1".to_string())
        );

        // Access HNSW configuration through pattern matching
        match &collection.config.vector_index {
            VectorIndexConfiguration::Hnsw(hnsw) => {
                assert_eq!(hnsw.ef_search, 10);
            }
            _ => panic!("Expected HNSW configuration"),
        }
    }

    #[tokio::test]
    async fn test_delete_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db.clone(), "default".to_string(), "default".to_string());

        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                vec![],
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // Delete non-existent collection
        let result = sysdb
            .delete_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                CollectionUuid::new(),
                vec![],
            )
            .await;

        assert!(result.is_err());

        // Simulate a segment committed after the caller's initial listing.
        let listed = sysdb
            .get_segments(None, None, None, collection_id)
            .await
            .unwrap();
        assert!(listed.is_empty());
        let late_segment = SegmentUuid::new();
        sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
            .bind(late_segment.to_string())
            .bind("urn:chroma:segment/vector/hnsw-local-memory")
            .bind("VECTOR")
            .bind(collection_id.to_string())
            .execute(db.get_conn())
            .await
            .unwrap();

        // Sparse payload cleanup must use the actual segment set and leave
        // other collection identities intact (foreign keys are disabled).
        for (id, segment) in [(1, late_segment), (2, SegmentUuid::new())] {
            sqlx::query(
                "INSERT INTO embeddings (id, segment_id, embedding_id, seq_id) VALUES (?, ?, ?, ?)",
            )
            .bind(id)
            .bind(segment.to_string())
            .bind(format!("row-{id}"))
            .bind(1_i64)
            .execute(db.get_conn())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO embedding_metadata_sparse (id, key, value) VALUES (?, 'sparse', ?)",
            )
            .bind(id)
            .bind(r##"{"#type":"sparse_vector","indices":[],"values":[]}"##)
            .execute(db.get_conn())
            .await
            .unwrap();
        }

        // Delete collection
        sysdb
            .delete_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                vec![],
            )
            .await
            .unwrap();

        // Should no longer exist
        let result = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
        let pending: Vec<String> = sqlx::query_scalar("SELECT segment_id FROM index_cleanup")
            .fetch_all(db.get_conn())
            .await
            .unwrap();
        assert_eq!(pending, vec![late_segment.to_string()]);
        let remaining: Vec<i64> = sqlx::query_scalar("SELECT id FROM embedding_metadata_sparse")
            .fetch_all(db.get_conn())
            .await
            .unwrap();
        assert_eq!(remaining, vec![2]);
    }

    #[tokio::test]
    async fn test_get_collection_with_segments() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let mut collection_metadata = Metadata::new();
        collection_metadata.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        collection_metadata.insert("key2".to_string(), MetadataValue::Int(42));
        collection_metadata.insert("key3".to_string(), MetadataValue::Float(42.0));
        collection_metadata.insert("key4".to_string(), MetadataValue::Bool(true));

        let segment_metadata = collection_metadata.clone();

        let collection_id = CollectionUuid::new();
        let segments = vec![
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::BlockfileMetadata,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                metadata: Some(segment_metadata),
                file_path: HashMap::new(),
            },
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::HnswDistributed,
                scope: SegmentScope::VECTOR,
                collection: collection_id,
                metadata: None,
                file_path: HashMap::new(),
            },
        ];
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                Some(collection_metadata.clone()),
                None,
                false,
            )
            .await
            .unwrap();

        let collection_and_segments = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();

        assert_eq!(collection_and_segments.collection.name, "test_collection");
        assert_eq!(collection_and_segments.metadata_segment.id, segments[0].id);
        assert_eq!(
            collection_and_segments.metadata_segment.metadata,
            segments[0].metadata
        );
    }

    #[tokio::test]
    async fn test_get_segments() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let mut collection_metadata = Metadata::new();
        collection_metadata.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        collection_metadata.insert("key2".to_string(), MetadataValue::Int(42));
        collection_metadata.insert("key3".to_string(), MetadataValue::Float(42.0));
        collection_metadata.insert("key4".to_string(), MetadataValue::Bool(true));

        let segment_metadata = collection_metadata.clone();

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: Some(segment_metadata),
            file_path: HashMap::new(),
        }];
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                Some(collection_metadata.clone()),
                None,
                false,
            )
            .await
            .unwrap();

        let fetched_segments = sysdb
            .get_segments(Some(segments[0].id), None, None, collection_id)
            .await
            .unwrap();
        assert_eq!(segments.len(), 1);
        let fetched_segment = fetched_segments.first().unwrap();
        assert_eq!(*fetched_segment, segments[0]);
    }

    #[tokio::test]
    async fn test_get_collection_with_old_config() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db, "default".to_string(), "default".to_string());

        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                vec![],
                Some(InternalCollectionConfiguration::default_hnsw()),
                Some(Schema::new_default(KnnIndex::Hnsw)),
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // Set to legacy config shape
        sqlx::query(
            r#"
            UPDATE collections
            SET config_json_str = $1
        "#,
        )
        .bind(r#"{"hnsw_configuration": {"space": "l2", "ef_construction": 100, "ef_search": 100, "num_threads": 16, "M": 16, "resize_factor": 1.2, "batch_size": 100, "sync_threshold": 1000, "_type": "HNSWConfigurationInternal"}, "_type": "CollectionConfigurationInternal"}"#)
        .execute(sysdb.db.get_conn())
        .await
        .unwrap();

        // Fetching the collection should not error and the config should be the default
        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .unwrap();
        let collection = collections.first().unwrap();
        assert_eq!(
            collection.config,
            InternalCollectionConfiguration::default_hnsw()
        );
    }

    // The attached functions of the single node answer as the coordinator of distributed Chroma.
    async fn function_collection(sysdb: &SqliteSysDb, name: &str) -> CollectionUuid {
        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                name.to_string(),
                Vec::new(),
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        collection_id
    }

    async fn attach(
        sysdb: &SqliteSysDb,
        name: &str,
        function: &str,
        input: CollectionUuid,
        output: &str,
    ) -> Result<(AttachedFunctionUuid, bool), tonic::Status> {
        sysdb
            .create_attached_function(
                name.to_string(),
                function.to_string(),
                input,
                output.to_string(),
                serde_json::Value::Null,
                "default_tenant".to_string(),
                "default_database".to_string(),
                100,
            )
            .await
    }

    async fn finish(sysdb: &SqliteSysDb, id: AttachedFunctionUuid) -> bool {
        sysdb
            .finish_create_attached_function(id, "{}".to_string(), |collection| {
                vec![Segment {
                    id: SegmentUuid::new(),
                    r#type: SegmentType::Sqlite,
                    scope: SegmentScope::METADATA,
                    collection,
                    metadata: None,
                    file_path: HashMap::new(),
                }]
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_attached_function_is_created_ready_read_and_detached() {
        let sysdb = SqliteSysDb::new(
            get_new_sqlite_db().await,
            "default".into(),
            "default".into(),
        );
        let input = function_collection(&sysdb, "input").await;
        let (id, created) = attach(&sysdb, "counter", "record_counter", input, "counts")
            .await
            .unwrap();
        assert!(created);
        // Not ready before its output collection: get with only_ready finds nothing.
        assert!(sysdb
            .get_attached_functions(Some("counter".into()), Some(input), vec![], true)
            .await
            .unwrap()
            .is_empty());
        assert!(finish(&sysdb, id).await);
        let got = sysdb
            .get_attached_functions(Some("counter".into()), Some(input), vec![], true)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, id);
        assert_eq!(got[0].function_id, chroma_types::FUNCTION_RECORD_COUNTER_ID);
        assert_eq!(got[0].completion_offset, 0);
        assert_eq!(got[0].params.as_deref(), Some("{}"));
        let output = got[0].output_collection_id.unwrap();
        assert_eq!(
            sysdb
                .get_segments(None, None, None, output)
                .await
                .unwrap()
                .len(),
            1
        );
        // The same request again is the same attached function, already created.
        assert_eq!(
            attach(&sysdb, "counter", "record_counter", input, "counts")
                .await
                .unwrap(),
            (id, false)
        );
        sysdb
            .soft_delete_attached_function("counter".into(), input, true)
            .await
            .unwrap();
        assert!(sysdb
            .get_attached_functions(None, Some(input), vec![], false)
            .await
            .unwrap()
            .is_empty());
        assert!(sysdb
            .get_segments(None, None, None, output)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            sysdb
                .soft_delete_attached_function("counter".into(), input, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }

    #[tokio::test]
    async fn an_attach_is_refused_as_the_coordinator_refuses_it() {
        let sysdb = SqliteSysDb::new(
            get_new_sqlite_db().await,
            "default".into(),
            "default".into(),
        );
        let input = function_collection(&sysdb, "input").await;
        let err = attach(&sysdb, "f", "no_such_function", input, "out")
            .await
            .unwrap_err();
        assert_eq!(
            (err.code(), err.message()),
            (tonic::Code::NotFound, "function not found")
        );
        let err = attach(&sysdb, "_deleted_f", "record_counter", input, "out")
            .await
            .unwrap_err();
        assert_eq!(err.message(), ERR_INVALID_ATTACHED_FUNCTION_NAME);
        let err = attach(&sysdb, "f", "record_counter", CollectionUuid::new(), "out")
            .await
            .unwrap_err();
        assert_eq!(err.message(), ERR_COLLECTION_NOT_FOUND);

        let (id, _) = attach(&sysdb, "f", "record_counter", input, "out")
            .await
            .unwrap();
        finish(&sysdb, id).await;
        // A second synchronous function on the same collection.
        let err = attach(&sysdb, "g", "statistics", input, "stats")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert!(err
            .message()
            .contains("same execution mode (pre-lock validation)"));
        // A function on the output collection of a synchronous function.
        let output = sysdb
            .get_attached_functions(None, Some(input), vec![], true)
            .await
            .unwrap()[0]
            .output_collection_id
            .unwrap();
        let err = attach(&sysdb, "h", "record_counter", output, "out2")
            .await
            .unwrap_err();
        assert_eq!(err.message(), ERR_CANNOT_ATTACH_TO_OUTPUT_COLLECTION);
        // More inputs only for an asynchronous function.
        let other = function_collection(&sysdb, "other").await;
        let err = sysdb
            .add_attached_function_input(id, other)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // As CreateAttachedFunction of the coordinator: an attach whose output collection reaches its input is refused, and
    // so is one past MAX_ATTACHED_FUNCTION_DEPTH functions in a chain, both on chains of asynchronous functions, which
    // the output collection of a function can take as input.
    #[tokio::test]
    async fn an_attach_that_closes_a_cycle_or_passes_the_maximum_depth_is_refused() {
        let sysdb = SqliteSysDb::new(
            get_new_sqlite_db().await,
            "default".into(),
            "default".into(),
        );
        let mut input = function_collection(&sysdb, "c0").await;
        for depth in 1..=MAX_ATTACHED_FUNCTION_DEPTH {
            let (id, _) = attach(&sysdb, "f", "dummy_async", input, &format!("c{depth}"))
                .await
                .unwrap();
            finish(&sysdb, id).await;
            input = sysdb
                .get_attached_functions(None, Some(input), vec![], true)
                .await
                .unwrap()[0]
                .output_collection_id
                .unwrap();
        }
        // c0 -> c1 -> ... -> c5: writing into c0 from c5 closes a cycle.
        let err = attach(&sysdb, "f", "dummy_async", input, "c0")
            .await
            .unwrap_err();
        assert_eq!(err.message(), ERR_CANNOT_ATTACH_TO_OUTPUT_COLLECTION);
        // A sixth function after the chain of five.
        let err = attach(&sysdb, "f", "dummy_async", input, "c6")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("depth exceeds maximum"));
    }

    #[test]
    fn the_graph_of_the_attached_functions_counts_depth_and_reach_as_the_coordinator() {
        // A chain c0 -> c1 -> ... of edges, each output collection the input of the next, one beyond the limit.
        let ids = (0..=MAX_ATTACHED_FUNCTION_DEPTH + 1)
            .map(|_| Uuid::new_v4().to_string())
            .collect::<Vec<_>>();
        let graph = AttachedFunctionGraph {
            attached_functions: ids
                .windows(2)
                .map(|pair| AttachedFunctionRow {
                    id: Uuid::new_v4(),
                    name: "f".into(),
                    tenant_id: "t".into(),
                    database_id: "d".into(),
                    input_collection_id: pair[0].clone(),
                    output_collection_name: pair[1].clone(),
                    output_collection_id: Some(pair[1].clone()),
                    function_id: Uuid::new_v4(),
                    function_params: "{}".into(),
                    completion_offset: 0,
                    min_records_for_invocation: 100,
                    is_ready: true,
                    created_at_micros: 0,
                    updated_at_micros: 0,
                    failure_count: 0,
                })
                .collect(),
            collections: HashMap::new(),
        };
        let depth = graph
            .max_path_length(
                ids.last().unwrap(),
                &mut HashMap::new(),
                &mut HashSet::new(),
                "cycle",
                &|id| graph.incoming_collection_ids(id),
            )
            .unwrap();
        assert_eq!(depth, MAX_ATTACHED_FUNCTION_DEPTH + 1);
        assert!(graph.reaches(&ids[0], ids.last().unwrap()));
        assert!(!graph.reaches(ids.last().unwrap(), &ids[0]));
    }

    #[tokio::test]
    async fn the_built_in_functions_are_those_of_distributed_chroma() {
        let sysdb = SqliteSysDb::new(
            get_new_sqlite_db().await,
            "default".into(),
            "default".into(),
        );
        let mut functions = sysdb.get_all_functions().await.unwrap();
        functions.sort();
        let mut expected = vec![
            (
                chroma_types::FUNCTION_RECORD_COUNTER_NAME,
                chroma_types::FUNCTION_RECORD_COUNTER_ID,
            ),
            (
                chroma_types::FUNCTION_STATISTICS_NAME,
                chroma_types::FUNCTION_STATISTICS_ID,
            ),
            (
                chroma_types::FUNCTION_DUMMY_ASYNC_NAME,
                chroma_types::FUNCTION_DUMMY_ASYNC_ID,
            ),
            (
                chroma_types::FUNCTION_HTTP_GENERATE_NAME,
                chroma_types::FUNCTION_HTTP_GENERATE_ID,
            ),
            (
                chroma_types::FUNCTION_HTTP_CURRENTS_NAME,
                chroma_types::FUNCTION_HTTP_CURRENTS_ID,
            ),
            (
                chroma_types::FUNCTION_REVISION_HISTORY_NAME,
                chroma_types::FUNCTION_REVISION_HISTORY_ID,
            ),
            (
                chroma_types::FUNCTION_COUNT_TO_FILE_ASYNC_NAME,
                chroma_types::FUNCTION_COUNT_TO_FILE_ASYNC_ID,
            ),
        ]
        .into_iter()
        .map(|(name, id)| (name.to_string(), id))
        .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(functions, expected);
    }

    #[tokio::test]
    async fn deleting_the_output_collection_deletes_the_attached_functions_that_write_into_it() {
        let sysdb = SqliteSysDb::new(
            get_new_sqlite_db().await,
            "default".into(),
            "default".into(),
        );
        let input = function_collection(&sysdb, "input").await;
        let (id, _) = attach(&sysdb, "f", "record_counter", input, "out")
            .await
            .unwrap();
        finish(&sysdb, id).await;
        let output = sysdb
            .get_attached_functions(None, Some(input), vec![], true)
            .await
            .unwrap()[0]
            .output_collection_id
            .unwrap();
        sysdb
            .delete_collection(
                "default_tenant".into(),
                "default_database".into(),
                output,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(sysdb
            .get_attached_functions(None, Some(input), vec![], false)
            .await
            .unwrap()
            .is_empty());
    }

    // The calls of the work queue of the asynchronous functions answer as the coordinator of distributed Chroma.
    #[tokio::test]
    async fn an_asynchronous_function_finishes_fails_and_is_checked_as_the_coordinator_does() {
        let sysdb = SqliteSysDb::new(
            get_new_sqlite_db().await,
            "default".into(),
            "default".into(),
        );
        let input = function_collection(&sysdb, "input").await;
        let (id, _) = attach(&sysdb, "dummy", "dummy_async", input, "out")
            .await
            .unwrap();
        finish(&sysdb, id).await;
        let try_finish = |offset: u64| {
            sysdb.try_finish_async_attached_function_invocation(
                chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest {
                    attached_function_id: id.0.to_string(),
                    collection_id: input.to_string(),
                    new_completion_offset: offset,
                },
            )
        };
        use chroma_proto::try_finish_async_attached_function_invocation_response::Result as FinishResult;
        // The log position of a collection with no segment applied is 0: an offset at it is done.
        assert!(matches!(
            try_finish(0).await.unwrap().result,
            Some(FinishResult::Success(_))
        ));
        assert!(matches!(
            try_finish(5).await.unwrap().result,
            Some(FinishResult::Success(_))
        ));
        assert_eq!(
            try_finish(3).await.unwrap_err().message(),
            "completion offset cannot move backwards"
        );
        let input_str = input.to_string();
        let id_str = id.0.to_string();
        let fail = || sysdb.change_attached_function_failure_count(&id_str, &input_str, None);
        assert_eq!(fail().await.unwrap(), 1);
        assert_eq!(fail().await.unwrap(), 2);
        assert_eq!(
            sysdb
                .change_attached_function_failure_count(&id_str, &input_str, Some(7))
                .await
                .unwrap(),
            7
        );
        assert_eq!(
            sysdb
                .change_attached_function_failure_count(&id_str, &input_str, Some(-1))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        // A finish resets the failure count, as UpdateCompletionOffsetAndHeapEntry does.
        try_finish(6).await.unwrap();
        assert_eq!(fail().await.unwrap(), 1);
        sysdb
            .finalize_async_attached_function_repair(
                chroma_proto::FinalizeAsyncAttachedFunctionRepairRequest {
                    attached_function_id: id_str.clone(),
                    collection_id: input_str.clone(),
                },
            )
            .await
            .unwrap();
        let check = |offset: i64, function_id: String| chroma_proto::InvocationCheckItem {
            function_id,
            input_collection_id: input_str.clone(),
            completion_offset: offset,
        };
        let statuses = sysdb
            .check_invocation_status(chroma_proto::CheckInvocationStatusRequest {
                items: vec![
                    check(6, id_str.clone()),
                    check(5, id_str.clone()),
                    check(0, Uuid::new_v4().to_string()),
                ],
            })
            .await
            .unwrap()
            .results
            .into_iter()
            .map(|result| (result.status, result.current_completion_offset))
            .collect::<Vec<_>>();
        assert_eq!(
            statuses,
            vec![
                (chroma_proto::InvocationStatus::NotDone as i32, 6),
                (chroma_proto::InvocationStatus::Done as i32, 6),
                (chroma_proto::InvocationStatus::Done as i32, 0),
            ]
        );
        // A synchronous function is not finished through the work queue.
        let other = function_collection(&sysdb, "other").await;
        let (sync_id, _) = attach(&sysdb, "counter", "record_counter", other, "counts")
            .await
            .unwrap();
        finish(&sysdb, sync_id).await;
        let err = sysdb
            .try_finish_async_attached_function_invocation(
                chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest {
                    attached_function_id: sync_id.0.to_string(),
                    collection_id: other.to_string(),
                    new_completion_offset: 1,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.message(), "attached function is not async");
    }
}
