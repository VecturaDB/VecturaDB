// Changed from the original Chroma file; see the git history for the changes and their authors.
use super::config::LocalExecutorConfig;
use async_trait::async_trait;
use chroma_config::{registry::Registry, Configurable};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::ChromaError;
use chroma_log::{BackfillMessage, LocalCompactionManager, PurgeLogsMessage};
use chroma_segment::blockfile_record::{
    RecordSegmentReaderOptions, RecordSegmentReaderShard, RecordSegmentReaderShardCreationError,
};
use chroma_segment::distributed_spann::{SpannSegmentReaderShard, SpannSegmentReaderShardError};
use chroma_segment::{
    local_hnsw::LocalHnswSegmentReader, local_segment_manager::LocalSegmentManager,
    sqlite_metadata::SqliteMetadataReader,
};
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::ComponentHandle;
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, KnnBatchResult, KnnProjection, KnnProjectionOutput,
        KnnProjectionRecord, Limit, Merge, Projection, ProjectionRecord, RecordMeasure, Scan,
        SearchResult,
    },
    plan::{Count, Get, Knn, Search},
    CollectionAndSegments, ExecutorError, Segment, SegmentShard, SegmentType, SegmentUuid, Space,
};
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    future::Future,
};

#[derive(Clone, Debug)]
pub struct LocalExecutor {
    hnsw_manager: LocalSegmentManager,
    metadata_reader: SqliteMetadataReader,
    compactor_handle: ComponentHandle<LocalCompactionManager>,
    spann: bool,
    // The catalog, for the files of the last flush of a SPANN collection.
    sysdb: Option<SysDb>,
}

impl LocalExecutor {
    pub fn new(
        hnsw_manager: LocalSegmentManager,
        sqlite_db: SqliteDb,
        compactor_handle: ComponentHandle<LocalCompactionManager>,
    ) -> Self {
        Self {
            hnsw_manager,
            metadata_reader: SqliteMetadataReader::new(sqlite_db),
            compactor_handle,
            spann: false,
            sysdb: None,
        }
    }

    /// SPANN collections, when the option asks for them and the data path can hold their files.
    pub fn with_spann(mut self, spann: bool, sysdb: SysDb) -> Self {
        self.spann = spann && self.hnsw_manager.spann_provider().is_some();
        self.sysdb = Some(sysdb);
        self
    }

    // The segments of a SPANN collection with the files of its last flush: a cache of collections can hold the files
    // of an older one.
    async fn last_flush(
        &self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<CollectionAndSegments, ExecutorError> {
        let mut collection_and_segments = collection_and_segments.clone();
        if let Some(sysdb) = &self.sysdb {
            let stored = sysdb
                .clone()
                .get_collection_with_segments(
                    None,
                    collection_and_segments.collection.collection_id,
                )
                .await
                .map_err(|err| ExecutorError::Internal(err.boxed()))?;
            collection_and_segments.record_segment.file_path = stored.record_segment.file_path;
            collection_and_segments.vector_segment.file_path = stored.vector_segment.file_path;
        }
        Ok(collection_and_segments)
    }

    pub fn get_supported_segment_types(&self) -> Vec<SegmentType> {
        let mut types = vec![
            SegmentType::HnswLocalMemory,
            SegmentType::HnswLocalPersisted,
            SegmentType::Sqlite,
        ];
        if self.spann {
            types.extend([SegmentType::Spann, SegmentType::BlockfileRecord]);
        }
        types
    }
    pub async fn validate_index(
        &self,
        collection: &CollectionAndSegments,
    ) -> Result<(), Box<dyn ChromaError>> {
        if let Some(dim) = collection.collection.dimension {
            self.hnsw_manager
                .get_hnsw_writer(
                    &collection.collection,
                    &collection.vector_segment,
                    dim as usize,
                )
                .await
                .map_err(|err| err.boxed())?;
        }
        Ok(())
    }

    pub async fn delete_segments(
        &mut self,
        segments: &[Segment],
    ) -> Result<(), Box<dyn ChromaError>> {
        let (removed, failure) = self.remove_segment_files(segments).await;
        self.forget_deleted_indexes(&removed).await?;
        failure.map_or(Ok(()), Err)
    }
    /// The index files of deleted segments, removed. Their rows in index_cleanup wait for the sweep of
    /// cleanup_deleted_indexes, or go at once for indexes in memory, which no sweep visits.
    pub async fn delete_segment_files(
        &mut self,
        segments: &[Segment],
    ) -> Result<(), Box<dyn ChromaError>> {
        let (removed, failure) = self.remove_segment_files(segments).await;
        if !self.hnsw_manager.persists() {
            self.forget_deleted_indexes(&removed).await?;
        }
        failure.map_or(Ok(()), Err)
    }

    // Every segment is tried, so one that fails leaves the others removed.
    async fn remove_segment_files(
        &mut self,
        segments: &[Segment],
    ) -> (Vec<SegmentUuid>, Option<Box<dyn ChromaError>>) {
        let mut removed = Vec::new();
        let mut failure = None;
        for segment in segments {
            if !matches!(
                segment.r#type,
                SegmentType::HnswLocalMemory | SegmentType::HnswLocalPersisted
            ) {
                continue;
            }
            match self.hnsw_manager.delete_hnsw_index_files(segment.id).await {
                Ok(true) => removed.push(segment.id),
                Ok(false) => {}
                Err(err) => {
                    failure.get_or_insert(err.boxed());
                }
            }
        }
        (removed, failure)
    }

    pub async fn forget_deleted_indexes(
        &self,
        segments: &[SegmentUuid],
    ) -> Result<(), Box<dyn ChromaError>> {
        self.hnsw_manager
            .forget_deleted_hnsw_indexes(segments)
            .await
            .map_err(|err| err.boxed())
    }

    pub async fn cleanup_deleted_indexes(&self) -> Result<(), Box<dyn ChromaError>> {
        self.hnsw_manager
            .cleanup_deleted_indexes()
            .await
            .map_err(|err| err.boxed())
    }
}

impl LocalExecutor {
    pub async fn count<F, Fut>(&mut self, plan: Count, _: F) -> Result<CountResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Count, Box<dyn ChromaError>>>,
    {
        self.try_backfill_collection(&plan.scan.collection_and_segments)
            .await?;
        self.metadata_reader
            .count(plan)
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))
    }

    // Pin the index through replay and the read. A collection-level "backfilled"
    // flag outlives evicted indexes and cannot establish that this instance has
    // applied the unpersisted tail of the log.
    pub async fn try_backfill_collection(
        &mut self,
        collection_and_segment: &CollectionAndSegments,
    ) -> Result<Option<LocalHnswSegmentReader>, ExecutorError> {
        // Capture pinning and replay failures so both paths still attempt purge.
        let spann = collection_and_segment.vector_segment.r#type == SegmentType::Spann;
        let backfill_result = async {
            let reader = if spann {
                None
            } else if let Some(dim) = collection_and_segment.collection.dimension {
                let writer = self
                    .hnsw_manager
                    .get_hnsw_writer(
                        &collection_and_segment.collection,
                        &collection_and_segment.vector_segment,
                        dim as usize,
                    )
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
                Some(LocalHnswSegmentReader::from_index(writer.index))
            } else {
                None
            };
            let replay_complete = match &reader {
                Some(reader) => reader.index.replay_complete().await,
                None => false,
            };
            if !replay_complete {
                let backfill_msg = BackfillMessage {
                    collection_id: collection_and_segment.collection.collection_id,
                };
                self.compactor_handle
                    .request(backfill_msg, None)
                    .await
                    .map_err(|err| ExecutorError::BackfillError(Box::new(err)))?
                    .map_err(|err| ExecutorError::BackfillError(Box::new(err)))?;
            }
            Ok::<_, ExecutorError>(reader)
        }
        .await;
        // Even a fully replayed instance may have a failed purge to retry. The
        // compactor checks for eligible rows before inspecting the checkpoint.
        let purge_log_msg = PurgeLogsMessage {
            collection_id: collection_and_segment.collection.collection_id,
        };
        let purge_result = self
            .compactor_handle
            .request(purge_log_msg, None)
            .await
            .map_err(|err| ExecutorError::BackfillError(Box::new(err)))
            .and_then(|result| result.map_err(|err| ExecutorError::BackfillError(Box::new(err))));
        // Preserve purge failures even when the backfill error takes precedence.
        if let Err(err) = &purge_result {
            tracing::error!(
                collection_id = %collection_and_segment.collection.collection_id,
                error = %err,
                "Failed to purge logs after backfill attempt"
            );
        }
        let reader = backfill_result?;
        purge_result?;
        Ok(reader)
    }

    pub async fn get<F, Fut>(&mut self, plan: Get, _: F) -> Result<GetResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Get, Box<dyn ChromaError>>>,
    {
        let collection_and_segments = plan.scan.collection_and_segments.clone();
        let hnsw_reader = self
            .try_backfill_collection(&collection_and_segments)
            .await?;
        let load_embedding = plan.proj.embedding;
        let mut result = self
            .metadata_reader
            .get(plan)
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
        if load_embedding && collection_and_segments.vector_segment.r#type == SegmentType::Spann {
            let flushed = self.last_flush(&collection_and_segments).await?;
            if let Some(records) = Box::pin(self.spann_records(&flushed)).await? {
                for record in &mut result.result.records {
                    let offset_id = records
                        .get_offset_id_for_user_id(
                            &record.id,
                            &RecordSegmentReaderOptions::default(),
                        )
                        .await
                        .map_err(|err| ExecutorError::Internal(err.boxed()))?;
                    if let Some(offset_id) = offset_id {
                        record.embedding = records
                            .get_data_for_offset_id(offset_id)
                            .await
                            .map_err(|err| ExecutorError::Internal(err.boxed()))?
                            .map(|data| data.embedding.to_vec());
                    }
                }
            }
        } else if load_embedding {
            if let Some(hnsw_reader) = hnsw_reader {
                for record in &mut result.result.records {
                    record.embedding = Some(
                        hnsw_reader
                            .get_embedding_by_user_id(&record.id)
                            .await
                            .map_err(|err| ExecutorError::Internal(Box::new(err)))?,
                    );
                }
            }
        }
        Ok(result)
    }

    pub async fn knn<F, Fut>(&mut self, plan: Knn, _: F) -> Result<KnnBatchResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Knn, Box<dyn ChromaError>>>,
    {
        let collection_and_segments = plan.scan.collection_and_segments.clone();
        let hnsw_reader = self
            .try_backfill_collection(&collection_and_segments)
            .await?;

        let empty_result = Ok(KnnBatchResult {
            pulled_log_bytes: 0,
            results: vec![Default::default(); plan.knn.embeddings.len()],
        });

        let spann = collection_and_segments.vector_segment.r#type == SegmentType::Spann;
        if !spann && hnsw_reader.is_none() {
            return empty_result;
        }

        let allowed_user_ids = match plan.filter {
            Filter {
                query_ids: None,
                where_clause: None,
            } => Vec::new(),
            Filter {
                query_ids: Some(uids),
                where_clause: _,
            } if uids.is_empty() => return empty_result,
            Filter {
                query_ids: Some(uids),
                where_clause: None,
            } => uids,
            filter => {
                let filter_plan = Get {
                    scan: plan.scan.clone(),
                    filter: filter.clone(),
                    limit: Limit {
                        offset: 0,
                        limit: None,
                    },
                    proj: Default::default(),
                };

                let replan = {
                    let plan = filter_plan.clone();
                    move |_: tonic::Code| {
                        let plan = plan.clone();
                        async move { Ok(plan) }
                    }
                };
                let allowed_uids = self
                    .get(filter_plan, replan)
                    .await?
                    .result
                    .records
                    .into_iter()
                    .map(|record| record.id)
                    .collect::<Vec<_>>();

                if allowed_uids.is_empty() {
                    return empty_result;
                }

                allowed_uids
            }
        };

        let Some(hnsw_reader) = hnsw_reader else {
            let (results, returned_user_ids) = Box::pin(self.spann_query(
                &collection_and_segments,
                plan.knn.embeddings,
                plan.knn.fetch,
                &plan.proj,
                allowed_user_ids,
            ))
            .await?;
            return Box::pin(self.hydrate_knn(plan.scan, plan.proj, results, returned_user_ids))
                .await;
        };

        let mut allowed_offset_ids = Vec::new();
        for user_id in allowed_user_ids {
            let offset_id = hnsw_reader
                .get_offset_id_by_user_id(&user_id)
                .await
                .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
            allowed_offset_ids.push(offset_id);
        }

        let hnsw_config = collection_and_segments
            .collection
            .schema
            .as_ref()
            .map(|schema| {
                schema.get_internal_hnsw_config_with_legacy_fallback(
                    &plan.scan.collection_and_segments.vector_segment,
                )
            })
            .transpose()
            .map_err(|err| ExecutorError::Internal(Box::new(err)))?
            .flatten()
            .ok_or(ExecutorError::CollectionMissingHnswConfiguration)?;

        let distance_function = hnsw_config.space;

        let mut results = Vec::new();
        let mut returned_user_ids = Vec::new();
        for embedding in plan.knn.embeddings {
            let query_embedding = if let Space::Cosine = distance_function {
                normalize(&embedding)
            } else {
                embedding
            };
            let distances = hnsw_reader
                .query_embedding(
                    allowed_offset_ids.as_slice(),
                    query_embedding,
                    plan.knn.fetch,
                )
                .await
                .map_err(|err| ExecutorError::Internal(Box::new(err)))?;

            let mut records = Vec::new();
            for RecordMeasure { offset_id, measure } in distances {
                let user_id = hnsw_reader
                    .get_user_id_by_offset_id(offset_id)
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
                returned_user_ids.push(user_id.clone());
                let knn_projection = KnnProjectionRecord {
                    record: ProjectionRecord {
                        id: user_id,
                        document: None,
                        embedding: plan.proj.projection.embedding.then_some(
                            hnsw_reader
                                .get_embedding_by_offset_id(offset_id)
                                .await
                                .map_err(|err| ExecutorError::Internal(Box::new(err)))?,
                        ),
                        metadata: None,
                    },
                    distance: plan.proj.distance.then_some(measure),
                };
                records.push(knn_projection);
            }

            results.push(KnnProjectionOutput { records });
        }

        Box::pin(self.hydrate_knn(plan.scan, plan.proj, results, returned_user_ids)).await
    }

    // The documents and metadata of the nearest records, from SQLite.
    async fn hydrate_knn(
        &mut self,
        scan: Scan,
        proj: KnnProjection,
        mut results: Vec<KnnProjectionOutput>,
        returned_user_ids: Vec<String>,
    ) -> Result<KnnBatchResult, ExecutorError> {
        let plan_proj = proj;
        if plan_proj.projection.document || plan_proj.projection.metadata {
            let projection_plan = Get {
                scan,
                filter: Filter {
                    query_ids: Some(returned_user_ids),
                    where_clause: None,
                },
                limit: Limit {
                    offset: 0,
                    limit: None,
                },
                proj: Projection {
                    document: plan_proj.projection.document,
                    embedding: false,
                    metadata: plan_proj.projection.metadata,
                },
            };

            let replan = {
                let plan = projection_plan.clone();
                move |_: tonic::Code| {
                    let plan = plan.clone();
                    async move { Ok(plan) }
                }
            };
            let hydrated_records = self.get(projection_plan, replan).await?;
            let mut user_id_to_document = HashMap::new();
            let mut user_id_to_metadata = HashMap::new();
            for ProjectionRecord {
                id,
                document,
                embedding: _,
                metadata,
            } in hydrated_records.result.records
            {
                user_id_to_document.insert(id.clone(), document);
                user_id_to_metadata.insert(id, metadata);
            }

            for result in &mut results {
                for record in &mut result.records {
                    record.record.document = user_id_to_document
                        .get(&record.record.id)
                        .cloned()
                        .flatten();
                    record.record.metadata = user_id_to_metadata
                        .get(&record.record.id)
                        .cloned()
                        .flatten();
                }
            }
        }

        Ok(KnnBatchResult {
            pulled_log_bytes: 0,
            results,
        })
    }

    // The record segment of a SPANN collection, from its files; none before its first flush.
    async fn spann_records(
        &self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<Option<RecordSegmentReaderShard<'_>>, ExecutorError> {
        let Some(provider) = self.hnsw_manager.spann_provider() else {
            return Ok(None);
        };
        let shard = SegmentShard::try_from((&collection_and_segments.record_segment, 0))
            .map_err(|err| ExecutorError::Internal(err.boxed()))?;
        match RecordSegmentReaderShard::from_segment(&shard, &provider.blockfile_provider, None)
            .await
        {
            Ok(records) => Ok(Some(records)),
            Err(err)
                if matches!(
                    *err,
                    RecordSegmentReaderShardCreationError::UninitializedSegment
                ) =>
            {
                Ok(None)
            }
            Err(err) => Err(ExecutorError::Internal(err.boxed())),
        }
    }

    // The KNN of distributed Chroma (rust/worker/src/execution/orchestration/spann_knn.rs) on the files of a SPANN
    // collection: the nearest centers of each query, their posting lists in brute force under the filter, and the merge
    // of the k nearest. An empty filter allows every record, as in the HNSW path.
    async fn spann_query(
        &self,
        collection_and_segments: &CollectionAndSegments,
        embeddings: Vec<Vec<f32>>,
        k: u32,
        proj: &KnnProjection,
        allowed_user_ids: Vec<String>,
    ) -> Result<(Vec<KnnProjectionOutput>, Vec<String>), ExecutorError> {
        let internal = |err: Box<dyn ChromaError>| ExecutorError::Internal(err);
        let empty = (vec![Default::default(); embeddings.len()], Vec::new());
        let collection_and_segments = &self.last_flush(collection_and_segments).await?;
        let (Some(provider), Some(dimension)) = (
            self.hnsw_manager.spann_provider(),
            collection_and_segments.collection.dimension,
        ) else {
            return Ok(empty);
        };
        let Some(records) = self.spann_records(collection_and_segments).await? else {
            return Ok(empty);
        };
        let shard = SegmentShard::try_from((&collection_and_segments.vector_segment, 0))
            .map_err(|err| internal(err.boxed()))?;
        let reader = match SpannSegmentReaderShard::from_segment(
            &collection_and_segments.collection,
            &shard,
            &provider.blockfile_provider,
            &provider.hnsw_provider,
            dimension as usize,
            provider.adaptive_search_nprobe,
        )
        .await
        {
            Ok(reader) => reader,
            Err(SpannSegmentReaderShardError::UninitializedSegment) => return Ok(empty),
            Err(err) => return Err(internal(err.boxed())),
        };
        let allowed = if allowed_user_ids.is_empty() {
            None
        } else {
            let mut allowed = HashSet::new();
            for user_id in &allowed_user_ids {
                if let Some(offset_id) = records
                    .get_offset_id_for_user_id(user_id, &RecordSegmentReaderOptions::default())
                    .await
                    .map_err(|err| internal(err.boxed()))?
                {
                    allowed.insert(offset_id);
                }
            }
            Some(allowed)
        };
        let space = collection_and_segments
            .collection
            .schema
            .as_ref()
            .and_then(|schema| schema.get_internal_spann_config())
            .map(|config| config.space)
            .ok_or(ExecutorError::CollectionMissingHnswConfiguration)?;
        let distance_function = DistanceFunction::from(space);
        // An upper bound of the records, for the adaptive number of probes; the SQLite catalog keeps no count.
        let collection_size = records.get_max_offset_id() as usize;
        let mut results = Vec::new();
        let mut returned_user_ids = Vec::new();
        for embedding in embeddings {
            let query = if distance_function == DistanceFunction::Cosine {
                normalize(&embedding)
            } else {
                embedding
            };
            let (centers, _, _) = reader
                .rng_query(&query, collection_size, k as usize)
                .await
                .map_err(|err| internal(err.boxed()))?;
            let mut batches = Vec::with_capacity(centers.len());
            for center in centers {
                let postings = reader
                    .fetch_posting_list(center as u32)
                    .await
                    .map_err(|err| internal(err.boxed()))?;
                let mut nearest = BinaryHeap::with_capacity(k as usize);
                for posting in postings {
                    if allowed
                        .as_ref()
                        .is_some_and(|allowed| !allowed.contains(&posting.doc_offset_id))
                    {
                        continue;
                    }
                    let record = RecordMeasure {
                        offset_id: posting.doc_offset_id,
                        measure: distance_function.distance(&posting.doc_embedding, &query),
                    };
                    if nearest.len() < k as usize {
                        nearest.push(record);
                    } else if nearest.peek().is_some_and(|furthest| &record < furthest) {
                        nearest.pop();
                        nearest.push(record);
                    }
                }
                // Merge takes the largest first, so the nearest go in reversed, as in the KnnMerge of the worker.
                batches.push(
                    nearest
                        .into_sorted_vec()
                        .into_iter()
                        .map(std::cmp::Reverse)
                        .collect::<Vec<_>>(),
                );
            }
            let mut output = Vec::new();
            for std::cmp::Reverse(RecordMeasure { offset_id, measure }) in
                (Merge { k }).merge(batches)
            {
                let user_id = records
                    .get_user_id_for_offset_id(offset_id)
                    .await
                    .map_err(|err| internal(err.boxed()))?
                    .to_string();
                let embedding = if proj.projection.embedding {
                    records
                        .get_data_for_offset_id(offset_id)
                        .await
                        .map_err(|err| internal(err.boxed()))?
                        .map(|data| data.embedding.to_vec())
                } else {
                    None
                };
                returned_user_ids.push(user_id.clone());
                output.push(KnnProjectionRecord {
                    record: ProjectionRecord {
                        id: user_id,
                        document: None,
                        embedding,
                        metadata: None,
                    },
                    distance: proj.distance.then_some(measure),
                });
            }
            results.push(KnnProjectionOutput { records: output });
        }
        Ok((results, returned_user_ids))
    }

    pub async fn search<F, Fut>(
        &mut self,
        plan: Search,
        _: F,
    ) -> Result<SearchResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Search, Box<dyn ChromaError>>>,
    {
        // Every read level reads the same records: the local writes reach the indexes before they are acknowledged,
        // and the replay below applies any write a previous process left in the log.
        let hnsw_reader = self
            .try_backfill_collection(&plan.scan.collection_and_segments)
            .await?;
        // As distributed Chroma (rust/worker/src/server.rs:697-701): an uninitialized collection has no results.
        if plan.scan.collection_and_segments.is_uninitialized() {
            return Ok(SearchResult {
                results: vec![Default::default(); plan.payloads.len()],
                pulled_log_bytes: 0,
            });
        }
        self.metadata_reader
            .search(
                plan.scan.collection_and_segments,
                hnsw_reader.as_ref(),
                plan.payloads,
            )
            .await
            .map_err(|error| ExecutorError::Internal(Box::new(error)))
    }

    pub async fn reset(&mut self) -> Result<(), Box<dyn ChromaError>> {
        self.hnsw_manager.reset().await.map_err(|err| err.boxed())?;
        Ok(())
    }
}

#[async_trait]
impl Configurable<LocalExecutorConfig> for LocalExecutor {
    async fn try_from_config(
        config: &LocalExecutorConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let hnsw_manager = registry
            .get::<LocalSegmentManager>()
            .map_err(|err| err.boxed())?;
        let sqlite_db = registry.get::<SqliteDb>().map_err(|err| err.boxed())?;
        let compactor_handle = registry
            .get::<ComponentHandle<LocalCompactionManager>>()
            .map_err(|err| err.boxed())?;
        let sysdb = registry.get::<SysDb>().map_err(|err| err.boxed())?;
        Ok(Self::new(hnsw_manager, sqlite_db, compactor_handle.clone())
            .with_spann(config.spann, sysdb))
    }
}

// The BM25 setting of a sparse key, as the search reads it.
#[cfg(test)]
use chroma_segment::sqlite_metadata::sparse_bm25_enabled;

#[cfg(test)]
#[path = "local_backfill_tests.rs"]
mod backfill_tests;

#[cfg(test)]
mod tests {
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use chroma_system::System;
    use chroma_types::{
        AddCollectionRecordsRequest, CreateCollectionRequest, DatabaseName, IncludeList,
        QueryRequest,
    };

    use crate::{Frontend, FrontendConfig};

    #[test]
    fn sparse_bm25_key_override_and_default_inheritance() {
        use chroma_types::{
            KnnIndex, Schema, SparseIndexAlgorithm, SparseVectorIndexConfig, SparseVectorIndexType,
            SparseVectorValueType,
        };
        let value = |flag| SparseVectorValueType {
            sparse_vector_index: Some(SparseVectorIndexType {
                enabled: true,
                config: SparseVectorIndexConfig {
                    bm25: flag,
                    embedding_function: None,
                    source_key: None,
                    algorithm: SparseIndexAlgorithm::Wand,
                },
            }),
        };
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        assert!(!super::sparse_bm25_enabled(&schema, "sparse"));
        schema.defaults.sparse_vector = Some(value(Some(true)));
        assert!(super::sparse_bm25_enabled(&schema, "sparse"));
        schema
            .keys
            .entry("sparse".into())
            .or_default()
            .sparse_vector = Some(value(None));
        assert!(super::sparse_bm25_enabled(&schema, "sparse"));
        schema.keys.get_mut("sparse").unwrap().sparse_vector = Some(value(Some(false)));
        assert!(!super::sparse_bm25_enabled(&schema, "sparse"));
        schema.defaults.sparse_vector = Some(value(Some(false)));
        schema.keys.get_mut("sparse").unwrap().sparse_vector = Some(value(Some(true)));
        assert!(super::sparse_bm25_enabled(&schema, "sparse"));
    }

    #[tokio::test]
    async fn test_query() {
        let registry = Registry::new();
        let system = System::new();

        let config_and_system = (FrontendConfig::sqlite_in_memory(), system);
        let mut frontend = Frontend::try_from_config(&config_and_system, &registry)
            .await
            .unwrap();

        // Add data
        let database_name =
            DatabaseName::new("default_database").expect("database name should be valid");
        let collection = frontend
            .create_collection(
                CreateCollectionRequest::try_new(
                    "default_tenant".to_string(),
                    database_name,
                    "test".to_string(),
                    None,
                    None,
                    None,
                    false,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        frontend
            .add(
                AddCollectionRecordsRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    collection.collection_id,
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![-1.0, -1.0], vec![1.0, 1.0]],
                    None,
                    None,
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Knn should work
        let result = Box::pin(
            frontend.query(
                QueryRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    collection.collection_id,
                    None,
                    None,
                    vec![vec![0.5, 0.5]],
                    10,
                    IncludeList::default_query(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.ids[0], vec!["id2".to_string(), "id1".to_string()]);

        // An empty list of IDs should return no results
        let result = Box::pin(
            frontend.query(
                QueryRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    collection.collection_id,
                    Some(vec![]),
                    None,
                    vec![vec![0.5, 0.5]],
                    10,
                    IncludeList::default_query(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.ids[0].len(), 0);
    }
}
