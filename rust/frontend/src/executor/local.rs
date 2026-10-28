// Changed from the original Chroma file; see the git history for the changes and their authors.
use super::config::{LocalExecutorConfig, RetryConfig};
use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use chroma_config::{registry::Registry, Configurable};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::{BackfillMessage, LocalCompactionManager, PurgeLogsMessage};
use chroma_segment::blockfile_record::{
    RecordSegmentReaderOptions, RecordSegmentReaderShard, RecordSegmentReaderShardCreationError,
};
use chroma_segment::distributed_spann::{SpannSegmentReaderShard, SpannSegmentReaderShardError};
use chroma_segment::spann_provider::SpannProvider;
use chroma_segment::{
    local_hnsw::LocalHnswSegmentReader,
    local_segment_manager::LocalSegmentManager,
    sqlite_metadata::{
        DenseVectorReader, MetadataSnapshot, SqliteMetadataError, SqliteMetadataReader,
    },
};
use chroma_sqlite::db::SqliteDb;
use chroma_system::ComponentHandle;
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, KnnBatchResult, KnnProjection, KnnProjectionOutput,
        KnnProjectionRecord, Limit, Merge, Projection, ProjectionRecord, RecordMeasure,
        SearchResult,
    },
    plan::{Count, Get, Knn, Search},
    CollectionAndSegments, ExecutorError, Segment, SegmentShard, SegmentType, SegmentUuid, Space,
};
use futures::future::try_join_all;
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
    retry: RetryConfig,
    spann_warm_up: SpannWarmUp,
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
            retry: RetryConfig::default(),
            spann_warm_up: SpannWarmUp::default(),
        }
    }

    /// SPANN collections, when the option asks for them and the data path can hold their files. Each flush of a SPANN
    /// collection read since its previous one then opens the new version of its record segment: see SpannWarmUp.
    pub fn with_spann(mut self, spann: bool) -> Self {
        self.spann = spann && self.hnsw_manager.spann_provider().is_some();
        if let Some(provider) = self
            .hnsw_manager
            .spann_provider()
            .filter(|_| self.spann)
            .cloned()
        {
            let warm_up = self.spann_warm_up.clone();
            self.hnsw_manager
                .on_spann_flushed(move |collection_id, record_segment| {
                    warm_up.flushed(collection_id, record_segment, provider.clone())
                });
        }
        self
    }

    /// How a read of a SPANN collection reads again, from the config, as the retry of the distributed executor.
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
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
                SegmentType::HnswLocalMemory
                    | SegmentType::HnswLocalPersisted
                    | SegmentType::Spann
                    | SegmentType::BlockfileRecord
            ) {
                continue;
            }
            if segment.r#type == SegmentType::Spann {
                self.hnsw_manager
                    .set_spann_applied(segment.collection, false);
                self.spann_warm_up.forget(segment.collection);
            }
            match self.hnsw_manager.delete_hnsw_index_files(segment.id).await {
                Ok(true) => removed.push(segment.id),
                Ok(false) => {}
                Err(err) => {
                    failure.get_or_insert(err.boxed());
                }
            }
            // Only a segment of a SPANN collection has blockfiles to look for.
            if matches!(
                segment.r#type,
                SegmentType::Spann | SegmentType::BlockfileRecord
            ) {
                if let Err(err) = self
                    .hnsw_manager
                    .delete_blockfile_segment_files(segment.id)
                    .await
                {
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
    pub async fn count<F, Fut>(
        &mut self,
        plan: Count,
        replan: F,
    ) -> Result<CountResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Count, Box<dyn ChromaError>>>,
    {
        if plan.scan.collection_and_segments.vector_segment.r#type == SegmentType::Spann {
            // A read that fails with an error that distributed Chroma retries reads again, as a get does.
            let mut delays = spann_backoff(&self.retry);
            let mut plan = plan;
            loop {
                match Box::pin(self.spann_count(plan.clone())).await {
                    Err(err) => plan = spann_replan(err, &mut delays, &replan).await?,
                    result => return result,
                }
            }
        }
        self.try_backfill_collection(&plan.scan.collection_and_segments)
            .await?;
        self.metadata_reader
            .count(plan)
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))
    }

    // A count of a SPANN collection as distributed Chroma counts it (rust/worker/src/execution/operators/count_records.rs):
    // the records of the record segment, at the files of the last flush that a snapshot of SQLite gives before it closes.
    async fn spann_count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        let (snapshot, collection_and_segments) = self
            .spann_snapshot(&plan.scan.collection_and_segments)
            .await?;
        snapshot
            .close()
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
        let count = match Box::pin(self.spann_records(&collection_and_segments)).await? {
            Some(records) => records.count().await.map_err(ExecutorError::Internal)?,
            None => 0,
        };
        Ok(CountResult {
            count: count as u32,
            pulled_log_bytes: 0,
        })
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

    pub async fn get<F, Fut>(&mut self, plan: Get, replan: F) -> Result<GetResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Get, Box<dyn ChromaError>>>,
    {
        if plan.scan.collection_and_segments.vector_segment.r#type == SegmentType::Spann {
            // As the executor of distributed Chroma (distributed.rs): a read that fails with an error it retries, as the
            // NotFound of the files of its flush that later flushes removed, reads again from the plan that the frontend
            // gives back.
            let mut delays = spann_backoff(&self.retry);
            let mut plan = plan;
            loop {
                match Box::pin(self.spann_get(plan.clone())).await {
                    Err(err) => plan = spann_replan(err, &mut delays, &replan).await?,
                    result => return result,
                }
            }
        }
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
        if load_embedding {
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

    // One read of a SPANN collection: its log applied first, then a snapshot of SQLite, which reads the files of the
    // last flush with the records at one moment, as a read of distributed Chroma reads one version of every segment.
    // The files of a flush never change, and those of a replaced flush stay at least a grace: a get, a query and a count
    // close the snapshot before they read the files, so that writers commit meanwhile. A search keeps it through its
    // files, since it numbers the records by their rows of SQLite.
    async fn spann_snapshot(
        &mut self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<(MetadataSnapshot, CollectionAndSegments), ExecutorError> {
        self.spann_warm_up
            .read(collection_and_segments.collection.collection_id);
        self.spann_replay(collection_and_segments).await?;
        self.snapshot_files(collection_and_segments).await
    }

    // A snapshot of SQLite, with the segments of a SPANN collection at the files of the last flush it reads.
    async fn snapshot_files(
        &self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<(MetadataSnapshot, CollectionAndSegments), ExecutorError> {
        let internal = |err: SqliteMetadataError| ExecutorError::Internal(Box::new(err));
        let mut snapshot = self.metadata_reader.snapshot().await.map_err(internal)?;
        let mut files = snapshot
            .segment_files(collection_and_segments.collection.collection_id)
            .await
            .map_err(internal)?;
        let mut collection_and_segments = collection_and_segments.clone();
        collection_and_segments.record_segment.file_path = files
            .remove(&collection_and_segments.record_segment.id.to_string())
            .unwrap_or_default();
        collection_and_segments.vector_segment.file_path = files
            .remove(&collection_and_segments.vector_segment.id.to_string())
            .unwrap_or_default();
        Ok((snapshot, collection_and_segments))
    }

    /// Rejects a write to a SPANN collection whose files of the last flush cannot be read before it reaches the log, as
    /// validate_index does for a local HNSW index: the compaction would fail on those files with the write in the log
    /// and its records in SQLite. A collection never written has no files, and passes.
    pub async fn validate_spann_index(
        &self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<(), Box<dyn ChromaError>> {
        let (_, collection_and_segments) = self
            .snapshot_files(collection_and_segments)
            .await
            .map_err(|err| err.boxed())?;
        self.spann_vectors(&collection_and_segments)
            .await
            .map_err(|err| err.boxed())?;
        Ok(())
    }

    // The log of a SPANN collection replayed, unless the compaction has applied all of it: a read of an HNSW index
    // replays nothing once its replay is complete, the same.
    async fn spann_replay(
        &mut self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<(), ExecutorError> {
        if !self
            .hnsw_manager
            .spann_applied(collection_and_segments.collection.collection_id)
        {
            self.try_backfill_collection(collection_and_segments)
                .await?;
        }
        Ok(())
    }

    // A get of a SPANN collection as distributed Chroma runs it (rust/worker/src/execution/orchestration/get.rs): the filter
    // and the limit, then the projection from the record segment. The filter and the limit run on SQLite, the metadata
    // segment of the single node, in a snapshot that gives the files of the same flush and closes before them.
    async fn spann_get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let internal = |err: SqliteMetadataError| ExecutorError::Internal(Box::new(err));
        let (mut snapshot, collection_and_segments) = self
            .spann_snapshot(&plan.scan.collection_and_segments)
            .await?;
        let projection = plan.proj.clone();
        let filter_limit = Get {
            proj: Projection {
                document: false,
                embedding: false,
                metadata: false,
            },
            ..plan
        };
        let mut result = self
            .metadata_reader
            .get_at(&mut snapshot, filter_limit)
            .await
            .map_err(internal)?;
        snapshot.close().await.map_err(internal)?;
        if result.result.records.is_empty()
            || !(projection.document || projection.embedding || projection.metadata)
        {
            return Ok(result);
        }
        let records = Box::pin(self.spann_records(&collection_and_segments))
            .await?
            .ok_or(ExecutorError::InconsistentData)?;
        // The offsets of the records, one by one, as the filter of distributed Chroma takes those of its user ids
        // (rust/worker/src/execution/operators/filter.rs).
        let mut offset_ids = Vec::with_capacity(result.result.records.len());
        for record in &result.result.records {
            offset_ids.push(
                records
                    .get_offset_id_for_user_id(&record.id, &RecordSegmentReaderOptions::default())
                    .await
                    .map_err(|err| ExecutorError::Internal(err.boxed()))?
                    .ok_or(ExecutorError::InconsistentData)?,
            );
        }
        result.result.records = spann_projection(&records, &offset_ids, &projection).await?;
        Ok(result)
    }

    pub async fn knn<F, Fut>(
        &mut self,
        plan: Knn,
        replan: F,
    ) -> Result<KnnBatchResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Knn, Box<dyn ChromaError>>>,
    {
        if plan.scan.collection_and_segments.vector_segment.r#type == SegmentType::Spann {
            // A read that fails with an error that distributed Chroma retries reads again, as a get does.
            let mut delays = spann_backoff(&self.retry);
            let mut plan = plan;
            loop {
                match Box::pin(self.spann_knn(plan.clone())).await {
                    Err(err) => plan = spann_replan(err, &mut delays, &replan).await?,
                    result => return result,
                }
            }
        }
        let collection_and_segments = plan.scan.collection_and_segments.clone();
        let hnsw_reader = self
            .try_backfill_collection(&collection_and_segments)
            .await?;

        let empty_result = Ok(KnnBatchResult {
            pulled_log_bytes: 0,
            results: vec![Default::default(); plan.knn.embeddings.len()],
        });

        let Some(hnsw_reader) = hnsw_reader else {
            return empty_result;
        };

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

        if plan.proj.projection.document || plan.proj.projection.metadata {
            let projection_plan = Get {
                scan: plan.scan,
                filter: Filter {
                    query_ids: Some(returned_user_ids),
                    where_clause: None,
                },
                limit: Limit {
                    offset: 0,
                    limit: None,
                },
                proj: Projection {
                    document: plan.proj.projection.document,
                    embedding: false,
                    metadata: plan.proj.projection.metadata,
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

    // A KNN of a SPANN collection: its filter at the moment of a snapshot of SQLite, which closes before the files, then
    // its nearest records with their documents and metadata from the files of that moment.
    async fn spann_knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        let internal = |err: SqliteMetadataError| ExecutorError::Internal(Box::new(err));
        let empty_result = Ok(KnnBatchResult {
            pulled_log_bytes: 0,
            results: vec![Default::default(); plan.knn.embeddings.len()],
        });
        let (mut snapshot, collection_and_segments) = self
            .spann_snapshot(&plan.scan.collection_and_segments)
            .await?;
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
                    filter,
                    limit: Limit {
                        offset: 0,
                        limit: None,
                    },
                    proj: Default::default(),
                };
                let allowed_uids = self
                    .metadata_reader
                    .get_at(&mut snapshot, filter_plan)
                    .await
                    .map_err(internal)?
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
        snapshot.close().await.map_err(internal)?;
        let results = Box::pin(self.spann_query(
            &collection_and_segments,
            plan.knn.embeddings,
            plan.knn.fetch,
            &plan.proj,
            allowed_user_ids,
        ))
        .await?;
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
        #[cfg(test)]
        stop_before_files(collection_and_segments.collection.collection_id).await;
        #[cfg(test)]
        fail_before_files(collection_and_segments.collection.collection_id)?;
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

    // The SPANN index and the record segment of a SPANN collection, at the files of its last flush; none before its
    // first flush.
    async fn spann_vectors(
        &self,
        collection_and_segments: &CollectionAndSegments,
    ) -> Result<Option<SpannVectors<'_>>, ExecutorError> {
        let internal = |err: Box<dyn ChromaError>| ExecutorError::Internal(err);
        let (Some(provider), Some(dimension)) = (
            self.hnsw_manager.spann_provider(),
            collection_and_segments.collection.dimension,
        ) else {
            return Ok(None);
        };
        let Some(records) = self.spann_records(collection_and_segments).await? else {
            return Ok(None);
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
            Err(SpannSegmentReaderShardError::UninitializedSegment) => return Ok(None),
            Err(err) => return Err(internal(err.boxed())),
        };
        let space = collection_and_segments
            .collection
            .schema
            .as_ref()
            .and_then(|schema| schema.get_internal_spann_config())
            .map(|config| config.space)
            .ok_or(ExecutorError::CollectionMissingHnswConfiguration)?;
        Ok(Some(SpannVectors {
            reader,
            records,
            distance_function: DistanceFunction::from(space),
            adaptive: provider.adaptive_search_nprobe,
        }))
    }

    // The KNN of a SPANN collection for the query API. An empty filter allows every record, as in the HNSW path. The
    // nearest records come from the record segment, with their documents, embeddings and metadata as the projection
    // asks, as the projection of distributed Chroma takes them (rust/worker/src/execution/operators/projection.rs).
    async fn spann_query(
        &self,
        collection_and_segments: &CollectionAndSegments,
        embeddings: Vec<Vec<f32>>,
        k: u32,
        proj: &KnnProjection,
        allowed_user_ids: Vec<String>,
    ) -> Result<Vec<KnnProjectionOutput>, ExecutorError> {
        let internal = |err: Box<dyn ChromaError>| ExecutorError::Internal(err);
        let Some(vectors) = self.spann_vectors(collection_and_segments).await? else {
            return Ok(vec![Default::default(); embeddings.len()]);
        };
        let allowed = if allowed_user_ids.is_empty() {
            None
        } else {
            Some(
                vectors
                    .allowed_offsets(&allowed_user_ids)
                    .await
                    .map_err(internal)?,
            )
        };
        let mut results = Vec::new();
        for embedding in embeddings {
            let nearest = vectors
                .nearest(embedding, k, allowed.as_ref())
                .await
                .map_err(internal)?;
            let offset_ids = nearest
                .iter()
                .map(|record| record.offset_id)
                .collect::<Vec<_>>();
            let records = spann_projection(&vectors.records, &offset_ids, &proj.projection).await?;
            results.push(KnnProjectionOutput {
                records: records
                    .into_iter()
                    .zip(nearest)
                    .map(
                        |(record, RecordMeasure { measure, .. })| KnnProjectionRecord {
                            record,
                            distance: proj.distance.then_some(measure),
                        },
                    )
                    .collect(),
            });
        }
        Ok(results)
    }

    pub async fn search<F, Fut>(
        &mut self,
        plan: Search,
        replan: F,
    ) -> Result<SearchResult, ExecutorError>
    where
        F: Fn(tonic::Code) -> Fut,
        Fut: Future<Output = Result<Search, Box<dyn ChromaError>>>,
    {
        if plan.scan.collection_and_segments.vector_segment.r#type == SegmentType::Spann {
            // A read that fails with an error that distributed Chroma retries reads again, as a get does.
            let mut delays = spann_backoff(&self.retry);
            let mut plan = plan;
            loop {
                match Box::pin(self.spann_search(plan.clone())).await {
                    Err(err) => plan = spann_replan(err, &mut delays, &replan).await?,
                    result => return result,
                }
            }
        }
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
        let vectors = hnsw_reader
            .as_ref()
            .map(|hnsw| hnsw as &dyn DenseVectorReader);
        self.metadata_reader
            .search(plan.scan.collection_and_segments, vectors, plan.payloads)
            .await
            .map_err(|error| ExecutorError::Internal(Box::new(error)))
    }

    // A search of a SPANN collection: its files and its records at one moment.
    async fn spann_search(&mut self, plan: Search) -> Result<SearchResult, ExecutorError> {
        let (mut snapshot, collection_and_segments) = self
            .spann_snapshot(&plan.scan.collection_and_segments)
            .await?;
        // As distributed Chroma (rust/worker/src/server.rs:697-701): an uninitialized collection has no results.
        if collection_and_segments.is_uninitialized() {
            return Ok(SearchResult {
                results: vec![Default::default(); plan.payloads.len()],
                pulled_log_bytes: 0,
            });
        }
        let spann_vectors = self.spann_vectors(&collection_and_segments).await?;
        let vectors = spann_vectors
            .as_ref()
            .map(|spann| spann as &dyn DenseVectorReader);
        self.metadata_reader
            .search_at(
                &mut snapshot,
                collection_and_segments,
                vectors,
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

// The records of the given offsets from the record segment, in their order, with their documents, embeddings and metadata
// as the projection asks, as the projection of distributed Chroma reads them (rust/worker/src/execution/operators/
// projection.rs): the blocks of the records loaded first, then the records read at once, or their user ids one by one
// from the blocks loaded when no data is asked.
async fn spann_projection(
    records: &RecordSegmentReaderShard<'_>,
    offset_ids: &[u32],
    projection: &Projection,
) -> Result<Vec<ProjectionRecord>, ExecutorError> {
    if offset_ids.is_empty() {
        return Ok(Vec::new());
    }
    let internal = |err: Box<dyn ChromaError>| ExecutorError::Internal(err);
    let offset_id_set = offset_ids.iter().copied().collect::<HashSet<_>>();
    if projection.document || projection.embedding || projection.metadata {
        records.load_id_to_data(offset_id_set.into_iter()).await;
        try_join_all(offset_ids.iter().map(|offset_id| async move {
            let data = records
                .get_data_for_offset_id(*offset_id)
                .await
                .map_err(|err| internal(err.boxed()))?
                .ok_or(ExecutorError::InconsistentData)?;
            Ok::<_, ExecutorError>(ProjectionRecord {
                id: data.id.to_string(),
                document: data
                    .document
                    .filter(|_| projection.document)
                    .map(str::to_string),
                embedding: projection.embedding.then(|| data.embedding.to_vec()),
                metadata: data.metadata.filter(|_| projection.metadata),
            })
        }))
        .await
    } else {
        records.load_id_to_user_id(offset_id_set.into_iter()).await;
        let mut projected = Vec::with_capacity(offset_ids.len());
        for offset_id in offset_ids {
            projected.push(ProjectionRecord {
                id: records
                    .get_user_id_for_offset_id(*offset_id)
                    .await
                    .map_err(|err| internal(err.boxed()))?
                    .to_string(),
                document: None,
                embedding: None,
                metadata: None,
            });
        }
        Ok(projected)
    }
}

// The opening of the record segment of a SPANN collection after each of its flushes, in the background, so that the next
// read finds the roots of its blockfiles in memory: a flush puts in the cache the blocks it writes but not the roots, and
// the first read after it loads them from the files (37 ms on a folder of Windows mounted in Docker). Only for a collection
// read since its previous flush, so that a collection that is only written never pushes out of the cache the roots of the
// collections that are read. One opening at a time for each collection, of its last version: a flush during an opening
// replaces the version that waits for it. An opening that fails, as on files that a later flush or a deletion removed,
// is only logged. A read never waits for an opening, and opens what it does not find in memory, as before.
// Distributed Chroma prefetches the record and the metadata segments at the start of each get instead
// (rust/worker/src/execution/orchestration/get.rs), into a cache on disk: its compactor runs in another process.
#[derive(Clone, Debug, Default)]
#[allow(clippy::type_complexity)]
struct SpannWarmUp {
    // The collections read since their last flush.
    read: std::sync::Arc<std::sync::Mutex<HashSet<chroma_types::CollectionUuid>>>,
    // The collections with an opening running, each with the version to open after it, if any.
    running:
        std::sync::Arc<std::sync::Mutex<HashMap<chroma_types::CollectionUuid, Option<Segment>>>>,
}

impl SpannWarmUp {
    fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // A read of the collection: its next flush opens the new version.
    fn read(&self, collection_id: chroma_types::CollectionUuid) {
        Self::lock(&self.read).insert(collection_id);
    }

    // A deleted collection: its mark of a read goes with it.
    fn forget(&self, collection_id: chroma_types::CollectionUuid) {
        Self::lock(&self.read).remove(&collection_id);
    }

    // A flush of the collection, with its record segment at the files of the flush.
    fn flushed(
        &self,
        collection_id: chroma_types::CollectionUuid,
        record_segment: Segment,
        provider: SpannProvider,
    ) {
        if !Self::lock(&self.read).remove(&collection_id) {
            return;
        }
        {
            let mut running = Self::lock(&self.running);
            if let Some(next) = running.get_mut(&collection_id) {
                *next = Some(record_segment);
                return;
            }
            running.insert(collection_id, None);
        }
        let running = self.running.clone();
        tokio::spawn(async move {
            let mut record_segment = record_segment;
            loop {
                #[cfg(test)]
                stop_warm_up(collection_id).await;
                let opened = open_record_segment(&record_segment, &provider).await;
                match &opened {
                    Err(err) if err.code() == ErrorCodes::NotFound => tracing::debug!(
                        collection_id = %collection_id,
                        error = %err,
                        "a later flush or a deletion removed the files of a SPANN flush before their opening"
                    ),
                    Err(err) => tracing::warn!(
                        collection_id = %collection_id,
                        error = %err,
                        "the opening of the files of a SPANN flush failed; the next read opens them"
                    ),
                    Ok(()) => {}
                }
                #[cfg(test)]
                WARMED.lock().unwrap().push((
                    collection_id,
                    record_segment.file_path.clone(),
                    opened.is_ok(),
                ));
                let next = {
                    let mut running = Self::lock(&running);
                    let next = running.get_mut(&collection_id).and_then(Option::take);
                    if next.is_none() {
                        running.remove(&collection_id);
                    }
                    next
                };
                match next {
                    Some(next) => record_segment = next,
                    None => break,
                }
            }
            #[cfg(test)]
            warm_up_ended(collection_id);
        });
    }
}

// The record segment of a SPANN collection opened at the files given, which loads the roots of its blockfiles in the
// cache, then closed.
async fn open_record_segment(
    record_segment: &Segment,
    provider: &SpannProvider,
) -> Result<(), Box<dyn ChromaError>> {
    let shard = SegmentShard::try_from((record_segment, 0)).map_err(|err| err.boxed())?;
    RecordSegmentReaderShard::from_segment(&shard, &provider.blockfile_provider, None)
        .await
        .map(|_| ())
        .map_err(|err| err.boxed())
}

// The backoff of the reads of SPANN collections, from a retry config as the executor of distributed Chroma builds it.
fn spann_backoff(retry: &RetryConfig) -> ExponentialBackoff {
    ExponentialBuilder::from(retry).build()
}

// After a failed read of a SPANN collection, the plan to read again, as the executor of distributed Chroma decides it
// (is_retryable_error and its retries in distributed.rs): on the errors it retries, after the next delay of the backoff,
// or at once after a NotFound, the plan that the frontend gives back for that error; on any other error, or once the
// backoff is over, the error.
async fn spann_replan<P, F, Fut>(
    err: ExecutorError,
    delays: &mut ExponentialBackoff,
    replan: &F,
) -> Result<P, ExecutorError>
where
    F: Fn(tonic::Code) -> Fut,
    Fut: Future<Output = Result<P, Box<dyn ChromaError>>>,
{
    let code = err.code();
    let retried = matches!(
        code,
        ErrorCodes::Unavailable
            | ErrorCodes::DeadlineExceeded
            | ErrorCodes::Aborted
            | ErrorCodes::ResourceExhausted
            | ErrorCodes::NotFound
            | ErrorCodes::Unknown
    );
    let Some(delay) = delays.next().filter(|_| retried) else {
        return Err(err);
    };
    if code != ErrorCodes::NotFound {
        tokio::time::sleep(delay).await;
    }
    replan(code.into()).await.map_err(ExecutorError::Internal)
}

// The SPANN index of a collection with its record segment, for the KNN of distributed Chroma
// (rust/worker/src/execution/orchestration/spann_knn.rs): the nearest centers of a query, their posting lists in brute
// force under the filter, and the merge of the k nearest.
struct SpannVectors<'me> {
    reader: SpannSegmentReaderShard<'me>,
    records: RecordSegmentReaderShard<'me>,
    distance_function: DistanceFunction,
    adaptive: bool,
}

// The records of a SPANN collection for the adaptive number of probes of a query, which distributed Chroma takes from the
// count of its last compaction (total_records_post_compaction): the count of the record segment, read only when the probes
// adapt, since their number does not depend on it otherwise.
pub(crate) async fn spann_probe_records(
    adaptive: bool,
    records: &RecordSegmentReaderShard<'_>,
) -> Result<usize, Box<dyn ChromaError>> {
    if adaptive {
        records.count().await
    } else {
        Ok(0)
    }
}

impl SpannVectors<'_> {
    // The offsets of the user ids in the record segment; ids it does not hold are left out.
    async fn allowed_offsets(
        &self,
        user_ids: &[String],
    ) -> Result<HashSet<u32>, Box<dyn ChromaError>> {
        let mut allowed = HashSet::new();
        for user_id in user_ids {
            if let Some(offset_id) = self
                .records
                .get_offset_id_for_user_id(user_id, &RecordSegmentReaderOptions::default())
                .await
                .map_err(|err| err.boxed())?
            {
                allowed.insert(offset_id);
            }
        }
        Ok(allowed)
    }

    // The k nearest of the query among the allowed offsets, or all the records without them, nearest first.
    async fn nearest(
        &self,
        query: Vec<f32>,
        k: u32,
        allowed: Option<&HashSet<u32>>,
    ) -> Result<Vec<RecordMeasure>, Box<dyn ChromaError>> {
        let query = if self.distance_function == DistanceFunction::Cosine {
            normalize(&query)
        } else {
            query
        };
        let collection_size = spann_probe_records(self.adaptive, &self.records).await?;
        let (centers, _, _) = self
            .reader
            .rng_query(&query, collection_size, k as usize)
            .await
            .map_err(|err| err.boxed())?;
        let mut batches = Vec::with_capacity(centers.len());
        for center in centers {
            let postings = self
                .reader
                .fetch_posting_list(center as u32)
                .await
                .map_err(|err| err.boxed())?;
            let mut nearest = BinaryHeap::with_capacity(k as usize);
            for posting in postings {
                if allowed.is_some_and(|allowed| !allowed.contains(&posting.doc_offset_id)) {
                    continue;
                }
                let record = RecordMeasure {
                    offset_id: posting.doc_offset_id,
                    measure: self
                        .distance_function
                        .distance(&posting.doc_embedding, &query),
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
        Ok((Merge { k })
            .merge(batches)
            .into_iter()
            .map(|std::cmp::Reverse(record)| record)
            .collect())
    }
}

// The dense leaves of the Search API on a SPANN collection.
#[async_trait]
impl DenseVectorReader for SpannVectors<'_> {
    async fn nearest(
        &self,
        _: &CollectionAndSegments,
        query: Vec<f32>,
        allowed_user_ids: Option<&[String]>,
        limit: u32,
    ) -> Result<Vec<(u32, String, f32)>, SqliteMetadataError> {
        let spann = SqliteMetadataError::DenseVectors;
        let allowed = match allowed_user_ids {
            Some(user_ids) => Some(self.allowed_offsets(user_ids).await.map_err(spann)?),
            None => None,
        };
        let mut nearest = Vec::new();
        for RecordMeasure { offset_id, measure } in
            SpannVectors::nearest(self, query, limit, allowed.as_ref())
                .await
                .map_err(spann)?
        {
            let user_id = self
                .records
                .get_user_id_for_offset_id(offset_id)
                .await
                .map_err(|err| spann(err.boxed()))?
                .to_string();
            nearest.push((offset_id, user_id, measure));
        }
        Ok(nearest)
    }

    async fn embedding(&self, user_id: &str) -> Result<Vec<f32>, SqliteMetadataError> {
        let spann = SqliteMetadataError::DenseVectors;
        let offset_id = self
            .records
            .get_offset_id_for_user_id(user_id, &RecordSegmentReaderOptions::default())
            .await
            .map_err(|err| spann(err.boxed()))?;
        let data = match offset_id {
            Some(offset_id) => self
                .records
                .get_data_for_offset_id(offset_id)
                .await
                .map_err(|err| spann(err.boxed()))?,
            None => None,
        };
        data.map(|data| data.embedding.to_vec())
            .ok_or_else(|| SqliteMetadataError::MissingEmbedding(user_id.to_string()))
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
        Ok(Self::new(hnsw_manager, sqlite_db, compactor_handle.clone())
            .with_spann(config.spann)
            .with_retry(config.retry.clone()))
    }
}

/// Test hook: the stops of reads of SPANN collections before their files, so that a test puts writes and their flushes
/// between the snapshot of SQLite of a read and the files it reads. The first opening of the files of a collection
/// takes its stop, tells the test, and waits until the test lets it go on.
#[cfg(test)]
pub(crate) static STOP_BEFORE_FILES: std::sync::Mutex<Vec<StopBeforeFiles>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) struct StopBeforeFiles {
    pub(crate) collection_id: chroma_types::CollectionUuid,
    pub(crate) stopped: tokio::sync::oneshot::Sender<()>,
    pub(crate) go_on: tokio::sync::oneshot::Receiver<()>,
}

// The stop of STOP_BEFORE_FILES for the collection, when a test set one.
#[cfg(test)]
async fn stop_before_files(collection_id: chroma_types::CollectionUuid) {
    let stop = {
        let mut stops = STOP_BEFORE_FILES.lock().unwrap();
        stops
            .iter()
            .position(|stop| stop.collection_id == collection_id)
            .map(|at| stops.swap_remove(at))
    };
    if let Some(stop) = stop {
        let _ = stop.stopped.send(());
        let _ = stop.go_on.await;
    }
}

/// Test hook: the failures of reads of SPANN collections at their opening of the files, so that a test sees which
/// errors a read takes again. Each opening of the files of a collection takes its first failure, in order.
#[cfg(test)]
pub(crate) static FAIL_BEFORE_FILES: std::sync::Mutex<
    Vec<(chroma_types::CollectionUuid, ErrorCodes)>,
> = std::sync::Mutex::new(Vec::new());

/// The error of a failure of FAIL_BEFORE_FILES.
#[cfg(test)]
#[derive(Debug, thiserror::Error)]
#[error("failure of a test: {0:?}")]
pub(crate) struct TestFailure(pub(crate) ErrorCodes);

#[cfg(test)]
impl ChromaError for TestFailure {
    fn code(&self) -> ErrorCodes {
        self.0
    }
}

// The failure of FAIL_BEFORE_FILES for the collection, when a test set one.
#[cfg(test)]
fn fail_before_files(collection_id: chroma_types::CollectionUuid) -> Result<(), ExecutorError> {
    let failure = {
        let mut failures = FAIL_BEFORE_FILES.lock().unwrap();
        failures
            .iter()
            .position(|(id, _)| *id == collection_id)
            .map(|at| failures.remove(at))
    };
    match failure {
        Some((_, code)) => Err(ExecutorError::Internal(Box::new(TestFailure(code)))),
        None => Ok(()),
    }
}

/// Test hook: the stops of the openings after a flush of a SPANN collection, before they open the files, until the test
/// lets them go on.
#[cfg(test)]
pub(crate) static STOP_WARM_UP: std::sync::Mutex<Vec<StopBeforeFiles>> =
    std::sync::Mutex::new(Vec::new());

// The stop of STOP_WARM_UP for the collection, when a test set one.
#[cfg(test)]
async fn stop_warm_up(collection_id: chroma_types::CollectionUuid) {
    let stop = {
        let mut stops = STOP_WARM_UP.lock().unwrap();
        stops
            .iter()
            .position(|stop| stop.collection_id == collection_id)
            .map(|at| stops.swap_remove(at))
    };
    if let Some(stop) = stop {
        let _ = stop.stopped.send(());
        let _ = stop.go_on.await;
    }
}

/// Test hook: the openings after a flush, each with its collection, the files it opened and whether it opened them.
#[cfg(test)]
#[allow(clippy::type_complexity)]
pub(crate) static WARMED: std::sync::Mutex<
    Vec<(
        chroma_types::CollectionUuid,
        HashMap<String, Vec<String>>,
        bool,
    )>,
> = std::sync::Mutex::new(Vec::new());

/// Test hook: the tests that wait for the end of the openings of a collection.
#[cfg(test)]
pub(crate) static WARM_UP_ENDS: std::sync::Mutex<
    Vec<(
        chroma_types::CollectionUuid,
        tokio::sync::oneshot::Sender<()>,
    )>,
> = std::sync::Mutex::new(Vec::new());

// The end of the openings of a collection, to the tests that wait for it.
#[cfg(test)]
fn warm_up_ended(collection_id: chroma_types::CollectionUuid) {
    let mut ends = WARM_UP_ENDS.lock().unwrap();
    let mut at = 0;
    while at < ends.len() {
        if ends[at].0 == collection_id {
            let (_, end) = ends.swap_remove(at);
            let _ = end.send(());
        } else {
            at += 1;
        }
    }
}

#[cfg(test)]
impl LocalExecutor {
    /// Whether an opening after a flush runs for the collection.
    pub(crate) fn spann_warm_up_runs(&self, collection_id: chroma_types::CollectionUuid) -> bool {
        SpannWarmUp::lock(&self.spann_warm_up.running).contains_key(&collection_id)
    }

    /// Whether the collection is marked as read since its last flush.
    pub(crate) fn spann_marked_read(&self, collection_id: chroma_types::CollectionUuid) -> bool {
        SpannWarmUp::lock(&self.spann_warm_up.read).contains(&collection_id)
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
