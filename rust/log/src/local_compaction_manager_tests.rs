// Changed from the original Chroma file; see the git history for the changes and their authors.
use super::*;
use crate::sqlite_log::{LegacyEmbeddingsQueueConfig, SqliteLog, SqlitePushLogsError};
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sysdb::test_sysdb::TestSysDb;
use chroma_system::{ComponentHandle, System};
use chroma_types::{
    Collection, KnnIndex, Operation, OperationRecord, Segment, SegmentScope, SegmentType,
};

struct Fixture {
    db: SqliteDb,
    log: SqliteLog,
    collection: Collection,
    metadata: SegmentUuid,
    vector: SegmentUuid,
    sysdb: TestSysDb,
    handle: ComponentHandle<LocalCompactionManager>,
    system: System,
    compactor_joined: bool,
    path: tempfile::TempDir,
    persistent: bool,
    manager: LocalSegmentManager,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_persistence(true).await
    }

    async fn with_persistence(persistent: bool) -> Self {
        let db = chroma_sqlite::db::test_utils::get_new_sqlite_db().await;
        let registry = Registry::new();
        registry.register(db.clone());
        let path = tempfile::tempdir().unwrap();
        let manager = LocalSegmentManager::try_from_config(
            &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
                "persist_path": persistent.then(|| path.path().to_str().unwrap())
            }))
            .unwrap(),
            &registry,
        )
        .await
        .unwrap();
        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let metadata = SegmentUuid::new();
        let vector = SegmentUuid::new();
        let mut sysdb = TestSysDb::new();
        sysdb.add_collection(collection.clone());
        for (id, scope, r#type) in [
            (metadata, SegmentScope::METADATA, SegmentType::Sqlite),
            (
                vector,
                SegmentScope::VECTOR,
                SegmentType::HnswLocalPersisted,
            ),
        ] {
            // The segment manager checks SQLite for deletion even with a mock sysdb.
            sqlx::query("INSERT INTO segments (id, type, scope, collection) VALUES (?, ?, ?, ?)")
                .bind(id.to_string())
                .bind(String::from(r#type))
                .bind(String::from(scope.clone()))
                .bind(collection.collection_id.to_string())
                .execute(db.get_conn())
                .await
                .unwrap();
            sysdb.add_segment(Segment {
                id,
                scope,
                r#type,
                collection: collection.collection_id,
                metadata: None,
                file_path: Default::default(),
            });
        }
        let log = SqliteLog::new(db.clone(), "default".into(), "default".into());
        log.update_legacy_embeddings_queue_config(LegacyEmbeddingsQueueConfig {
            automatically_purge: true,
            kind: "EmbeddingsQueueConfigurationInternal".into(),
        })
        .await
        .unwrap();
        let system = System::new();
        let handle = system.start_component(LocalCompactionManager {
            log: Log::Sqlite(log.clone()),
            sqlite_db: db.clone(),
            hnsw_segment_manager: manager.clone(),
            metrics: LocalCompactionMetrics::default(),
            sysdb: SysDb::Test(sysdb.clone()),
            purged: HashSet::new(),
            purges: PurgeSchedule::new(Duration::ZERO),
            purging: false,
            spann_versions: HashMap::new(),
            spann_version_grace: SpannVersionGrace::default().0,
            compaction_interval: Duration::from_secs(10),
            min_compaction_size: 10,
            max_compaction_size: 10_000,
            max_partition_size: 5_000,
            max_concurrent_jobs: 50,
            compaction_timeout: Duration::from_secs(86_400),
            spann_flushes: SpannFlushes::default(),
            spann_flush_stop: None,
            background: tokio_util::task::TaskTracker::new(),
            shutdown_grace: Duration::from_secs(30),
            function_runs: Default::default(),
            sync_retries: Default::default(),
            function_storage: None,
            purge_after_save: None,
        });
        Self {
            db,
            log,
            collection,
            metadata,
            vector,
            sysdb,
            handle,
            system,
            compactor_joined: false,
            path,
            persistent,
            manager,
        }
    }

    async fn watermark(&self, segment: SegmentUuid, seq: i64) {
        sqlx::query("INSERT OR REPLACE INTO max_seq_id (segment_id, seq_id) VALUES (?, ?)")
            .bind(segment.to_string())
            .bind(seq)
            .execute(self.db.get_conn())
            .await
            .unwrap();
    }

    async fn seed(&mut self) -> Vec<LogRecord> {
        self.log
            .push_logs(
                self.collection.collection_id,
                (0..5).map(operation).collect(),
            )
            .await
            .unwrap();
        self.records().await
    }

    // Materialize real files for tests that authorize deleting checkpointed logs.
    // Keep watermarks under each test's control, including missing-watermark cases.
    async fn checkpoint(&mut self) {
        let mut collection = self.collection.clone();
        if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
            &mut collection.config.vector_index
        {
            config.sync_threshold = 2;
        }
        collection.schema = Some(Schema::try_from(&collection.config).unwrap());
        let segments = SysDb::Test(self.sysdb.clone())
            .get_collection_with_segments(None, collection.collection_id)
            .await
            .unwrap();
        let mut writer = chroma_segment::local_hnsw::LocalHnswSegmentWriter::from_segment(
            &collection,
            &segments.vector_segment,
            3,
            Some(self.path.path().to_str().unwrap().into()),
            self.db.clone(),
        )
        .await
        .unwrap();
        writer
            .apply_log_chunk(Chunk::new(self.records().await.into()))
            .await
            .unwrap();
        writer.index.close().await;
        sqlx::query("DELETE FROM max_seq_id WHERE segment_id = ?")
            .bind(self.vector.to_string())
            .execute(self.db.get_conn())
            .await
            .unwrap();
    }

    fn break_collection_configuration(&mut self) {
        self.collection.dimension = Some(4);
        self.sysdb.add_collection(self.collection.clone());
    }

    // Waits for the save in the background of the vector index, if any: the tests check its files and its position
    // in SQLite after it.
    async fn saved(&self) {
        let folder = self.path.path().join(self.vector.to_string());
        drop(chroma_segment::local_hnsw::files_lock(&folder).lock().await);
    }

    async fn records(&mut self) -> Vec<LogRecord> {
        self.log
            .read(self.collection.collection_id, 0, -1, None)
            .await
            .unwrap()
    }

    async fn purge(&self) -> Result<(), CompactionManagerError> {
        self.handle
            .request(
                PurgeLogsMessage {
                    collection_id: self.collection.collection_id,
                },
                None,
            )
            .await
            .unwrap()
    }

    async fn stop_compactor(&mut self) {
        if !self.compactor_joined {
            self.handle.stop();
            self.handle.join().await.unwrap();
            self.compactor_joined = true;
        }
    }

    async fn restart_compactor(&mut self) {
        let log = SqliteLog::new(self.db.clone(), "default".into(), "default".into());
        self.restart_compactor_with(log, PurgeSchedule::new(Duration::ZERO))
            .await;
    }

    // The compactor again, with the log and the timer of its purges given: the tracker of its work out of the actor.
    async fn restart_compactor_with(
        &mut self,
        log: SqliteLog,
        purges: PurgeSchedule,
    ) -> tokio_util::task::TaskTracker {
        self.stop_compactor().await;
        let background = tokio_util::task::TaskTracker::new();
        let registry = Registry::new();
        registry.register(self.db.clone());
        let manager = LocalSegmentManager::try_from_config(
            &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
                "persist_path": self.persistent.then(|| self.path.path().to_str().unwrap())
            }))
            .unwrap(),
            &registry,
        )
        .await
        .unwrap();
        self.compactor_joined = false;
        self.manager = manager.clone();
        self.handle = self.system.start_component(LocalCompactionManager {
            log: Log::Sqlite(log),
            sqlite_db: self.db.clone(),
            hnsw_segment_manager: manager.clone(),
            metrics: LocalCompactionMetrics::default(),
            sysdb: SysDb::Test(self.sysdb.clone()),
            purged: HashSet::new(),
            purges,
            purging: false,
            spann_versions: HashMap::new(),
            spann_version_grace: SpannVersionGrace::default().0,
            compaction_interval: Duration::from_secs(10),
            min_compaction_size: 10,
            max_compaction_size: 10_000,
            max_partition_size: 5_000,
            max_concurrent_jobs: 50,
            compaction_timeout: Duration::from_secs(86_400),
            spann_flushes: SpannFlushes::default(),
            spann_flush_stop: None,
            background: background.clone(),
            shutdown_grace: Duration::from_secs(30),
            function_runs: Default::default(),
            sync_retries: Default::default(),
            function_storage: None,
            purge_after_save: None,
        });
        background
    }

    // The end of a round of the timer of the flushes, which starts the purges that are due.
    async fn round_done(&self) {
        let (round, _) = oneshot::channel();
        self.handle
            .request(SpannRoundDone { round }, None)
            .await
            .unwrap();
    }

    // The compactor again with the interval of its purges given, for a collection in use by conditional transactions, with
    // no cutoff: what it applied goes at the purge of the timer.
    async fn in_use_with_a_timer_of(
        &mut self,
        interval: Duration,
    ) -> tokio_util::task::TaskTracker {
        self.log
            .note_conditional_use(self.collection.collection_id)
            .await
            .unwrap();
        let log = self.log.clone().with_purge_cutoff_time_seconds(0);
        self.restart_compactor_with(log, PurgeSchedule::new(interval))
            .await
    }

    // Waits for the records of the log to be the ones given, as a purge out of the actor ends on its own.
    async fn wait_for_records(&mut self, expected: Vec<LogRecord>) {
        for _ in 0..200 {
            if self.records().await.len() == expected.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_records(self.records().await, expected);
    }

    async fn stop(mut self) {
        self.stop_compactor().await;
        self.system.stop().await;
        self.system.join().await;
    }
}

fn operation(id: usize) -> OperationRecord {
    OperationRecord {
        id: format!("id-{id}"),
        embedding: Some(vec![1.0, 2.0, 3.0]),
        encoding: Some(chroma_types::ScalarEncoding::FLOAT32),
        metadata: None,
        document: Some(format!("document-{id}")),
        operation: Operation::Add,
    }
}

#[tokio::test]
async fn purge_watermark_matrix_preserves_boundary_and_other_collections() {
    // Missing, zero, equal, and either segment lagging must all be conservative.
    for (metadata, vector) in [
        (None, None),
        (Some(4), None),
        (None, Some(4)),
        (Some(0), Some(4)),
        (Some(4), Some(0)),
        (Some(4), Some(2)),
        (Some(2), Some(4)),
        (Some(3), Some(3)),
    ] {
        let mut f = Fixture::new().await;
        let original = f.seed().await;
        f.checkpoint().await;
        let other = CollectionUuid::new();
        f.log.push_logs(other, vec![operation(99)]).await.unwrap();
        let other_records = f.log.read(other, 0, -1, None).await.unwrap();
        // Unrelated watermarks must not influence the target collection.
        f.watermark(SegmentUuid::new(), 999).await;
        if let Some(seq) = metadata {
            f.watermark(f.metadata, seq).await;
        }
        if let Some(seq) = vector {
            f.watermark(f.vector, seq).await;
        }
        let boundary = metadata.unwrap_or(0).min(vector.unwrap_or(0));
        let expected: Vec<_> = original
            .into_iter()
            .filter(|r| r.log_offset >= boundary)
            .collect();
        f.purge().await.unwrap();
        assert_records(f.records().await, expected.clone());
        assert_records(f.log.read(other, 0, -1, None).await.unwrap(), other_records);
        f.purge().await.unwrap();
        assert_records(f.records().await, expected);
        f.stop().await;
    }
}

#[tokio::test]
async fn stale_checkpoint_never_authorizes_purge_even_with_cached_validation() {
    for cached in [false, true] {
        let mut f = Fixture::new().await;
        let original = f.seed().await;
        f.checkpoint().await;
        let checkpoint = original.last().unwrap().log_offset as u64;
        if cached {
            let segments = SysDb::Test(f.sysdb.clone())
                .get_collection_with_segments(None, f.collection.collection_id)
                .await
                .unwrap();
            f.manager
                .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
                .await
                .unwrap();
            f.manager
                .validate_persisted_checkpoint(&f.vector, checkpoint)
                .await
                .unwrap();
        }
        f.watermark(f.metadata, checkpoint as i64 + 2).await;
        f.watermark(f.vector, checkpoint as i64 + 2).await;
        assert!(matches!(
            f.purge().await,
            Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
        ));
        assert_records(f.records().await, original);
        f.stop().await;
    }
}

// As the garbage collector of distributed Chroma: the records after the lowest completion offset of the attached
// functions of a collection stay, as its functions have not run on them.
#[tokio::test]
async fn purge_keeps_the_records_its_attached_functions_have_not_run_on() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 4).await;
    let insert = |offset: i64, deleted: bool| {
        sqlx::query(
            "INSERT INTO attached_functions (id, name, tenant_id, database_id, input_collection_id, \
             output_collection_name, function_id, function_params, completion_offset, is_deleted) \
             VALUES (?, ?, 't', 'd', ?, 'out', 'f', '{}', ?, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(f.collection.collection_id.to_string())
        .bind(offset)
        .bind(deleted)
        .execute(f.db.get_conn())
    };
    insert(1, false).await.unwrap();
    // A deleted function does not keep any.
    insert(0, true).await.unwrap();
    f.purge().await.unwrap();
    let expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 2).collect();
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn purge_disabled_preserves_all_records() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 4).await;
    f.log
        .update_legacy_embeddings_queue_config(LegacyEmbeddingsQueueConfig {
            automatically_purge: false,
            kind: "EmbeddingsQueueConfigurationInternal".into(),
        })
        .await
        .unwrap();
    f.purge().await.unwrap();
    assert_records(f.records().await, original);
    f.stop().await;
}

// Hides the tables a purge reads, so that any purge which reads SQLite fails.
async fn hide_purge_tables(db: &SqliteDb, hide: bool) {
    for table in ["max_seq_id", "embeddings_queue", "embeddings_queue_config"] {
        let (from, to) = if hide {
            (table.to_string(), format!("hidden_{table}"))
        } else {
            (format!("hidden_{table}"), table.to_string())
        };
        sqlx::query(&format!("ALTER TABLE {from} RENAME TO {to}"))
            .execute(db.get_conn())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn purge_without_backfill_since_the_last_check_reads_nothing() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.purge().await.unwrap();
    let expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 3).collect();
    assert_records(f.records().await, expected.clone());
    // Every read asks for a purge: with nothing applied since the last check, there is nothing to read.
    hide_purge_tables(&f.db, true).await;
    let repeated = f.purge().await;
    hide_purge_tables(&f.db, false).await;
    assert!(repeated.is_ok(), "{repeated:?}");
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn failed_purge_is_checked_again_by_the_next_request() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    hide_purge_tables(&f.db, true).await;
    let failed = f.purge().await;
    hide_purge_tables(&f.db, false).await;
    assert!(failed.is_err());
    assert_records(f.records().await, original.clone());
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 3).collect(),
    );
    f.stop().await;
}

// A purge of the timer runs out of the actor, as the garbage collector of distributed Chroma runs apart from the log:
// while its DELETE waits for the lock of SQLite, held here by another connection, the compactor still answers.
#[tokio::test]
async fn a_purge_of_the_timer_does_not_hold_up_the_compactor() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    let background = f.in_use_with_a_timer_of(Duration::from_millis(1)).await;
    f.purge().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut writer = f.db.get_conn().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *writer)
        .await
        .unwrap();
    let answered = tokio::time::timeout(Duration::from_secs(5), async {
        f.round_done().await;
        f.purge().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        f.round_done().await;
    })
    .await;
    // Only one purge runs at a time: the request of the second round waits for its end.
    let running = background.len();
    // A stop waits for a purge that runs, as for the flushes.
    background.close();
    let stop_went_on = tokio::time::timeout(Duration::from_millis(200), background.wait())
        .await
        .is_ok();
    sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
    drop(writer);
    assert!(answered.is_ok(), "the compactor waited for the purge");
    assert_eq!(running, 1);
    assert!(!stop_went_on, "the stop did not wait for the purge");
    tokio::time::timeout(Duration::from_secs(10), background.wait())
        .await
        .unwrap();
    f.wait_for_records(original.into_iter().filter(|r| r.log_offset >= 3).collect())
        .await;
    f.stop().await;
}

// In a collection that conditional transactions use, a request of a purge only marks the collection: nothing goes before
// the timer.
#[tokio::test]
async fn a_purge_request_waits_for_the_timer() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.in_use_with_a_timer_of(Duration::from_secs(3_600)).await;
    f.purge().await.unwrap();
    f.round_done().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_records(f.records().await, original);
    f.stop().await;
}

// A purge of the timer that succeeded holds until a backfill moves the watermarks, as a purge at once does; the one after
// it runs, once the first has ended.
#[tokio::test]
async fn a_purge_of_the_timer_holds_until_a_backfill() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 2).await;
    f.watermark(f.vector, 2).await;
    f.in_use_with_a_timer_of(Duration::from_millis(1)).await;
    f.purge().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    f.round_done().await;
    let after_two: Vec<_> = original
        .iter()
        .filter(|r| r.log_offset >= 2)
        .cloned()
        .collect();
    f.wait_for_records(after_two.clone()).await;
    // The watermarks move without a backfill: the purge that succeeded still holds.
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.purge().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    f.round_done().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_records(f.records().await, after_two);
    // A backfill forgets it and applies the rest of the log: a later round purges up to there, as many rounds as the end
    // of the first purge needs.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    let after_backfill: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 5).collect();
    for _ in 0..100 {
        f.purge().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        f.round_done().await;
        if f.records().await.len() == after_backfill.len() {
            break;
        }
    }
    f.wait_for_records(after_backfill).await;
    f.stop().await;
}

// A purge of the timer that failed is not marked as done: the next request asks for it again.
#[tokio::test]
async fn a_failed_purge_of_the_timer_is_asked_again() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    let background = f.in_use_with_a_timer_of(Duration::from_millis(1)).await;
    hide_purge_tables(&f.db, true).await;
    f.purge().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    f.round_done().await;
    for _ in 0..100 {
        if background.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    hide_purge_tables(&f.db, false).await;
    assert_records(f.records().await, original.clone());
    let expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 3).collect();
    for _ in 0..100 {
        f.purge().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        f.round_done().await;
        if f.records().await.len() == expected.len() {
            break;
        }
    }
    f.wait_for_records(expected).await;
    f.stop().await;
}

#[tokio::test]
async fn backfill_makes_the_next_purge_check_again() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.purge().await.unwrap();
    // As a write: the backfill moves the watermarks, the next purge deletes what they now allow.
    f.watermark(f.metadata, 5).await;
    f.watermark(f.vector, 5).await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 5).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn failed_backfill_still_makes_the_next_purge_check_again() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.purge().await.unwrap();
    f.watermark(f.metadata, 5).await;
    f.watermark(f.vector, 5).await;
    f.break_collection_configuration();
    let backfill = f
        .handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap();
    assert!(backfill.is_err(), "{backfill:?}");
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 5).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn purge_does_not_require_dimension_or_schema() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 3).await;
    f.collection.dimension = None;
    f.collection.schema = None;
    f.sysdb.add_collection(f.collection.clone());
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 3).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn purge_preserves_all_logs_when_checkpoint_files_are_missing_or_corrupt() {
    for file in [
        "index_metadata.pickle",
        "header.bin",
        "data_level0.bin",
        "length.bin",
        "link_lists.bin",
    ] {
        for missing in [false, true] {
            let mut f = Fixture::new().await;
            let original = f.seed().await;
            f.checkpoint().await;
            f.watermark(f.metadata, 4).await;
            f.watermark(f.vector, 3).await;
            let path = f.path.path().join(f.vector.to_string()).join(file);
            let saved = std::fs::read(&path).unwrap();
            if missing {
                std::fs::remove_file(&path).unwrap();
            } else {
                std::fs::write(&path, b"bad").unwrap();
            }
            let backfill = f
                .handle
                .request(
                    BackfillMessage {
                        collection_id: f.collection.collection_id,
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(
                matches!(
                    backfill,
                    Err(CompactionManagerError::HnswReaderConstructionError(_))
                ),
                "{file}, missing={missing}: {backfill:?}"
            );
            for _ in 0..2 {
                let result = f.purge().await;
                assert!(
                    matches!(result, Err(CompactionManagerError::UnsafeHnswCheckpoint(_))),
                    "{file}, missing={missing}: {result:?}"
                );
                assert_records(f.records().await, original.clone());
            }
            // Restoring a usable checkpoint permits purging again.
            std::fs::write(&path, saved).unwrap();
            f.purge().await.unwrap();
            assert_records(
                f.records().await,
                original.into_iter().filter(|r| r.log_offset >= 3).collect(),
            );
            f.stop().await;
        }
    }
}

#[tokio::test]
async fn missing_checkpoint_directory_preserves_logs_and_new_writes() {
    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn cached_index_does_not_hide_corruption_from_purge() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.watermark(f.metadata, 5).await;
    f.watermark(f.vector, 5).await;
    // Load the healthy checkpoint into the manager's cache.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    let header = f.path.path().join(f.vector.to_string()).join("header.bin");
    let saved = std::fs::read(&header).unwrap();
    std::fs::write(&header, b"bad").unwrap();
    // The live index still works, but cannot establish that disk is recoverable.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    assert_records(f.records().await, original.clone());
    std::fs::write(&header, saved).unwrap();
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 5).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn interrupted_checkpoint_preserves_logs_until_files_are_consistent() {
    let mut f = Fixture::new().await;
    f.seed().await;
    f.checkpoint().await;
    f.watermark(f.vector, 5).await;
    let folder = f.path.path().join(f.vector.to_string());
    let pickle = folder.join("index_metadata.pickle");
    let old_map = std::fs::read(&pickle).unwrap();
    f.log
        .push_logs(f.collection.collection_id, vec![operation(5), operation(6)])
        .await
        .unwrap();
    let original = f.records().await;
    f.checkpoint().await;
    let new_map = std::fs::read(&pickle).unwrap();
    // Simulate native files being saved before the ID map and SQLite watermark.
    std::fs::write(&pickle, old_map).unwrap();
    f.watermark(f.metadata, 7).await;
    f.watermark(f.vector, 5).await;
    assert!(
        chroma_segment::local_hnsw::inspect_persisted_hnsw_index(&folder)
            .unwrap()
            .recovery_required
    );
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    assert_records(f.records().await, original.clone());
    std::fs::write(&pickle, new_map).unwrap();
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 5).collect(),
    );
    f.stop().await;
}

#[tokio::test]
async fn retained_logs_allow_full_vector_rebuild_after_checkpoint_loss() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    let original = f.seed().await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    let folder = f.path.path().join(f.vector.to_string());
    std::fs::write(folder.join("header.bin"), b"bad").unwrap();
    assert!(matches!(
        f.purge().await,
        Err(CompactionManagerError::UnsafeHnswCheckpoint(_))
    ));
    assert_records(f.records().await, original);

    // Model an operator discarding the unusable vector checkpoint to rebuild
    // from the preserved log. Production purging never removes these files or
    // resets watermarks on its own.
    f.stop_compactor().await;
    std::fs::remove_dir_all(&folder).unwrap();
    sqlx::query("DELETE FROM max_seq_id WHERE segment_id = ?")
        .bind(f.vector.to_string())
        .execute(f.db.get_conn())
        .await
        .unwrap();
    f.restart_compactor().await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    let watermarks = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            watermarks.current_max_seq_id(&f.metadata).await.unwrap(),
            watermarks.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 5)
    );
    f.purge().await.unwrap();
    f.stop_compactor().await;
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let reader = chroma_segment::local_hnsw::LocalHnswSegmentReader::from_segment(
        &f.collection,
        &segments.vector_segment,
        3,
        Some(f.path.path().to_str().unwrap().into()),
        f.db.clone(),
    )
    .await
    .unwrap();
    let mut embeddings = Vec::new();
    for id in 0..5 {
        embeddings.push(
            reader
                .get_embedding_by_user_id(&format!("id-{id}"))
                .await
                .unwrap(),
        );
    }
    assert_eq!(embeddings, vec![vec![1.0, 2.0, 3.0]; 5]);
    reader.index.close().await;
    f.stop().await;
}

#[tokio::test]
async fn purge_errors_leave_logs_intact() {
    for failure in ["watermark", "collection", "delete", "config"] {
        let mut f = Fixture::new().await;
        let original = f.seed().await;
        f.checkpoint().await;
        f.watermark(f.metadata, 4).await;
        f.watermark(f.vector, 4).await;
        match failure {
            "watermark" => {
                sqlx::query("DROP TABLE max_seq_id")
                    .execute(f.db.get_conn())
                    .await
                    .unwrap();
            }
            "collection" => f.sysdb.remove_collection(f.collection.collection_id),
            "delete" => {
                sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
                .execute(f.db.get_conn()).await.unwrap();
            }
            "config" => {
                sqlx::query("UPDATE embeddings_queue_config SET config_json_str = 'invalid json'")
                    .execute(f.db.get_conn())
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let result = f.purge().await;
        match failure {
            "watermark" => assert!(
                matches!(result, Err(CompactionManagerError::MetadataReaderError(_))),
                "{result:?}"
            ),
            "collection" => assert!(
                matches!(
                    result,
                    Err(CompactionManagerError::GetCollectionWithSegmentsError(_))
                ),
                "{result:?}"
            ),
            _ => assert!(
                matches!(result, Err(CompactionManagerError::PurgeLogsFailure)),
                "{result:?}"
            ),
        }
        assert_records(f.records().await, original);
        f.stop().await;
    }
}

#[tokio::test]
async fn push_attempts_purge_after_backfill_failure_and_retains_new_records() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.break_collection_configuration();
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    let mut expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 3).collect();
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

// On a file system that reports the attributes of the checkpoint late, as SMB, its fingerprint changes between the two
// reads around an inspection although nobody writes. The purge waits, the log keeps every record, and the write that
// asked for the purge succeeds.
#[tokio::test]
async fn a_checkpoint_that_changes_during_inspection_postpones_the_purge() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    let checkpoint = original.last().unwrap().log_offset;
    f.watermark(f.metadata, checkpoint).await;
    f.watermark(f.vector, checkpoint).await;
    let folder = f.path.path().join(f.vector.to_string());
    *chroma_segment::local_hnsw::TOUCH_AFTER_INSPECTION
        .lock()
        .unwrap() = Some(folder);
    let purged = f.purge().await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let pushed = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    *chroma_segment::local_hnsw::TOUCH_AFTER_INSPECTION
        .lock()
        .unwrap() = None;
    assert!(purged.is_ok(), "{purged:?}");
    assert!(pushed.is_ok(), "{pushed:?}");
    let mut expected = original;
    expected.push(LogRecord {
        log_offset: checkpoint + 1,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn push_returns_purge_failure_when_backfill_succeeds() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    // An uninitialized collection makes backfill a successful no-op.
    f.collection.dimension = None;
    f.sysdb.add_collection(f.collection.clone());
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
        .execute(f.db.get_conn()).await.unwrap();
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::PurgeLogsFailure
            ))
        ),
        "{result:?}"
    );
    let mut expected = original;
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn watermark_supports_sqlite_integer_limit() {
    let f = Fixture::new().await;
    f.watermark(f.metadata, i64::MAX).await;
    f.watermark(f.vector, i64::MAX - 1).await;
    assert_eq!(
        max_purge_seq_id(&f.db, &f.metadata, &f.vector)
            .await
            .unwrap(),
        (i64::MAX - 1) as u64
    );
    f.stop().await;
}

// Compare the complete wire representation because LogRecord lacks PartialEq.
fn assert_records(actual: Vec<LogRecord>, expected: Vec<LogRecord>) {
    fn wire(records: Vec<LogRecord>) -> Vec<chroma_types::chroma_proto::LogRecord> {
        records
            .into_iter()
            .map(|r| chroma_types::chroma_proto::LogRecord {
                log_offset: r.log_offset,
                record: Some(r.record.try_into().unwrap()),
            })
            .collect()
    }
    assert_eq!(wire(actual), wire(expected));
}

#[derive(Clone, Default)]
struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn push_logs_both_failures_but_returns_backfill_error() {
    // Keep a process-wide subscriber alive: scoped subscriber registration can
    // race with the same callsite firing in parallel tests without a subscriber.
    // Filter captured lines by this test's unique collection to avoid accepting
    // another test's purge failure as evidence.
    static CAPTURE: std::sync::OnceLock<Capture> = std::sync::OnceLock::new();
    let capture = CAPTURE.get_or_init(|| {
        let capture = Capture::default();
        let writer = capture.clone();
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::ERROR)
                .with_writer(move || writer.clone())
                .finish(),
        )
        .unwrap();
        capture
    });

    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.checkpoint().await;
    f.break_collection_configuration();
    f.watermark(f.metadata, 4).await;
    f.watermark(f.vector, 3).await;
    sqlx::query("CREATE TRIGGER reject_purge BEFORE DELETE ON embeddings_queue BEGIN SELECT RAISE(ABORT, 'injected purge failure'); END")
        .execute(f.db.get_conn()).await.unwrap();
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let collection_id = f.collection.collection_id.to_string();
    let output = output
        .lines()
        .filter(|line| line.contains(&collection_id))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        output.contains("Failed to purge logs after backfill attempt"),
        "{output}"
    );
    assert!(output.contains("Error purging logs"), "{output}");
    assert!(
        output.contains(&f.collection.collection_id.to_string()),
        "{output}"
    );
    f.stop().await;
}

#[tokio::test]
async fn push_to_stopped_compactor_preserves_committed_records() {
    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    f.stop_compactor().await;
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(result, Err(SqlitePushLogsError::MessageSendingError(_))),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn empty_push_does_not_request_backfill_or_purge() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    f.stop_compactor().await;
    f.log
        .push_logs(f.collection.collection_id, vec![])
        .await
        .unwrap();
    assert_records(f.records().await, original);
    f.stop().await;
}

#[tokio::test]
async fn successful_push_purges_and_keeps_new_record() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.checkpoint().await;
    f.collection.dimension = None;
    f.sysdb.add_collection(f.collection.clone());
    f.watermark(f.metadata, 3).await;
    f.watermark(f.vector, 4).await;
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    f.log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await
        .unwrap();
    let mut expected: Vec<_> = original.into_iter().filter(|r| r.log_offset >= 3).collect();
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn uncheckpointed_vectors_keep_all_logs_for_replay() {
    let mut f = Fixture::new().await;
    let original = f.seed().await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    // The batch is below HNSW's persistence threshold: metadata has advanced,
    // but the vector checkpoint must still be absent.
    let reader = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 0)
    );
    f.purge().await.unwrap();
    assert_records(f.records().await, original);
    f.stop().await;
}

#[tokio::test]
async fn incompatible_index_configuration_never_purges_from_metadata_alone() {
    let mut f = Fixture::new().await;
    let mut expected = f.seed().await;
    f.watermark(f.metadata, 5).await;
    f.collection.schema = Some(Schema::new_default(KnnIndex::Spann));
    f.sysdb.add_collection(f.collection.clone());
    f.log.init_compactor_handle(f.handle.clone()).unwrap();
    let result = f
        .log
        .push_logs(f.collection.collection_id, vec![operation(5)])
        .await;
    assert!(
        matches!(
            result,
            Err(SqlitePushLogsError::CompactionError(
                CompactionManagerError::HnswReaderConstructionError(_)
            ))
        ),
        "{result:?}"
    );
    expected.push(LogRecord {
        log_offset: 6,
        record: operation(5),
    });
    assert_records(f.records().await, expected);
    f.stop().await;
}

#[tokio::test]
async fn vector_checkpoint_sqlite_failure_preserves_replay_logs() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    } else {
        panic!("expected HNSW configuration");
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    let original = f.seed().await;
    // Fail only vector checkpoint publication, after native persistence and
    // metadata compaction have succeeded.
    sqlx::query(&format!("CREATE TRIGGER reject_vector_checkpoint BEFORE INSERT ON max_seq_id WHEN NEW.segment_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END", f.vector))
        .execute(f.db.get_conn()).await.unwrap();
    let result = f
        .handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap();
    // The save in the background fails to publish its position: the backfill does not wait for it, and its error
    // goes to the log of the server.
    result.unwrap();
    f.saved().await;
    let reader = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 0)
    );
    f.purge().await.unwrap();
    assert_records(f.records().await, original);
    sqlx::query("DROP TRIGGER reject_vector_checkpoint")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    // Retrying with no new logs must still publish the failed checkpoint.
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    assert_eq!(reader.current_max_seq_id(&f.vector).await.unwrap(), 5);
    f.purge().await.unwrap();
    assert_eq!(f.records().await.len(), 1);
    f.stop().await;
}

// A purge asked while a save in the background writes the files of the index: it does not wait for the save, which would
// hold the compactor and every write with it, and it removes nothing, as the files cannot be inspected then; once the
// save ends, the purge comes again on its own and removes the records the save covers. Were the inspection to wait, the
// purge would end only after the save; were the purge not to come again, the records would stay until the next write.
#[tokio::test(flavor = "multi_thread")]
async fn a_purge_during_a_save_removes_nothing_and_comes_again_after_it() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    } else {
        panic!("expected HNSW configuration");
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    let original = f.seed().await;
    // The index of the compactor, with its saves held before they write the files until the test lets them go on.
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let writer = f
        .manager
        .get_hnsw_writer(&f.collection, &segments.vector_segment, 3)
        .await
        .unwrap();
    let (started_tx, mut started) = tokio::sync::mpsc::unbounded_channel();
    let (release, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    writer
        .index
        .set_save_hook(Some(std::sync::Arc::new(move || {
            let _ = started_tx.send(());
            let _ = release_rx.lock().unwrap().recv();
            Ok::<(), String>(())
        })))
        .await;
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), started.recv())
        .await
        .expect("a save started")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), f.purge())
        .await
        .expect("the purge waits for no save")
        .unwrap();
    assert_records(f.records().await, original.clone());
    release.send(()).unwrap();
    f.wait_for_records(original.into_iter().filter(|r| r.log_offset >= 5).collect())
        .await;
    drop(writer);
    f.stop().await;
}

#[tokio::test]
async fn valid_checkpoint_recovers_retained_tail_after_configuration_is_repaired() {
    let mut f = Fixture::new().await;
    if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
        &mut f.collection.config.vector_index
    {
        config.sync_threshold = 2;
    } else {
        panic!("expected HNSW configuration");
    }
    f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
    f.sysdb.add_collection(f.collection.clone());
    f.log
        .push_logs(f.collection.collection_id, (0..3).map(operation).collect())
        .await
        .unwrap();
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    let reader = SqliteMetadataReader::new(f.db.clone());
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (3, 3)
    );
    f.log
        .push_logs(f.collection.collection_id, (3..5).map(operation).collect())
        .await
        .unwrap();
    let original = f.records().await;

    // A wrong collection dimension must not prevent purging the checkpointed
    // prefix, or discard the uncheckpointed tail needed after configuration repair.
    f.restart_compactor().await;
    f.collection.dimension = Some(4);
    f.sysdb.add_collection(f.collection.clone());
    let result = f
        .handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            result,
            Err(CompactionManagerError::HnswReaderConstructionError(_))
        ),
        "{result:?}"
    );
    f.purge().await.unwrap();
    assert_records(
        f.records().await,
        original.into_iter().filter(|r| r.log_offset >= 3).collect(),
    );
    f.collection.dimension = Some(3);
    f.sysdb.add_collection(f.collection.clone());
    f.handle
        .request(
            BackfillMessage {
                collection_id: f.collection.collection_id,
            },
            None,
        )
        .await
        .unwrap()
        .unwrap();
    f.saved().await;
    assert_eq!(
        (
            reader.current_max_seq_id(&f.metadata).await.unwrap(),
            reader.current_max_seq_id(&f.vector).await.unwrap()
        ),
        (5, 5)
    );

    // Reopen from disk through a fresh manager; checking only the live writer
    // would miss a watermark that advanced ahead of persisted vectors.
    f.stop_compactor().await;
    let registry = Registry::new();
    registry.register(f.db.clone());
    let manager = LocalSegmentManager::try_from_config(
        &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
            "persist_path": f.path.path().to_str().unwrap()
        }))
        .unwrap(),
        &registry,
    )
    .await
    .unwrap();
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let hnsw = manager
        .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
        .await
        .unwrap();
    let mut embeddings = Vec::new();
    for id in 0..5 {
        embeddings.push(
            hnsw.get_embedding_by_user_id(&format!("id-{id}"))
                .await
                .unwrap(),
        );
    }
    assert_eq!(embeddings, vec![vec![1.0, 2.0, 3.0]; 5]);
    f.stop().await;
}

#[tokio::test]
async fn live_backfill_skips_applied_logs_but_reload_replays_retained_history() {
    for persistent in [false, true] {
        let mut f = Fixture::with_persistence(persistent).await;
        let original = f.seed().await;
        let message = || BackfillMessage {
            collection_id: f.collection.collection_id,
        };
        f.handle.request(message(), None).await.unwrap().unwrap();
        assert_eq!(
            max_purge_seq_id(&f.db, &f.metadata, &f.vector)
                .await
                .unwrap(),
            0
        );
        // Poison only an already-applied log's encoding: decoding old records
        // again would fail, even though filtering later skips their mutations.
        sqlx::query("UPDATE embeddings_queue SET encoding = 'invalid' WHERE seq_id = ?")
            .bind(original[0].log_offset)
            .execute(f.db.get_conn())
            .await
            .unwrap();
        f.log
            .push_logs(f.collection.collection_id, vec![operation(99)])
            .await
            .unwrap();
        f.handle.request(message(), None).await.unwrap().unwrap();
        f.purge().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue")
            .fetch_one(f.db.get_conn())
            .await
            .unwrap();
        assert_eq!(
            count, 6,
            "live replay progress must never authorize purging"
        );
        sqlx::query("UPDATE embeddings_queue SET encoding = 'FLOAT32' WHERE seq_id = ?")
            .bind(original[0].log_offset)
            .execute(f.db.get_conn())
            .await
            .unwrap();
        f.restart_compactor().await;
        f.handle
            .request(
                BackfillMessage {
                    collection_id: f.collection.collection_id,
                },
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(f.records().await.len(), 6);
        let segments = SysDb::Test(f.sysdb.clone())
            .get_collection_with_segments(None, f.collection.collection_id)
            .await
            .unwrap();
        let reader = f
            .manager
            .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
            .await
            .unwrap();
        for id in [0, 1, 2, 3, 4, 99] {
            assert_eq!(
                reader
                    .get_embedding_by_user_id(&format!("id-{id}"))
                    .await
                    .unwrap(),
                vec![1.0, 2.0, 3.0]
            );
        }
        drop(reader);
        f.stop().await;
    }
}

#[tokio::test]
async fn recovered_short_tail_checkpoints_before_purge_and_retries_publication() {
    for fail_publication in [false, true] {
        let mut f = Fixture::new().await;
        f.seed().await;
        f.checkpoint().await;
        let folder = f.path.path().join(f.vector.to_string());
        let pickle = folder.join("index_metadata.pickle");
        let old_map = std::fs::read(&pickle).unwrap();
        f.log
            .push_logs(f.collection.collection_id, vec![operation(5), operation(6)])
            .await
            .unwrap();
        f.checkpoint().await;
        // Interrupt the save after native files, before the pickle and watermark.
        std::fs::write(&pickle, old_map).unwrap();
        f.watermark(f.metadata, 5).await;
        f.watermark(f.vector, 5).await;
        assert!(
            chroma_segment::local_hnsw::inspect_persisted_hnsw_index(&folder)
                .unwrap()
                .recovery_required
        );
        // The checkpoint helper used a threshold of 2. Reopen with a higher
        // threshold so replaying the two retained records cannot normally save.
        if let chroma_types::VectorIndexConfiguration::Hnsw(config) =
            &mut f.collection.config.vector_index
        {
            config.sync_threshold = 1000;
        }
        f.collection.schema = Some(Schema::try_from(&f.collection.config).unwrap());
        f.sysdb.add_collection(f.collection.clone());
        f.restart_compactor().await;
        if fail_publication {
            sqlx::query(&format!("CREATE TRIGGER reject_recovery_checkpoint BEFORE INSERT ON max_seq_id WHEN NEW.segment_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END", f.vector))
                .execute(f.db.get_conn()).await.unwrap();
        }
        let replay = f
            .handle
            .request(
                BackfillMessage {
                    collection_id: f.collection.collection_id,
                },
                None,
            )
            .await
            .unwrap();
        // The save in the background, which fails to publish its position when fail_publication: the backfill does not
        // wait for it, and its error goes to the log of the server.
        replay.unwrap();
        f.saved().await;
        if fail_publication {
            assert_eq!(
                SqliteMetadataReader::new(f.db.clone())
                    .current_max_seq_id(&f.vector)
                    .await
                    .unwrap(),
                5
            );
            sqlx::query("DROP TRIGGER reject_recovery_checkpoint")
                .execute(f.db.get_conn())
                .await
                .unwrap();
            // No new logs: the recovery checkpoint must remain pending even
            // though native mutations and file publication already succeeded.
            f.handle
                .request(
                    BackfillMessage {
                        collection_id: f.collection.collection_id,
                    },
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            f.saved().await;
        }
        assert_eq!(
            SqliteMetadataReader::new(f.db.clone())
                .current_max_seq_id(&f.vector)
                .await
                .unwrap(),
            7
        );
        assert!(
            !chroma_segment::local_hnsw::inspect_persisted_hnsw_index(&folder)
                .unwrap()
                .recovery_required
        );
        f.purge().await.unwrap();
        let retained = f.records().await;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].log_offset, 7);
        f.stop().await;
    }
}

#[tokio::test]
async fn watermark_read_failure_invalidates_replay_completion() {
    let mut f = Fixture::new().await;
    f.seed().await;
    let message = BackfillMessage {
        collection_id: f.collection.collection_id,
    };
    f.handle
        .request(message.clone(), None)
        .await
        .unwrap()
        .unwrap();
    let segments = SysDb::Test(f.sysdb.clone())
        .get_collection_with_segments(None, f.collection.collection_id)
        .await
        .unwrap();
    let reader = f
        .manager
        .get_hnsw_reader(&f.collection, &segments.vector_segment, 3)
        .await
        .unwrap();
    assert!(reader.index.replay_complete().await);
    f.log
        .push_logs(f.collection.collection_id, vec![operation(99)])
        .await
        .unwrap();

    // Fail the metadata watermark query while keeping the loaded index usable.
    sqlx::query("ALTER TABLE max_seq_id RENAME TO saved_max_seq_id")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    let result = f.handle.request(message.clone(), None).await.unwrap();
    assert!(matches!(
        result,
        Err(CompactionManagerError::MetadataReaderError(_))
    ));
    assert!(!reader.index.replay_complete().await);

    sqlx::query("ALTER TABLE saved_max_seq_id RENAME TO max_seq_id")
        .execute(f.db.get_conn())
        .await
        .unwrap();
    f.handle.request(message, None).await.unwrap().unwrap();
    assert!(reader.index.replay_complete().await);
    assert_eq!(
        reader
            .get_embedding_by_user_id(&"id-99".to_string())
            .await
            .unwrap(),
        vec![1.0, 2.0, 3.0]
    );
    drop(reader);
    f.stop().await;
}

// The replaced versions of SPANN segments that the compactor remembers are those that a flush can still remove: a
// segment that no flush has touched for longer than the grace, as one of a deleted collection, leaves nothing in memory.
#[tokio::test]
async fn replaced_spann_versions_are_forgotten_after_the_grace() {
    let db = chroma_sqlite::db::test_utils::get_new_sqlite_db().await;
    let registry = Registry::new();
    registry.register(db.clone());
    let manager = LocalSegmentManager::try_from_config(
        &serde_json::from_value::<LocalSegmentManagerConfig>(serde_json::json!({
            "persist_path": null
        }))
        .unwrap(),
        &registry,
    )
    .await
    .unwrap();
    let mut compactor = LocalCompactionManager {
        log: Log::Sqlite(
            SqliteLog::new(db.clone(), "default".into(), "default".into())
                .with_purge_cutoff_time_seconds(0),
        ),
        sqlite_db: db.clone(),
        hnsw_segment_manager: manager,
        metrics: LocalCompactionMetrics::default(),
        sysdb: SysDb::Test(TestSysDb::new()),
        purged: HashSet::new(),
        purges: PurgeSchedule::new(Duration::ZERO),
        purging: false,
        spann_versions: HashMap::new(),
        spann_version_grace: Duration::ZERO,
        compaction_interval: Duration::from_secs(10),
        min_compaction_size: 10,
        max_compaction_size: 10_000,
        max_partition_size: 5_000,
        max_concurrent_jobs: 50,
        compaction_timeout: Duration::from_secs(86_400),
        spann_flushes: SpannFlushes::default(),
        spann_flush_stop: None,
        background: tokio_util::task::TaskTracker::new(),
        shutdown_grace: Duration::from_secs(30),
        function_runs: Default::default(),
        sync_retries: Default::default(),
        function_storage: None,
        purge_after_save: None,
    };
    let collection = Collection::test_collection(3);
    for _ in 0..100 {
        let segment = Segment {
            id: SegmentUuid::new(),
            scope: SegmentScope::VECTOR,
            r#type: SegmentType::Spann,
            collection: collection.collection_id,
            metadata: None,
            file_path: HashMap::from([(
                "hnsw_index".to_string(),
                vec!["tenant/t/database/d/collection/c/segment/s/hnsw/i".to_string()],
            )]),
        };
        compactor
            .remove_replaced_versions(&collection, &segment, HashMap::new())
            .await
            .unwrap();
    }
    assert!(
        compactor.spann_versions.len() <= 1,
        "{} segments remembered",
        compactor.spann_versions.len()
    );
}

fn partition_log(ids: &[&str]) -> Chunk<LogRecord> {
    let logs: Vec<LogRecord> = ids
        .iter()
        .enumerate()
        .map(|(offset, id)| LogRecord {
            log_offset: offset as i64,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        })
        .collect();
    Chunk::new(logs.into())
}

fn partition_ids(partition: &Chunk<LogRecord>) -> Vec<String> {
    partition
        .iter()
        .map(|(log, _)| log.record.id.clone())
        .collect()
}

// Federico: «Ordine fisso». The partitions follow the first record of each id in the log, whatever the hashes of the ids:
// with the order of a HashMap, as distributed Chroma, this fails.
#[test]
fn partitions_follow_the_order_of_the_log() {
    let ids: Vec<String> = (0..1000).map(|i| format!("id{i}")).collect();
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let partitions = partition_records(&partition_log(&refs), 10, false);
    assert_eq!(partitions.len(), 100);
    let mut seen = Vec::new();
    for partition in &partitions {
        seen.extend(partition_ids(partition));
    }
    assert_eq!(seen, ids);
}

#[test]
fn all_the_records_of_an_id_stay_in_one_partition() {
    let partitions = partition_records(&partition_log(&["a", "b", "a", "c", "b", "a"]), 2, false);
    assert_eq!(partition_ids(&partitions[0]), ["a", "a", "a"]);
    assert_eq!(partition_ids(&partitions[1]), ["b", "b"]);
    assert_eq!(partition_ids(&partitions[2]), ["c"]);
}

#[test]
fn the_chunks_of_a_document_stay_together_only_when_the_collection_asks() {
    let log = partition_log(&["doc-0", "x", "doc-1", "report-2024-a"]);
    let grouped = partition_records(&log, 1, true);
    assert_eq!(partition_ids(&grouped[0]), ["doc-0", "doc-1"]);
    let apart = partition_records(&log, 1, false);
    assert_eq!(partition_ids(&apart[0]), ["doc-0"]);
    assert_eq!(apart.len(), 4);
}

// The tasks of a flush run on the threads of the flushes, not on those of whoever spawns them, at most 4 at once, as the 4
// worker threads of the dispatcher of the compactor of distributed Chroma.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_tasks_run_on_their_own_threads_at_most_four_at_once() {
    let running = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let most = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for _ in 0..16 {
        let (running, most) = (running.clone(), most.clone());
        spawn_flush_task(&mut tasks, async move {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            most.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            running.fetch_sub(1, Ordering::SeqCst);
            Ok(std::thread::current().name().map(str::to_string))
        });
    }
    while let Some(thread) = next_flush_task(&mut tasks).await.unwrap() {
        assert_eq!(thread.as_deref(), Some(FLUSH_THREAD_NAME));
    }
    let most = most.load(Ordering::SeqCst);
    assert!(
        (2..=FLUSH_THREADS).contains(&most),
        "{most} tasks ran at once"
    );
}

// A failed task ends the flush at once: its error comes while another task still runs, and the end of the tasks stops that
// one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_task_ends_the_others() {
    let (running, stopped) = oneshot::channel::<()>();
    let mut tasks = JoinSet::new();
    spawn_flush_task(&mut tasks, async move {
        let _running = running;
        std::future::pending::<()>().await;
        Ok(())
    });
    spawn_flush_task(&mut tasks, async {
        Err(CompactionManagerError::SpannFlushPanicked)
    });
    let failed = tokio::time::timeout(Duration::from_secs(30), next_flush_task(&mut tasks))
        .await
        .expect("the error did not come in 30 s, the limit of the wait");
    assert!(matches!(
        failed,
        Err(CompactionManagerError::SpannFlushPanicked)
    ));
    drop(tasks);
    tokio::time::timeout(Duration::from_secs(30), stopped)
        .await
        .expect("the other task did not stop in 30 s, the limit of the wait")
        .unwrap_err();
}

// The files of a flush of 30 records, in partitions of 7, of a SPANN collection with no files yet, with a stage that fails.
async fn write_spann_failing_at(
    fail_at: Option<FlushStage>,
) -> Result<Option<SpannFlush>, CompactionManagerError> {
    let f = Fixture::new().await;
    let mut collection = f.collection.clone();
    collection.schema = Some(Schema::new_default(KnnIndex::Spann));
    let segment = |r#type, scope| Segment {
        id: SegmentUuid::new(),
        r#type,
        scope,
        collection: collection.collection_id,
        metadata: None,
        file_path: Default::default(),
    };
    let collection_and_segments = CollectionAndSegments {
        metadata_segment: segment(SegmentType::Sqlite, SegmentScope::METADATA),
        record_segment: segment(SegmentType::BlockfileRecord, SegmentScope::RECORD),
        vector_segment: segment(SegmentType::Spann, SegmentScope::VECTOR),
        collection: collection.clone(),
    };
    let flusher = SpannFlusher {
        log: Log::Sqlite(f.log.clone()),
        sqlite_db: f.db.clone(),
        hnsw_segment_manager: f.manager.clone(),
        sysdb: SysDb::Test(f.sysdb.clone()),
        max_compaction_size: 50_000,
        max_partition_size: 7,
        stop: None,
        fail_at,
    };
    let logs: Vec<LogRecord> = (0..30)
        .map(|id| LogRecord {
            log_offset: id as i64 + 1,
            record: OperationRecord {
                embedding: Some(vec![id as f32, 1.0, 2.0]),
                ..operation(id)
            },
        })
        .collect();
    let flush =
        Box::pin(flusher.write_spann(&collection_and_segments, 3, Chunk::new(logs.into()))).await;
    f.stop().await;
    flush
}

// A failure while the records are applied or the segments committed fails the flush. With the error lost, a flush that
// failed would look done, and its records would be registered as flushed without being in its files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_fails_when_any_of_its_stages_fails() {
    let done = write_spann_failing_at(None).await.unwrap().unwrap();
    assert_eq!(done.log_position, 30);
    for stage in [
        FlushStage::RecordApply,
        FlushStage::SpannApply,
        FlushStage::RecordCommit,
        FlushStage::SpannCommit,
    ] {
        let flush = write_spann_failing_at(Some(stage)).await;
        assert!(
            matches!(flush, Err(CompactionManagerError::SpannFlushPanicked)),
            "{stage:?}: the flush did not fail"
        );
    }
}
