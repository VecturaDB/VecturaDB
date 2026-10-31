// The records of the log after the last flush of a SPANN collection: the reads merge them with the files at one moment, as
// distributed Chroma merges the logs that its compaction has not applied, the count and the indexing status count them,
// and the timer flushes them, with the thresholds of distributed Chroma. The tests run on a large stack, as those of the
// other reads of SPANN.

use super::*;

// The reads of a SPANN collection, each in turn.
const READS: [SpannRead; 4] = [
    SpannRead::Get,
    SpannRead::Query,
    SpannRead::Search,
    SpannRead::Count,
];

// What the reads of the collection of the stopped reads answer otherwise than the records after the writes given.
async fn reads_differ(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    writes: usize,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for read in READS {
        match spann_read(frontend, collection_id, read).await {
            Ok(answer) => wrong.extend(
                differences_from(&answer, writes)
                    .into_iter()
                    .map(|difference| format!("{read:?}: {difference}")),
            ),
            Err(err) => wrong.push(format!("{read:?}: {err}")),
        }
    }
    wrong
}

// The indexing status of a collection: its progress, its operations not indexed, those indexed and their total.
async fn status(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
) -> (f32, u64, u64, u64) {
    let status = frontend
        .indexing_status(
            TENANT.to_string(),
            DatabaseName::new(DATABASE).unwrap(),
            collection_id,
        )
        .await
        .unwrap();
    (
        status.op_indexing_progress,
        status.num_unindexed_ops,
        status.num_indexed_ops,
        status.total_ops,
    )
}

// The files of the vector segment of a collection, empty before its first flush.
async fn vector_files(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
) -> std::collections::HashMap<String, Vec<String>> {
    frontend
        .sysdb_client
        .get_collection_with_segments(None, collection_id)
        .await
        .unwrap()
        .vector_segment
        .file_path
}

// A SPANN collection with no record yet.
async fn empty_spann_collection(frontend: &mut ServiceBasedFrontend, name: &str) -> CollectionUuid {
    frontend
        .create_collection(
            CreateCollectionRequest::try_new(
                TENANT.to_string(),
                DatabaseName::new(DATABASE).unwrap(),
                name.to_string(),
                None,
                None,
                Some(Schema::new_default(KnnIndex::Spann)),
                false,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .collection_id
}

// The records given, added with the vectors of their numbers and without a flush.
async fn add_numbers(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    numbers: std::ops::Range<usize>,
) {
    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                numbers.clone().map(|i| format!("r{i}")).collect(),
                numbers.map(|i| vec![i as f32, (i as f32).sin()]).collect(),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

// The reads of a SPANN collection merge the records of its log after its last flush with its files: r0, changed in the
// log, answers as the log has it, and the records added in the log answer with the others, before and after their
// flush.
#[test]
fn spann_reads_merge_the_log_after_the_last_flush() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        spann_reads_merge_the_log_after_the_last_flush_impl,
    )
}

async fn spann_reads_merge_the_log_after_the_last_flush_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    for write in 1..=2 {
        write_to_stop(&mut frontend, collection_id, write).await;
    }
    let before = reads_differ(&mut frontend, collection_id, 2).await;
    assert!(before.is_empty(), "before the flush: {before:#?}");
    flush_spann(&frontend, collection_id).await;
    let after = reads_differ(&mut frontend, collection_id, 2).await;
    assert!(after.is_empty(), "after the flush: {after:#?}");
}

// A read of a SPANN collection with records in its log answers for the moment of its snapshot of SQLite, the files and
// the log of that moment, while the flush of that log and a write with its flush come between its snapshot and its
// files. With the grace, the files of its moment stay; without it, the second flush removes them, and the read reads
// again from a new snapshot, as distributed Chroma does: it answers for the last flush.
#[test]
fn a_spann_read_with_a_log_answers_for_its_snapshot_across_flushes() {
    on_a_large_stack(
        TestRuntime::FourWorkers,
        a_spann_read_with_a_log_answers_for_its_snapshot_across_flushes_impl,
    )
}

async fn a_spann_read_with_a_log_answers_for_its_snapshot_across_flushes_impl() {
    let mut wrong = Vec::new();
    for (grace, answer_after) in [
        (chroma_log::SpannVersionGrace::default().0, 1),
        (std::time::Duration::ZERO, 2),
    ] {
        for read in READS {
            let root = tempfile::tempdir().unwrap();
            let registry = Registry::new();
            registry.register(chroma_log::SpannVersionGrace(grace));
            let mut frontend = frontend_with_spann_registry(root.path(), true, &registry).await;
            let collection_id = collection_to_stop(&mut frontend).await;
            write_to_stop(&mut frontend, collection_id, 1).await;
            let Some((go_on, reading)) = stopped_read(&frontend, collection_id, read).await else {
                wrong.push(format!("{read:?}: the read did not stop before its files"));
                continue;
            };
            let flushed = tokio::time::timeout(STOP_WAIT, async {
                flush_spann(&frontend, collection_id).await;
                write_to_stop(&mut frontend, collection_id, 2).await;
                flush_spann(&frontend, collection_id).await;
            })
            .await;
            if flushed.is_err() {
                wrong.push(format!(
                    "{read:?}: the flushes did not end within {STOP_WAIT:?} while the read stopped"
                ));
            }
            // The read takes its files from disk, as in read_across_flushes.
            registry
                .get::<chroma_segment::local_segment_manager::LocalSegmentManager>()
                .unwrap()
                .spann_provider()
                .unwrap()
                .blockfile_provider
                .clear()
                .await
                .unwrap();
            let _ = go_on.send(());
            wrong.extend(
                answer_differences(reading, answer_after)
                    .await
                    .into_iter()
                    .map(|difference| format!("{read:?}, grace {grace:?}: {difference}")),
            );
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

// The log that a read merges ends at the last record applied to the metadata: a record that a write put in the log and
// has not applied yet is not read, as a read of distributed Chroma reads no log past the upper bound of its offsets. The
// indexing status counts it among the operations not indexed. The next write applies it.
#[test]
fn the_log_of_a_spann_read_ends_at_the_records_applied_to_the_metadata() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_log_of_a_spann_read_ends_at_the_records_applied_to_the_metadata_impl,
    )
}

async fn the_log_of_a_spann_read_ends_at_the_records_applied_to_the_metadata_impl() {
    use chroma_config::Configurable;
    let root = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &registry).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    // A write applied and not flushed, so that the log of a read has records before the last one applied.
    write_to_stop(&mut frontend, collection_id, 1).await;
    // A log of its own, with no compaction manager: its writes reach the log and nothing else.
    let mut log = chroma_log::Log::Sqlite(
        chroma_log::sqlite_log::SqliteLog::try_from_config(&Default::default(), &registry)
            .await
            .unwrap(),
    );
    log.push_logs(
        TENANT,
        DatabaseName::new(DATABASE).unwrap(),
        collection_id,
        vec![chroma_types::OperationRecord {
            id: "late".to_string(),
            embedding: Some(vec![0.5, 0.5]),
            encoding: Some(chroma_types::ScalarEncoding::FLOAT32),
            metadata: None,
            document: Some("document late".to_string()),
            operation: chroma_types::Operation::Add,
        }],
        None,
        None,
    )
    .await
    .unwrap();
    let unapplied = reads_differ(&mut frontend, collection_id, 1).await;
    assert!(unapplied.is_empty(), "{unapplied:#?}");
    assert_eq!(
        status(&mut frontend, collection_id).await,
        (10.0 / 13.0, 3, 10, 13)
    );
    write_to_stop(&mut frontend, collection_id, 2).await;
    match spann_read(&mut frontend, collection_id, SpannRead::Get).await {
        Ok(ReadAnswer::Records(records)) => {
            let ids = records[0]
                .iter()
                .map(|record| record.0.as_str())
                .collect::<Vec<_>>();
            assert_eq!(ids.len(), 13, "{ids:?}");
            assert!(ids.contains(&"late"), "{ids:?}");
        }
        other => panic!("the get after the write answered {other:?}"),
    }
}

// After a restart, a SPANN collection reads the records of its log after its last flush, counts them in its indexing
// status, and flushes them.
#[test]
fn a_spann_collection_reads_its_log_after_a_restart() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_collection_reads_its_log_after_a_restart_impl,
    )
}

async fn a_spann_collection_reads_its_log_after_a_restart_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    for write in 1..=2 {
        write_to_stop(&mut frontend, collection_id, write).await;
    }
    drop(frontend);
    let mut reopened = frontend_with_spann(root.path(), true).await;
    let restarted = reads_differ(&mut reopened, collection_id, 2).await;
    assert!(restarted.is_empty(), "after the restart: {restarted:#?}");
    assert_eq!(
        status(&mut reopened, collection_id).await,
        (10.0 / 14.0, 4, 10, 14)
    );
    flush_spann(&reopened, collection_id).await;
    let flushed = reads_differ(&mut reopened, collection_id, 2).await;
    assert!(flushed.is_empty(), "after the flush: {flushed:#?}");
    assert_eq!(status(&mut reopened, collection_id).await, (1.0, 0, 12, 12));
}

// The records of the files that the log updates, overwrites or deletes answer as the log has them: an update of the
// document alone keeps the embedding of the files, an update of the embedding moves the record in a query, a delete
// leaves the record out, and an upsert overwrites it, as the filter of distributed Chroma leaves out the offsets that the
// logs overwrite.
#[test]
fn the_log_of_a_spann_collection_overrides_its_files() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_log_of_a_spann_collection_overrides_its_files_impl,
    )
}

async fn the_log_of_a_spann_collection_overrides_its_files_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    frontend
        .update(
            UpdateCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec!["r1".to_string(), "r4".to_string()],
                Some(vec![Some(vec![50.0, 50.0]), None]),
                Some(vec![
                    Some("document r1 changed".to_string()),
                    Some("document r4 changed".to_string()),
                ]),
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(vec!["r2".to_string()]),
                None,
                None,
            )
            .unwrap(),
            String::new(),
        )
        .await
        .unwrap();
    frontend
        .upsert(
            UpsertCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec!["r3".to_string()],
                vec![vec![60.0, 60.0]],
                Some(vec![Some("document r3 changed".to_string())]),
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // By id, as spann_read lists them.
    let expected = (0..10)
        .filter(|&i| i != 2)
        .map(|i| match i {
            1 => (
                "r1".to_string(),
                Some("document r1 changed".to_string()),
                Some(vec![50.0, 50.0]),
            ),
            3 => (
                "r3".to_string(),
                Some("document r3 changed".to_string()),
                Some(vec![60.0, 60.0]),
            ),
            4 => (
                "r4".to_string(),
                Some("document r4 changed".to_string()),
                Some(vec![4.0, 4f32.sin()]),
            ),
            _ => (
                format!("r{i}"),
                Some(format!("document r{i}")),
                Some(vec![i as f32, (i as f32).sin()]),
            ),
        })
        .collect::<Vec<_>>();
    for read in READS {
        match spann_read(&mut frontend, collection_id, read).await {
            Ok(ReadAnswer::Records(lists)) => {
                for list in lists {
                    assert_eq!(list, expected, "{read:?}");
                }
            }
            Ok(ReadAnswer::Count(count)) => assert_eq!(count, 9, "{read:?}"),
            Err(err) => panic!("{read:?}: {err}"),
        }
    }
    // The nearest to the vector of r1 in the files is not r1, which the log moved: the nearest to its new vector is.
    let nearest = query_ids(&mut frontend, collection_id, vec![1.0, 1f32.sin()], 1).await;
    assert_ne!(nearest, vec!["r1".to_string()]);
    let nearest = query_ids(&mut frontend, collection_id, vec![50.0, 50.0], 1).await;
    assert_eq!(nearest, vec!["r1".to_string()]);
}

// The indexing status of a SPANN collection counts the records of the files of its last flush as indexed and those of
// its log after that flush as not indexed, as the count counts both as records.
#[test]
fn the_indexing_status_of_a_spann_collection_counts_its_log() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_indexing_status_of_a_spann_collection_counts_its_log_impl,
    )
}

async fn the_indexing_status_of_a_spann_collection_counts_its_log_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = empty_spann_collection(&mut frontend, "statuses").await;
    let count = |frontend: &mut ServiceBasedFrontend| {
        let mut frontend = frontend.clone();
        async move {
            match spann_read(&mut frontend, collection_id, SpannRead::Count).await {
                Ok(ReadAnswer::Count(count)) => count,
                other => panic!("the count answered {other:?}"),
            }
        }
    };
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 0, 0));
    add_numbers(&mut frontend, collection_id, 0..3).await;
    assert_eq!(status(&mut frontend, collection_id).await, (0.0, 3, 0, 3));
    assert_eq!(count(&mut frontend).await, 3);
    flush_spann(&frontend, collection_id).await;
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 3, 3));
    add_numbers(&mut frontend, collection_id, 3..5).await;
    frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(vec!["r0".to_string()]),
                None,
                None,
            )
            .unwrap(),
            String::new(),
        )
        .await
        .unwrap();
    // Two adds and a delete wait, on three records indexed.
    assert_eq!(status(&mut frontend, collection_id).await, (0.5, 3, 3, 6));
    assert_eq!(count(&mut frontend).await, 4);
    flush_spann(&frontend, collection_id).await;
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 4, 4));
    assert_eq!(count(&mut frontend).await, 4);
}

// A round of the timer flushes a SPANN collection with at least min_compaction_size records after its last flush, and
// one whose oldest record after its last flush waited at least timeout_us, as the log service of distributed Chroma
// picks the collections to compact; it leaves the others in the log.
#[test]
fn a_round_of_the_timer_flushes_the_spann_collections_over_its_thresholds() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_round_of_the_timer_flushes_the_spann_collections_over_its_thresholds_impl,
    )
}

async fn a_round_of_the_timer_flushes_the_spann_collections_over_its_thresholds_impl() {
    let root = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &registry).await;
    let round = |frontend: &ServiceBasedFrontend| {
        let frontend = frontend.clone();
        async move { local_executor(&frontend).compaction_round().await.unwrap() }
    };
    let counted = empty_spann_collection(&mut frontend, "counted").await;
    let waited = empty_spann_collection(&mut frontend, "waited").await;
    add_numbers(&mut frontend, counted, 0..9).await;
    add_numbers(&mut frontend, waited, 0..1).await;
    round(&frontend).await;
    assert!(vector_files(&mut frontend, counted).await.is_empty());
    assert!(vector_files(&mut frontend, waited).await.is_empty());

    // The tenth record of the first collection reaches min_compaction_size, 10 by default.
    add_numbers(&mut frontend, counted, 9..10).await;
    round(&frontend).await;
    assert!(!vector_files(&mut frontend, counted).await.is_empty());
    assert_eq!(status(&mut frontend, counted).await, (1.0, 0, 10, 10));
    assert!(vector_files(&mut frontend, waited).await.is_empty());

    // The record of the second collection waited a day, the timeout_us of distributed Chroma.
    sqlx::query("UPDATE embeddings_queue SET created_at = datetime('now', '-1 day', '-1 minute')")
        .execute(registry.get::<SqliteDb>().unwrap().get_conn())
        .await
        .unwrap();
    round(&frontend).await;
    assert!(!vector_files(&mut frontend, waited).await.is_empty());
    assert_eq!(status(&mut frontend, waited).await, (1.0, 0, 1, 1));
}

// A flush takes at most max_compaction_size records of the log, as the fetch of the logs of a compaction of distributed
// Chroma: the next flushes take the rest, and the reads answer every record meanwhile.
#[test]
fn a_spann_flush_takes_at_most_max_compaction_size_records() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_flush_takes_at_most_max_compaction_size_records_impl,
    )
}

async fn a_spann_flush_takes_at_most_max_compaction_size_records_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_flushes(
        root.path(),
        crate::executor::config::LocalExecutorConfig {
            spann: true,
            ..Default::default()
        },
        chroma_log::LocalCompactionManagerConfig {
            max_compaction_size: 5,
            ..no_timer_flush()
        },
        &Registry::new(),
    )
    .await;
    let collection_id = empty_spann_collection(&mut frontend, "limited").await;
    add_numbers(&mut frontend, collection_id, 0..12).await;
    for (indexed, waiting) in [(5, 7), (10, 2), (12, 0)] {
        flush_spann(&frontend, collection_id).await;
        assert_eq!(
            status(&mut frontend, collection_id).await,
            (indexed as f32 / 12.0, waiting, indexed, 12)
        );
        match spann_read(&mut frontend, collection_id, SpannRead::Get).await {
            Ok(ReadAnswer::Records(records)) => assert_eq!(records[0].len(), 12),
            other => panic!("the get answered {other:?}"),
        }
    }
}

// The timer runs a round every compaction_interval_sec, from the start of the server: a record written waits for the
// next round, which flushes it.
#[test]
fn the_timer_flushes_every_compaction_interval() {
    on_a_large_stack(
        TestRuntime::FourWorkers,
        the_timer_flushes_every_compaction_interval_impl,
    )
}

async fn the_timer_flushes_every_compaction_interval_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_flushes(
        root.path(),
        crate::executor::config::LocalExecutorConfig {
            spann: true,
            ..Default::default()
        },
        chroma_log::LocalCompactionManagerConfig {
            compaction_interval_sec: 1,
            min_compaction_size: 1,
            ..Default::default()
        },
        &Registry::new(),
    )
    .await;
    let collection_id = empty_spann_collection(&mut frontend, "timed").await;
    add_numbers(&mut frontend, collection_id, 0..3).await;
    let flushed = tokio::time::timeout(STOP_WAIT, async {
        while status(&mut frontend, collection_id).await != (1.0, 0, 3, 3) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        flushed.is_ok(),
        "no round flushed the collection within {STOP_WAIT:?}"
    );
    assert!(!vector_files(&mut frontend, collection_id).await.is_empty());
}

// One record added to a collection, with its vector and its document, and no flush.
async fn add_one(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    id: &str,
    vector: Vec<f32>,
    document: &str,
) {
    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec![id.to_string()],
                vec![vector],
                Some(vec![Some(document.to_string())]),
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

// The records given deleted from a collection, without a flush.
async fn delete_ids(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    ids: Vec<String>,
) {
    frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(ids),
                None,
                None,
            )
            .unwrap(),
            String::new(),
        )
        .await
        .unwrap();
}

// What the reads of a collection answer otherwise than the records given, by id, and their number.
async fn reads_differ_from(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    expected: &[ReadRecord],
) -> Vec<String> {
    let mut wrong = Vec::new();
    for read in READS {
        match spann_read(frontend, collection_id, read).await {
            Ok(ReadAnswer::Records(lists)) => {
                for list in lists {
                    if list != expected {
                        wrong.push(format!("{read:?}: {list:?}"));
                    }
                }
            }
            Ok(ReadAnswer::Count(count)) if count as usize == expected.len() => {}
            Ok(ReadAnswer::Count(count)) => wrong.push(format!("{read:?}: {count}")),
            Err(err) => wrong.push(format!("{read:?}: {err}")),
        }
    }
    wrong
}

// The ids that a get of a collection answers, with the filter, the limit and the offset given.
async fn get_ids(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    where_clause: Option<chroma_types::Where>,
    limit: Option<u32>,
    offset: u32,
) -> Vec<String> {
    frontend
        .get(
            GetRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                where_clause,
                limit,
                offset,
                chroma_types::IncludeList(vec![]),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .ids
}

// A SPANN collection deleted with records in its log takes them with it: its deletion removes its log, and the next round
// of the timer flushes the other collection and finds nothing of it.
#[test]
fn a_spann_collection_deleted_with_a_log_leaves_nothing_to_flush() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_collection_deleted_with_a_log_leaves_nothing_to_flush_impl,
    )
}

async fn a_spann_collection_deleted_with_a_log_leaves_nothing_to_flush_impl() {
    let root = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &registry).await;
    let deleted = empty_spann_collection(&mut frontend, "deleted").await;
    let kept = empty_spann_collection(&mut frontend, "kept").await;
    add_numbers(&mut frontend, deleted, 0..12).await;
    add_numbers(&mut frontend, kept, 0..12).await;
    frontend
        .delete_collection(delete_collection_request("deleted"))
        .await
        .unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue WHERE topic LIKE ?")
        .bind(format!("%{deleted}"))
        .fetch_one(registry.get::<SqliteDb>().unwrap().get_conn())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    local_executor(&frontend).compaction_round().await.unwrap();
    assert_eq!(status(&mut frontend, kept).await, (1.0, 0, 12, 12));
    assert!(files_under(root.path().join("tenant").as_path())
        .iter()
        .all(|file| !file.to_string_lossy().contains(&deleted.to_string())));
}

// One round of the timer flushes every SPANN collection over its thresholds, one after the other, and leaves an HNSW
// collection as it is.
#[test]
fn a_round_of_the_timer_flushes_every_spann_collection_over_its_thresholds() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_round_of_the_timer_flushes_every_spann_collection_over_its_thresholds_impl,
    )
}

async fn a_round_of_the_timer_flushes_every_spann_collection_over_its_thresholds_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let first = empty_spann_collection(&mut frontend, "first").await;
    let second = empty_spann_collection(&mut frontend, "second").await;
    let hnsw = create_collection_for(&mut frontend, TENANT, DATABASE, "hnsw")
        .await
        .collection_id;
    add_numbers(&mut frontend, first, 0..10).await;
    add_numbers(&mut frontend, second, 0..15).await;
    add_numbers(&mut frontend, hnsw, 0..12).await;
    local_executor(&frontend).compaction_round().await.unwrap();
    assert_eq!(status(&mut frontend, first).await, (1.0, 0, 10, 10));
    assert_eq!(status(&mut frontend, second).await, (1.0, 0, 15, 15));
    assert_eq!(status(&mut frontend, hnsw).await, (1.0, 0, 12, 12));
    assert_eq!(get_ids(&mut frontend, hnsw, None, None, 0).await.len(), 12);
}

// A record that the log changes thirty times answers its last change, before and after the flush, among thirty records
// that the log adds.
#[test]
fn a_record_changed_many_times_in_the_log_answers_its_last_change() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_record_changed_many_times_in_the_log_answers_its_last_change_impl,
    )
}

async fn a_record_changed_many_times_in_the_log_answers_its_last_change_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    for write in 1..=30 {
        write_to_stop(&mut frontend, collection_id, write).await;
    }
    let before = reads_differ(&mut frontend, collection_id, 30).await;
    assert!(before.is_empty(), "before the flush: {before:#?}");
    flush_spann(&frontend, collection_id).await;
    let after = reads_differ(&mut frontend, collection_id, 30).await;
    assert!(after.is_empty(), "after the flush: {after:#?}");
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 40, 40));
}

// A record that the log adds and deletes is not read, and a record of the files that the log deletes and adds again
// answers as the log adds it, before and after the flush.
#[test]
fn records_added_and_deleted_in_the_log_answer_as_the_log_leaves_them() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        records_added_and_deleted_in_the_log_answer_as_the_log_leaves_them_impl,
    )
}

async fn records_added_and_deleted_in_the_log_answer_as_the_log_leaves_them_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    add_one(
        &mut frontend,
        collection_id,
        "x",
        vec![0.5, 0.5],
        "document x",
    )
    .await;
    delete_ids(&mut frontend, collection_id, vec!["x".to_string()]).await;
    delete_ids(&mut frontend, collection_id, vec!["r3".to_string()]).await;
    add_one(
        &mut frontend,
        collection_id,
        "r3",
        vec![30.0, 30.0],
        "document r3 again",
    )
    .await;
    let mut expected = records_after(0);
    expected[3] = (
        "r3".to_string(),
        Some("document r3 again".to_string()),
        Some(vec![30.0, 30.0]),
    );
    let before = reads_differ_from(&mut frontend, collection_id, &expected).await;
    assert!(before.is_empty(), "before the flush: {before:#?}");
    flush_spann(&frontend, collection_id).await;
    let after = reads_differ_from(&mut frontend, collection_id, &expected).await;
    assert!(after.is_empty(), "after the flush: {after:#?}");
}

// A collection whose records the log deletes, all of them, reads none, counts none, and its indexing status waits for the
// deletes; after the flush it holds nothing.
#[test]
fn a_spann_collection_whose_log_deletes_every_record_reads_none() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_collection_whose_log_deletes_every_record_reads_none_impl,
    )
}

async fn a_spann_collection_whose_log_deletes_every_record_reads_none_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    delete_ids(
        &mut frontend,
        collection_id,
        (0..10).map(|i| format!("r{i}")).collect(),
    )
    .await;
    let before = reads_differ_from(&mut frontend, collection_id, &[]).await;
    assert!(before.is_empty(), "before the flush: {before:#?}");
    assert_eq!(
        status(&mut frontend, collection_id).await,
        (0.5, 10, 10, 20)
    );
    flush_spann(&frontend, collection_id).await;
    let after = reads_differ_from(&mut frontend, collection_id, &[]).await;
    assert!(after.is_empty(), "after the flush: {after:#?}");
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 0, 0));
}

// The filters of a get and of a query by metadata and by document read the records of the log with those of the files,
// as they read SQLite, which holds both.
#[test]
fn the_filters_of_a_spann_read_take_the_records_of_the_log() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_filters_of_a_spann_read_take_the_records_of_the_log_impl,
    )
}

async fn the_filters_of_a_spann_read_take_the_records_of_the_log_impl() {
    use chroma_types::{
        DocumentExpression, DocumentOperator, MetadataComparison, MetadataExpression,
        PrimitiveOperator, Where,
    };
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = empty_spann_collection(&mut frontend, "filters").await;
    let add = |numbers: std::ops::Range<usize>| {
        AddCollectionRecordsRequest::try_new(
            TENANT.to_string(),
            DATABASE.to_string(),
            collection_id,
            numbers.clone().map(|i| format!("r{i}")).collect(),
            numbers
                .clone()
                .map(|i| vec![i as f32, (i as f32).sin()])
                .collect(),
            Some(
                numbers
                    .clone()
                    .map(|i| Some(format!("document r{i}")))
                    .collect(),
            ),
            None,
            Some(
                numbers
                    .map(|i| {
                        Some(chroma_types::Metadata::from([(
                            "n".to_string(),
                            MetadataValue::Int(i as i64),
                        )]))
                    })
                    .collect(),
            ),
        )
        .unwrap()
    };
    frontend.add(add(0..10)).await.unwrap();
    flush_spann(&frontend, collection_id).await;
    frontend.add(add(10..15)).await.unwrap();
    let at_least = |n: i64| {
        Where::Metadata(MetadataExpression {
            key: "n".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::GreaterThanOrEqual,
                MetadataValue::Int(n),
            ),
        })
    };
    assert_eq!(
        get_ids(&mut frontend, collection_id, Some(at_least(8)), None, 0).await,
        (8..15).map(|i| format!("r{i}")).collect::<Vec<_>>()
    );
    let contains = Where::Document(DocumentExpression {
        operator: DocumentOperator::Contains,
        pattern: "r12".to_string(),
    });
    assert_eq!(
        get_ids(&mut frontend, collection_id, Some(contains), None, 0).await,
        vec!["r12".to_string()]
    );
    let mut nearest = frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                Some(at_least(12)),
                vec![vec![0.0, 0.0]],
                10,
                chroma_types::IncludeList::default_query(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .ids
        .remove(0);
    nearest.sort();
    assert_eq!(nearest, vec!["r12", "r13", "r14"]);
}

// A get with a limit and an offset takes its page across the records of the files and those of the log, in the order of
// the rows of SQLite.
#[test]
fn a_spann_get_pages_across_the_files_and_the_log() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_get_pages_across_the_files_and_the_log_impl,
    )
}

async fn a_spann_get_pages_across_the_files_and_the_log_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    add_numbers(&mut frontend, collection_id, 10..15).await;
    assert_eq!(
        get_ids(&mut frontend, collection_id, None, Some(4), 8).await,
        vec!["r8", "r9", "r10", "r11"]
    );
}

// A fork of a SPANN collection with records in its log copies them too, as the reads of the source give them, and its
// own flush keeps them; the source keeps its own.
#[test]
fn a_fork_of_a_spann_collection_takes_the_records_of_its_log() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_fork_of_a_spann_collection_takes_the_records_of_its_log_impl,
    )
}

async fn a_fork_of_a_spann_collection_takes_the_records_of_its_log_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let source = collection_to_stop(&mut frontend).await;
    for write in 1..=2 {
        write_to_stop(&mut frontend, source, write).await;
    }
    let fork = frontend
        .fork_collection(
            ForkCollectionRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                source,
                "stops-fork".to_string(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .collection_id;
    let forked = reads_differ(&mut frontend, fork, 2).await;
    assert!(forked.is_empty(), "the fork: {forked:#?}");
    flush_spann(&frontend, fork).await;
    let flushed = reads_differ(&mut frontend, fork, 2).await;
    assert!(flushed.is_empty(), "the fork after its flush: {flushed:#?}");
    let kept = reads_differ(&mut frontend, source, 2).await;
    assert!(kept.is_empty(), "the source: {kept:#?}");
}

// A reset with records in the log of a SPANN collection leaves a server whose new SPANN collection writes, reads and
// flushes, with no file of the old one.
#[test]
fn a_reset_with_a_spann_log_leaves_a_server_that_writes_and_flushes() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_reset_with_a_spann_log_leaves_a_server_that_writes_and_flushes_impl,
    )
}

async fn a_reset_with_a_spann_log_leaves_a_server_that_writes_and_flushes_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut config = FrontendConfig::sqlite_in_memory();
    config.allow_reset = true;
    config.sqlitedb.as_mut().unwrap().url =
        Some(root.path().join("chroma.sqlite3").to_str().unwrap().into());
    config.segment_manager.as_mut().unwrap().persist_path =
        Some(root.path().to_str().unwrap().into());
    config.executor = crate::executor::config::ExecutorConfig::Local(
        crate::executor::config::LocalExecutorConfig {
            spann: true,
            ..Default::default()
        },
    );
    config.local_compaction_manager = no_timer_flush();
    let mut frontend =
        ServiceBasedFrontend::try_from_config(&(config, System::new()), &Registry::new())
            .await
            .unwrap();
    let before = empty_spann_collection(&mut frontend, "before").await;
    add_numbers(&mut frontend, before, 0..12).await;
    frontend.reset().await.unwrap();
    let after = empty_spann_collection(&mut frontend, "after").await;
    add_numbers(&mut frontend, after, 0..12).await;
    local_executor(&frontend).compaction_round().await.unwrap();
    assert_eq!(status(&mut frontend, after).await, (1.0, 0, 12, 12));
    assert_eq!(get_ids(&mut frontend, after, None, None, 0).await.len(), 12);
    assert!(files_under(root.path().join("tenant").as_path())
        .iter()
        .all(|file| !file.to_string_lossy().contains(&before.to_string())));
}

// With the option spann off, a SPANN collection with records in its log still reads them, refuses a write, and the timer
// flushes them, as the replay of its log on a read flushed them before the timer: the files take the writes that the
// server accepted with the option on.
#[test]
fn with_the_option_off_a_spann_collection_reads_and_flushes_its_log() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        with_the_option_off_a_spann_collection_reads_and_flushes_its_log_impl,
    )
}

async fn with_the_option_off_a_spann_collection_reads_and_flushes_its_log_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = collection_to_stop(&mut frontend).await;
    for write in 1..=6 {
        write_to_stop(&mut frontend, collection_id, write).await;
    }
    drop(frontend);
    let mut off = frontend_with_spann(root.path(), false).await;
    let read = reads_differ(&mut off, collection_id, 6).await;
    assert!(read.is_empty(), "with the option off: {read:#?}");
    let written = off
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec!["refused".to_string()],
                vec![vec![0.5, 0.5]],
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await;
    assert!(
        written
            .as_ref()
            .is_err_and(|err| err.to_string().contains("need the executor option spann")),
        "{:?}",
        written.as_ref().err().map(ToString::to_string)
    );
    local_executor(&off).compaction_round().await.unwrap();
    assert_eq!(status(&mut off, collection_id).await, (1.0, 0, 16, 16));
    let flushed = reads_differ(&mut off, collection_id, 6).await;
    assert!(flushed.is_empty(), "after the flush: {flushed:#?}");
}

// The count of a collection at the read level given.
async fn count_at(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    read_level: chroma_types::plan::ReadLevel,
) -> u32 {
    frontend
        .count(
            CountRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                read_level,
            )
            .unwrap(),
        )
        .await
        .unwrap()
}

// The counts of a collection at IndexAndWal, IndexOnly and IndexAndBoundedWal.
async fn counts_at_the_read_levels(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
) -> Vec<u32> {
    use chroma_types::plan::ReadLevel;
    let mut counts = Vec::new();
    for read_level in [
        ReadLevel::IndexAndWal,
        ReadLevel::IndexOnly,
        ReadLevel::IndexAndBoundedWal,
    ] {
        counts.push(count_at(frontend, collection_id, read_level).await);
    }
    counts
}

// A count of a SPANN collection reads the log after its last flush at the read level of its request, as the count of
// distributed Chroma (rust/worker/src/execution/orchestration/count.rs): IndexAndWal all of it, IndexOnly none of it,
// IndexAndBoundedWal its first bounded_wal_limit records, 250 by default. 20 records flushed, 300 in the log.
#[test]
fn a_spann_count_reads_the_log_at_its_read_level() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_count_reads_the_log_at_its_read_level_impl,
    )
}

async fn a_spann_count_reads_the_log_at_its_read_level_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = empty_spann_collection(&mut frontend, "levels").await;
    add_numbers(&mut frontend, collection_id, 0..20).await;
    flush_spann(&frontend, collection_id).await;
    add_numbers(&mut frontend, collection_id, 20..320).await;
    assert_eq!(
        counts_at_the_read_levels(&mut frontend, collection_id).await,
        vec![320, 20, 270]
    );
    flush_spann(&frontend, collection_id).await;
    assert_eq!(
        counts_at_the_read_levels(&mut frontend, collection_id).await,
        vec![320, 320, 320]
    );
}

// At IndexAndBoundedWal a count of a SPANN collection reads the first bounded_wal_limit records of its log, as the
// config sets it, and as the log leaves them: 20 records flushed, then a log that deletes 5 of them and adds 300. A limit
// under the log reads part of it, the first records; a limit at its length or over it reads all of it, as IndexAndWal.
#[test]
fn a_spann_count_reads_the_first_records_of_the_log_up_to_its_limit() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_count_reads_the_first_records_of_the_log_up_to_its_limit_impl,
    )
}

async fn a_spann_count_reads_the_first_records_of_the_log_up_to_its_limit_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = empty_spann_collection(&mut frontend, "limits").await;
    add_numbers(&mut frontend, collection_id, 0..20).await;
    flush_spann(&frontend, collection_id).await;
    delete_ids(
        &mut frontend,
        collection_id,
        (0..5).map(|i| format!("r{i}")).collect(),
    )
    .await;
    add_numbers(&mut frontend, collection_id, 20..320).await;
    drop(frontend);
    for (limit, bounded) in [(3, 17), (5, 15), (100, 110), (305, 315), (1000, 315)] {
        let mut frontend = frontend_with_local_executor(
            root.path(),
            crate::executor::config::LocalExecutorConfig {
                spann: true,
                bounded_wal_limit: limit,
                ..Default::default()
            },
            &Registry::new(),
        )
        .await;
        assert_eq!(
            counts_at_the_read_levels(&mut frontend, collection_id).await,
            vec![315, 20, bounded],
            "limit {limit}"
        );
    }
}

// A frontend of the single node with SPANN on whose first flush out of the actor stops, with its log read and before it
// writes its files, until the test lets it go on.
async fn frontend_with_a_stopped_flush(
    root: &std::path::Path,
) -> (ServiceBasedFrontend, chroma_log::SpannFlushStop) {
    let registry = Registry::new();
    let stop = chroma_log::SpannFlushStop::default();
    registry.register(stop.clone());
    (
        frontend_with_spann_registry(root, true, &registry).await,
        stop,
    )
}

// A round of the timer on a task of its own, once its flush stopped.
async fn a_round_that_stops(
    frontend: &ServiceBasedFrontend,
    stop: &chroma_log::SpannFlushStop,
) -> tokio::task::JoinHandle<Result<(), Box<dyn ChromaError>>> {
    let executor = local_executor(frontend).clone();
    let round = tokio::spawn(async move { executor.compaction_round().await });
    tokio::time::timeout(std::time::Duration::from_secs(60), stop.reached())
        .await
        .expect("the flush did not reach its stop in 60 s, the limit of the wait");
    round
}

// A write answers while a flush of its collection runs, out of the actor, as a write of distributed Chroma does not wait
// for a compaction: its record waits for the next flush, and the reads see it. The flush then ends as the others.
#[test]
fn a_write_answers_while_a_flush_of_its_collection_runs() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_write_answers_while_a_flush_of_its_collection_runs_impl,
    )
}

async fn a_write_answers_while_a_flush_of_its_collection_runs_impl() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, stop) = frontend_with_a_stopped_flush(root.path()).await;
    let collection_id = empty_spann_collection(&mut frontend, "written").await;
    add_numbers(&mut frontend, collection_id, 0..12).await;
    let round = a_round_that_stops(&frontend, &stop).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        add_numbers(&mut frontend, collection_id, 12..13),
    )
    .await
    .expect("the write waited for the flush up to 10 s, the limit of the wait");
    assert_eq!(
        get_ids(&mut frontend, collection_id, None, None, 0)
            .await
            .len(),
        13
    );
    stop.go_on();
    round.await.unwrap().unwrap();
    assert_eq!(
        status(&mut frontend, collection_id).await,
        (12.0 / 13.0, 1, 12, 13)
    );
}

// A SPANN collection deleted while its first flush runs leaves no file: the flush, stopped before its files, writes them
// after the delete removed those of the collection, its registration fails, as the catalog no longer has the collection,
// and its files go with the segments that the catalog lost. The delete does not wait for the flush.
#[test]
fn a_spann_collection_deleted_during_its_flush_leaves_no_file() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_collection_deleted_during_its_flush_leaves_no_file_impl,
    )
}

async fn a_spann_collection_deleted_during_its_flush_leaves_no_file_impl() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, stop) = frontend_with_a_stopped_flush(root.path()).await;
    let deleted = empty_spann_collection(&mut frontend, "deleted").await;
    add_numbers(&mut frontend, deleted, 0..12).await;
    let round = a_round_that_stops(&frontend, &stop).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        frontend.delete_collection(delete_collection_request("deleted")),
    )
    .await
    .expect("the delete waited for the flush up to 10 s, the limit of the wait")
    .unwrap();
    stop.go_on();
    round.await.unwrap().unwrap();
    let files = files_under(root.path().join("tenant").as_path());
    assert!(
        files
            .iter()
            .all(|file| !file.to_string_lossy().contains(&deleted.to_string())),
        "{files:#?}"
    );
}

// A reset while the first flush of a SPANN collection runs leaves no file of it: the flush writes its files after the
// reset, its registration fails, and its files go, as after a delete. The reset does not wait for the flush.
#[test]
fn a_reset_during_a_spann_flush_leaves_no_file_of_it() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_reset_during_a_spann_flush_leaves_no_file_of_it_impl,
    )
}

async fn a_reset_during_a_spann_flush_leaves_no_file_of_it_impl() {
    let root = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let stop = chroma_log::SpannFlushStop::default();
    registry.register(stop.clone());
    let mut config = FrontendConfig::sqlite_in_memory();
    config.allow_reset = true;
    config.sqlitedb.as_mut().unwrap().url =
        Some(root.path().join("chroma.sqlite3").to_str().unwrap().into());
    config.segment_manager.as_mut().unwrap().persist_path =
        Some(root.path().to_str().unwrap().into());
    config.executor = crate::executor::config::ExecutorConfig::Local(
        crate::executor::config::LocalExecutorConfig {
            spann: true,
            ..Default::default()
        },
    );
    config.local_compaction_manager = no_timer_flush();
    let mut frontend = ServiceBasedFrontend::try_from_config(&(config, System::new()), &registry)
        .await
        .unwrap();
    let before = empty_spann_collection(&mut frontend, "before").await;
    add_numbers(&mut frontend, before, 0..12).await;
    let round = a_round_that_stops(&frontend, &stop).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), frontend.reset())
        .await
        .expect("the reset waited for the flush up to 10 s, the limit of the wait")
        .unwrap();
    stop.go_on();
    round.await.unwrap().unwrap();
    let files = files_under(root.path().join("tenant").as_path());
    assert!(
        files
            .iter()
            .all(|file| !file.to_string_lossy().contains(&before.to_string())),
        "{files:#?}"
    );
}

// A one-off flush asked while a flush of its collection runs gets a flush of its own after that one, with no error, as a
// one-off compaction of distributed Chroma asked during one in progress runs at a later round
// (rust/worker/src/compactor/scheduler.rs): it answers once its own flush took the records written meanwhile.
#[test]
fn a_one_off_flush_asked_during_a_flush_gets_its_own_after_it() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_one_off_flush_asked_during_a_flush_gets_its_own_after_it_impl,
    )
}

async fn a_one_off_flush_asked_during_a_flush_gets_its_own_after_it_impl() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, stop) = frontend_with_a_stopped_flush(root.path()).await;
    let collection_id = empty_spann_collection(&mut frontend, "queued").await;
    add_numbers(&mut frontend, collection_id, 0..12).await;
    let round = a_round_that_stops(&frontend, &stop).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        add_numbers(&mut frontend, collection_id, 12..15),
    )
    .await
    .expect("the write waited for the flush up to 10 s, the limit of the wait");
    let executor = local_executor(&frontend).clone();
    let one_off = tokio::spawn(async move { executor.flush_spann(collection_id).await });
    // The time for the request to reach the manager, which has nothing else to do: it waits for the flush in progress.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !one_off.is_finished(),
        "the one-off flush answered before the flush in progress ended"
    );
    stop.go_on();
    round.await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(60), one_off)
        .await
        .expect("the one-off flush did not end in 60 s, the limit of the wait")
        .unwrap()
        .unwrap();
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 15, 15));
}

// A round of the timer while a flush of a collection runs leaves that collection to a later round, as the scheduler of
// distributed Chroma skips a collection whose compaction is in progress: one flush per collection at a time.
#[test]
fn a_round_leaves_a_collection_whose_flush_runs_to_a_later_one() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_round_leaves_a_collection_whose_flush_runs_to_a_later_one_impl,
    )
}

async fn a_round_leaves_a_collection_whose_flush_runs_to_a_later_one_impl() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, stop) = frontend_with_a_stopped_flush(root.path()).await;
    let collection_id = empty_spann_collection(&mut frontend, "skipped").await;
    add_numbers(&mut frontend, collection_id, 0..12).await;
    let round = a_round_that_stops(&frontend, &stop).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        local_executor(&frontend).compaction_round(),
    )
    .await
    .expect("the second round did not end in 10 s, the limit of the wait")
    .unwrap();
    assert_eq!(status(&mut frontend, collection_id).await, (0.0, 12, 0, 12));
    stop.go_on();
    round.await.unwrap().unwrap();
    assert_eq!(status(&mut frontend, collection_id).await, (1.0, 0, 12, 12));
}

// A flush writes with the providers of the compactor: the HNSW index of the centers that it forks and changes is in their
// cache, not in the cache of the readers, where a reader would wait on its lock (see compaction_spann_provider).
#[test]
fn a_spann_flush_keeps_its_index_of_the_centers_out_of_the_cache_of_the_readers() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_flush_keeps_its_index_of_the_centers_out_of_the_cache_of_the_readers_impl,
    )
}

async fn a_spann_flush_keeps_its_index_of_the_centers_out_of_the_cache_of_the_readers_impl() {
    let root = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &registry).await;
    let collection_id = empty_spann_collection(&mut frontend, "centers_of_a_flush").await;
    add_numbers(&mut frontend, collection_id, 0..20).await;
    flush_spann(&frontend, collection_id).await;
    let files = vector_files(&mut frontend, collection_id).await;
    let (_, id) =
        chroma_types::Segment::extract_prefix_and_id(&files[chroma_types::HNSW_PATH][0]).unwrap();
    let id = chroma_index::IndexUuid(id);
    let manager = registry
        .get::<chroma_segment::local_segment_manager::LocalSegmentManager>()
        .unwrap();
    let cached = |provider: &chroma_segment::spann_provider::SpannProvider| {
        let provider = provider.hnsw_provider.clone();
        async move { provider.get(&id, &collection_id).await.is_some() }
    };
    assert!(cached(manager.compaction_spann_provider().unwrap()).await);
    assert!(!cached(manager.spann_provider().unwrap()).await);
}

// As the log position of distributed Chroma, which its compaction moves: a SPANN collection answers 0 before its first
// flush and, after it, the records that every segment applied.
#[test]
fn a_spann_collection_answers_the_log_position_of_its_last_flush() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_collection_answers_the_log_position_of_its_last_flush_impl,
    )
}

async fn a_spann_collection_answers_the_log_position_of_its_last_flush_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = empty_spann_collection(&mut frontend, "log_position").await;
    add_numbers(&mut frontend, collection_id, 0..20).await;
    let log_position = |frontend: &mut ServiceBasedFrontend| {
        let mut sysdb = frontend.sysdb_client.clone();
        async move {
            sysdb
                .get_collection_with_segments(None, collection_id)
                .await
                .unwrap()
                .collection
                .log_position
        }
    };
    assert_eq!(log_position(&mut frontend).await, 0);
    flush_spann(&frontend, collection_id).await;
    assert_eq!(log_position(&mut frontend).await, 20);
}

// The flushes write the blocks of a SPANN collection through the block cache of the readers: with the files of the
// blocks gone from the data path after a flush, a get and a query still answer, from the blocks in memory.
#[test]
fn a_spann_flush_writes_its_blocks_through_the_cache_of_the_readers() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_spann_flush_writes_its_blocks_through_the_cache_of_the_readers_impl,
    )
}

async fn a_spann_flush_writes_its_blocks_through_the_cache_of_the_readers_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = empty_spann_collection(&mut frontend, "blocks_of_a_flush").await;
    add_numbers(&mut frontend, collection_id, 0..20).await;
    flush_spann(&frontend, collection_id).await;
    let mut removed = 0;
    let mut folders = vec![root.path().to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                folders.push(path);
            } else if path.parent().and_then(|p| p.file_name())
                == Some(std::ffi::OsStr::new("block"))
            {
                std::fs::remove_file(&path).unwrap();
                removed += 1;
            }
        }
    }
    assert!(removed > 0, "the flush wrote no block");
    let Ok(ReadAnswer::Records(lists)) =
        spann_read(&mut frontend, collection_id, SpannRead::Get).await
    else {
        panic!("a get of the collection read its blocks from the data path");
    };
    assert_eq!(lists.iter().map(Vec::len).sum::<usize>(), 20);
    assert!(spann_read(&mut frontend, collection_id, SpannRead::Query)
        .await
        .is_ok());
}

// A frontend as frontend_with_a_stopped_flush, whose log refuses a write to a SPANN collection with at least the records
// given in its log after its last flush, and whose flushes take at most records_per_flush records.
async fn frontend_with_backpressure(
    root: &std::path::Path,
    records: u64,
    records_per_flush: usize,
) -> (ServiceBasedFrontend, chroma_log::SpannFlushStop) {
    let registry = Registry::new();
    let stop = chroma_log::SpannFlushStop::default();
    registry.register(stop.clone());
    let system = System::new();
    let mut config = FrontendConfig::sqlite_in_memory();
    config.sqlitedb.as_mut().unwrap().url =
        Some(root.join("chroma.sqlite3").to_str().unwrap().into());
    config.segment_manager.as_mut().unwrap().persist_path = Some(root.to_str().unwrap().into());
    config.executor = crate::executor::config::ExecutorConfig::Local(
        crate::executor::config::LocalExecutorConfig {
            spann: true,
            ..Default::default()
        },
    );
    config.local_compaction_manager = chroma_log::LocalCompactionManagerConfig {
        max_compaction_size: records_per_flush,
        ..no_timer_flush()
    };
    config.log = chroma_log::config::LogConfig::Sqlite(chroma_log::config::SqliteLogConfig {
        num_records_before_backpressure: records,
        ..Default::default()
    });
    (
        ServiceBasedFrontend::try_from_config(&(config, system), &registry)
            .await
            .unwrap(),
        stop,
    )
}

// As the log service of distributed Chroma, once a round of the compactor counts num_records_before_backpressure records
// in the log of a SPANN collection after its last flush, a write to it is refused, with the answer of distributed Chroma
// over HTTP: 429, the name and the message of a log too full. Its flush empties the log, and the next write is accepted.
#[test]
fn a_write_to_a_spann_collection_whose_log_is_too_full_waits_for_its_flush() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_write_to_a_spann_collection_whose_log_is_too_full_waits_for_its_flush_impl,
    )
}

async fn a_write_to_a_spann_collection_whose_log_is_too_full_waits_for_its_flush_impl() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, stop) = frontend_with_backpressure(root.path(), 30, 10_000).await;
    let collection_id = empty_spann_collection(&mut frontend, "too_full").await;
    add_numbers(&mut frontend, collection_id, 0..30).await;
    let round = a_round_that_stops(&frontend, &stop).await;
    let written = frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec!["r30".to_string()],
                vec![vec![30.0, 30f32.sin()]],
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await;
    let Err(refused) = written else {
        panic!("a write to a log too full was accepted");
    };
    let response = axum::response::IntoResponse::into_response(frontend_core::errors::ServerError(
        Box::new(refused),
    ));
    assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
    // One flush, in the round that runs: at least 1 s.
    assert_eq!(
        response.headers().get(axum::http::header::RETRY_AFTER),
        Some(&axum::http::HeaderValue::from(1u64))
    );
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "error": "ChromaError",
            "message": "log needs compaction before accepting more writes; please backoff exponentially and retry",
        })
    );
    stop.go_on();
    round.await.unwrap().unwrap();
    add_numbers(&mut frontend, collection_id, 30..31).await;
    assert_eq!(
        status(&mut frontend, collection_id).await,
        (30.0 / 31.0, 1, 30, 31)
    );
}

// The seconds of the header Retry-After of the answer 429 of a write refused by a log too full, which distributed Chroma
// does not send.
fn retry_after_of(refused: impl ChromaError + 'static) -> u64 {
    let response = axum::response::IntoResponse::into_response(frontend_core::errors::ServerError(
        Box::new(refused),
    ));
    assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
    response
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .expect("a write refused by a log too full has no header Retry-After")
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

// A write refused by a log too full says when to send it again: the seconds, at least 1, until the end of the flushes
// that take its log under the limit, from the rounds of the compactor, each compaction_interval_sec after the end of the
// one before and flushing at most max_compaction_size records. With 60 records, a limit of 30, flushes of 10 records
// and rounds every 3600 s: while the first round holds its flush, four flushes are needed, the end of this round and
// three rounds more, 10800 s, for an add, an update, an upsert and a delete; after it, three flushes, the rest of the
// interval, then the length of a round and two rounds more.
#[test]
fn a_write_refused_by_a_log_too_full_says_when_to_send_it_again() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_write_refused_by_a_log_too_full_says_when_to_send_it_again_impl,
    )
}

async fn a_write_refused_by_a_log_too_full_says_when_to_send_it_again_impl() {
    let root = tempfile::tempdir().unwrap();
    let (mut frontend, stop) = frontend_with_backpressure(root.path(), 30, 10).await;
    let collection_id = empty_spann_collection(&mut frontend, "too_full").await;
    add_numbers(&mut frontend, collection_id, 0..60).await;
    let started = std::time::Instant::now();
    let round = a_round_that_stops(&frontend, &stop).await;
    let add = || {
        AddCollectionRecordsRequest::try_new(
            TENANT.to_string(),
            DATABASE.to_string(),
            collection_id,
            vec!["r60".to_string()],
            vec![vec![60.0, 60f32.sin()]],
            None,
            None,
            None,
        )
        .unwrap()
    };
    let Err(refused) = frontend.add(add()).await else {
        panic!("an add was accepted");
    };
    assert_eq!(retry_after_of(refused), 10800);
    let Err(refused) = frontend
        .update(
            UpdateCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec!["r0".to_string()],
                Some(vec![Some(vec![0.5, 0.5])]),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
    else {
        panic!("an update was accepted");
    };
    assert_eq!(retry_after_of(refused), 10800);
    let Err(refused) = frontend
        .upsert(
            UpsertCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec!["r1".to_string()],
                vec![vec![1.5, 1.5]],
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
    else {
        panic!("an upsert was accepted");
    };
    assert_eq!(retry_after_of(refused), 10800);
    let Err(refused) = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(vec!["r2".to_string()]),
                None,
                None,
            )
            .unwrap(),
            String::new(),
        )
        .await
    else {
        panic!("a delete was accepted");
    };
    assert_eq!(retry_after_of(refused), 10800);
    stop.go_on();
    round.await.unwrap().unwrap();
    // The round took at most this long, and ended at most this long before the next refusal.
    let length = started.elapsed();
    let Err(refused) = frontend.add(add()).await else {
        panic!("an add was accepted");
    };
    let since_the_start = started.elapsed();
    let seconds = retry_after_of(refused);
    let lowest = (10800.0 - since_the_start.as_secs_f64()).floor() as u64;
    let highest = (10800.0 + 3.0 * length.as_secs_f64()).ceil() as u64;
    assert!(
        (lowest..=highest).contains(&seconds),
        "Retry-After {seconds} s after the round, not in {lowest}..={highest}"
    );
}
