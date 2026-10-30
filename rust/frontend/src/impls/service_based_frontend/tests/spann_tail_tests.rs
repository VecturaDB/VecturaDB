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
async fn empty_spann_collection(
    frontend: &mut ServiceBasedFrontend,
    name: &str,
) -> CollectionUuid {
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
    assert_eq!(status(&mut frontend, collection_id).await, (10.0 / 13.0, 3, 10, 13));
    write_to_stop(&mut frontend, collection_id, 2).await;
    match spann_read(&mut frontend, collection_id, SpannRead::Get).await {
        Ok(ReadAnswer::Records(records)) => {
            let ids = records[0].iter().map(|record| record.0.as_str()).collect::<Vec<_>>();
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
    assert_eq!(status(&mut reopened, collection_id).await, (10.0 / 14.0, 4, 10, 14));
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
            1 => ("r1".to_string(), Some("document r1 changed".to_string()), Some(vec![50.0, 50.0])),
            3 => ("r3".to_string(), Some("document r3 changed".to_string()), Some(vec![60.0, 60.0])),
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
