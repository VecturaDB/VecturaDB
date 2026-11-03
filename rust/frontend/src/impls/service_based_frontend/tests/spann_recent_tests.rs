// The index of the recent records of a SPANN collection (its tail after the last flush, kept by the local executor):
// the tail is read from SQLite once across reads with no write between them, then only the records written after the
// last read; a flush drops the index, and the next read takes only the tail after the new flushed position. Counted by
// the #[cfg(test)] counters of the executor: spann_tail_reads, the reads of the tail from SQLite by the default path,
// and spann_tail_records_read, the records they returned.

use super::spann_tail_tests::{empty_spann_collection, get_ids};
use super::*;

fn reads(frontend: &ServiceBasedFrontend) -> (usize, usize) {
    let executor = local_executor(frontend);
    (
        executor.spann_tail_reads(),
        executor.spann_tail_records_read(),
    )
}

async fn add_records(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    ids: std::ops::Range<usize>,
) {
    let records: Vec<usize> = ids.collect();
    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                records.iter().map(|i| format!("r{i}")).collect(),
                records.iter().map(|i| vec![*i as f32, 1.0]).collect(),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn query_ids(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
) -> Vec<String> {
    let mut ids = frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                None,
                vec![vec![0.0, 1.0]],
                100,
                chroma_types::IncludeList::default_query(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .ids
        .remove(0);
    ids.sort();
    ids
}

fn ids(range: std::ops::Range<usize>) -> Vec<String> {
    let mut ids: Vec<String> = range.map(|i| format!("r{i}")).collect();
    ids.sort();
    ids
}

// Queries with no write between them read the tail from SQLite once; a write then reads only the records written
// after the last read, and the answers hold every record.
#[test]
fn the_tail_is_read_once_across_reads_and_then_only_its_new_records() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_tail_is_read_once_across_reads_and_then_only_its_new_records_impl,
    )
}

async fn the_tail_is_read_once_across_reads_and_then_only_its_new_records_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    add_records(&mut frontend, collection_id, 0..30).await;
    let before = reads(&frontend);
    for _ in 0..5 {
        assert_eq!(query_ids(&mut frontend, collection_id).await, ids(0..30));
    }
    let after = reads(&frontend);
    assert_eq!(
        after.0 - before.0,
        1,
        "the tail read more than once across reads"
    );
    assert_eq!(after.1 - before.1, 30);
    add_records(&mut frontend, collection_id, 30..33).await;
    assert_eq!(query_ids(&mut frontend, collection_id).await, ids(0..33));
    let then = reads(&frontend);
    assert_eq!(then.0 - after.0, 1);
    assert_eq!(
        then.1 - after.1,
        3,
        "a read after a write read more than the new records"
    );
    assert_eq!(
        get_ids(&mut frontend, collection_id, None, None, 0)
            .await
            .len(),
        33
    );
}

// A flush drops the index of the recent records: the next read takes only the tail after the new flushed position,
// and reads nothing from SQLite when nothing was written after the flush, and the answers hold every record, from the files and the tail.
#[test]
fn a_flush_drops_the_index_of_the_recent_records() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_flush_drops_the_index_of_the_recent_records_impl,
    )
}

async fn a_flush_drops_the_index_of_the_recent_records_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    add_records(&mut frontend, collection_id, 0..20).await;
    assert_eq!(query_ids(&mut frontend, collection_id).await, ids(0..20));
    local_executor(&frontend)
        .clone()
        .compaction_round()
        .await
        .unwrap();
    let flushed = reads(&frontend);
    assert_eq!(query_ids(&mut frontend, collection_id).await, ids(0..20));
    let after = reads(&frontend);
    assert_eq!(
        after, flushed,
        "a read after a flush with nothing new read the tail from SQLite"
    );
    add_records(&mut frontend, collection_id, 20..24).await;
    assert_eq!(query_ids(&mut frontend, collection_id).await, ids(0..24));
    let then = reads(&frontend);
    assert_eq!(then.0 - after.0, 1);
    assert_eq!(
        then.1 - after.1,
        4,
        "a read took more than the tail after the new flushed position"
    );
    // A tail kept across the flush holds records the files hold too: every read must count and list each record once.
    let mut listed = get_ids(&mut frontend, collection_id, None, None, 0).await;
    listed.sort();
    assert_eq!(
        listed,
        ids(0..24),
        "a read listed a record of the files and of the tail twice"
    );
    for level in [
        chroma_types::plan::ReadLevel::IndexAndWal,
        chroma_types::plan::ReadLevel::IndexAndBoundedWal,
    ] {
        let count = frontend
            .count(
                CountRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    level,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            count, 24,
            "{level:?}: a record of the files counted again from the tail"
        );
    }
    let nearest = frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                None,
                vec![vec![0.0, 1.0]],
                24,
                chroma_types::IncludeList::default_query(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .ids
        .remove(0);
    assert_eq!(
        nearest.len(),
        24,
        "a query answered a record twice: {nearest:?}"
    );
}

async fn update_vector(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    id: &str,
    vector: Vec<f32>,
) {
    frontend
        .update(
            UpdateCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec![id.to_string()],
                Some(vec![Some(vector)]),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn nearest(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    vector: Vec<f32>,
    k: u32,
) -> Vec<String> {
    frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                None,
                vec![vector],
                k,
                chroma_types::IncludeList::default_query(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .ids
        .remove(0)
}

// An update of a vector after the tail was read, flushed before the next read, and an update of a record already
// flushed: the next query must rank the new vector, never the old one the index of the recent records held.
#[test]
fn a_query_ranks_the_new_vector_of_an_updated_record() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_query_ranks_the_new_vector_of_an_updated_record_impl,
    )
}

async fn a_query_ranks_the_new_vector_of_an_updated_record_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    add_records(&mut frontend, collection_id, 0..20).await;
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![0.0, 1.0], 1).await,
        ["r0"]
    );
    // r0 moves away, r9 moves onto the query point, and a flush follows with no read between: the index of the recent
    // records still holds the old vectors, and would answer r0 first.
    update_vector(&mut frontend, collection_id, "r0", vec![500.0, 1.0]).await;
    update_vector(&mut frontend, collection_id, "r9", vec![0.0, 1.0]).await;
    flush_spann(&frontend, collection_id).await;
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![0.0, 1.0], 2).await,
        ["r9", "r1"],
        "after the flush"
    );
    // r9 and r1 are flushed: r9 moves away, r12 moves onto the query point.
    update_vector(&mut frontend, collection_id, "r9", vec![900.0, 1.0]).await;
    update_vector(&mut frontend, collection_id, "r12", vec![0.0, 1.0]).await;
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![0.0, 1.0], 2).await,
        ["r12", "r1"],
        "an update of a flushed record"
    );
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![900.0, 1.0], 1).await,
        ["r9"]
    );
}

async fn delete_ids(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    ids: &[&str],
) {
    frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(ids.iter().map(|id| id.to_string()).collect()),
                None,
                None,
            )
            .unwrap(),
            String::new(),
        )
        .await
        .unwrap();
}

// A record read into the index of the recent records, deleted, then flushed: the files no longer hold it, and no read
// after the flush may bring it back from the records the index kept from before it. The sequence is seed 7 of the
// oracle, reduced to the writes and reads that keep it failing without the reset of the index at a flush.
#[test]
fn a_record_deleted_before_a_flush_does_not_come_back() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_record_deleted_before_a_flush_does_not_come_back_impl,
    )
}

async fn a_record_deleted_before_a_flush_does_not_come_back_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    add_records(&mut frontend, collection_id, 0..5).await;
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![0.0, 1.0], 1).await,
        ["r0"]
    );
    assert_eq!(
        get_ids(&mut frontend, collection_id, None, None, 0)
            .await
            .len(),
        5
    );
    for level in [
        chroma_types::plan::ReadLevel::IndexAndWal,
        chroma_types::plan::ReadLevel::IndexOnly,
        chroma_types::plan::ReadLevel::IndexAndBoundedWal,
    ] {
        frontend
            .count(
                CountRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    level,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    // r0 with ids the collection does not have, as in the seed.
    delete_ids(&mut frontend, collection_id, &["r0", "r7", "r8"]).await;
    flush_spann(&frontend, collection_id).await;
    // A write after the flush, so that the reads take a tail and the index of the recent records with it.
    delete_ids(&mut frontend, collection_id, &["r9"]).await;
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![0.0, 1.0], 1).await,
        ["r1"],
        "a deleted record came back"
    );
    let mut listed = get_ids(&mut frontend, collection_id, None, None, 0).await;
    listed.sort();
    assert_eq!(listed, ids(1..5));
    for level in [
        chroma_types::plan::ReadLevel::IndexAndWal,
        chroma_types::plan::ReadLevel::IndexAndBoundedWal,
    ] {
        let count = frontend
            .count(
                CountRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    level,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(count, 4, "{level:?}: a deleted record counted");
    }
}

// The incremental index of the recent records: a read materializes again only the ids that new records touch, and keeps
// the others. Each read answers every id once, with its last vector and metadata; offsets may change between reads, so
// they are not compared.

fn group_at_least(group: i64) -> chroma_types::Where {
    chroma_types::Where::Metadata(chroma_types::MetadataExpression {
        key: "g".to_string(),
        comparison: chroma_types::MetadataComparison::Primitive(
            chroma_types::PrimitiveOperator::GreaterThanOrEqual,
            chroma_types::MetadataValue::Int(group),
        ),
    })
}

// An upsert of one record with its vector and its group g.
async fn put(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    id: &str,
    vector: [f32; 2],
    group: i64,
) {
    frontend
        .upsert(
            UpsertCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec![id.to_string()],
                vec![vector.to_vec()],
                None,
                None,
                Some(vec![Some(chroma_types::UpdateMetadata::from([(
                    "g".to_string(),
                    chroma_types::UpdateMetadataValue::Int(group),
                )]))]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

// Every record as (id, vector, group), sorted by id, from a get; the count at IndexAndWal must agree with it.
async fn records(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
) -> Vec<(String, Vec<f32>, i64)> {
    let answer = frontend
        .get(
            GetRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                None,
                None,
                0,
                chroma_types::IncludeList(vec![
                    chroma_types::Include::Embedding,
                    chroma_types::Include::Metadata,
                ]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut records: Vec<(String, Vec<f32>, i64)> = answer
        .ids
        .into_iter()
        .zip(answer.embeddings.unwrap())
        .zip(answer.metadatas.unwrap())
        .map(|((id, vector), metadata)| {
            let group = match metadata.and_then(|metadata| metadata.get("g").cloned()) {
                Some(chroma_types::MetadataValue::Int(group)) => group,
                other => panic!("{id}: group {other:?}"),
            };
            (id, vector, group)
        })
        .collect();
    records.sort_by(|a, b| a.0.cmp(&b.0));
    let count = frontend
        .count(
            CountRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                chroma_types::plan::ReadLevel::IndexAndWal,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        count as usize,
        records.len(),
        "the count and the get disagree: {records:?}"
    );
    records
}

fn record(id: &str, vector: [f32; 2], group: i64) -> (String, Vec<f32>, i64) {
    (id.to_string(), vector.to_vec(), group)
}

// (a) An id updated three times across reads, with a flush between two of them, answers its last vector and metadata
// at every read, once.
#[test]
fn an_id_updated_across_reads_answers_its_last_version_once() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        an_id_updated_across_reads_answers_its_last_version_once_impl,
    )
}

async fn an_id_updated_across_reads_answers_its_last_version_once_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    put(&mut frontend, collection_id, "r0", [0.0, 1.0], 0).await;
    put(&mut frontend, collection_id, "r1", [10.0, 1.0], 0).await;
    assert_eq!(
        records(&mut frontend, collection_id).await,
        [record("r0", [0.0, 1.0], 0), record("r1", [10.0, 1.0], 0)]
    );
    for (round, x) in [(1, 20.0), (2, 30.0), (3, 40.0)] {
        put(&mut frontend, collection_id, "r0", [x, 1.0], round).await;
        assert_eq!(
            records(&mut frontend, collection_id).await,
            [record("r0", [x, 1.0], round), record("r1", [10.0, 1.0], 0)],
            "update {round}"
        );
        assert_eq!(
            nearest(&mut frontend, collection_id, vec![x, 1.0], 2).await,
            ["r0", "r1"],
            "update {round}"
        );
        if round == 1 {
            flush_spann(&frontend, collection_id).await;
        }
    }
}

// (b) An id deleted across reads is gone; added again, it answers its new vector.
#[test]
fn an_id_deleted_and_added_again_across_reads_answers_its_new_version() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        an_id_deleted_and_added_again_across_reads_answers_its_new_version_impl,
    )
}

async fn an_id_deleted_and_added_again_across_reads_answers_its_new_version_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    put(&mut frontend, collection_id, "r0", [0.0, 1.0], 0).await;
    put(&mut frontend, collection_id, "r1", [10.0, 1.0], 0).await;
    assert_eq!(records(&mut frontend, collection_id).await.len(), 2);
    delete_ids(&mut frontend, collection_id, &["r0"]).await;
    assert_eq!(
        records(&mut frontend, collection_id).await,
        [record("r1", [10.0, 1.0], 0)]
    );
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![0.0, 1.0], 2).await,
        ["r1"]
    );
    put(&mut frontend, collection_id, "r0", [50.0, 1.0], 3).await;
    assert_eq!(
        records(&mut frontend, collection_id).await,
        [record("r0", [50.0, 1.0], 3), record("r1", [10.0, 1.0], 0)]
    );
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![50.0, 1.0], 2).await,
        ["r0", "r1"]
    );
}

// (c) Of two ids, the one new records touch answers its new version, the other the version the index kept.
#[test]
fn an_id_no_new_record_touches_keeps_its_version() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        an_id_no_new_record_touches_keeps_its_version_impl,
    )
}

async fn an_id_no_new_record_touches_keeps_its_version_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    put(&mut frontend, collection_id, "r0", [0.0, 1.0], 0).await;
    put(&mut frontend, collection_id, "r1", [10.0, 1.0], 1).await;
    put(&mut frontend, collection_id, "r1", [11.0, 1.0], 2).await;
    assert_eq!(
        records(&mut frontend, collection_id).await,
        [record("r0", [0.0, 1.0], 0), record("r1", [11.0, 1.0], 2)]
    );
    put(&mut frontend, collection_id, "r0", [5.0, 1.0], 3).await;
    assert_eq!(
        records(&mut frontend, collection_id).await,
        [record("r0", [5.0, 1.0], 3), record("r1", [11.0, 1.0], 2)]
    );
    assert_eq!(
        nearest(&mut frontend, collection_id, vec![11.0, 1.0], 2).await,
        ["r1", "r0"]
    );
}

// (d) A filter on metadata that an update in the tail changed selects by the new value.
#[test]
fn a_filter_selects_by_the_metadata_of_the_last_update() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        a_filter_selects_by_the_metadata_of_the_last_update_impl,
    )
}

async fn a_filter_selects_by_the_metadata_of_the_last_update_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "recent").await;
    put(&mut frontend, collection_id, "r0", [0.0, 1.0], 0).await;
    put(&mut frontend, collection_id, "r1", [1.0, 1.0], 2).await;
    assert_eq!(
        get_ids(
            &mut frontend,
            collection_id,
            Some(group_at_least(1)),
            None,
            0
        )
        .await,
        ["r1"]
    );
    put(&mut frontend, collection_id, "r0", [0.0, 1.0], 2).await;
    put(&mut frontend, collection_id, "r1", [1.0, 1.0], 0).await;
    assert_eq!(
        get_ids(
            &mut frontend,
            collection_id,
            Some(group_at_least(1)),
            None,
            0
        )
        .await,
        ["r0"]
    );
    let filtered = frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                Some(group_at_least(1)),
                vec![vec![1.0, 1.0]],
                10,
                chroma_types::IncludeList::default_query(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .ids
        .remove(0);
    assert_eq!(filtered, ["r0"]);
}
