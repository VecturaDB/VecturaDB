// The oracle of the tail of a SPANN collection: under random writes (add, upsert, update, delete) and flushes at random
// points, every read by the default path of the tail answers as the reference path, the whole tail read from the log
// and searched by brute force (LocalExecutor::spann_tail_reference): the query, with and without filters, the get with
// filters, and the count at the three read levels. A faster tail, an index of the recent records, goes behind the
// default path only, and this test keeps its answers those of the reference.

use super::spann_tail_tests::{empty_spann_collection, get_ids};
use super::*;
use chroma_types::{
    Metadata, MetadataComparison, MetadataExpression, MetadataValue, PrimitiveOperator,
    UpdateMetadata, UpdateMetadataValue, Where,
};
use std::sync::atomic::Ordering;

const DIMENSION: usize = 4;
const IDS: u64 = 60;
const STEPS: usize = 120;

// A generator of its own (xorshift64*), the same sequence at every run of a seed.
struct Draw(u64);

impl Draw {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        (self.next() >> 33) % n
    }

    fn vector(&mut self) -> Vec<f32> {
        (0..DIMENSION)
            .map(|_| (self.next() >> 40) as f32 / (1u64 << 24) as f32)
            .collect()
    }

    // A few distinct ids of the pool.
    fn ids(&mut self) -> Vec<String> {
        let count = 1 + self.below(5);
        let mut ids: Vec<String> = (0..count)
            .map(|_| format!("id{}", self.below(IDS)))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

fn group_at_least(group: i64) -> Where {
    Where::Metadata(MetadataExpression {
        key: "g".to_string(),
        comparison: MetadataComparison::Primitive(
            PrimitiveOperator::GreaterThanOrEqual,
            MetadataValue::Int(group),
        ),
    })
}

// One random write of the collection. The writes the collection refuses, an add of ids it has or an update of ids it
// does not have, are part of the sequence: both paths read the same collection.
async fn write(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    draw: &mut Draw,
) {
    let ids = draw.ids();
    let vectors: Vec<Vec<f32>> = ids.iter().map(|_| draw.vector()).collect();
    let documents: Vec<Option<String>> = ids
        .iter()
        .map(|id| Some(format!("doc {id} {}", draw.below(100))))
        .collect();
    let groups: Vec<i64> = ids.iter().map(|_| draw.below(4) as i64).collect();
    match draw.below(4) {
        0 => {
            let metadatas: Vec<Option<Metadata>> = groups
                .iter()
                .map(|group| {
                    Some(Metadata::from([(
                        "g".to_string(),
                        MetadataValue::Int(*group),
                    )]))
                })
                .collect();
            let _ = frontend
                .add(
                    AddCollectionRecordsRequest::try_new(
                        TENANT.to_string(),
                        DATABASE.to_string(),
                        collection_id,
                        ids,
                        vectors,
                        Some(documents),
                        None,
                        Some(metadatas),
                    )
                    .unwrap(),
                )
                .await;
        }
        1 => {
            let metadatas: Vec<Option<UpdateMetadata>> = groups
                .iter()
                .map(|group| {
                    Some(UpdateMetadata::from([(
                        "g".to_string(),
                        UpdateMetadataValue::Int(*group),
                    )]))
                })
                .collect();
            let _ = frontend
                .upsert(
                    UpsertCollectionRecordsRequest::try_new(
                        TENANT.to_string(),
                        DATABASE.to_string(),
                        collection_id,
                        ids,
                        vectors,
                        Some(documents),
                        None,
                        Some(metadatas),
                    )
                    .unwrap(),
                )
                .await;
        }
        2 => {
            let keep: Vec<bool> = vectors.iter().map(|_| draw.below(2) == 0).collect();
            let embeddings: Vec<Option<Vec<f32>>> = vectors
                .into_iter()
                .zip(keep)
                .map(|(vector, keep)| keep.then_some(vector))
                .collect();
            let _ = frontend
                .update(
                    UpdateCollectionRecordsRequest::try_new(
                        TENANT.to_string(),
                        DATABASE.to_string(),
                        collection_id,
                        ids,
                        Some(embeddings),
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
                )
                .await;
        }
        _ => {
            let _ = frontend
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
                .await;
        }
    }
}

// Every read of the oracle, as text, by the path of the tail given; distances to 1e-5.
async fn reads(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    queries: &[(Vec<f32>, Option<Where>)],
    reference: bool,
) -> Vec<String> {
    use chroma_types::plan::ReadLevel;
    local_executor(frontend)
        .spann_tail_reference
        .store(reference, Ordering::SeqCst);
    let mut answers = Vec::new();
    for (vector, filter) in queries {
        let answer = frontend
            .query(
                QueryRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    None,
                    filter.clone(),
                    vec![vector.clone()],
                    10,
                    chroma_types::IncludeList::default_query(),
                )
                .unwrap(),
            )
            .await;
        answers.push(match answer {
            Ok(answer) => {
                let distances: Option<Vec<Vec<Option<i64>>>> = answer.distances.map(|lists| {
                    lists
                        .into_iter()
                        .map(|list| {
                            list.into_iter()
                                .map(|distance| distance.map(|d| (d * 1e5).round() as i64))
                                .collect()
                        })
                        .collect()
                });
                format!(
                    "query {vector:?} {filter:?}: {:?} {distances:?}",
                    answer.ids
                )
            }
            Err(err) => format!("query {vector:?} {filter:?}: error {err}"),
        });
    }
    for filter in [None, Some(group_at_least(2))] {
        let ids = get_ids(frontend, collection_id, filter.clone(), None, 0).await;
        answers.push(format!("get {filter:?}: {ids:?}"));
    }
    for level in [
        ReadLevel::IndexAndWal,
        ReadLevel::IndexOnly,
        ReadLevel::IndexAndBoundedWal,
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
            .await;
        answers.push(format!("count {level:?}: {count:?}"));
    }
    local_executor(frontend)
        .spann_tail_reference
        .store(false, Ordering::SeqCst);
    answers
}

#[test]
fn the_default_tail_of_a_spann_collection_answers_as_the_reference() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_default_tail_of_a_spann_collection_answers_as_the_reference_impl,
    )
}

async fn the_default_tail_of_a_spann_collection_answers_as_the_reference_impl() {
    for seed in [7u64, 11, 13] {
        let root = tempfile::tempdir().unwrap();
        let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
        let collection_id = empty_spann_collection(&mut frontend, "oracle").await;
        let mut draw = Draw(seed);
        let mut flushes = 0;
        for step in 0..STEPS {
            write(&mut frontend, collection_id, &mut draw).await;
            if draw.below(10) == 0 {
                local_executor(&frontend)
                    .clone()
                    .compaction_round()
                    .await
                    .unwrap();
                flushes += 1;
            }
            if step % 5 == 4 {
                let queries: Vec<(Vec<f32>, Option<Where>)> = (0..3)
                    .map(|i| (draw.vector(), (i == 2).then(|| group_at_least(1))))
                    .collect();
                let default = reads(&mut frontend, collection_id, &queries, false).await;
                let reference = reads(&mut frontend, collection_id, &queries, true).await;
                assert_eq!(
                    default, reference,
                    "seed {seed}, step {step}: the default tail answers otherwise than the reference"
                );
            }
        }
        assert!(flushes > 0, "seed {seed}: no flush in the sequence");
    }
}

// Seed 7 reduced to the five items that answer otherwise without the reset of the index of the recent records at a
// flush: the add of step 44, the reads after it, the delete of step 45, the flush at step 46, and the write of step 49.
// The draws of the items left out are taken all the same, so the values are those of the seed.
#[test]
fn the_reduced_seed_7_answers_as_the_reference() {
    on_a_large_stack(
        TestRuntime::CurrentThread,
        the_reduced_seed_7_answers_as_the_reference_impl,
    )
}

async fn the_reduced_seed_7_answers_as_the_reference_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "oracle").await;
    let mut draw = Draw(7);
    for step in 0..50 {
        if [44, 45, 49].contains(&step) {
            write(&mut frontend, collection_id, &mut draw).await;
        } else {
            draw_write(&mut draw);
        }
        if draw.below(10) == 0 && step == 46 {
            local_executor(&frontend)
                .clone()
                .compaction_round()
                .await
                .unwrap();
        }
        if step % 5 == 4 {
            let queries: Vec<(Vec<f32>, Option<Where>)> = (0..3)
                .map(|i| (draw.vector(), (i == 2).then(|| group_at_least(1))))
                .collect();
            if step == 44 || step == 49 {
                let default = reads(&mut frontend, collection_id, &queries, false).await;
                let reference = reads(&mut frontend, collection_id, &queries, true).await;
                assert_eq!(
                    default, reference,
                    "step {step}: the default tail answers otherwise than the reference"
                );
            }
        }
    }
}

// The draws of write, in the same order, with no request.
fn draw_write(draw: &mut Draw) {
    let ids = draw.ids();
    for _ in &ids {
        draw.vector();
    }
    for _ in &ids {
        draw.below(100);
    }
    for _ in &ids {
        draw.below(4);
    }
    if draw.below(4) == 2 {
        for _ in &ids {
            draw.below(2);
        }
    }
}

// Reads in parallel, which materialize the tail at the same time, while a task writes and flushes. During a round every
// answer lists each id once and holds every record written before the read began; at the end of a round, with no write
// running, the default path answers as the reference, so a result of an older moment left in the cache shows.
#[test]
fn parallel_reads_during_writes_and_flushes_answer_as_the_reference() {
    on_a_large_stack(
        TestRuntime::FourWorkers,
        parallel_reads_during_writes_and_flushes_answer_as_the_reference_impl,
    )
}

const PARALLEL_READERS: usize = 3;
const PARALLEL_ROUNDS: usize = 20;
const PARALLEL_WRITES: usize = 8;

async fn parallel_reader(
    mut frontend: ServiceBasedFrontend,
    collection_id: CollectionUuid,
    seed: u64,
    written: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> usize {
    let mut rounds = 0;
    let mut draw = Draw(seed);
    while !stop.load(Ordering::SeqCst) {
        let before = written.load(Ordering::SeqCst);
        let ids = get_ids(&mut frontend, collection_id, None, None, 0).await;
        let distinct: std::collections::HashSet<&String> = ids.iter().collect();
        assert_eq!(
            distinct.len(),
            ids.len(),
            "a get listed an id twice: {ids:?}"
        );
        for number in 0..before {
            assert!(
                distinct.contains(&format!("kept{number}")),
                "kept{number}, written before the read, is not in {ids:?}"
            );
        }
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
        assert!(
            count as usize >= before,
            "count {count}, with {before} records kept"
        );
        let nearest = frontend
            .query(
                QueryRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    None,
                    None,
                    vec![draw.vector()],
                    10,
                    chroma_types::IncludeList::default_query(),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .ids
            .remove(0);
        let distinct: std::collections::HashSet<&String> = nearest.iter().collect();
        assert_eq!(
            distinct.len(),
            nearest.len(),
            "a query answered an id twice: {nearest:?}"
        );
        rounds += 1;
    }
    rounds
}

async fn parallel_reads_during_writes_and_flushes_answer_as_the_reference_impl() {
    for seed in [7u64, 11, 13] {
        let root = tempfile::tempdir().unwrap();
        let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
        let collection_id = empty_spann_collection(&mut frontend, "parallel").await;
        let mut draw = Draw(seed);
        let written = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut parallel_reads = 0;
        for round in 0..PARALLEL_ROUNDS {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let readers: Vec<_> = (0..PARALLEL_READERS)
                .map(|reader| {
                    tokio::spawn(Box::pin(parallel_reader(
                        frontend.clone(),
                        collection_id,
                        seed + 100 * (round + reader + 1) as u64,
                        written.clone(),
                        stop.clone(),
                    )))
                })
                .collect();
            for write_number in 0..PARALLEL_WRITES {
                write(&mut frontend, collection_id, &mut draw).await;
                // A record no write of the sequence touches again, for the reads to look for.
                let number = written.load(Ordering::SeqCst);
                frontend
                    .add(
                        AddCollectionRecordsRequest::try_new(
                            TENANT.to_string(),
                            DATABASE.to_string(),
                            collection_id,
                            vec![format!("kept{number}")],
                            vec![draw.vector()],
                            None,
                            None,
                            None,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                written.store(number + 1, Ordering::SeqCst);
                if write_number % 3 == 2 {
                    flush_spann(&frontend, collection_id).await;
                }
                tokio::task::yield_now().await;
            }
            stop.store(true, Ordering::SeqCst);
            for reader in readers {
                parallel_reads += reader.await.unwrap();
            }
            let queries: Vec<(Vec<f32>, Option<Where>)> = (0..3)
                .map(|i| (draw.vector(), (i == 2).then(|| group_at_least(1))))
                .collect();
            let default = reads(&mut frontend, collection_id, &queries, false).await;
            let reference = reads(&mut frontend, collection_id, &queries, true).await;
            assert_eq!(
                default, reference,
                "seed {seed}, round {round}: the default tail answers otherwise than the reference"
            );
        }
        assert!(
            parallel_reads > 0,
            "seed {seed}: no read ran during the writes"
        );
        println!("seed {seed}: {parallel_reads} parallel reads");
    }
}

// A read that materialized the tail of a flush and stores its result after a newer flush: the result must not stay in
// the cache, or the next read builds on it and a record deleted before the newer flush comes back.
#[test]
fn a_materialization_of_an_older_flush_does_not_stay_in_the_cache() {
    on_a_large_stack(
        TestRuntime::FourWorkers,
        a_materialization_of_an_older_flush_does_not_stay_in_the_cache_impl,
    )
}

async fn query_in_a_task(
    frontend: &ServiceBasedFrontend,
    collection_id: CollectionUuid,
) -> tokio::task::JoinHandle<()> {
    let mut frontend = frontend.clone();
    tokio::spawn(Box::pin(async move {
        // Its answer is of its own moment, and its files may be gone: only its store in the cache counts here.
        let _ = frontend
            .query(
                QueryRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    None,
                    None,
                    vec![vec![0.0; DIMENSION]],
                    100,
                    chroma_types::IncludeList::default_query(),
                )
                .unwrap(),
            )
            .await;
    }))
}

async fn add_one(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    number: usize,
) {
    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec![format!("r{number}")],
                vec![vec![number as f32; DIMENSION]],
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn a_materialization_of_an_older_flush_does_not_stay_in_the_cache_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "gate").await;
    for number in 0..5 {
        add_one(&mut frontend, collection_id, number).await;
    }
    flush_spann(&frontend, collection_id).await;
    // The first read stops with three records materialized on the first flush: more than the tail of the next flush
    // will hold, as a stale result of the same length reads its indices in the new tail and answers right by chance.
    for number in [5, 8, 9] {
        add_one(&mut frontend, collection_id, number).await;
    }
    let (older_reached, older_go) = local_executor(&frontend).spann_tail_gate();
    let older = query_in_a_task(&frontend, collection_id).await;
    older_reached.notified().await;
    // They go, a second flush takes them away, and a second read empties the cache for that flush and stops too.
    frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(vec!["r5".to_string(), "r8".to_string(), "r9".to_string()]),
                None,
                None,
            )
            .unwrap(),
            String::new(),
        )
        .await
        .unwrap();
    flush_spann(&frontend, collection_id).await;
    add_one(&mut frontend, collection_id, 6).await;
    let (newer_reached, newer_go) = local_executor(&frontend).spann_tail_gate();
    let newer = query_in_a_task(&frontend, collection_id).await;
    newer_reached.notified().await;
    older_go.notify_one();
    older.await.unwrap();
    // The next read finds the cache as the first read left it.
    add_one(&mut frontend, collection_id, 7).await;
    let queries = vec![(vec![5.0; DIMENSION], None)];
    let default = reads(&mut frontend, collection_id, &queries, false).await;
    let reference = reads(&mut frontend, collection_id, &queries, true).await;
    assert_eq!(
        default, reference,
        "a result of the older flush stayed in the cache"
    );
    newer_go.notify_one();
    newer.await.unwrap();
    let default = reads(&mut frontend, collection_id, &queries, false).await;
    let reference = reads(&mut frontend, collection_id, &queries, true).await;
    assert_eq!(
        default, reference,
        "after the second read stored its result"
    );
}

// A read that stores its result after a read of a newer position: the newer result stays, and a read at that position
// takes it from the cache without materializing again.
#[test]
fn an_older_materialization_does_not_replace_a_newer_one() {
    on_a_large_stack(
        TestRuntime::FourWorkers,
        an_older_materialization_does_not_replace_a_newer_one_impl,
    )
}

async fn an_older_materialization_does_not_replace_a_newer_one_impl() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann_registry(root.path(), true, &Registry::new()).await;
    let collection_id = empty_spann_collection(&mut frontend, "gate").await;
    add_one(&mut frontend, collection_id, 0).await;
    add_one(&mut frontend, collection_id, 1).await;
    let (older_reached, older_go) = local_executor(&frontend).spann_tail_gate();
    let older = query_in_a_task(&frontend, collection_id).await;
    older_reached.notified().await;
    add_one(&mut frontend, collection_id, 2).await;
    let newer = query_in_a_task(&frontend, collection_id).await;
    newer.await.unwrap();
    older_go.notify_one();
    older.await.unwrap();
    let before = local_executor(&frontend).spann_tail_materializations();
    let answer = query_in_a_task(&frontend, collection_id).await;
    answer.await.unwrap();
    assert_eq!(
        local_executor(&frontend).spann_tail_materializations(),
        before,
        "a read at the newest position materialized again: an older result replaced the newer one"
    );
}
