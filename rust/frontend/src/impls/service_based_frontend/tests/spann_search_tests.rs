// The Search API on a SPANN collection, with the search in two moments (SQLite in its snapshot, then the files) against
// the search that does all of it in one snapshot of SQLite, as before: the same searches on the same collection answer
// the same records, in the same order, with the same documents, embeddings, metadata and scores.

use super::*;
use chroma_types::operator::{Aggregate, GroupBy, Key, QueryVector, Rank, RankExpr, Select};
use chroma_types::{
    IndexConfig, Metadata, MetadataComparison, MetadataExpression, MetadataValue,
    PrimitiveOperator, SparseIndexAlgorithm, SparseVector, SparseVectorIndexConfig, UpdateMetadata,
    UpdateMetadataValue, Where,
};
use std::sync::atomic::Ordering;

const DIMENSION: usize = 8;
const RECORDS: usize = 200;
const SPARSE_KEY: &str = "sparse";

// Numbers in [0, 1) of a generator of its own (xorshift64*), the same at every run: random embeddings and sparse values
// leave no two records with the same score, where the two searches may order them differently.
struct Numbers(u64);

impl Numbers {
    fn next(&mut self) -> f32 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 40) as f32 / (1u64 << 24) as f32
    }

    fn dense(&mut self) -> Vec<f32> {
        (0..DIMENSION).map(|_| self.next() * 2.0 - 1.0).collect()
    }

    // Three of the 40 dimensions, in order, with their values.
    fn sparse(&mut self) -> SparseVector {
        let mut indices = Vec::new();
        while indices.len() < 3 {
            let index = (self.next() * 40.0) as u32;
            if !indices.contains(&index) {
                indices.push(index);
            }
        }
        indices.sort_unstable();
        SparseVector::from_pairs(indices.into_iter().map(|index| (index, self.next() + 0.01)))
    }
}

// The metadata of record i, for an add: a group of five, a kind of three, a sparse vector, and one key of each other
// type of metadata, since the search in two moments takes the metadata from the record segment and the search in one
// snapshot from SQLite.
fn metadata_of(i: usize, numbers: &mut Numbers) -> Metadata {
    [
        ("group".to_string(), MetadataValue::Int((i % 5) as i64)),
        (
            "kind".to_string(),
            MetadataValue::Str(["a", "b", "c"][i % 3].to_string()),
        ),
        (
            SPARSE_KEY.to_string(),
            MetadataValue::SparseVector(numbers.sparse()),
        ),
        ("even".to_string(), MetadataValue::Bool(i % 2 == 0)),
        (
            "weight".to_string(),
            MetadataValue::Float(numbers.next() as f64),
        ),
        (
            "flags".to_string(),
            MetadataValue::BoolArray(vec![i % 2 == 0, i % 3 == 0]),
        ),
        (
            "counts".to_string(),
            MetadataValue::IntArray(vec![i as i64, (i * 7 % 11) as i64]),
        ),
        (
            "weights".to_string(),
            MetadataValue::FloatArray(vec![numbers.next() as f64, numbers.next() as f64]),
        ),
        (
            "tags".to_string(),
            MetadataValue::StringArray(vec![format!("t{}", i % 4), format!("u{}", i % 6)]),
        ),
    ]
    .into_iter()
    .collect()
}

// The same, for an upsert.
fn update_metadata_of(i: usize, numbers: &mut Numbers) -> UpdateMetadata {
    [
        (
            "group".to_string(),
            UpdateMetadataValue::Int((i % 5) as i64),
        ),
        (
            "kind".to_string(),
            UpdateMetadataValue::Str(["a", "b", "c"][i % 3].to_string()),
        ),
        (
            SPARSE_KEY.to_string(),
            UpdateMetadataValue::SparseVector(numbers.sparse()),
        ),
        ("even".to_string(), UpdateMetadataValue::Bool(i % 2 == 0)),
        (
            "weight".to_string(),
            UpdateMetadataValue::Float(numbers.next() as f64),
        ),
        (
            "flags".to_string(),
            UpdateMetadataValue::BoolArray(vec![i % 2 == 0, i % 3 == 0]),
        ),
        (
            "counts".to_string(),
            UpdateMetadataValue::IntArray(vec![i as i64, (i * 7 % 11) as i64]),
        ),
        (
            "weights".to_string(),
            UpdateMetadataValue::FloatArray(vec![numbers.next() as f64, numbers.next() as f64]),
        ),
        (
            "tags".to_string(),
            UpdateMetadataValue::StringArray(vec![format!("t{}", i % 4), format!("u{}", i % 6)]),
        ),
    ]
    .into_iter()
    .collect()
}

// A SPANN collection with a sparse index on SPARSE_KEY and the records given with a document and metadata, added in
// four writes, each with its flush; then a delete and an upsert, so that the offsets of the record segment are not the
// rows of SQLite.
async fn collection_to_search(
    frontend: &mut ServiceBasedFrontend,
    numbers: &mut Numbers,
    records: usize,
) -> CollectionUuid {
    let schema = Schema::new_default(KnnIndex::Spann)
        .create_index(
            Some(SPARSE_KEY),
            IndexConfig::SparseVector(SparseVectorIndexConfig {
                embedding_function: None,
                source_key: None,
                bm25: None,
                algorithm: SparseIndexAlgorithm::default(),
            }),
        )
        .expect("a sparse index on the key sparse");
    let collection_id = frontend
        .create_collection(
            CreateCollectionRequest::try_new(
                TENANT.to_string(),
                DatabaseName::new(DATABASE).unwrap(),
                "two-moments".to_string(),
                None,
                None,
                Some(schema),
                false,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .collection_id;
    let numbers_of = |range: std::ops::Range<usize>| range.collect::<Vec<_>>();
    for chunk in numbers_of(0..records).chunks(records / 4) {
        frontend
            .add(
                AddCollectionRecordsRequest::try_new(
                    TENANT.to_string(),
                    DATABASE.to_string(),
                    collection_id,
                    chunk.iter().map(|i| format!("r{i}")).collect(),
                    chunk.iter().map(|_| numbers.dense()).collect(),
                    Some(
                        chunk
                            .iter()
                            .map(|i| Some(format!("document r{i}")))
                            .collect(),
                    ),
                    None,
                    Some(
                        chunk
                            .iter()
                            .map(|&i| Some(metadata_of(i, numbers)))
                            .collect(),
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                Some(vec!["r3".to_string(), "r42".to_string()]),
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
                vec!["r7".to_string(), "r3".to_string()],
                vec![numbers.dense(), numbers.dense()],
                Some(vec![
                    Some("document r7 changed".to_string()),
                    Some("document r3 again".to_string()),
                ]),
                None,
                Some(vec![
                    Some(update_metadata_of(7, numbers)),
                    Some(update_metadata_of(3, numbers)),
                ]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    collection_id
}

fn knn(query: QueryVector, key: Key, limit: u32, return_rank: bool) -> RankExpr {
    RankExpr::Knn {
        query,
        key,
        limit,
        default: if return_rank {
            Some(limit as f32)
        } else {
            None
        },
        return_rank,
    }
}

fn group_is(group: i64) -> Where {
    Where::Metadata(MetadataExpression {
        key: "group".to_string(),
        comparison: MetadataComparison::Primitive(
            PrimitiveOperator::Equal,
            MetadataValue::Int(group),
        ),
    })
}

fn select(keys: &[Key]) -> Select {
    Select {
        keys: keys.iter().cloned().collect(),
    }
}

// The searches to compare, by name: dense, sparse and hybrid ranks, with a filter by metadata, by ids and by both, with
// limit and offset, with group_by, with each key of the selection, and without a rank.
fn searches(numbers: &mut Numbers) -> Vec<(&'static str, SearchPayload)> {
    let all = [Key::Document, Key::Embedding, Key::Metadata, Key::Score];
    let dense = QueryVector::Dense(numbers.dense());
    let sparse = QueryVector::Sparse(numbers.sparse());
    let sparse_key = Key::MetadataField(SPARSE_KEY.to_string());
    let some_ids = (0..RECORDS)
        .step_by(3)
        .map(|i| format!("r{i}"))
        .collect::<Vec<_>>();
    let ranked =
        |expr: RankExpr, filter: Filter, offset: u32, limit: u32, keys: &[Key]| SearchPayload {
            filter,
            rank: Rank { expr: Some(expr) },
            limit: Limit {
                offset,
                limit: Some(limit),
            },
            select: select(keys),
            ..Default::default()
        };
    let no_filter = || Filter {
        query_ids: None,
        where_clause: None,
    };
    // Reciprocal rank fusion of the dense and the sparse rank.
    let hybrid = RankExpr::Summation(
        [
            knn(dense.clone(), Key::Embedding, 60, true),
            knn(sparse.clone(), sparse_key.clone(), 60, true),
        ]
        .into_iter()
        .map(|rank| RankExpr::Division {
            left: Box::new(RankExpr::Value(-1.0)),
            right: Box::new(RankExpr::Summation(vec![RankExpr::Value(60.0), rank])),
        })
        .collect(),
    );
    vec![
        (
            "dense",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                no_filter(),
                0,
                10,
                &all,
            ),
        ),
        (
            "dense, where",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                Filter {
                    query_ids: None,
                    where_clause: Some(group_is(2)),
                },
                0,
                10,
                &all,
            ),
        ),
        (
            "dense, ids",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                Filter {
                    query_ids: Some(some_ids.clone()),
                    where_clause: None,
                },
                0,
                10,
                &all,
            ),
        ),
        (
            "dense, where and ids",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                Filter {
                    query_ids: Some(some_ids.clone()),
                    where_clause: Some(group_is(1)),
                },
                0,
                10,
                &all,
            ),
        ),
        (
            "dense, offset",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                no_filter(),
                3,
                5,
                &all,
            ),
        ),
        (
            "sparse",
            ranked(
                knn(sparse.clone(), sparse_key.clone(), 20, false),
                no_filter(),
                0,
                10,
                &all,
            ),
        ),
        (
            "sparse, where",
            ranked(
                knn(sparse.clone(), sparse_key.clone(), 20, false),
                Filter {
                    query_ids: None,
                    where_clause: Some(group_is(4)),
                },
                0,
                10,
                &all,
            ),
        ),
        ("hybrid", ranked(hybrid.clone(), no_filter(), 0, 10, &all)),
        (
            "hybrid, where and offset",
            ranked(
                hybrid,
                Filter {
                    query_ids: None,
                    where_clause: Some(group_is(3)),
                },
                2,
                6,
                &all,
            ),
        ),
        (
            "dense, group_by",
            SearchPayload {
                group_by: GroupBy {
                    keys: vec![Key::MetadataField("kind".to_string())],
                    aggregate: Some(Aggregate::MinK {
                        keys: vec![Key::Score],
                        k: 2,
                    }),
                },
                ..ranked(
                    knn(dense.clone(), Key::Embedding, 50, false),
                    no_filter(),
                    0,
                    6,
                    &all,
                )
            },
        ),
        (
            "dense, document only",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                no_filter(),
                0,
                10,
                &[Key::Document],
            ),
        ),
        (
            "dense, metadata only",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                no_filter(),
                0,
                10,
                &[Key::Metadata],
            ),
        ),
        (
            "dense, embedding only",
            ranked(
                knn(dense.clone(), Key::Embedding, 20, false),
                no_filter(),
                0,
                10,
                &[Key::Embedding],
            ),
        ),
        (
            "dense, score only",
            ranked(
                knn(dense, Key::Embedding, 20, false),
                no_filter(),
                0,
                10,
                &[Key::Score],
            ),
        ),
        (
            "no rank, where and offset",
            SearchPayload {
                filter: Filter {
                    query_ids: None,
                    where_clause: Some(group_is(1)),
                },
                rank: Rank { expr: None },
                limit: Limit {
                    offset: 2,
                    limit: Some(7),
                },
                select: select(&[Key::Document, Key::Metadata]),
                ..Default::default()
            },
        ),
        (
            "no rank, ids",
            SearchPayload {
                filter: Filter {
                    query_ids: Some(some_ids),
                    where_clause: None,
                },
                rank: Rank { expr: None },
                limit: Limit {
                    offset: 0,
                    limit: Some(500),
                },
                select: select(&[Key::Document, Key::Embedding]),
                ..Default::default()
            },
        ),
    ]
}

// One search of the collection, with the search in one snapshot or in two moments.
async fn search_with(
    frontend: &mut ServiceBasedFrontend,
    collection_id: CollectionUuid,
    payload: SearchPayload,
    in_one_snapshot: bool,
) -> Result<SearchResponse, String> {
    local_executor(frontend)
        .search_in_one_snapshot
        .store(in_one_snapshot, Ordering::SeqCst);
    frontend
        .search(
            SearchRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                vec![payload],
                chroma_types::plan::ReadLevel::default(),
            )
            .map_err(|err| format!("request: {err}"))?,
        )
        .await
        .map_err(|err| format!("search: {err}"))
}

// What differs between the answers of the two searches.
fn differences(
    name: &str,
    one_snapshot: &SearchResponse,
    two_moments: &SearchResponse,
) -> Vec<String> {
    let mut wrong = Vec::new();
    let mut compare = |part: &str, same: bool, both: String| {
        if !same {
            wrong.push(format!("{name}: the {part} differ: {both}"));
        }
    };
    compare(
        "ids",
        one_snapshot.ids == two_moments.ids,
        format!("{:?} against {:?}", one_snapshot.ids, two_moments.ids),
    );
    compare(
        "documents",
        one_snapshot.documents == two_moments.documents,
        format!(
            "{:?} against {:?}",
            one_snapshot.documents, two_moments.documents
        ),
    );
    compare(
        "embeddings",
        one_snapshot.embeddings == two_moments.embeddings,
        format!(
            "{:?} against {:?}",
            one_snapshot.embeddings, two_moments.embeddings
        ),
    );
    compare(
        "metadata",
        one_snapshot.metadatas == two_moments.metadatas,
        format!(
            "{:?} against {:?}",
            one_snapshot.metadatas, two_moments.metadatas
        ),
    );
    compare(
        "scores",
        one_snapshot.scores == two_moments.scores,
        format!("{:?} against {:?}", one_snapshot.scores, two_moments.scores),
    );
    wrong
}

// The same searches on the same SPANN collection, in one snapshot and in two moments, answer the same, and none of them
// answers nothing: two empty answers would prove nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spann_search_in_two_moments_answers_as_in_one_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let mut numbers = Numbers(0x5eed_1234_abcd_ef01);
    let collection_id = collection_to_search(&mut frontend, &mut numbers, RECORDS).await;
    let mut wrong = Vec::new();
    for (name, payload) in searches(&mut numbers) {
        let one_snapshot = search_with(&mut frontend, collection_id, payload.clone(), true).await;
        let two_moments = search_with(&mut frontend, collection_id, payload, false).await;
        match (one_snapshot, two_moments) {
            (Ok(one_snapshot), Ok(two_moments)) => {
                if one_snapshot.ids.iter().all(|ids| ids.is_empty()) {
                    wrong.push(format!("{name}: no records"));
                }
                wrong.extend(differences(name, &one_snapshot, &two_moments));
            }
            (one_snapshot, two_moments) => wrong.push(format!(
                "{name}: in one snapshot {:?}, in two moments {:?}",
                one_snapshot.err(),
                two_moments.err()
            )),
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

// A SPANN collection with its dimension but no files yet, as after a first write that failed once the frontend gave the
// collection its dimension: a search answers none, as a query and a get do, in two moments as in one snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spann_search_of_a_collection_with_a_dimension_and_no_files_answers_none() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let collection_id = frontend
        .create_collection(
            CreateCollectionRequest::try_new(
                TENANT.to_string(),
                DatabaseName::new(DATABASE).unwrap(),
                "dimension-only".to_string(),
                None,
                None,
                Some(Schema::new_default(KnnIndex::Spann)),
                false,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .collection_id;
    frontend
        .set_collection_dimension(DatabaseName::new(DATABASE).unwrap(), collection_id, 2)
        .await
        .unwrap();
    let payloads = [
        SearchPayload {
            rank: Rank {
                expr: Some(knn(
                    QueryVector::Dense(vec![1.0, 0.5]),
                    Key::Embedding,
                    10,
                    false,
                )),
            },
            select: select(&[Key::Document, Key::Score]),
            ..Default::default()
        },
        SearchPayload::default(),
    ];
    for in_one_snapshot in [false, true] {
        for payload in payloads.clone() {
            match search_with(&mut frontend, collection_id, payload, in_one_snapshot).await {
                Ok(response) => assert!(
                    response.ids.iter().all(|ids| ids.is_empty()),
                    "in one snapshot {in_one_snapshot}: {:?}",
                    response.ids
                ),
                Err(err) => panic!("in one snapshot {in_one_snapshot}: {err}"),
            }
        }
    }
    let query = frontend
        .query(
            QueryRequest::try_new(
                TENANT.to_string(),
                DATABASE.to_string(),
                collection_id,
                None,
                None,
                vec![vec![1.0, 0.5]],
                10,
                chroma_types::IncludeList::default_query(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(query.ids.iter().all(|ids| ids.is_empty()));
    match spann_read(&mut frontend, collection_id, SpannRead::Get).await {
        Ok(ReadAnswer::Records(records)) => assert!(records[0].is_empty()),
        other => panic!("the get answered {other:?}"),
    }
}

// The time of the searches in two moments and in one snapshot, on the same collection of 2000 records: run by hand, in
// release (cargo test --release ... -- --ignored --nocapture). For each search, 20 rounds of one run of each, the
// first of the two in turn; it prints the median, the lowest and the highest of each, in ms, and in how many rounds the
// search in two moments was faster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measure of the time of the searches, run by hand in release"]
async fn spann_search_times_in_two_moments_and_in_one_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut frontend = frontend_with_spann(root.path(), true).await;
    let mut numbers = Numbers(0x5eed_1234_abcd_ef01);
    let collection_id = collection_to_search(&mut frontend, &mut numbers, 2000).await;
    for (name, payload) in searches(&mut numbers) {
        let mut two_moments = Vec::new();
        let mut one_snapshot = Vec::new();
        let mut faster = 0;
        for round in 0..20 {
            // The time of the search in two moments, then of the search in one snapshot.
            let mut took = [0.0; 2];
            let order = if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for in_one_snapshot in order {
                let start = std::time::Instant::now();
                search_with(
                    &mut frontend,
                    collection_id,
                    payload.clone(),
                    in_one_snapshot,
                )
                .await
                .unwrap();
                took[in_one_snapshot as usize] = start.elapsed().as_secs_f64() * 1000.0;
            }
            if took[0] < took[1] {
                faster += 1;
            }
            two_moments.push(took[0]);
            one_snapshot.push(took[1]);
        }
        let spread = |mut times: Vec<f64>| {
            times.sort_by(|a, b| a.total_cmp(b));
            (times[times.len() / 2], times[0], times[times.len() - 1])
        };
        let (two, two_lowest, two_highest) = spread(two_moments);
        let (one, one_lowest, one_highest) = spread(one_snapshot);
        let two = format!("in two moments {two:.2} ms ({two_lowest:.2}-{two_highest:.2})");
        let one = format!("in one snapshot {one:.2} ms ({one_lowest:.2}-{one_highest:.2})");
        println!("{name}: {two}, {one}, the two moments faster in {faster} of 20 rounds");
    }
}
