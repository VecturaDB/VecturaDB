use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{RankExpr, RecordMeasure};
use thiserror::Error;

// NOTE: We assume that the provided vector of knn results are in the DFS order of Rank expression.
#[derive(Clone, Debug)]
pub struct RankInput {
    pub knn_results: Vec<Vec<RecordMeasure>>,
}

#[derive(Clone, Debug)]
pub struct RankOutput {
    pub ranks: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
#[error("Rank error (unreachable)")]
pub struct RankError;

impl ChromaError for RankError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

// The evaluation is in chroma_types (execution/ranking.rs), shared with single-node Chroma.
#[async_trait]
impl Operator<RankInput, RankOutput> for RankExpr {
    type Error = RankError;

    async fn run(&self, input: &RankInput) -> Result<RankOutput, RankError> {
        Ok(RankOutput {
            ranks: self.rank(input.knn_results.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chroma_types::operator::{Key, KnnQuery};

    use super::*;

    #[tokio::test]
    async fn test_rank_with_knn_results() {
        let query = KnnQuery {
            query: chroma_types::operator::QueryVector::Dense(vec![0.1, 0.2, 0.3]),
            key: Key::Embedding,
            limit: 3,
        };
        let knn_results = vec![vec![
            RecordMeasure {
                offset_id: 1,
                measure: 0.9,
            },
            RecordMeasure {
                offset_id: 2,
                measure: 0.7,
            },
            RecordMeasure {
                offset_id: 3,
                measure: 0.5,
            },
        ]];

        // Test simple KNN rank
        let expr = RankExpr::Knn {
            query: query.query.clone(),
            key: Key::Embedding,
            limit: query.limit,
            default: None,
            return_rank: false,
        };
        let input = RankInput { knn_results };

        let output = expr.run(&input).await.expect("Rank should succeed");
        assert_eq!(output.ranks.len(), 3);
        // After removing .reverse(), results are in ascending order by measure
        assert_eq!(output.ranks[0].offset_id, 3);
        assert_eq!(output.ranks[0].measure, 0.5);
    }

    #[tokio::test]
    async fn test_rank_arithmetic_operations() {
        let query1 = KnnQuery {
            query: chroma_types::operator::QueryVector::Dense(vec![0.1]),
            key: Key::Embedding,
            limit: 2,
        };
        let query2 = KnnQuery {
            query: chroma_types::operator::QueryVector::Sparse(chroma_types::SparseVector {
                indices: vec![0],
                values: vec![1.0],
                tokens: None,
            }),
            key: Key::field("sparse"),
            limit: 2,
        };
        let mut knn_results = vec![
            vec![
                RecordMeasure {
                    offset_id: 1,
                    measure: 0.8,
                },
                RecordMeasure {
                    offset_id: 2,
                    measure: 0.6,
                },
            ],
            vec![
                RecordMeasure {
                    offset_id: 1,
                    measure: 0.4,
                },
                RecordMeasure {
                    offset_id: 3,
                    measure: 0.2,
                },
            ],
        ];

        // Test summation
        let expr = RankExpr::Summation(vec![
            RankExpr::Knn {
                query: query1.query.clone(),
                key: Key::Embedding,
                limit: query1.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Knn {
                query: query2.query.clone(),
                key: Key::field("sparse"),
                limit: query2.limit,
                default: None,
                return_rank: false,
            },
        ]);
        let input = RankInput {
            knn_results: knn_results.clone(),
        };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Summation results:
        // Only Record 1 appears in both lists: 0.8 + 0.4 = 1.2
        // Records 2 and 3 are filtered out since they don't appear in both lists
        // and both Knn operations have default: None
        assert_eq!(output.ranks.len(), 1);
        assert_eq!(output.ranks[0].offset_id, 1);
        assert_eq!(output.ranks[0].measure, 1.2);

        // Test multiplication with constant
        knn_results.pop();
        let expr = RankExpr::Multiplication(vec![
            RankExpr::Knn {
                query: query1.query.clone(),
                key: Key::Embedding,
                limit: query1.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Value(0.5),
        ]);
        let input = RankInput { knn_results };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Results are in ascending order, so the record with the lowest measure comes first
        // After multiplication by 0.5: record 1 = 0.8 * 0.5 = 0.4, record 2 = 0.6 * 0.5 = 0.3
        assert_eq!(output.ranks[0].offset_id, 2);
        assert_eq!(output.ranks[0].measure, 0.3); // 0.6 * 0.5
    }

    #[tokio::test]
    async fn test_rank_min_max_functions() {
        let query = KnnQuery {
            query: chroma_types::operator::QueryVector::Dense(vec![0.1]),
            key: Key::Embedding,
            limit: 2,
        };
        let knn_results = vec![vec![
            RecordMeasure {
                offset_id: 1,
                measure: 0.8,
            },
            RecordMeasure {
                offset_id: 2,
                measure: 0.3,
            },
        ]];

        // Test max
        let expr = RankExpr::Maximum(vec![
            RankExpr::Knn {
                query: query.query.clone(),
                key: Key::Embedding,
                limit: query.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Value(0.5),
        ]);
        let input = RankInput {
            knn_results: knn_results.clone(),
        };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Results are in ascending order
        assert_eq!(output.ranks[0].offset_id, 2);
        assert_eq!(output.ranks[0].measure, 0.5); // max(0.3, 0.5) = 0.5
        assert_eq!(output.ranks[1].offset_id, 1);
        assert_eq!(output.ranks[1].measure, 0.8); // max(0.8, 0.5) = 0.8

        // Test min
        let expr = RankExpr::Minimum(vec![
            RankExpr::Knn {
                query: query.query.clone(),
                key: Key::Embedding,
                limit: query.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Value(0.5),
        ]);
        let input = RankInput { knn_results };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Results are in ascending order
        assert_eq!(output.ranks[0].offset_id, 2);
        assert_eq!(output.ranks[0].measure, 0.3); // min(0.3, 0.5) = 0.3
        assert_eq!(output.ranks[1].offset_id, 1);
        assert_eq!(output.ranks[1].measure, 0.5); // min(0.8, 0.5) = 0.5
    }

    fn sparse_leaf(index: u32, key: &str) -> RankExpr {
        RankExpr::Knn {
            query: chroma_types::operator::QueryVector::Sparse(chroma_types::SparseVector {
                indices: vec![index],
                values: vec![1.0],
                tokens: None,
            }),
            key: Key::field(key),
            limit: 10,
            // Zero default so records missing from one index still participate.
            default: Some(0.0),
            return_rank: false,
        }
    }

    /// Weighted arithmetic fusion across two distinct sparse indices:
    /// `sparse_a * 0.7 + sparse_b * 0.3`. The two leaves consume independent
    /// per-key result slots in DFS order.
    #[tokio::test]
    async fn test_fuse_multiple_sparse_indices_weighted_sum() {
        let expr = RankExpr::Summation(vec![
            RankExpr::Multiplication(vec![sparse_leaf(0, "sparse_a"), RankExpr::Value(0.7)]),
            RankExpr::Multiplication(vec![sparse_leaf(1, "sparse_b"), RankExpr::Value(0.3)]),
        ]);

        // DFS leaf order: [sparse_a, sparse_b].
        let knn_results = vec![
            vec![
                RecordMeasure {
                    offset_id: 1,
                    measure: 1.0,
                },
                RecordMeasure {
                    offset_id: 2,
                    measure: 0.5,
                },
            ],
            vec![
                RecordMeasure {
                    offset_id: 1,
                    measure: 0.2,
                },
                RecordMeasure {
                    offset_id: 3,
                    measure: 0.8,
                },
            ],
        ];
        let input = RankInput { knn_results };
        let output = expr.run(&input).await.expect("Rank should succeed");

        let scores: HashMap<u32, f32> = output
            .ranks
            .iter()
            .map(|r| (r.offset_id, r.measure))
            .collect();
        // id1: 1.0*0.7 + 0.2*0.3 = 0.76
        // id2: 0.5*0.7 + 0.0*0.3 = 0.35
        // id3: 0.0*0.7 + 0.8*0.3 = 0.24
        assert_eq!(scores.len(), 3);
        assert!((scores[&1] - 0.76).abs() < 1e-6);
        assert!((scores[&2] - 0.35).abs() < 1e-6);
        assert!((scores[&3] - 0.24).abs() < 1e-6);
        // Ascending sort by measure.
        assert_eq!(output.ranks[0].offset_id, 3);
        assert_eq!(output.ranks[2].offset_id, 1);
    }

    /// Two `$knn` leaves on the SAME key with different query vectors must run
    /// independently — each consumes its own result slot rather than collapsing.
    #[tokio::test]
    async fn test_same_key_sparse_leaves_run_independently() {
        let expr =
            RankExpr::Summation(vec![sparse_leaf(0, "sparse_x"), sparse_leaf(1, "sparse_x")]);

        // Distinct result sets prove the leaves did not collapse into one.
        let knn_results = vec![
            vec![RecordMeasure {
                offset_id: 1,
                measure: 1.0,
            }],
            vec![RecordMeasure {
                offset_id: 2,
                measure: 1.0,
            }],
        ];
        let input = RankInput { knn_results };
        let output = expr.run(&input).await.expect("Rank should succeed");

        let scores: HashMap<u32, f32> = output
            .ranks
            .iter()
            .map(|r| (r.offset_id, r.measure))
            .collect();
        // Both records present: id1 from the first leaf, id2 from the second.
        assert_eq!(scores.len(), 2);
        assert!((scores[&1] - 1.0).abs() < 1e-6);
        assert!((scores[&2] - 1.0).abs() < 1e-6);
    }
}
