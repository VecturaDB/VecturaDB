// Changed from the original Chroma file; see the git history for the changes and their authors.
//! Evaluation of rank expressions and grouping of ranked records, shared by the operators of distributed Chroma
//! (rust/worker/src/execution/operators/rank.rs and ranked_group_by.rs) and the search of single-node Chroma.

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    ops::{Add, Div, Mul, Sub},
};

use crate::{
    operator::{Aggregate, GroupBy, Key, RankExpr, RecordMeasure},
    MetadataValue,
};

// NOTE: `RankDomain` represents evaluated scores for records
// - `support`: scores of specific records
// - `default`: scores of records not specified in `support`
//    where `None` suggests no other record is considered for evaluation
struct RankDomain {
    support: HashMap<u32, f32>,
    default: Option<f32>,
}

impl RankDomain {
    fn flat(default: f32) -> Self {
        Self {
            support: HashMap::new(),
            default: Some(default),
        }
    }
    fn map(self, op: impl Fn(f32) -> f32) -> Self {
        Self {
            support: self.support.into_iter().map(|(k, v)| (k, op(v))).collect(),
            default: self.default.map(op),
        }
    }
    fn merge(left: Self, right: Self, op: impl Fn(f32, f32) -> f32) -> Self {
        let left_base = left.support.keys().cloned().collect::<HashSet<_>>();
        let right_base = right.support.keys().cloned().collect::<HashSet<_>>();
        match (left.default, right.default) {
            (Some(left_default), Some(right_default)) => RankDomain {
                support: (&left_base | &right_base)
                    .into_iter()
                    .map(|id| {
                        (
                            id,
                            op(
                                left.support.get(&id).cloned().unwrap_or(left_default),
                                right.support.get(&id).cloned().unwrap_or(right_default),
                            ),
                        )
                    })
                    .collect(),
                default: Some(op(left_default, right_default)),
            },
            (Some(left_default), None) => RankDomain {
                support: right
                    .support
                    .into_iter()
                    .map(|(id, right_value)| {
                        (
                            id,
                            op(
                                left.support.get(&id).cloned().unwrap_or(left_default),
                                right_value,
                            ),
                        )
                    })
                    .collect(),
                default: None,
            },
            (None, Some(right_default)) => RankDomain {
                support: left
                    .support
                    .into_iter()
                    .map(|(id, left_value)| {
                        (
                            id,
                            op(
                                left_value,
                                right.support.get(&id).cloned().unwrap_or(right_default),
                            ),
                        )
                    })
                    .collect(),
                default: None,
            },
            (None, None) => RankDomain {
                support: (&left_base & &right_base)
                    .into_iter()
                    .filter_map(|id| {
                        let left_val = left.support.get(&id).cloned()?;
                        let right_val = right.support.get(&id).cloned()?;
                        Some((id, op(left_val, right_val)))
                    })
                    .collect(),
                default: None,
            },
        }
    }
}

struct RankProvider<R> {
    knn_result_iter: R,
}

impl<R> RankProvider<R>
where
    R: Iterator<Item = Vec<RecordMeasure>>,
{
    fn eval(&mut self, expr: RankExpr) -> RankDomain {
        match expr {
            RankExpr::Absolute(expr) => self.eval(*expr).map(f32::abs),
            RankExpr::Division { left, right } => {
                RankDomain::merge(self.eval(*left), self.eval(*right), f32::div)
            }
            RankExpr::Exponentiation(expr) => self.eval(*expr).map(f32::exp),
            RankExpr::Logarithm(expr) => self.eval(*expr).map(f32::ln),
            RankExpr::Maximum(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(f32::MIN), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::max)
                }),
            RankExpr::Minimum(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(f32::MAX), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::min)
                }),
            RankExpr::Multiplication(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(1.0), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::mul)
                }),
            RankExpr::Knn {
                query: _,
                key: _,
                limit: _,
                default,
                return_rank,
            } => {
                let support = self
                    .knn_result_iter
                    .next()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .map(|(index, RecordMeasure { offset_id, measure })| {
                        (offset_id, if return_rank { index as f32 } else { measure })
                    })
                    .collect();
                RankDomain { support, default }
            }
            RankExpr::Subtraction { left, right } => {
                RankDomain::merge(self.eval(*left), self.eval(*right), f32::sub)
            }
            RankExpr::Summation(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(0.0), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::add)
                }),
            RankExpr::Value(val) => RankDomain::flat(val),
        }
    }
}

impl RankExpr {
    /// The scores of the expression, sorted in ascending order.
    ///
    /// NOTE: We assume that the provided vector of knn results are in the DFS order of Rank expression.
    pub fn rank(&self, knn_results: Vec<Vec<RecordMeasure>>) -> Vec<RecordMeasure> {
        let mut rank_provider = RankProvider {
            knn_result_iter: knn_results.into_iter(),
        };
        let rank_domain = rank_provider.eval(self.clone());
        let mut ranks = rank_domain
            .support
            .into_iter()
            .map(|(offset_id, measure)| RecordMeasure { offset_id, measure })
            .collect::<Vec<_>>();
        ranks.sort_unstable();
        ranks
    }
}

/// Record enriched with metadata for grouping and sorting
#[derive(Clone)]
pub struct EnrichedRecord {
    pub record: RecordMeasure,
    pub metadata: HashMap<String, MetadataValue>,
}

impl EnrichedRecord {
    /// Extract key values for grouping or sorting
    fn extract_key(&self, keys: &[Key]) -> Vec<Option<MetadataValue>> {
        keys.iter()
            .map(|key| match key {
                Key::MetadataField(field) => self.metadata.get(field).cloned(),
                Key::Score => Some(MetadataValue::Float(self.record.measure as f64)),
                // Other keys shouldn't appear (validation prevents them)
                _ => None,
            })
            .collect()
    }
}

impl GroupBy {
    /// Groups the enriched records by the keys, keeps the top k of each group by the aggregate and flattens the
    /// groups back into a single list, sorted by score.
    pub fn group_ranked(&self, mut enriched_records: Vec<EnrichedRecord>) -> Vec<RecordMeasure> {
        // Fast path: no grouping configured or no records
        let aggregate = match &self.aggregate {
            Some(agg) if !self.keys.is_empty() && !enriched_records.is_empty() => agg,
            _ => {
                return enriched_records
                    .into_iter()
                    .map(|record| record.record)
                    .collect();
            }
        };

        // --- Group by ---

        enriched_records.sort_by_cached_key(|r| r.extract_key(&self.keys));

        let groups = enriched_records
            .chunk_by(|a, b| a.extract_key(&self.keys) == b.extract_key(&self.keys));

        // --- Aggregate ---

        let mut records = groups
            .flat_map(|group| {
                let mut group_vec = group.iter().collect::<Vec<_>>();

                match aggregate {
                    Aggregate::MinK { keys, k } => {
                        group_vec.sort_by_cached_key(|record| record.extract_key(keys));
                        group_vec
                            .into_iter()
                            .take(*k as usize)
                            .map(|record| record.record)
                            .collect::<Vec<_>>()
                    }
                    Aggregate::MaxK { keys, k } => {
                        group_vec.sort_by_cached_key(|record| Reverse(record.extract_key(keys)));
                        group_vec
                            .into_iter()
                            .map(|record| record.record)
                            .take(*k as usize)
                            .collect::<Vec<_>>()
                    }
                }
            })
            .collect::<Vec<_>>();

        // --- Flatten and re-sort ---

        records.sort();

        records
    }
}
