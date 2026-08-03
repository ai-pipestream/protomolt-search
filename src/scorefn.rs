//! Score-function chains over numeric columns (`docs/score-functions.md`).
//!
//! The final score of a document is a chain of stages applied in
//! request-list order to its BM25 score, on the node, BEFORE the floor
//! test and heap insertion. Every stage satisfies one contract that
//! makes chaining sound under block-max pruning:
//!
//! - `eval` is monotone non-decreasing in the incoming score, and
//! - `bound` lifts an upper bound through the stage using the column's
//!   min/max metadata, valid over the whole domain INCLUDING absence
//!   (a document without a value passes through unchanged).
//!
//! Under that contract "upper bound in, upper bound out" survives
//! composition: the pruned scorer lifts every bound sum through
//! [`ScoreChain::bound`] before its inert tests and inserts candidates
//! by their [`ScoreChain::eval`] result, and MaxScore, seeded floors,
//! and `kth_best` all keep working on the final-score scale with no
//! new theorems. Lifted bounds stay non-negative (BM25 bounds start at
//! zero or above, multiplicative factors stay at least 1 on the bound
//! side because absence caps them at identity, and additive lifts add
//! nothing negative), which is what keeps the multiplicative lift an
//! upper bound even when a negative additive stage pushes some
//! document's true score below zero.
//!
//! Chain-list order is the pinned evaluation order (IEEE float math is
//! not associative across reorderings), identical on every shard, so
//! distributed results stay bitwise equal to the monolith's.

/// Read surface for numeric columns during scoring, implemented by the
/// heap store and the mmap reader (via the node's shard wrapper).
pub trait NumericRead {
    /// `doc_id`'s value in numeric column `ni`, `None` when absent.
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64>;
    /// `doc_id`'s value under `key_ord` in map-numeric column
    /// `column` (`docs/map-columns.md`), `None` when absent.
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64>;
    /// `doc_id`'s value in i64 column `ii` (`docs/range-facets.md`),
    /// `None` when absent. Kept i64 here and cast at the eval site so
    /// the storage stays exact and only the arithmetic is float.
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64>;
}

/// A stage's resolved column on THIS shard: a plain f64 column, an
/// i64 column, or a map-numeric column entry under a shard-local key
/// ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnRef {
    /// Index into the shard's numeric table.
    Numeric(usize),
    /// Index into the shard's integer table. Values reach the stage
    /// arithmetic through `as f64`, which is monotone non-decreasing,
    /// so a bound computed from the column's min/max converted the
    /// same way is still a bound (rounding cannot reorder the two
    /// sides). Above 2^53 the stage arithmetic loses the exactness the
    /// STORAGE keeps — the point of the kind is that the value comes
    /// back intact, not that ln() suddenly has 64 bits.
    Integer(usize),
    /// A map-numeric column and a key ordinal in ITS key dictionary
    /// (ordinals are shard-local, like every dictionary here).
    MapKey {
        /// Index into the shard's map-numeric table.
        column: usize,
        /// Key ordinal within that column's key dictionary.
        key_ord: u32,
    },
}

/// One stage's transform. Every op is monotone non-decreasing in the
/// incoming score; parameters are validated at request parse (finite,
/// `scale > 0`, `MULT_LOG` weight >= 0 — a negative log weight could
/// turn the factor negative, which breaks monotonicity, so it is
/// refused rather than admitted).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StageOp {
    /// `score * exp(-|x - origin| / scale)`: recency decay. Factor in
    /// (0, 1], so the bound lift is identity (absence already caps the
    /// factor at 1).
    MultExpDecay {
        /// The value at which the factor is 1.
        origin: f64,
        /// Decay scale (> 0).
        scale: f64,
    },
    /// `score * (1 + weight * ln(1 + max(x, 0)))`: citation-count
    /// style boost, `weight >= 0`.
    MultLog {
        /// Boost weight (>= 0).
        weight: f64,
    },
    /// `score + weight * x`: additive boost, any finite weight.
    AddLinear {
        /// Addend weight.
        weight: f64,
    },
}

/// One resolved stage: the op plus THIS shard's view of its column.
#[derive(Debug, Clone, Copy)]
pub struct Stage {
    /// The transform.
    pub op: StageOp,
    /// The resolved column reference, `None` when the shard lacks the
    /// column (or, for map stages, the key) — every document is then
    /// absent and the stage is identity, which is exact, not degraded
    /// (the coordinator refuses a column NO shard knows).
    pub column: Option<ColumnRef>,
    /// This shard's (min, max) over the read values — the whole column
    /// for a plain stage, the KEY's values for a map stage; NaN when
    /// missing or empty (an i64 column's metadata arrives through the
    /// same monotone `as f64` cast its values do, and its empty range
    /// arrives as NaN). Feeds [`ScoreChain::bound`].
    pub min_max: (f64, f64),
}

/// A resolved score-function chain; empty means identity.
#[derive(Debug, Clone, Default)]
pub struct ScoreChain {
    /// Stages in the pinned evaluation order (request-list order).
    pub stages: Vec<Stage>,
}

impl ScoreChain {
    /// The document's final score: stages applied in order. Absent
    /// values (or an unresolved column) leave the score unchanged.
    pub fn eval(&self, score: f64, doc_id: u32, columns: &dyn NumericRead) -> f64 {
        let mut s = score;
        for stage in &self.stages {
            let x = match stage.column {
                Some(ColumnRef::Numeric(ni)) => columns.value(ni, doc_id),
                Some(ColumnRef::Integer(ii)) => {
                    columns.int_value(ii, doc_id).map(|v| v as f64)
                }
                Some(ColumnRef::MapKey { column, key_ord }) => {
                    columns.map_value(column, key_ord, doc_id)
                }
                None => None,
            };
            let Some(x) = x else {
                continue;
            };
            s = match stage.op {
                StageOp::MultExpDecay { origin, scale } => s * (-((x - origin).abs()) / scale).exp(),
                StageOp::MultLog { weight } => s * (1.0 + weight * (1.0 + x.max(0.0)).ln()),
                StageOp::AddLinear { weight } => s + weight * x,
            };
        }
        s
    }

    /// Lift an upper bound through the chain: for every stage, the
    /// most favorable value in the column's domain — including absence
    /// (identity) — bounds its effect. Monotone, and maps non-negative
    /// bounds to non-negative bounds (see the module docs for why that
    /// matters).
    pub fn bound(&self, mut ub: f64) -> f64 {
        for stage in &self.stages {
            let (_, max) = stage.min_max;
            ub = match stage.op {
                // Factor <= 1 everywhere and absence means 1 exactly.
                StageOp::MultExpDecay { .. } => ub,
                StageOp::MultLog { weight } => {
                    let factor = if max.is_nan() {
                        1.0
                    } else {
                        1.0 + weight * (1.0 + max.max(0.0)).ln()
                    };
                    ub * factor.max(1.0)
                }
                StageOp::AddLinear { weight } => {
                    let (min, _) = stage.min_max;
                    let addend = if max.is_nan() {
                        0.0
                    } else {
                        (weight * min).max(weight * max)
                    };
                    ub + addend.max(0.0)
                }
            };
        }
        ub
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Cols(Vec<Vec<Option<f64>>>);
    impl NumericRead for Cols {
        fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
            self.0[ni][doc_id as usize]
        }
        fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
            // The unit tests model a map key as one more plain column.
            self.0[column + key_ord as usize][doc_id as usize]
        }
        fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
            self.0[ii][doc_id as usize].map(|v| v as i64)
        }
    }

    /// Every op: hand-computed eval, absence identity, and the bound
    /// dominating eval over the whole domain.
    #[test]
    fn stage_eval_matches_hand_computed_and_bound_dominates()  {
        let cols = Cols(vec![vec![Some(3.0), None, Some(-2.0)]]);
        let chain = |op| ScoreChain {
            stages: vec![Stage {
                op,
                column: Some(ColumnRef::Numeric(0)),
                min_max: (-2.0, 3.0),
            }],
        };

        let decay = chain(StageOp::MultExpDecay {
            origin: 5.0,
            scale: 2.0,
        });
        assert_eq!(decay.eval(2.0, 0, &cols), 2.0 * (-1.0f64).exp());
        assert_eq!(decay.eval(2.0, 1, &cols), 2.0, "absent = identity");
        assert_eq!(decay.bound(2.0), 2.0);

        let log = chain(StageOp::MultLog { weight: 0.5 });
        assert_eq!(log.eval(2.0, 0, &cols), 2.0 * (1.0 + 0.5 * 4.0f64.ln()));
        assert_eq!(log.eval(2.0, 2, &cols), 2.0, "negative x clamps to factor 1");
        assert!(log.bound(2.0) >= log.eval(2.0, 0, &cols));

        let add = chain(StageOp::AddLinear { weight: -1.0 });
        assert_eq!(add.eval(2.0, 0, &cols), -1.0);
        assert_eq!(add.eval(2.0, 1, &cols), 2.0);
        // Most favorable addend is -1.0 * -2.0 = 2.0.
        assert_eq!(add.bound(2.0), 4.0);
    }

    /// Chained bound dominates chained eval for every document,
    /// including one that goes negative mid-chain.
    #[test]
    fn chained_bound_dominates_chained_eval() {
        let cols = Cols(vec![
            vec![Some(10.0), Some(-10.0), None],
            vec![Some(2.0), Some(7.0), Some(0.5)],
        ]);
        let chain = ScoreChain {
            stages: vec![
                Stage {
                    op: StageOp::AddLinear { weight: 0.3 },
                    column: Some(ColumnRef::Numeric(0)),
                    min_max: (-10.0, 10.0),
                },
                Stage {
                    op: StageOp::MultExpDecay {
                        origin: 0.0,
                        scale: 3.0,
                    },
                    column: Some(ColumnRef::Numeric(1)),
                    min_max: (0.5, 7.0),
                },
                Stage {
                    op: StageOp::MultLog { weight: 1.0 },
                    column: Some(ColumnRef::Numeric(1)),
                    min_max: (0.5, 7.0),
                },
            ],
        };
        for score in [0.0, 0.5, 4.0] {
            let ub = chain.bound(score);
            for doc in 0..3u32 {
                assert!(
                    chain.eval(score, doc, &cols) <= ub,
                    "doc {doc} at score {score}"
                );
            }
        }
        // A column this shard lacks: identity everywhere.
        let unresolved = ScoreChain {
            stages: vec![Stage {
                op: StageOp::MultLog { weight: 2.0 },
                column: None,
                min_max: (f64::NAN, f64::NAN),
            }],
        };
        assert_eq!(unresolved.eval(1.5, 0, &cols), 1.5);
        assert_eq!(unresolved.bound(1.5), 1.5);
    }
}
