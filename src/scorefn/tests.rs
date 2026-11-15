use super::*;

struct Cols(Vec<Vec<Option<f64>>>);
impl Cols {
    /// A geo read surface backed by one hard-coded column: doc 0 has
    /// a point, doc 1 does not.
    fn geo() -> Self {
        Cols(Vec::new())
    }
}
impl NumericRead for Cols {
    fn uint_value(&self, _ii: usize, _doc_id: u32) -> Option<u64> {
        None
    }
    fn geo_value(&self, _gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        match doc_id {
            0 => Some((38.8977, -77.0365)),
            _ => None,
        }
    }
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0[ni][doc_id as usize]
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        // The unit tests model a map key as one more plain column.
        self.0[column + key_ord as usize][doc_id as usize]
    }
    fn map_int_value(&self, _: usize, _: u32, _: u32) -> Option<i64> {
        None
    }
    fn map_uint_value(&self, _: usize, _: u32, _: u32) -> Option<u64> {
        None
    }
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.0[ii][doc_id as usize].map(|v| v as i64)
    }
    fn facet_ord(&self, _fi: usize, _doc_id: u32) -> Option<u32> {
        // The chain tests hold no facet columns; the filter tests
        // (src/filter.rs) bring their own surface.
        None
    }
    fn map_facet_value_ord(&self, _ci: usize, _key_ord: u32, _doc_id: u32) -> Option<u32> {
        None
    }
}

/// Every op: hand-computed eval, absence identity, and the bound
/// dominating eval over the whole domain.
#[test]
fn stage_eval_matches_hand_computed_and_bound_dominates() {
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
    assert_eq!(
        log.eval(2.0, 2, &cols),
        2.0,
        "negative x clamps to factor 1"
    );
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

/// The geo decay stage: hand-computed multiplier, absence as exact
/// identity, an unresolved geo column as identity, and the bound
/// lift at 1 (which is what absence forces — see `bound`).
#[test]
fn geo_decay_eval_matches_hand_computed_and_bounds_at_one() {
    let cols = Cols::geo();
    let stage = |metric| ScoreChain {
        stages: vec![Stage {
            op: StageOp::MultGeoDecay {
                metric,
                origin_lat: 38.8899,
                origin_lon: -77.0091,
                scale: 5_000.0,
            },
            column: Some(ColumnRef::Geo(0)),
            min_max: (f64::NAN, f64::NAN),
        }],
    };
    for metric in [
        crate::geo::GeoMetric::Haversine,
        crate::geo::GeoMetric::Manhattan,
    ] {
        let chain = stage(metric);
        let d = metric.meters(38.8899, -77.0091, 38.8977, -77.0365);
        assert_eq!(
            chain.eval(2.0, 0, &cols).to_bits(),
            (2.0 * (-d / 5_000.0).exp()).to_bits(),
            "{metric:?}: the multiplier is exp(-distance/scale), exactly"
        );
        let factor = chain.eval(2.0, 0, &cols) / 2.0;
        assert!(factor > 0.0 && factor <= 1.0, "{metric:?}: factor {factor}");
        assert_eq!(chain.eval(2.0, 1, &cols), 2.0, "absent = identity");
        // The bound must dominate both documents, and it is 1.0
        // BECAUSE doc 1 exists: absence caps the factor at identity.
        assert_eq!(chain.bound(2.0), 2.0);
        assert!(chain.eval(2.0, 0, &cols) <= chain.bound(2.0));
        assert!(chain.eval(2.0, 1, &cols) <= chain.bound(2.0));
    }
    // A shard whose table lacks the geo column: identity everywhere.
    let unresolved = ScoreChain {
        stages: vec![Stage {
            op: StageOp::MultGeoDecay {
                metric: crate::geo::GeoMetric::Haversine,
                origin_lat: 0.0,
                origin_lon: 0.0,
                scale: 1.0,
            },
            column: None,
            min_max: (f64::NAN, f64::NAN),
        }],
    };
    assert_eq!(unresolved.eval(1.5, 0, &cols), 1.5);
    assert_eq!(unresolved.bound(1.5), 1.5);
}
