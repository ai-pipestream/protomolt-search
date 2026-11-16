use super::*;
use crate::pb::{
    DenseCoverage, DenseExecutionMode as Mode, DenseFilterRegimes, DenseFilterStrategy as Strategy,
    DensePrecisionRequest as Request, DenseQuery, DenseScorePrecision as Precision, DenseTechnique,
    DenseTermination,
};

fn turbovec() -> ProviderCaps {
    ProviderCaps {
        provider: "embedded-turbovec".into(),
        native: Precision::Quantized,
        exhaustive_scan: true,
        uncertified_scan: false,
        fp32_sidecar: true,
        walks: false,
        enumerates: false,
        staged: false,
        patience: false,
        floor_pruning: false,
        candidate_stream: true,
        rows: 100,
    }
}

fn hnsw() -> ProviderCaps {
    ProviderCaps {
        provider: "hnsw".into(),
        native: Precision::Fp32,
        exhaustive_scan: false,
        uncertified_scan: false,
        fp32_sidecar: false,
        walks: true,
        enumerates: true,
        staged: false,
        patience: true,
        floor_pruning: true,
        candidate_stream: true,
        rows: 100,
    }
}

fn clustered(fp32_sidecar: bool) -> ProviderCaps {
    ProviderCaps {
        provider: "clustered-turbovec".into(),
        native: Precision::Quantized,
        exhaustive_scan: false,
        uncertified_scan: true,
        fp32_sidecar,
        walks: false,
        enumerates: false,
        staged: false,
        patience: false,
        floor_pruning: false,
        candidate_stream: true,
        rows: 100,
    }
}

fn request(mode: Mode, precision: Request) -> DenseRequest {
    DenseRequest {
        mode,
        precision,
        filter_strategy: Strategy::Default,
    }
}

fn plan(
    mode: Mode,
    precision: Request,
    caps: &ProviderCaps,
) -> Result<ResolvedDensePlan, DenseExecutionError> {
    resolve(request(mode, precision), caps)
}

#[test]
fn requests_name_a_mode_and_routing_only_where_the_caller_owns_it() {
    let query = |mode: Mode, strategy: Strategy| DenseQuery {
        execution_mode: mode as i32,
        filter_strategy: strategy as i32,
        ..Default::default()
    };
    assert!(parse_request(&query(Mode::Unspecified, Strategy::Default)).is_err());
    for retired in 1..=3 {
        let mut q = query(Mode::Balanced, Strategy::Default);
        q.execution_mode = retired;
        assert!(parse_request(&q).is_err(), "retired value {retired}");
    }
    assert!(parse_request(&query(Mode::Exhaustive, Strategy::Speed)).is_err());
    assert!(parse_request(&query(Mode::Auto, Strategy::EnumerateSmall)).is_err());
    let mut q = query(Mode::Fast, Strategy::Speed);
    q.precision = 9;
    assert!(parse_request(&q).is_err());
    let parsed = parse_request(&query(Mode::Balanced, Strategy::Speed)).unwrap();
    assert_eq!(parsed.filter_strategy, Strategy::Speed);
    assert_eq!(parsed.precision, Request::Auto);
}

#[test]
fn raw_controls_reject_contradictions_and_verify_peer_capabilities() {
    let contradicted = RawDenseControls {
        enumerate_all: true,
        filter_strategy: Strategy::Speed as i32,
        ..Default::default()
    };
    assert!(matches!(
        validate_raw_controls(&contradicted, Some(&hnsw())),
        Err(DenseExecutionError::InvalidRequest(_))
    ));
    let enum_on_scan = RawDenseControls {
        enumerate_all: true,
        ..Default::default()
    };
    assert!(matches!(
        validate_raw_controls(&enum_on_scan, Some(&turbovec())),
        Err(DenseExecutionError::FailedPrecondition(_))
    ));
    assert!(validate_raw_controls(&enum_on_scan, Some(&hnsw())).is_ok());
}

#[test]
fn quantized_provider_matrix() {
    let caps = turbovec();
    let p = plan(Mode::Exhaustive, Request::Auto, &caps).unwrap();
    assert_eq!(
        (p.route, p.output_precision),
        (Route::NativeScan, Precision::Quantized)
    );
    let p = plan(Mode::Exhaustive, Request::Fp32, &caps).unwrap();
    assert_eq!(
        (p.route, p.selection_precision),
        (Route::Fp32AllRows, Precision::Fp32)
    );
    let p = plan(Mode::Balanced, Request::Fp32, &caps).unwrap();
    assert_eq!(p.route, Route::Fp32Pool);
    assert_eq!(p.selection_precision, Precision::Quantized);
    assert_eq!(p.output_precision, Precision::Fp32);
    assert!(!p.fallback_reason.is_empty());
    let p = plan(Mode::Balanced, Request::Quantized, &caps).unwrap();
    assert_eq!(
        (p.resolved_mode, p.route),
        (Mode::Exhaustive, Route::NativeScan)
    );
    assert!(p.fallback_reason.contains("same precision"));
    assert!(plan(Mode::Fast, Request::Auto, &caps).is_err());
    let p = plan(Mode::Auto, Request::Auto, &caps).unwrap();
    assert_eq!(
        (p.resolved_mode, p.route),
        (Mode::Exhaustive, Route::NativeScan)
    );
}

#[test]
fn graph_provider_matrix() {
    let caps = hnsw();
    assert!(plan(Mode::Exhaustive, Request::Quantized, &caps).is_err());
    let p = plan(Mode::Exhaustive, Request::Auto, &caps).unwrap();
    assert_eq!(
        (p.route, p.output_precision),
        (Route::EnumerateAll, Precision::Fp32)
    );
    let p = plan(Mode::Balanced, Request::Fp32, &caps).unwrap();
    assert_eq!((p.route, p.preset), (Route::Walk, Some(HNSW_BALANCED)));
    assert_eq!(p.filter_strategy, Strategy::EnumerateSmall);
    let p = plan(Mode::Fast, Request::Auto, &caps).unwrap();
    assert_eq!(
        (p.preset, p.filter_strategy),
        (Some(HNSW_FAST), Strategy::Speed)
    );
    let mut no_patience = caps.clone();
    no_patience.patience = false;
    assert!(plan(Mode::Fast, Request::Auto, &no_patience).is_err());
    let explicit = DenseRequest {
        filter_strategy: Strategy::Speed,
        ..request(Mode::Balanced, Request::Auto)
    };
    assert_eq!(
        resolve(explicit, &caps).unwrap().filter_strategy,
        Strategy::Speed
    );
    let p = plan(Mode::Auto, Request::Fp32, &caps).unwrap();
    assert_eq!(p.route, Route::EnumerateAll);
}

#[test]
fn quantized_walker_matrix() {
    let caps = ProviderCaps {
        provider: "fake-ann".into(),
        native: Precision::Quantized,
        exhaustive_scan: false,
        uncertified_scan: false,
        fp32_sidecar: false,
        walks: true,
        enumerates: false,
        staged: false,
        patience: false,
        floor_pruning: false,
        candidate_stream: false,
        rows: 100,
    };
    assert!(plan(Mode::Exhaustive, Request::Auto, &caps).is_err());
    assert!(plan(Mode::Fast, Request::Auto, &caps).is_err());
    let p = plan(Mode::Balanced, Request::Auto, &caps).unwrap();
    assert_eq!(
        (p.route, p.output_precision, p.filter_strategy),
        (Route::Walk, Precision::Quantized, Strategy::Speed)
    );
    let p = plan(Mode::Balanced, Request::Fp32, &caps).unwrap();
    assert_eq!(
        (p.route, p.selection_precision),
        (Route::Fp32Pool, Precision::Quantized)
    );
    assert_eq!(p.pool_selection, DenseTechnique::HnswWalk);
    let mut outcome = planned_outcome(&p);
    let walked = Evidence {
        consulted_shards: 1,
        reported_shards: 1,
        parts_total: 2,
        parts_walked: 2,
        rows: 100,
        ..Default::default()
    };
    stamp(&p, &walked, &mut outcome).unwrap();
    assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
    assert_eq!(
        outcome.techniques[0].technique,
        DenseTechnique::HnswWalk as i32
    );
}

#[test]
fn exhaustive_receipts_require_every_consulted_part_without_walks_or_stops() {
    let p = plan(Mode::Exhaustive, Request::Auto, &hnsw()).unwrap();
    let full_enum = Evidence {
        consulted_shards: 2,
        reported_shards: 2,
        parts_total: 2,
        parts_enumerated: 2,
        rows: 100,
        ..Default::default()
    };
    let mut outcome = planned_outcome(&p);
    stamp(&p, &full_enum, &mut outcome).unwrap();
    assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert_eq!(outcome.termination, DenseTermination::Exhausted as i32);
    assert_eq!(outcome.techniques[0].parts, 2);
    for bad in [
        Evidence {
            reported_shards: 1,
            ..full_enum.clone()
        },
        Evidence {
            parts_enumerated: 1,
            parts_walked: 1,
            ..full_enum.clone()
        },
        Evidence {
            parts_enumerated: 0,
            ..full_enum.clone()
        },
        Evidence {
            patience_stopped: 1,
            ..full_enum.clone()
        },
        Evidence {
            shards_finished_early: 1,
            ..full_enum.clone()
        },
    ] {
        assert!(stamp(&p, &bad, &mut planned_outcome(&p)).is_err());
    }
    let fp32 = plan(Mode::Exhaustive, Request::Fp32, &turbovec()).unwrap();
    assert!(stamp(&fp32, &full_enum, &mut planned_outcome(&fp32)).is_err());
    let mut fp32_out = planned_outcome(&fp32);
    stamp(
        &fp32,
        &Evidence {
            fp32_rows_scored: 100,
            ..full_enum
        },
        &mut fp32_out,
    )
    .unwrap();
    assert_eq!(
        fp32_out.techniques,
        vec![crate::pb::DenseTechniqueCount {
            technique: DenseTechnique::Fp32Scan as i32,
            parts: 2,
        }]
    );
}

#[test]
fn approximate_receipts_distinguish_tail_enumeration_from_graph_walks() {
    let bal = plan(Mode::Balanced, Request::Fp32, &hnsw()).unwrap();
    let mut tail_only = planned_outcome(&bal);
    stamp(
        &bal,
        &Evidence {
            consulted_shards: 1,
            reported_shards: 1,
            parts_total: 1,
            parts_enumerated: 1,
            tail_enumerated_rows: 16,
            rows: 16,
            ..Default::default()
        },
        &mut tail_only,
    )
    .unwrap();
    assert_eq!(tail_only.coverage, DenseCoverage::CandidateSet as i32);
    assert_eq!(tail_only.termination, DenseTermination::Exhausted as i32);
    assert_eq!(
        tail_only.techniques,
        vec![crate::pb::DenseTechniqueCount {
            technique: DenseTechnique::Fp32Scan as i32,
            parts: 1,
        }]
    );
    let p = plan(Mode::Fast, Request::Auto, &hnsw()).unwrap();
    let mut mixed = planned_outcome(&p);
    stamp(
        &p,
        &Evidence {
            regimes: Some(DenseFilterRegimes {
                enumerate: 1,
                filter_aware: 1,
                ..Default::default()
            }),
            consulted_shards: 1,
            reported_shards: 1,
            parts_total: 2,
            parts_walked: 1,
            parts_enumerated: 1,
            patience_stopped: 1,
            rows: 100,
            ..Default::default()
        },
        &mut mixed,
    )
    .unwrap();
    assert_eq!(mixed.termination, DenseTermination::BudgetStopped as i32);
    assert_eq!(mixed.techniques.len(), 2);
}

#[test]
fn per_shard_fp32_over_reporting_and_uncertified_backends_fail_closed() {
    use super::receipt::{verify_exhaustive_scan, ShardPartEvidence};

    let fp32 = plan(Mode::Exhaustive, Request::Fp32, &turbovec()).unwrap();
    let piggybacked = Evidence {
        consulted_shards: 2,
        reported_shards: 2,
        parts_total: 2,
        parts_enumerated: 2,
        fp32_rows_scored: 64,
        rows: 128,
        shards: vec![
            ShardPartEvidence {
                parts_total: 1,
                parts_enumerated: 1,
                fp32_rows_scored: 64,
                ..Default::default()
            },
            ShardPartEvidence {
                parts_total: 1,
                parts_enumerated: 1,
                fp32_rows_scored: 0,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert!(stamp(&fp32, &piggybacked, &mut planned_outcome(&fp32)).is_err());

    let walk = plan(Mode::Balanced, Request::Auto, &hnsw()).unwrap();
    let over_reported = Evidence {
        consulted_shards: 2,
        reported_shards: 2,
        parts_total: 2,
        parts_walked: 2,
        shards_finished_early: 1,
        rows: 100,
        shards: vec![
            ShardPartEvidence {
                parts_total: 1,
                parts_walked: 0,
                finished_early: true,
                ..Default::default()
            },
            ShardPartEvidence {
                parts_total: 1,
                parts_walked: 1,
                parts_enumerated: 1,
                finished_early: false,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert!(stamp(&walk, &over_reported, &mut planned_outcome(&walk)).is_err());
    assert!(verify_exhaustive_scan(
        Route::NativeScan,
        &Evidence {
            rows: 200,
            consulted_shards: 1,
            ..Default::default()
        }
    )
    .is_err());
}

#[test]
fn clustered_turbovec_refuses_uncertified_exhaustive_and_stamps_balanced() {
    let c = clustered(true);
    for mode in [Mode::Exhaustive, Mode::Auto] {
        assert!(plan(mode, Request::Auto, &c).is_err());
        assert!(plan(mode, Request::Quantized, &c).is_err());
        assert_eq!(
            plan(mode, Request::Fp32, &c).unwrap().route,
            Route::Fp32AllRows
        );
        assert!(plan(mode, Request::Fp32, &clustered(false)).is_err());
    }
    assert!(plan(Mode::Balanced, Request::Fp32, &clustered(false)).is_err());
    for req in [Request::Auto, Request::Fp32] {
        let bal = plan(Mode::Balanced, req, &c).unwrap();
        assert_eq!(bal.resolved_mode, Mode::Balanced);
        assert!(bal.fallback_reason.is_empty(), "{}", bal.fallback_reason);
        let mut out = planned_outcome(&bal);
        stamp(
            &bal,
            &Evidence {
                rows: 100,
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        assert_eq!(out.resolved_mode, Mode::Balanced as i32);
        assert_eq!(out.coverage, DenseCoverage::CandidateSet as i32);
        assert_eq!(out.termination, DenseTermination::Unspecified as i32);
        assert!(out.techniques.is_empty());
    }
}
