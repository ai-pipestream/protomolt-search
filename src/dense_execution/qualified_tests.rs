//! Unit tests for staged bit-plane planning and policy-qualified dense plans.

use super::*;
use crate::pb::{
    DenseCoverage, DenseExecutionMode as Mode, DenseExecutionShape,
    DenseFilterStrategy as Strategy, DensePolicyPoint, DensePrecisionRequest as Request,
    DenseScorePrecision as Precision, DenseTechnique,
};

fn turbovec(staged: bool) -> ProviderCaps {
    ProviderCaps {
        provider: "embedded-turbovec".into(),
        native: Precision::Quantized,
        exhaustive_scan: true,
        uncertified_scan: false,
        fp32_sidecar: true,
        walks: false,
        enumerates: false,
        staged,
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

fn request(mode: Mode, precision: Request) -> DenseRequest {
    DenseRequest {
        mode,
        precision,
        filter_strategy: Strategy::Default,
    }
}

fn measured(technique: DenseTechnique, output: Precision) -> DensePolicyPoint {
    let strategy = if technique == DenseTechnique::BitplaneStaged {
        Strategy::Default
    } else {
        Strategy::Speed
    };
    DensePolicyPoint {
        k: 10,
        candidates: 40,
        filter_selectivity_ppm_min: 1,
        filter_selectivity_ppm_max: 1_000_000,
        measured_recall_ppm: 990_000,
        mode: Mode::Balanced as i32,
        technique: technique as i32,
        selection_precision: Precision::Quantized as i32,
        output_precision: output as i32,
        filter_strategy: strategy as i32,
        shape: DenseExecutionShape::Unary as i32,
        ..Default::default()
    }
}

#[test]
fn staged_bit_planes_serve_balanced_without_certifying() {
    let caps = turbovec(true);
    let p = resolve(request(Mode::Balanced, Request::Auto), &caps).unwrap();
    assert_eq!((p.route, p.resolved_mode), (Route::Staged, Mode::Balanced));
    assert!(p.fallback_reason.is_empty());
    let mut outcome = planned_outcome(&p);
    let staged_ev = Evidence {
        consulted_shards: 2,
        reported_shards: 2,
        parts_total: 2,
        parts_staged: 2,
        rows: 100,
        ..Default::default()
    };
    stamp(&p, &staged_ev, &mut outcome).unwrap();
    assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
    assert_eq!(
        outcome.techniques[0].technique,
        DenseTechnique::BitplaneStaged as i32
    );
    assert_eq!(outcome.preset, "bitplane-balanced-v1");
    let unstaged_ev = Evidence {
        parts_staged: 0,
        parts_scanned: 2,
        ..staged_ev
    };
    assert!(stamp(&p, &unstaged_ev, &mut planned_outcome(&p)).is_err());
    let p = resolve(request(Mode::Balanced, Request::Fp32), &caps).unwrap();
    assert_eq!(
        (p.route, p.pool_selection),
        (Route::Fp32Pool, DenseTechnique::BitplaneStaged)
    );
}

#[test]
fn qualified_points_bind_their_technique_and_refuse_unserved_regimes() {
    let caps = turbovec(true);
    let auto = request(Mode::Auto, Request::Auto);
    let point = measured(DenseTechnique::BitplaneStaged, Precision::Quantized);
    let p = qualified(auto, &caps, &point).unwrap();
    assert_eq!((p.route, p.resolved_mode), (Route::Staged, Mode::Balanced));
    assert!(p.policy_qualified);
    assert_eq!(p.filter_strategy, Strategy::Default);
    assert!(planned_outcome(&p).policy_qualified);
    let fp32 = measured(DenseTechnique::BitplaneStaged, Precision::Fp32);
    let p = qualified(request(Mode::Auto, Request::Fp32), &caps, &fp32).unwrap();
    assert_eq!(p.route, Route::Fp32Pool);
    let walk = measured(DenseTechnique::HnswWalk, Precision::Quantized);
    assert!(qualified(auto, &caps, &walk).is_err());
    assert!(qualified(auto, &turbovec(false), &point).is_err());

    let mut graph = measured(DenseTechnique::HnswWalk, Precision::Fp32);
    graph.selection_precision = Precision::Fp32 as i32;
    graph.patience_saturation_ppm = Some(990_000);
    graph.patience_hops = Some(15);
    let p = qualified(auto, &hnsw(), &graph).unwrap();
    assert_eq!((p.route, p.preset), (Route::Walk, None));
    let mut no_patience = hnsw();
    no_patience.patience = false;
    assert!(qualified(auto, &no_patience, &graph).is_err());
    graph.shape = DenseExecutionShape::Streaming as i32;
    graph.finish_settle_us = Some(250);
    graph.floor_greediness_ppm = Some(950_000);
    graph.floor_clamp_min = Some(8);
    assert!(qualified(auto, &hnsw(), &graph).is_ok());
    let mut no_stream = hnsw();
    no_stream.candidate_stream = false;
    assert!(qualified(auto, &no_stream, &graph).is_err());
    let mut no_floor = hnsw();
    no_floor.floor_pruning = false;
    assert!(qualified(auto, &no_floor, &graph).is_err());
}

#[test]
fn raw_controls_reject_collapse_parents_with_staged_selection() {
    let collapse_only = RawDenseControls {
        collapse_parents: true,
        ..Default::default()
    };
    assert!(validate_raw_controls(&collapse_only, None).is_ok());

    let both = RawDenseControls {
        collapse_parents: true,
        staged_selection: true,
        ..Default::default()
    };
    let err = validate_raw_controls(&both, None).unwrap_err();
    assert!(matches!(err, DenseExecutionError::InvalidRequest(_)));
    assert!(err
        .to_string()
        .contains("collapse_parents and staged_selection are mutually exclusive"));
}
