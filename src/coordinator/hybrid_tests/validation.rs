//! Sealed query, input, and constructor validation.

use super::*;

#[test]
fn sealed_queries_reject_nan_floors_and_nonpositive_rrf_constants() {
    let filters = RequestFilters::default();
    let input = HybridInput::new("id", "", &[], 0, None, &filters).unwrap();
    for rrf_k in [0.0, -1.0, f64::NAN] {
        let error = HybridQuery::new(
            input,
            HybridLegs {
                rrf_k,
                ..default_legs()
            },
            false,
        )
        .err()
        .unwrap();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    let invalid_floor = HybridLegs {
        min_vector_score: f32::NAN,
        ..default_legs()
    };
    assert_eq!(
        HybridQuery::new(input, invalid_floor, false)
            .err()
            .unwrap()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        CascadeQuery::new(input, f32::NAN, false)
            .err()
            .unwrap()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert!(HybridQuery::new(input, default_legs(), false).is_ok());
    assert!(CascadeQuery::new(input, 0.0, false).is_ok());
}

#[test]
fn sealed_hybrid_query_rejects_negative_nan_and_incompatible_weights() {
    let filters = RequestFilters::default();
    let input = query(&filters).input();
    for weight in [-1.0, f32::NAN] {
        for legs in [
            HybridLegs {
                vector_weight: weight,
                ..default_legs()
            },
            HybridLegs {
                bm25_weight: weight,
                ..default_legs()
            },
        ] {
            assert_eq!(
                HybridQuery::new(input, legs, false).err().unwrap().code(),
                tonic::Code::InvalidArgument
            );
        }
    }
    for legs in [
        HybridLegs {
            vector_weight: 0.0,
            bm25_weight: 0.0,
            ..default_legs()
        },
        HybridLegs {
            vector_weight: 0.0,
            fusion_mode: FusionMode::TwoLevel,
            ..default_legs()
        },
        HybridLegs {
            vector_weight: 0.0,
            min_vector_score: 0.5,
            ..default_legs()
        },
        HybridLegs {
            vector_weight: f32::INFINITY,
            fusion_mode: FusionMode::Decomposed,
            ..default_legs()
        },
    ] {
        assert_eq!(
            HybridQuery::new(input, legs, false).err().unwrap().code(),
            tonic::Code::InvalidArgument
        );
    }
    assert!(HybridQuery::new(
        input,
        HybridLegs {
            vector_weight: 0.0,
            ..default_legs()
        },
        false
    )
    .is_ok());
}

#[test]
fn hybrid_inputs_validate_filters_and_coordinates_even_for_empty_searches() {
    let invalid_tree = RequestFilters {
        tree: Some(crate::pb::FilterExpr::default()),
        ..Default::default()
    };
    let invalid_geo = RequestFilters {
        geo: vec![crate::pb::GeoFilter::default()],
        ..Default::default()
    };
    for filters in [&invalid_tree, &invalid_geo] {
        assert_eq!(
            HybridInput::new("id", "", &[], 0, None, filters)
                .err()
                .unwrap()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    let filters = RequestFilters::default();
    for coordinate in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        assert_eq!(
            HybridInput::new("id", "", &[coordinate], 0, None, &filters)
                .err()
                .unwrap()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn borrowed_request_constructors_preserve_preanalysis_and_normalized_options() {
    let filters = RequestFilters::default();
    let request = HybridSearchRequest {
        text: "raw text".into(),
        vector: vec![0.5],
        pre_analyzed_terms: vec!["prepared".into()],
        debug: true,
        ..Default::default()
    };
    let hybrid = HybridQuery::from_request("id", &request, 3, default_legs(), &filters).unwrap();
    let cascade = CascadeQuery::from_request("id", &request, 3, 0.25, &filters).unwrap();
    assert_eq!(hybrid.input().pre_analyzed_terms(), ["prepared"]);
    assert_eq!(hybrid.input().vector(), [0.5]);
    assert_eq!(hybrid.input().k(), 3);
    assert_eq!(hybrid.legs().rrf_k, 60.0);
    assert!(hybrid.debug() && cascade.debug());
    assert_eq!(cascade.input().pre_analyzed_terms(), ["prepared"]);
    assert_eq!(cascade.min_vector_score(), 0.25);
}

#[tokio::test]
async fn legacy_empty_searches_keep_their_early_return_after_validation() {
    let coordinator = CoordinatorServiceImpl::new(Vec::new());
    let filters = RequestFilters::default();
    assert!(coordinator
        .fanout_hybrid("id", "", &[], 0, None, default_legs(), false, &filters)
        .await
        .unwrap()
        .0
        .is_empty());
    assert!(coordinator
        .fanout_cascade("id", "", &[], 0, None, 0.0, false, &filters)
        .await
        .unwrap()
        .0
        .is_empty());
    assert_eq!(
        coordinator
            .fanout_hybrid(
                "id",
                "",
                &[],
                0,
                None,
                HybridLegs {
                    rrf_k: f64::NAN,
                    ..default_legs()
                },
                false,
                &filters
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        coordinator
            .fanout_cascade("id", "", &[], 0, None, f32::NAN, false, &filters)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}
