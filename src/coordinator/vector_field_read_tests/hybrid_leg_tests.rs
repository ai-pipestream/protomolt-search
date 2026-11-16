//! Scoped hybrid legs and the lexical candidate rescore round.

use super::*;

#[tokio::test]
async fn fused_legs_admit_empty_shards_and_apply_the_document_view_to_both_lists() {
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "body".into(),
            actions: vec![FieldAction::Use as i32],
        });
    for mode in [FusionMode::GlobalRank, FusionMode::TwoLevel] {
        for empty_incompatible in [false, true] {
            let nodes = vec![node(0, false), node_rows(100, empty_incompatible, 0, 0)];
            let reader = CoordinatorServiceImpl::with_local_nodes(nodes)
                .with_bm25(
                    Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                    Default::default(),
                )
                .for_access(Some(&decision), "bm25_search")
                .unwrap()
                .for_vector_field("semantic")
                .unwrap();
            let result = reader
                .fanout_hybrid(
                    "scoped-legs",
                    "word",
                    &[0.25; 16],
                    4,
                    Some(&crate::analyzer::body_spec()),
                    HybridLegs {
                        leg_k: 4,
                        vector_weight: 1.0,
                        bm25_weight: 1.0,
                        rrf_k: 60.0,
                        fusion_mode: mode,
                        normalization: fusion::Normalization::MinMax,
                        combination: fusion::Combination::Arithmetic,
                        min_vector_score: 0.0,
                    },
                    false,
                    &RequestFilters::default(),
                )
                .await;
            if empty_incompatible {
                let error = result.unwrap_err();
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert!(error.message().contains("binding"), "{error}");
            } else {
                let hits = result.unwrap().0;
                assert_eq!(
                    hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                    vec![0]
                );
                assert!(hits[0].vector_rank.is_some());
                assert!(hits[0].bm25_rank.is_some());
            }
        }
    }
}

#[tokio::test]
async fn decomposed_lexical_admission_matches_a_physically_restricted_corpus() {
    let reference = CoordinatorServiceImpl::with_local_nodes(vec![
        node_rows(0, false, 1, 1),
        node_rows(100, false, 1, 1),
    ])
    .with_bm25(
        Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        Default::default(),
    )
    .for_vector_field("semantic")
    .unwrap();
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "body".into(),
            actions: vec![FieldAction::Use as i32],
        });
    let scoped = CoordinatorServiceImpl::with_local_nodes(vec![
        node_rows_with_private_terms(0, false, 2, 2, 100),
        node_rows_with_private_terms(100, false, 2, 2, 100),
    ])
    .with_bm25(
        Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        Default::default(),
    )
    .for_access(Some(&decision), "bm25_search")
    .unwrap()
    .for_vector_field("semantic")
    .unwrap();
    let legs = HybridLegs {
        leg_k: 1,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: FusionMode::Decomposed,
        normalization: fusion::Normalization::MinMax,
        combination: fusion::Combination::Arithmetic,
        min_vector_score: 0.0,
    };
    let expected = reference
        .fanout_hybrid(
            "restricted",
            "word",
            &[0.25; 16],
            1,
            Some(&crate::analyzer::body_spec()),
            legs,
            false,
            &RequestFilters::default(),
        )
        .await
        .unwrap()
        .0;
    let actual = scoped
        .fanout_hybrid(
            "scoped",
            "word",
            &[0.25; 16],
            1,
            Some(&crate::analyzer::body_spec()),
            legs,
            false,
            &RequestFilters::default(),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(expected[0].bm25_rank, Some(1));
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn lexical_candidate_round_enforces_views_fields_and_empty_read_versions() {
    let nodes = vec![node(0, false), node(100, false)];
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let reader = owner
        .for_access(Some(&access("body", FieldAction::Use)), "bm25_search")
        .unwrap();
    let terms = vec!["word".into()];
    let (global, claims) = reader.body_stats(&terms, false).await.unwrap();
    let (scores, debug) = reader
        .bm25_rescore_round(
            &terms,
            0,
            &global,
            &claims,
            &HashMap::from([(0, vec![0, 1, 0]), (1, vec![100, 101])]),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        scores
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 100].into_iter().collect()
    );
    assert_eq!(debug.len(), 2);
    assert!(debug.values().all(|(_, hits)| *hits == 1));
    let (scores, debug) = reader
        .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
        .await
        .unwrap();
    assert!(scores.is_empty());
    assert_eq!(
        debug.len(),
        2,
        "empty owners must acknowledge the authority view"
    );
    let denied = owner
        .for_access(Some(&access("body", FieldAction::Disclose)), "bm25_search")
        .unwrap();
    assert_eq!(
        denied
            .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_stage_column_denied(&reader, &terms, &global, &claims).await;
    nodes[0]
        .delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        reader
            .bm25_rescore_round(&terms, 0, &global, &claims, &HashMap::new(), &[])
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

/// A score stage over a column the caller cannot use is refused.
async fn assert_stage_column_denied(
    reader: &CoordinatorServiceImpl,
    terms: &[String],
    global: &crate::bm25::CorpusStats,
    claims: &[crate::stats_identity::StatsClaim],
) {
    assert_eq!(
        reader
            .bm25_rescore_round(
                terms,
                0,
                global,
                claims,
                &HashMap::new(),
                &[ScoreStage {
                    column: "audience".into(),
                    operation: Some(crate::pb::score_stage::Operation::Op(
                        ScoreOp::AddLinear as i32
                    )),
                    ..Default::default()
                },]
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}
