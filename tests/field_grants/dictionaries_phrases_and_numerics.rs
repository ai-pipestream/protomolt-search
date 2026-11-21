//! Dictionary disclosure, phrase/fused field grants, revocation, and numeric stages.

use super::*;

#[tokio::test]
async fn document_and_field_grants_compose_and_dictionary_disclosure_is_explicit() {
    let coordinator = cluster(false).await;
    for fields in [
        FieldPermissions::default(),
        permissions(&[("*", &[FieldAction::Use, FieldAction::Disclose])], false),
        permissions(&[("body", &[FieldAction::Disclose])], false),
    ] {
        let reader = service(
            coordinator.clone(),
            Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
        );
        assert_eq!(
            SearchService::bm25_search(&reader, request(query()))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    let fields = permissions(
        &[("body", &[FieldAction::Use, FieldAction::Disclose])],
        false,
    );
    let reader = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields), true)).unwrap()),
    );
    let out = SearchService::bm25_search(&reader, request(query()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.hits.len(), 2); // The private audience predicate does not grant its field to the caller.
    let mut q = query();
    q.filter = "audience == 'public'".into();
    let mut forged = request(q);
    forged.extensions_mut().insert(AccessDecision::default());
    assert_eq!(
        SearchService::bm25_search(&reader, forged)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    for field in ["body", "title"] {
        let suggestion = SuggestRequest {
            field: field.into(),
            prefix: "al".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        };
        let correction = TermSuggestRequest {
            field: field.into(),
            text: "alpa".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        };
        if field == "body" {
            let out = SearchService::suggest(&reader, request(suggestion))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(out.suggestions.len(), 1);
            assert_eq!(out.suggestions[0].df, 2);
            SearchService::term_suggest(&reader, request(correction))
                .await
                .unwrap();
        } else {
            assert_eq!(
                SearchService::suggest(&reader, request(suggestion))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
            assert_eq!(
                SearchService::term_suggest(&reader, request(correction))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
        }
    }
    let reader = service(
        coordinator,
        Arc::new(
            PolicyAuthority::new(policy(
                Some(permissions(&[("body", &[FieldAction::Use])], false)),
                false,
            ))
            .unwrap(),
        ),
    );
    assert_eq!(
        SearchService::suggest(
            &reader,
            request(SuggestRequest {
                field: "body".into(),
                prefix: "al".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
        )
        .await
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        SearchService::query(&reader, request(QueryRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
}

#[tokio::test]
async fn fused_details_and_auxiliary_phrase_fields_follow_their_own_grants() {
    let coordinator = cluster(true).await;
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use, FieldAction::Disclose]),
            ("title", &[FieldAction::Use]),
        ],
        false,
    );
    let reader = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    );
    let mut q = query();
    q.analysis = None;
    q.fields = ["body", "title"]
        .into_iter()
        .map(|field| QueryField {
            field: field.into(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .collect();
    let expected = SearchService::bm25_search(&coordinator, Request::new(q.clone()))
        .await
        .unwrap()
        .into_inner();
    let out = SearchService::bm25_search(&reader, request(q))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        out.hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    );
    assert!(out.field_details_redacted);
    assert!(out
        .hits
        .iter()
        .flat_map(|h| &h.terms)
        .all(|term| term.field == "body"));
    let mut phrase = query();
    phrase.text = "alpha beta".into();
    phrase.phrase = Some(PhraseMatch::default());
    phrase.explain = true;
    let owner = SearchService::bm25_search(&coordinator, Request::new(phrase.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(owner.phrase_routing[0].bigram_column);
    let out = SearchService::bm25_search(&reader, request(phrase.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!out.phrase_routing[0].bigram_column);
    assert_eq!(out.phrase_routing[0].served_field, "body");
    assert_eq!(
        out.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![0, 100]
    );
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use, FieldAction::Disclose]),
            ("body.bigrams", &[FieldAction::Use]),
        ],
        false,
    );
    let reader = service(
        coordinator,
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    );
    // An explanation must not expose the auxiliary column's tf/df/lengths.
    let out = SearchService::bm25_search(&reader, request(phrase.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!out.phrase_routing[0].bigram_column);
    phrase.explain = false;
    let out = SearchService::bm25_search(&reader, request(phrase))
        .await
        .unwrap()
        .into_inner();
    assert!(out.hits.iter().all(|h| h.terms.is_empty()));
    assert!(out.phrase_routing.is_empty());
    assert!(out.field_details_redacted);
}

#[tokio::test]
async fn field_revocation_invalidates_computed_results_without_a_revision_bump() {
    let coordinator = cluster(false).await;
    for route in 0..4 {
        let fields = permissions(
            &[("body", &[FieldAction::Use, FieldAction::Disclose])],
            true,
        );
        let reader = service(
            coordinator.clone(),
            Arc::new(MovingFields {
                authority: PolicyAuthority::new(policy(Some(fields), false)).unwrap(),
                calls: AtomicUsize::new(0),
            }),
        );
        let error = match route {
            0 => SearchService::bm25_search(&reader, request(query()))
                .await
                .unwrap_err(),
            1 => SearchService::suggest(
                &reader,
                request(SuggestRequest {
                    field: "body".into(),
                    prefix: "al".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err(),
            2 => SearchService::term_suggest(
                &reader,
                request(TermSuggestRequest {
                    field: "body".into(),
                    text: "alpa".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err(),
            _ => SearchService::aggregate(&reader, request(count_request()))
                .await
                .unwrap_err(),
        };
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(error.message().contains("changed"));
    }
}

#[tokio::test]
async fn numeric_use_does_not_grant_explanation_or_projection_and_network_bypasses_refuse() {
    let coordinator = cluster(false).await;
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use, FieldAction::Disclose]),
            ("boost", &[FieldAction::Use]),
            ("color", &[FieldAction::Use, FieldAction::Disclose]),
        ],
        false,
    );
    let authority = Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap());
    let reader = service(coordinator.clone(), authority.clone());
    let mut q = query();
    q.score_stages = vec![ScoreStage {
        column: "boost".into(),
        operation: Some(pipestream_search::pb::score_stage::Operation::Op(
            ScoreOp::AddLinear as i32,
        )),
        weight: 1.,
        ..Default::default()
    }];
    q.facet_fields = vec!["color".into()];
    let expected = SearchService::bm25_search(&coordinator, Request::new(q.clone()))
        .await
        .unwrap()
        .into_inner();
    let out = SearchService::bm25_search(&reader, request(q.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.facets, expected.facets);
    assert_eq!(
        out.hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    );
    q.explain = true;
    assert_eq!(
        SearchService::bm25_search(&reader, request(q.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    q.explain = false;
    q.projections = vec![NamedProjection {
        name: "out".into(),
        expression: "boost".into(),
    }];
    assert_eq!(
        SearchService::bm25_search(&reader, request(q))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let network = service(
        CoordinatorServiceImpl::new(vec!["http://must-not-resolve.invalid:50051".into()]),
        authority,
    );
    assert_eq!(
        SearchService::bm25_search(&network, request(query()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}
