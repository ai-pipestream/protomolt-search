//! Diagnostic redaction, policy validation, use-only disclosure, and pre-stats admission.

use super::*;

#[tokio::test]
async fn restricted_collection_errors_do_not_disclose_late_diagnostics() {
    use prost::Message;
    let owner = cluster(false).await;
    for (field_grant, document_grant) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let fields = field_grant.then(|| {
            permissions(
                &[("body", &[FieldAction::Use, FieldAction::Disclose])],
                false,
            )
        });
        let authority = Arc::new(DiagnosticFailureAfterAdmission {
            inner: PolicyAuthority::new(policy(fields, document_grant)).unwrap(),
            calls: AtomicUsize::new(0),
            fail_at: 2,
        });
        let reader = service(owner.clone(), authority.clone());
        let error = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap_err();
        assert_eq!(
            authority.calls.load(Ordering::SeqCst),
            3,
            "failure must occur after handler execution"
        );
        assert_eq!(error.code(), Code::Internal);
        if field_grant || document_grant {
            assert!(!error.message().contains("PRIVATE"));
            assert!(error.metadata().is_empty());
            let envelope = google_rpc::Status::decode(error.details()).unwrap();
            assert_eq!(envelope.code, Code::Internal as i32);
            assert_eq!(envelope.message, error.message());
            assert_eq!(envelope.details.len(), 1);
            assert_eq!(
                envelope.details[0].type_url,
                "type.googleapis.com/ai.protomolt.search.v1.ErrorDisclosure"
            );
            let detail = ErrorDisclosure::decode(envelope.details[0].value.as_slice()).unwrap();
            assert!(detail.details_redacted);
        } else {
            assert!(error.message().contains("PRIVATE"));
            assert_eq!(error.details(), b"PRIVATE detail bytes");
            assert!(error.metadata().contains_key("x-policy-debug"));
        }
    }
}

#[tokio::test]
async fn authority_errors_before_a_grant_never_disclose_backend_details() {
    use prost::Message;
    for fail_at in [0, 1] {
        let authority = Arc::new(DiagnosticFailureAfterAdmission {
            inner: PolicyAuthority::new(policy(None, false)).unwrap(),
            calls: AtomicUsize::new(0),
            fail_at,
        });
        let reader = service(
            CoordinatorServiceImpl::with_local_nodes(vec![]),
            authority.clone(),
        );
        let error = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Internal);
        assert_eq!(authority.calls.load(Ordering::SeqCst), fail_at + 1);
        assert!(!error.message().contains("PRIVATE"));
        assert!(error.metadata().is_empty());
        let envelope = google_rpc::Status::decode(error.details()).unwrap();
        let detail = ErrorDisclosure::decode(envelope.details[0].value.as_slice()).unwrap();
        assert!(detail.details_redacted);
    }
}

#[test]
fn policy_versions_and_explicit_field_actions_refuse_ambiguous_grants() {
    let fields = permissions(
        &[("body", &[FieldAction::Use, FieldAction::Disclose])],
        false,
    );
    for version in [1, 2] {
        let mut p = policy(Some(fields.clone()), false);
        p.format_version = version;
        assert!(PolicyAuthority::new(p).unwrap_err().contains("expected 3"));
    }
    for actions in [vec![], vec![0], vec![100], vec![1, 1]] {
        let mut fields = fields.clone();
        fields.grants[0].actions = actions;
        assert!(PolicyAuthority::new(policy(Some(fields), false)).is_err());
    }
    let mut duplicate = fields.clone();
    duplicate.grants.push(duplicate.grants[0].clone());
    assert!(PolicyAuthority::new(policy(Some(duplicate), false)).is_err());
    let mut p = policy(Some(fields), true);
    p.grants[0].actions.push(AccessAction::Admin as i32);
    let authority = PolicyAuthority::new(p).unwrap();
    assert!(authority
        .authorize("reader", "", AccessAction::Admin)
        .unwrap()
        .field_permissions
        .is_none());
    assert!(authority
        .authorize("reader", "", AccessAction::Search)
        .unwrap()
        .field_permissions
        .is_some());
    PolicyAuthority::new(policy(Some(FieldPermissions::default()), false)).unwrap();
}

#[tokio::test]
async fn use_only_fields_keep_ranking_but_omit_automatic_details_and_raw_keys() {
    for streaming in [false, true] {
        let coordinator = cluster(streaming).await;
        let expected = SearchService::bm25_search(&coordinator, Request::new(query()))
            .await
            .unwrap()
            .into_inner();
        assert!(expected
            .hits
            .iter()
            .all(|h| h.identity.is_some() && !h.terms.is_empty()));
        let cache = coordinator.stats_cache();
        let fetched = cache.fetch_count();
        let reader = service(
            coordinator.clone(),
            Arc::new(
                PolicyAuthority::new(policy(
                    Some(permissions(&[("body", &[FieldAction::Use])], false)),
                    false,
                ))
                .unwrap(),
            ),
        );
        let actual = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cache.fetch_count(), fetched);
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        );
        assert!(actual.field_details_redacted);
        assert!(actual
            .hits
            .iter()
            .all(|h| h.terms.is_empty() && h.identity.is_none()));
        let reader = service(
            coordinator,
            Arc::new(
                PolicyAuthority::new(policy(
                    Some(permissions(
                        &[("body", &[FieldAction::Use, FieldAction::Disclose])],
                        true,
                    )),
                    false,
                ))
                .unwrap(),
            ),
        );
        let actual = SearchService::bm25_search(&reader, request(query()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(actual.hits, expected.hits);
        assert!(!actual.field_details_redacted);
    }
}

#[tokio::test]
async fn every_field_read_is_checked_before_statistics_and_user_aliases_cannot_bypass_it() {
    let coordinator = cluster(false).await;
    let cache = coordinator.stats_cache();
    let fields = permissions(
        &[
            ("body", &[FieldAction::Use]),
            ("color", &[FieldAction::Use, FieldAction::Disclose]),
        ],
        false,
    );
    let reader = service(
        coordinator.clone(),
        Arc::new(PolicyAuthority::new(policy(Some(fields), false)).unwrap()),
    );
    let mut requests = Vec::new();
    for filter in [
        "secret == 'x'",
        "has(secret)",
        "secret.startsWith('x')",
        "secret < 'x'",
        "boost > 0",
        "tags['k'] == 'x'",
        "tags[''] >= 'x'",
        "tags[''].startsWith('x')",
        "'k' in tags",
        "metrics['k'] > 0",
        "color == 'red' && secret == 'x'",
    ] {
        let mut q = query();
        q.filter = filter.into();
        requests.push(q);
    }
    for expression in [
        "secret",
        "tags['k']",
        "boost + 1.0",
        "true ? color : secret",
    ] {
        let mut q = query();
        q.projections = vec![NamedProjection {
            name: "color".into(),
            expression: expression.into(),
        }];
        requests.push(q);
    }
    let mut q = query();
    q.facet_fields = vec!["secret".into()];
    requests.push(q);
    let mut q = query();
    q.stats_fields = vec!["boost".into()];
    requests.push(q);
    let mut q = query();
    q.cardinality_fields = vec!["secret".into()];
    requests.push(q);
    let mut q = query();
    q.map_facet_fields = vec![MapFacetField {
        column: "tags".into(),
        key: "k".into(),
    }];
    requests.push(q);
    let mut q = query();
    q.range_facet_fields = vec![RangeFacetField {
        column: "boost".into(),
        edges: vec![FilterBound::num(0.), FilterBound::num(2.)],
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.range_facet_fields = vec![RangeFacetField {
        column: "metrics".into(),
        map_key: Some(String::new()),
        edges: vec![FilterBound::num(0.0), FilterBound::num(1.0)],
    }];
    requests.push(q);
    let mut q = query();
    q.map_facet_fields = vec![MapFacetField {
        column: "tags".into(),
        key: String::new(),
    }];
    requests.push(q);

    let mut q = query();
    q.score_stages = vec![ScoreStage {
        column: "boost".into(),
        op: ScoreOp::AddLinear as i32,
        weight: 1.,
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.score_stages = vec![ScoreStage {
        column: "metrics".into(),
        op: ScoreOp::AddLinear as i32,
        map_key: Some(String::new()),
        weight: 1.0,
        ..Default::default()
    }];
    requests.push(q);

    let mut q = query();
    q.geo_filters = vec![GeoFilter {
        column: "location".into(),
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.analysis = None;
    q.fields = vec![QueryField {
        field: "title".into(),
        analysis: Some(body_spec()),
        ..Default::default()
    }];
    requests.push(q);
    let mut q = query();
    q.highlight = Some(HighlightSpec::default());
    requests.push(q);
    let mut q = query();
    q.explain = true;
    requests.push(q);
    let mut q = query();
    q.prefixes = vec![TermPrefix {
        prefix: "al".into(),
        max_expansions: 10,
    }];
    requests.push(q);
    for (i, q) in requests.into_iter().enumerate() {
        let error = SearchService::bm25_search(&reader, request(q))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied, "case {i}: {error}");
    }
    assert_eq!(cache.fetch_count(), 0);
    let mut q = query();
    q.projections = vec![NamedProjection {
        name: "out".into(),
        expression: "color".into(),
    }];
    let out = SearchService::bm25_search(&reader, request(q))
        .await
        .unwrap()
        .into_inner();
    assert!(out.hits.iter().all(|h| h.projected.len() == 1));
}
