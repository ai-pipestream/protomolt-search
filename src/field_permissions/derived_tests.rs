use super::*;

fn scope(grants: &[(&str, &[FieldAction])], identity: bool) -> FieldScope {
    let permissions = FieldPermissions {
        grants: grants
            .iter()
            .map(|(field, actions)| FieldGrant {
                field: field.to_string(),
                actions: actions.iter().map(|a| *a as i32).collect(),
            })
            .collect(),
        disclose_document_identity: identity,
    };
    let declaration = crate::derived::Declaration::compile(&DerivedColumns {
        columns: vec![
            DerivedColumn {
                name: "court_hash".into(),
                expression: "hash.fnv64(court)".into(),
                kind: MaterializeKind::U64 as i32,
                disclosure: DerivedDisclosure::Inputs as i32,
            },
            DerivedColumn {
                name: "key_bucket".into(),
                expression: "hash.fnv64(stable_key()) % 64u".into(),
                kind: MaterializeKind::U64 as i32,
                disclosure: DerivedDisclosure::Inputs as i32,
            },
            DerivedColumn {
                name: "decade".into(),
                expression: "year / 10".into(),
                kind: MaterializeKind::I64 as i32,
                disclosure: DerivedDisclosure::Own as i32,
            },
        ],
    })
    .unwrap();
    FieldScope::new(&permissions)
        .unwrap()
        .with_derived(Some(std::sync::Arc::new(declaration)))
}

#[test]
fn a_derived_column_needs_the_grants_of_its_inputs() {
    use FieldAction::{Disclose, Use};
    // The column's own grant is not enough: hashing court does not
    // make court public.
    let s = scope(&[("court_hash", &[Use, Disclose])], false);
    assert!(!s.can_use("court_hash"));
    assert!(!s.can_disclose("court_hash"));
    let s = scope(
        &[("court_hash", &[Use, Disclose]), ("court", &[Use])],
        false,
    );
    assert!(s.can_use("court_hash"));
    assert!(
        !s.can_disclose("court_hash"),
        "disclosing needs disclose on the input"
    );
    let s = scope(
        &[
            ("court_hash", &[Use, Disclose]),
            ("court", &[Use, Disclose]),
        ],
        false,
    );
    assert!(s.can_disclose("court_hash"));
    // An input grant without the column's own grant is not enough either.
    let s = scope(&[("court", &[Use, Disclose])], false);
    assert!(!s.can_use("court_hash"));
    // The stable key is the document identity.
    let s = scope(&[("key_bucket", &[Use, Disclose])], false);
    assert!(!s.can_use("key_bucket"));
    let s = scope(&[("key_bucket", &[Use, Disclose])], true);
    assert!(s.can_use("key_bucket") && s.can_disclose("key_bucket"));
    // A column declared `own` is judged on its own grant.
    let s = scope(&[("decade", &[Use])], false);
    assert!(s.can_use("decade") && !s.can_disclose("decade"));
    // Source columns are unchanged.
    let s = scope(&[("year", &[Use, Disclose])], false);
    assert!(s.can_use("year") && s.can_disclose("year") && !s.can_use("decade"));
}

/// A lexical selection tree for the Query route's checks.
fn selection() -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: String::new(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "alpha".into(),
                ..Default::default()
            })),
        })),
    }
}

fn query_request(column: &str) -> QueryRequest {
    QueryRequest {
        selection: Some(selection()),
        sort: vec![QuerySort {
            column: column.into(),
            descending: false,
            map: None,
        }],
        projections: vec![NamedProjection {
            name: "p".into(),
            expression: column.into(),
        }],
        ..Default::default()
    }
}

fn bm25_request(column: &str, explain: bool) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: "alpha".into(),
        facet_fields: vec![column.into()],
        score_stages: vec![ScoreStage {
            column: column.into(),
            ..Default::default()
        }],
        projections: vec![NamedProjection {
            name: "p".into(),
            expression: column.into(),
        }],
        explain,
        ..Default::default()
    }
}

fn aggregate_request(column: &str) -> AggregateRequest {
    AggregateRequest {
        aggregations: vec![Aggregation {
            name: "total".into(),
            expression: column.into(),
            op: AggregateOp::Sum as i32,
            max_distinct: 0,
        }],
        ..Default::default()
    }
}

/// A refused read is PermissionDenied, whatever the route.
fn denied<T>(r: Result<T, Status>) {
    match r {
        Ok(_) => panic!("the read must refuse"),
        Err(e) => assert_eq!(e.code(), tonic::Code::PermissionDenied),
    }
}

/// body is the lexical field every request reads; the variants differ
/// only in the derived column's grants.
const BODY: (&str, &[FieldAction]) = ("body", &[FieldAction::Use, FieldAction::Disclose]);

fn both() -> FieldScope {
    use FieldAction::{Disclose, Use};
    scope(
        &[
            BODY,
            ("court_hash", &[Use, Disclose]),
            ("court", &[Use, Disclose]),
        ],
        false,
    )
}

fn no_input() -> FieldScope {
    use FieldAction::{Disclose, Use};
    scope(&[BODY, ("court_hash", &[Use, Disclose])], false)
}

fn use_only_input() -> FieldScope {
    use FieldAction::{Disclose, Use};
    scope(
        &[BODY, ("court_hash", &[Use, Disclose]), ("court", &[Use])],
        false,
    )
}

#[test]
fn every_query_path_judges_a_derived_column_on_its_inputs() {
    // Filters.
    let filter = crate::cel::compile_filter("court_hash == 42u").unwrap();
    assert!(both().filter(&[], filter.as_ref()).is_ok());
    denied(no_input().filter(&[], filter.as_ref()));
    assert!(use_only_input().filter(&[], filter.as_ref()).is_ok());

    // Facets, score stages and projections on the BM25 route.
    let req = bm25_request("court_hash", false);
    let projections = crate::coordinator::compile_projections(&req.projections).unwrap();
    assert!(both().bm25(&req, None, &projections).is_ok());
    denied(no_input().bm25(&req, None, &projections));
    // The input's Use grant admits the filter and the scoring read,
    // but facets and projections disclose, so they still refuse.
    denied(use_only_input().bm25(&req, None, &projections));
    let stage_only = Bm25SearchRequest {
        text: "alpha".into(),
        score_stages: req.score_stages.clone(),
        ..Default::default()
    };
    assert!(use_only_input().bm25(&stage_only, None, &[]).is_ok());

    // Explanations disclose the column.
    let req = bm25_request("court_hash", true);
    let projections = crate::coordinator::compile_projections(&req.projections).unwrap();
    denied(no_input().bm25(&req, None, &projections));
    denied(use_only_input().bm25(&req, None, &projections));

    // Sorts and projections on the Query route.
    let req = query_request("court_hash");
    assert!(both().query(&req).is_ok());
    denied(no_input().query(&req).map(|_| ()));
    denied(use_only_input().query(&req).map(|_| ()));

    aggregate_browse_and_response_paths_judge_inputs();
}

fn aggregate_browse_and_response_paths_judge_inputs() {
    // Aggregations read the column's values, dictionary-style.
    let req = aggregate_request("court_hash");
    let compiled = crate::coordinator::compile_aggregations(&req).unwrap();
    let filters = crate::coordinator::RequestFilters::default();
    assert!(both().aggregate(&filters, &compiled).is_ok());
    denied(no_input().aggregate(&filters, &compiled));
    denied(use_only_input().aggregate(&filters, &compiled));

    // Browse sorts disclose.
    let sort = vec![BrowseSort {
        column: "court_hash".into(),
        descending: false,
        map: None,
    }];
    assert!(both().browse(&filters, &sort, &[]).is_ok());
    denied(no_input().browse(&filters, &sort, &[]));

    // The disclosure pass on a response: an explain stage naming
    // the derived column discloses it.
    let response = |column: &str| Bm25SearchResponse {
        hits: vec![Bm25Hit {
            doc_id: 1,
            score: 1.0,
            explain: Some(Bm25Explain {
                stages: vec![ScoreStageExplain {
                    column: column.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut r = response("court_hash");
    assert!(both().disclose(&mut r).is_ok());
    denied(no_input().disclose(&mut response("court_hash")));
    denied(use_only_input().disclose(&mut response("court_hash")));
}

#[test]
fn the_stable_key_column_needs_the_identity_grant_and_own_needs_its_own() {
    use FieldAction::{Disclose, Use};
    let body: (&str, &[FieldAction]) = ("body", &[Use, Disclose]);
    // key_bucket reads stable_key(): the column's grants plus the
    // document-identity grant, on every path.
    let req = query_request("key_bucket");
    denied(scope(&[body, ("key_bucket", &[Use, Disclose])], false).query(&req));
    assert!(scope(&[body, ("key_bucket", &[Use, Disclose])], true)
        .query(&req)
        .is_ok());
    // A column declared `own` is judged on its own grant: the
    // input's grant neither helps nor is required.
    let req = query_request("decade");
    denied(scope(&[body, ("year", &[Use, Disclose])], false).query(&req));
    assert!(scope(&[body, ("decade", &[Use, Disclose])], false)
        .query(&req)
        .is_ok());
    // The aggregate route judges `own` the same way.
    let req = aggregate_request("decade");
    let compiled = crate::coordinator::compile_aggregations(&req).unwrap();
    let filters = crate::coordinator::RequestFilters::default();
    denied(scope(&[body, ("year", &[Use, Disclose])], false).aggregate(&filters, &compiled));
    assert!(scope(&[body, ("decade", &[Use, Disclose])], false)
        .aggregate(&filters, &compiled)
        .is_ok());
}
