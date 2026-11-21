//! Relayed boolean/pool aggregations, sorted browse pages, and CEL projections.

use super::*;

/// A root aggregate on a boolean tree folds on the shards in the root's
/// shard order; a relay does not compose that fold and says so by name,
/// while the flat fleet serves it.
fn aggregation(
    name: &str,
    expression: &str,
    op: pipestream_search::pb::AggregateOp,
) -> pipestream_search::pb::Aggregation {
    pipestream_search::pb::Aggregation {
        name: name.into(),
        expression: expression.into(),
        op: op as i32,
        max_distinct: 0,
    }
}

/// Count, the exact int folds, an exact double sum, a facet group-by, a
/// histogram, and exact percentiles: the folds a relay reproduces bit
/// for bit in any grouping of its children.
fn exact_aggregate(filter: &str) -> pipestream_search::pb::AggregateRequest {
    use pipestream_search::pb::AggregateOp as Op;
    pipestream_search::pb::AggregateRequest {
        filter: filter.into(),
        aggregations: vec![
            aggregation("n", "1", Op::Count),
            aggregation("ysum", "year", Op::Sum),
            aggregation("ymin", "year", Op::Min),
            aggregation("ymax", "year", Op::Max),
            aggregation("psum", "pages", Op::Sum),
            aggregation("courts", "court", Op::Cardinality),
        ],
        group_by: "court".into(),
        max_groups: 8,
        histograms: vec![pipestream_search::pb::HistogramSpec {
            name: "page_bands".into(),
            expression: "pages".into(),
            interval: 6.0,
            max_buckets: 16,
            ..Default::default()
        }],
        percentiles: vec![pipestream_search::pb::PercentileSpec {
            name: "pages".into(),
            expression: "pages".into(),
            percentiles: vec![50.0, 90.0],
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boolean_aggregate_through_relays_equals_the_direct_root() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let selection = SelectionQuery {
        node: Some(selection_query::Node::Boolean(
            pipestream_search::pb::BooleanQuery {
                must: vec![
                    lexical_clause("lex", "court"),
                    cel_clause("f", "year >= 2002"),
                ],
                should: Vec::new(),
                must_not: Vec::new(),
                minimum_should_match: 0,
                aggregate: Some(exact_aggregate("")),
            },
        )),
    };
    let flat = with_bm25(l.clone());
    let want = run_query_full(&flat, selection.clone())
        .await
        .expect("flat");
    let aggregate = want.aggregate.as_ref().expect("the flat root folds");
    assert!(!want.hits.is_empty());
    assert!(aggregate.matched > 0 && aggregate.groups.len() == 2);
    for (name, children) in relay_trees(l).await {
        let relayed = with_bm25(children);
        let through = run_query_full(&relayed, selection.clone())
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            query_bits(&through.hits),
            query_bits(&want.hits),
            "{name}: hits"
        );
        assert_eq!(
            through.aggregate, want.aggregate,
            "{name}: the Boolean fold"
        );
    }
}

async fn run_query_full(
    coordinator: &CoordinatorServiceImpl,
    selection: SelectionQuery,
) -> Result<pipestream_search::pb::QueryResponse, tonic::Status> {
    run_request(
        coordinator,
        QueryRequest {
            request_id: "relayed".into(),
            k: LEX_K,
            selection: Some(selection),
            ..Default::default()
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregates_through_relays_equal_the_flat_root() {
    use pipestream_search::pb::AggregateOp as Op;
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let flat = with_bm25(l.clone());
    let request = exact_aggregate("year >= 2002");
    let want = flat
        .aggregate(tonic::Request::new(request.clone()))
        .await
        .expect("flat")
        .into_inner();
    assert_eq!(want.matched, (LEX_LEAVES * LEX_ROWS - 2) as u64);
    assert_eq!(want.groups.len(), 2);
    assert!(!want.histograms[0].buckets.is_empty());
    assert_eq!(want.percentiles[0].values.len(), 2);
    for (name, children) in relay_trees(l).await {
        let relayed = with_bm25(children);
        let through = relayed
            .aggregate(tonic::Request::new(request.clone()))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        assert_eq!(through, want, "{name}: the fold");
    }
    // The moment folds (mean, variance) are exact through a chain: a
    // relay over every child, or relays nested one under another, fold
    // in the root's own order, so the merged state is the root's.
    let moments = pipestream_search::pb::AggregateRequest {
        filter: "year >= 2001".into(),
        aggregations: vec![
            aggregation("pmean", "pages", Op::Mean),
            aggregation("pvar", "pages", Op::Variance),
        ],
        ..Default::default()
    };
    let want = flat
        .aggregate(tonic::Request::new(moments.clone()))
        .await
        .expect("flat moments")
        .into_inner();
    let all = relay_over(&[&l[0], &l[1], &l[2], &l[3]]).await;
    let a = relay_over(&[&l[0], &l[1]]).await;
    let chain = relay_over(&[&a]).await;
    for (name, children) in [
        ("one relay over all", vec![all]),
        ("a chain", vec![chain, l[2].clone(), l[3].clone()]),
    ] {
        let relayed = with_bm25(children);
        let through = relayed
            .aggregate(tonic::Request::new(moments.clone()))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        assert_eq!(through, want, "{name}: the moments");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browse_pages_and_projections_through_relays_equal_the_flat_root() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let flat = with_bm25(l.clone());
    let page = |cursor: String, sort: Vec<pipestream_search::pb::QuerySort>| QueryRequest {
        request_id: "browse".into(),
        k: 5,
        selection: Some(cel_clause("f", "year >= 2003")),
        sort,
        cursor,
        projections: vec![
            pipestream_search::pb::NamedProjection {
                name: "pages".into(),
                expression: "pages".into(),
            },
            pipestream_search::pb::NamedProjection {
                name: "year".into(),
                expression: "year".into(),
            },
        ],
        ..Default::default()
    };
    let by_year = vec![pipestream_search::pb::QuerySort {
        map: None,
        column: "year".into(),
        descending: true,
    }];
    let seen = |hits: &[QueryHit]| -> Vec<(
        u64,
        u32,
        Vec<pipestream_search::pb::SortValue>,
        Vec<pipestream_search::pb::ProjectedValue>,
    )> {
        hits.iter()
            .map(|h| (h.doc_id, h.rank, h.sort_values.clone(), h.projected.clone()))
            .collect()
    };
    let trees = relay_trees(l).await;
    for (label, sort) in [("sorted", by_year), ("by id", Vec::new())] {
        let first = run_request(&flat, page(String::new(), sort.clone()))
            .await
            .expect("flat first page");
        assert_eq!(first.hits.len(), 5, "{label}: the first page is full");
        assert!(!first.next_cursor.is_empty(), "{label}: a next page");
        let second = run_request(&flat, page(first.next_cursor.clone(), sort.clone()))
            .await
            .expect("flat second page");
        assert!(!second.hits.is_empty());
        for (name, children) in &trees {
            let relayed = with_bm25(children.clone());
            let through = run_request(&relayed, page(String::new(), sort.clone()))
                .await
                .unwrap_or_else(|e| panic!("{name} {label}: {e}"));
            assert_eq!(
                seen(&through.hits),
                seen(&first.hits),
                "{name} {label}: page one"
            );
            // A cursor binds the read versions of the root that minted
            // it (relay tokens under a relay root), so each root pages
            // with its own; the pages agree.
            assert!(
                !through.next_cursor.is_empty(),
                "{name} {label}: a next page"
            );
            let through = run_request(&relayed, page(through.next_cursor.clone(), sort.clone()))
                .await
                .unwrap_or_else(|e| panic!("{name} {label}: page two: {e}"));
            assert_eq!(
                seen(&through.hits),
                seen(&second.hits),
                "{name} {label}: page two"
            );
        }
    }
}
