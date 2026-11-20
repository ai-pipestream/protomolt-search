//! Relayed unary vector search, cascade/decomposed fusion, and filtered boolean queries.

use super::*;

/// `(vector id, score bits, parent id)` of a unary fan-out's hits.
fn unary_bits(hits: &[ScoredHit]) -> Vec<(u64, u32, u64)> {
    hits.iter()
        .map(|h| (h.vector_id, h.score.to_bits(), h.parent_id))
        .collect()
}

/// The cascade's gate over a fan-out's raw lists: every candidate at or
/// above the k-th best score, shard assignments dropped. The raw lists
/// themselves depend on which floors reached each leaf when; the gate
/// is score-defined and must not depend on grouping.
fn pool(result: &pipestream_search::coordinator::FanoutResult, k: usize) -> Vec<(u64, u32)> {
    let mut all: Vec<(u64, f32)> = result
        .shard_hits
        .iter()
        .flat_map(|(_, hits)| hits.iter().copied())
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let boundary = all.get(k - 1).map_or(f32::NEG_INFINITY, |h| h.1);
    let mut gate: Vec<(u64, u32)> = all
        .into_iter()
        .filter(|h| h.1 >= boundary)
        .map(|(id, score)| (id, score.to_bits()))
        .collect();
    gate.sort_unstable();
    gate
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_vector_search_through_relays_equals_the_flat_fanout() {
    let leaves = leaves().await;
    let l = &leaves.addrs;
    let flat = CoordinatorServiceImpl::new(l.clone());
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    // Children out of slot order, relays out of order at the root; the
    // groups are contiguous, as the routes routed by slot range require.
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees = [
        (
            "one level",
            CoordinatorServiceImpl::new(vec![a.clone(), b.clone()]),
        ),
        ("permuted", CoordinatorServiceImpl::new(vec![q, p])),
        ("two levels", CoordinatorServiceImpl::new(vec![top])),
    ];
    let mut queries: Vec<(String, Vec<f32>)> = (0..4u64)
        .map(|qi| (format!("q{qi}"), unit_vectors(1, DIM, 0x57AE_2E1A + qi)))
        .collect();
    let tie = leaves.corpus[TIE_SOURCE * DIM..(TIE_SOURCE + 1) * DIM].to_vec();
    queries.push(("tie".to_string(), tie));
    let filters = Default::default();
    for (name, query) in &queries {
        let want = monolithic_topk(&leaves.monolithic, query, K as usize);
        for tie_complete in [false, true] {
            let flat_result = flat
                .fanout_search(name, query, K, tie_complete, &filters)
                .await
                .expect("flat unary fan-out");
            assert_eq!(bits(&flat_result.hits), want, "{name}: flat != monolithic");
            let flat_pool = pool(&flat_result, K as usize);
            let flat_scanned: u64 = flat_result
                .shard_stats
                .iter()
                .flatten()
                .map(|s| s.candidates_collected)
                .sum();
            for (label, tree) in &trees {
                let got = tree
                    .fanout_search(name, query, K, tie_complete, &filters)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {label}: {e}"));
                assert_eq!(
                    bits(&got.hits),
                    want,
                    "{name}: {label} tie_complete={tie_complete}"
                );
                assert_eq!(
                    pool(&got, K as usize),
                    flat_pool,
                    "{name}: {label} tie_complete={tie_complete}: the gate is the same set"
                );
                let scanned: u64 = got
                    .shard_stats
                    .iter()
                    .flatten()
                    .map(|s| s.candidates_collected)
                    .sum();
                assert!(
                    scanned > 0 && scanned <= flat_scanned.max(scanned),
                    "{name}: {label}: the relays' scan counters sum the leaves'"
                );
            }
        }
        // Collapse by parent: leaves without a document store parent
        // themselves, and the relay concatenates the representatives.
        let flat_collapse = flat
            .fanout_search_collapse(name, query, K, &filters)
            .await
            .expect("flat collapse");
        for (label, tree) in &trees {
            let got = tree
                .fanout_search_collapse(name, query, K, &filters)
                .await
                .unwrap_or_else(|e| panic!("{name}: {label}: {e}"));
            assert_eq!(
                unary_bits(&got.hits),
                unary_bits(&flat_collapse.hits),
                "{name}: {label}: collapse"
            );
        }
    }
}

/// `(doc id, rank, vector score bits, bm25 score bits)`: a cascade hit
/// without the shard index, which names the relay through a relay.
fn cascade_bits(hits: &[pipestream_search::pb::CascadeHit]) -> Vec<(u64, u32, u32, u32)> {
    hits.iter()
        .map(|h| {
            (
                h.doc_id,
                h.rank,
                h.vector_score.to_bits(),
                h.bm25_score.to_bits(),
            )
        })
        .collect()
}

fn decomposed_legs() -> HybridLegs {
    HybridLegs {
        vector_weight: 0.6,
        bm25_weight: 0.4,
        fusion_mode: FusionMode::Decomposed,
        ..rrf_legs()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_and_decomposed_fusion_through_relays_equal_the_flat_fanout() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    // Children out of slot order, relays out of order at the root; the
    // groups are contiguous, as the routes routed by slot range require.
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees: Vec<(&str, Vec<String>)> = vec![
        ("one level", vec![a.clone(), b.clone()]),
        ("permuted", vec![q, p]),
        ("two levels", vec![top]),
    ];
    let flat = with_bm25(l.clone());
    let filters = Default::default();
    for (qi, text) in ["court", "zebra court", "opinion"].into_iter().enumerate() {
        let query = leaves.corpus[qi * DIM..(qi + 1) * DIM].to_vec();
        let (want_cascade, _) =
            common::fanout::cascade(&flat, "c", text, &query, LEX_K, 0.0, &filters)
                .await
                .expect("flat cascade");
        assert!(!want_cascade.is_empty(), "{text}: the cascade matched");
        let (want_decomposed, _) = common::fanout::hybrid_filtered(
            &flat,
            "d",
            text,
            &query,
            LEX_K,
            decomposed_legs(),
            &filters,
        )
        .await
        .expect("flat decomposed");
        assert!(!want_decomposed.is_empty(), "{text}: decomposed matched");
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone());
            let (cascade, _) =
                common::fanout::cascade(&relayed, "c", text, &query, LEX_K, 0.0, &filters)
                    .await
                    .unwrap_or_else(|e| panic!("{name} {text:?}: cascade: {e}"));
            assert_eq!(
                cascade_bits(&cascade),
                cascade_bits(&want_cascade),
                "{name} {text:?}: cascade"
            );
            let (decomposed, _) = common::fanout::hybrid_filtered(
                &relayed,
                "d",
                text,
                &query,
                LEX_K,
                decomposed_legs(),
                &filters,
            )
            .await
            .unwrap_or_else(|e| panic!("{name} {text:?}: decomposed: {e}"));
            assert_eq!(
                hybrid_bits(&decomposed),
                hybrid_bits(&want_decomposed),
                "{name} {text:?}: decomposed fusion"
            );
        }
    }
}

fn dense_clause(id: &str, vector: &[f32]) -> SelectionQuery {
    common::dense_selection(
        id,
        vector.to_vec(),
        DenseExecutionMode::Exhaustive as i32,
        0,
    )
}

fn fp32_clause(id: &str, vector: &[f32], mode: DenseExecutionMode) -> SelectionQuery {
    let fp32 = pipestream_search::pb::DensePrecisionRequest::Fp32 as i32;
    common::dense_selection(id, vector.to_vec(), mode as i32, fp32)
}

fn and_filtered(filter: SelectionQuery, leaf: SelectionQuery) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses: vec![filter, leaf],
            scoring: None,
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_and_boolean_queries_through_relays_equal_the_flat_fanout() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    // Children out of slot order, relays out of order at the root; the
    // groups are contiguous, as the routes routed by slot range require.
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees: Vec<(&str, Vec<String>)> = vec![
        ("one level", vec![a.clone(), b.clone()]),
        ("permuted", vec![q, p]),
        ("two levels", vec![top]),
    ];
    let flat = with_bm25(l.clone());
    let vector = leaves.corpus[..DIM].to_vec();
    let shapes: Vec<(&str, SelectionQuery)> = vec![
        (
            "filtered lexical",
            and_filtered(
                cel_clause("f", r#"court == "scotus""#),
                lexical_clause("lex", "court"),
            ),
        ),
        (
            "filtered dense",
            and_filtered(
                cel_clause("f", r#"court == "ca9""#),
                dense_clause("vec", &vector),
            ),
        ),
        (
            "boolean lexical must filter",
            boolean(
                vec![
                    lexical_clause("lex", "court"),
                    cel_clause("f", r#"court == "scotus""#),
                ],
                Vec::new(),
                Vec::new(),
            ),
        ),
        (
            "boolean dense must filter",
            boolean(
                vec![
                    dense_clause("vec", &vector),
                    cel_clause("f", r#"court == "ca9""#),
                ],
                Vec::new(),
                Vec::new(),
            ),
        ),
        (
            "fp32 rerank",
            fp32_clause("vec", &vector, DenseExecutionMode::Balanced),
        ),
        (
            "boolean fp32 must filter",
            boolean(
                vec![
                    fp32_clause("vec", &vector, DenseExecutionMode::Exhaustive),
                    cel_clause("f", r#"court == "scotus""#),
                ],
                Vec::new(),
                Vec::new(),
            ),
        ),
        (
            "boolean should and must_not",
            boolean(
                vec![lexical_clause("lex", "court")],
                vec![lexical_clause("z", "zebra"), dense_clause("vec", &vector)],
                vec![cel_clause("f", r#"court == "ca9""#)],
            ),
        ),
    ];
    for (shape, selection) in &shapes {
        let want = run_query(&flat, selection.clone())
            .await
            .unwrap_or_else(|e| panic!("{shape}: flat: {e}"));
        assert!(!want.is_empty(), "{shape}: the flat query matched");
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone());
            let got = run_query(&relayed, selection.clone())
                .await
                .unwrap_or_else(|e| panic!("{shape}: {name}: {e}"));
            assert_eq!(query_bits(&got), query_bits(&want), "{shape}: {name}");
        }
    }
}
