//! Shared filter references must cover each occurrence's Boolean domain.
use pipestream_search::analyzer::{analysis_fingerprint, analyze_document_native, body_spec};
use pipestream_search::node::{Bm25Shard, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_server::NodeService;
use pipestream_search::pb::*;
use pipestream_search::postings::Bm25Store;
use tonic::Request;

fn leaf(index: u32) -> BooleanPlanNode {
    BooleanPlanNode {
        node: Some(boolean_plan_node::Node::Leaf(index)),
    }
}
fn nested() -> BooleanPlanNode {
    BooleanPlanNode {
        node: Some(boolean_plan_node::Node::Group(BooleanPlanGroup {
            must: vec![leaf(0), leaf(1)],
            ..Default::default()
        })),
    }
}
fn lexical(term: &str, df: u32) -> BooleanPlanLeaf {
    BooleanPlanLeaf {
        leaf: Some(boolean_plan_leaf::Leaf::Lexical(BooleanPlanLexical {
            terms: vec![term.into()],
            global_doc_count: 4,
            global_total_doc_length: 8,
            global_doc_frequencies: vec![df],
            analysis_fingerprint: analysis_fingerprint(Some(&body_spec())),
            ..Default::default()
        })),
    }
}
fn fixture() -> (NodeServiceImpl, Vec<BooleanPlanLeaf>) {
    let mut store = Bm25Store::with_fields(&["body"]).with_facets(&["audience"]);
    for (id, text, audience) in [
        (0, "alpha all", "public"),
        (1, "beta all", "public"),
        (2, "beta all", "private"),
        (3, "alpha all", "private"),
    ] {
        store.add_document(
            id,
            text.into(),
            analyze_document_native(text, Some(&body_spec())).unwrap(),
        );
        store.set_facet(0, id, audience);
    }
    let node = NodeServiceImpl::new(None, NodeConfig::default())
        .with_bm25(Some(Bm25Shard::Building(store)));
    let filter = pipestream_search::cel::compile_filter("audience == 'public'").unwrap();
    let leaves = vec![
        lexical("alpha", 2),
        BooleanPlanLeaf {
            leaf: Some(boolean_plan_leaf::Leaf::Filter(BooleanPlanFilter {
                filter,
                ..Default::default()
            })),
        },
        lexical("all", 4),
    ];
    (node, leaves)
}
#[tokio::test]
async fn shared_filter_under_direct_should_or_must_not_keeps_its_full_domain() {
    let (node, leaves) = fixture();
    for audience in [None, Some("public"), Some("private")] {
        for negative in [false, true] {
            let root = if negative {
                BooleanPlanGroup {
                    should: vec![nested(), leaf(2)],
                    must_not: vec![leaf(1)],
                    minimum_should_match: 1,
                    ..Default::default()
                }
            } else {
                BooleanPlanGroup {
                    should: vec![nested(), leaf(1)],
                    minimum_should_match: 1,
                    ..Default::default()
                }
            };
            let visibility = audience.map(|a| DocumentVisibility {
                filter: pipestream_search::cel::compile_filter(&format!("audience == '{a}'"))
                    .unwrap(),
            });
            let response = node
                .evaluate_boolean(Request::new(BooleanShardRequest {
                    root: Some(root),
                    leaves: leaves.clone(),
                    depth: 4,
                    visibility,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            let mut ids: Vec<_> = response.candidates.iter().map(|c| c.doc_id).collect();
            ids.sort_unstable();
            let expected = match (negative, audience) {
                (false, Some("private")) | (true, Some("public")) => vec![],
                (false, _) => vec![0, 1],
                (true, _) => vec![2, 3],
            };
            assert_eq!(ids, expected, "negative={negative}, audience={audience:?}");
        }
    }
}

#[tokio::test]
async fn nested_filter_provenance_is_complete_for_rows_admitted_elsewhere() {
    let (node, leaves) = fixture();
    let response = node
        .evaluate_boolean(Request::new(BooleanShardRequest {
            root: Some(BooleanPlanGroup {
                should: vec![nested(), leaf(2)],
                minimum_should_match: 1,
                ..Default::default()
            }),
            leaves,
            depth: 4,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.candidates.len(), 4);
    let outside = response.candidates.iter().find(|c| c.doc_id == 1).unwrap();
    // This public beta row misses the nested alpha group but still matches
    // its filter. The wire contract reports every matching leaf on a candidate.
    assert_eq!(outside.matched, vec![1, 2]);
    let private = response.candidates.iter().find(|c| c.doc_id == 2).unwrap();
    assert_eq!(private.matched, vec![2]);
}
