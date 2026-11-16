//! Vector field authorization and pinned read-set checks.

use super::*;
use crate::pb::node_service_server::NodeService;
use crate::pb::*;
use prost::Message;

mod dense_policy_tests;
mod hybrid_leg_tests;
mod query_grant_tests;
mod scan_tests;
mod vector_read_tests;

fn node(offset: u64, fingerprint_override: bool) -> Arc<crate::node::NodeServiceImpl> {
    node_rows(offset, fingerprint_override, 2, 2)
}
fn node_rows(
    offset: u64,
    fingerprint_override: bool,
    docs: u32,
    rows: usize,
) -> Arc<crate::node::NodeServiceImpl> {
    node_rows_with_private_terms(offset, fingerprint_override, docs, rows, 1)
}
fn node_rows_with_private_terms(
    offset: u64,
    fingerprint_override: bool,
    docs: u32,
    rows: usize,
    private_terms: u32,
) -> Arc<crate::node::NodeServiceImpl> {
    let plan = crate::mapping::derive_plan(
        include_bytes!("../../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    let mut binding = plan.vector_binding.unwrap();
    if fingerprint_override {
        binding.plan_fingerprint = "b".repeat(64);
    }
    let mut store = crate::postings::Bm25Store::new().with_facets(&["audience"]);
    store.set_binding(Some(crate::postings::StoredBinding {
        plan_fingerprint: binding.plan_fingerprint.clone(),
        body_path: "body".into(),
        vector_binding: binding.encode_to_vec(),
        ..Default::default()
    }));
    for row in 0..docs {
        let count = if row == 0 { 1 } else { private_terms };
        store.add_document(
            row,
            vec!["word"; count as usize].join(" "),
            crate::postings::AnalyzedDoc::body(
                vec![(
                    "word".into(),
                    count,
                    (0..count).map(|i| (i * 5, i * 5 + 4)).collect(),
                )],
                count,
            ),
        );
        store.set_facet(0, row, if row == 0 { "public" } else { "private" });
    }
    let vectors = vec![0.25; rows * 16];
    let config = crate::vector::embedded_turbovec_config(4, &[0.0; 16], &[1.0; 16]).unwrap();
    let mut index = crate::vector::VectorIndex::from_backend_config(16, &config).unwrap();
    index.add(&vectors, 16).unwrap();
    Arc::new(
        crate::node::NodeServiceImpl::new(
            Some(index),
            crate::node::NodeConfig {
                slot_offset: offset,
                facet_fields: vec!["audience".into()],
                ..Default::default()
            },
        )
        .with_bm25(Some(crate::node::Bm25Shard::Building(store)))
        .with_exact_vectors(Some(
            crate::exact_vectors::ExactVectorStore::from_values(16, vectors).unwrap(),
        ))
        .unwrap(),
    )
}
fn access(field: &str, action: FieldAction) -> AccessDecision {
    AccessDecision {
        action: AccessAction::Search as i32,
        document_visibility: Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        }),
        field_permissions: Some(FieldPermissions {
            grants: vec![FieldGrant {
                field: field.into(),
                actions: vec![action as i32],
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn hybrid_query(strategy: selection_score_strategy::Strategy, field: &str) -> QueryRequest {
    let operator = if matches!(strategy, selection_score_strategy::Strategy::Cascade(_)) {
        SelectionOperator::Unspecified
    } else {
        SelectionOperator::Or
    };
    QueryRequest {
        k: 4,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: operator as i32,
                clauses: vec![
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "dense".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                field: field.into(),
                                vector: vec![0.25; 16],
                                ..Default::default()
                            })),
                        })),
                    },
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "lexical".into(),
                            query: Some(search_query::Query::Lexical(LexicalQuery {
                                text: "word".into(),
                                analysis: Some(crate::analyzer::body_spec()),
                                ..Default::default()
                            })),
                        })),
                    },
                ],
                scoring: Some(SelectionScoreStrategy {
                    strategy: Some(strategy),
                }),
            })),
        }),
        ..Default::default()
    }
}
