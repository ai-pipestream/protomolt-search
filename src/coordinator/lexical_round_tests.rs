//! Lexical validation and certified winner regressions.

use super::lexical_round::finish_bm25_hits;
use super::*;

fn hit(id: u64, score: f32) -> Bm25Hit {
    Bm25Hit {
        doc_id: id,
        score,
        ..Default::default()
    }
}

fn certified(winners: &[(u32, u64, f32)]) -> Option<Arc<Mutex<Bm25StreamHeap>>> {
    Some(Arc::new(Mutex::new(Bm25StreamHeap {
        heap: winners
            .iter()
            .map(|&(shard, vector_id, score)| {
                StreamHeapEntry(MergedHit {
                    shard,
                    vector_id,
                    score,
                })
            })
            .collect(),
        floors_sent: 0,
        progress: None,
    })))
}

#[test]
fn certified_winners_require_the_same_shard_and_exact_score_bits() {
    let wrong_shard =
        finish_bm25_hits(vec![(1, hit(7, 2.0))], certified(&[(0, 7, 2.0)]), 1).unwrap_err();
    assert_eq!(wrong_shard.code(), tonic::Code::DataLoss);
    assert!(wrong_shard
        .message()
        .contains("absent from its certified local top-k"));
    let changed =
        finish_bm25_hits(vec![(0, hit(7, -0.0))], certified(&[(0, 7, 0.0)]), 1).unwrap_err();
    assert_eq!(changed.code(), tonic::Code::DataLoss);
    assert!(changed.message().contains("changed score"));
}

#[test]
fn certified_and_unary_winners_preserve_their_existing_tie_orders() {
    let mut detail = hit(9, 1.0);
    detail.snippets = vec![crate::pb::Snippet {
        field: "body".into(),
        text: "certified payload".into(),
        ..Default::default()
    }];
    let input = vec![
        (1, hit(2, 1.0)),
        (0, detail),
        (0, hit(4, 1.0)),
        (1, hit(1, 0.5)),
    ];
    let unary = finish_bm25_hits(input.clone(), None, 3).unwrap();
    let streamed = finish_bm25_hits(
        input,
        certified(&[(1, 2, 1.0), (0, 9, 1.0), (0, 4, 1.0)]),
        3,
    )
    .unwrap();
    // The existing unary route orders shard before id; certified winners use cmp_hits.
    assert_eq!(
        streamed.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [2, 4, 9]
    );
    assert_eq!(streamed[2].snippets[0].text, "certified payload");
    assert_eq!(
        unary.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [4, 9, 2]
    );
    assert_eq!(unary[1].snippets[0].text, "certified payload");
}

#[tokio::test]
async fn malformed_facets_are_rejected_before_empty_query_or_analysis_backend() {
    let coordinator = CoordinatorServiceImpl::new(Vec::new());
    let ranges = [crate::pb::RangeFacetField {
        column: "price".into(),
        edges: vec![2.0, 1.0],
        ..Default::default()
    }];
    let query = LexicalFanout {
        range_facet_fields: &ranges,
        ..Default::default()
    };
    let flat = coordinator.fanout_bm25_context(query).await.unwrap_err();
    let fused = coordinator
        .fanout_bm25_fused_context(query)
        .await
        .unwrap_err();
    assert_eq!(flat.code(), tonic::Code::InvalidArgument);
    assert_eq!(flat.code(), fused.code());
    assert_eq!(flat.message(), fused.message());
    assert!(flat.message().contains("price"));
}

#[tokio::test]
async fn public_contexts_refuse_options_their_route_cannot_serve() {
    let coordinator = CoordinatorServiceImpl::new(Vec::new());
    let fields = [crate::pb::QueryField {
        field: "body".into(),
        ..Default::default()
    }];
    let stats = ["price".to_string()];
    let terms = ["word".to_string()];
    let stages = [crate::pb::ScoreStage::default()];
    let projections = [crate::pb::CompiledProjection::default()];
    let spec = crate::pb::AnalysisSpec::default();
    let prefixes = [crate::pb::TermPrefix::default()];
    let synonyms = [crate::pb::SynonymRule::default()];
    for query in [
        LexicalFanout {
            fields: &fields,
            ..Default::default()
        },
        LexicalFanout {
            stats_fields: &stats,
            ..Default::default()
        },
        LexicalFanout {
            cardinality_fields: &stats,
            ..Default::default()
        },
    ] {
        assert_eq!(
            coordinator
                .fanout_bm25_context(query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    for query in [
        LexicalFanout {
            spec: Some(&spec),
            ..Default::default()
        },
        LexicalFanout {
            pre_analyzed_terms: &terms,
            ..Default::default()
        },
        LexicalFanout {
            prefixes: &prefixes,
            ..Default::default()
        },
        LexicalFanout {
            synonyms: &synonyms,
            ..Default::default()
        },
        LexicalFanout {
            synonyms_off: true,
            ..Default::default()
        },
        LexicalFanout {
            score_stages: &stages,
            ..Default::default()
        },
        LexicalFanout {
            projections: &projections,
            ..Default::default()
        },
        LexicalFanout {
            stats_fields: &stats,
            ..Default::default()
        },
        LexicalFanout {
            cardinality_fields: &stats,
            ..Default::default()
        },
    ] {
        assert_eq!(
            coordinator
                .fanout_bm25_fused_context(query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
