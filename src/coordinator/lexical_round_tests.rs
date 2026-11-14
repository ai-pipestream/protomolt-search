//! Lexical validation and certified winner regressions.

use super::lexical_context::{FlatLexicalQuery, FusedLexicalQuery};
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

#[test]
fn malformed_facets_are_rejected_before_empty_query_or_analysis_backend() {
    let request = Bm25SearchRequest {
        range_facet_fields: vec![crate::pb::RangeFacetField {
            column: "price".into(),
            edges: vec![2.0, 1.0],
            ..Default::default()
        }],
        ..Default::default()
    };
    let flat = FlatLexicalQuery::new(&request, 0, &[], None)
        .err()
        .expect("invalid flat range");
    let fused = FusedLexicalQuery::new(&request, 0, None)
        .err()
        .expect("invalid fused range");
    assert_eq!(flat.code(), tonic::Code::InvalidArgument);
    assert_eq!(flat.message(), fused.message());
    assert!(flat.message().contains("price"));
}

#[test]
fn constructors_refuse_malformed_floors_highlights_geo_and_filters_before_empty_queries() {
    for score in [f32::NAN, f32::NEG_INFINITY] {
        let request = Bm25SearchRequest {
            min_score: score,
            ..Default::default()
        };
        assert_invalid_contexts(&request, None, "min_score");
    }
    let request = Bm25SearchRequest {
        highlight: Some(crate::pb::HighlightSpec {
            mode: i32::MAX,
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_invalid_contexts(&request, None, "HighlightSpec.mode");
    let request = Bm25SearchRequest {
        geo_filters: vec![crate::pb::GeoFilter {
            column: "place".into(),
            region: None,
        }],
        ..Default::default()
    };
    assert_invalid_contexts(&request, None, "place");
    assert_invalid_contexts(
        &Bm25SearchRequest::default(),
        Some(&crate::pb::FilterExpr::default()),
        "",
    );
}

fn assert_invalid_contexts(
    request: &Bm25SearchRequest,
    filter: Option<&crate::pb::FilterExpr>,
    message: &str,
) {
    let flat = FlatLexicalQuery::new(request, 0, &[], filter)
        .err()
        .expect("flat constructor must refuse");
    let fused = FusedLexicalQuery::new(request, 0, filter)
        .err()
        .expect("fused constructor must refuse");
    assert_eq!(flat.code(), tonic::Code::InvalidArgument);
    assert_eq!(fused.code(), tonic::Code::InvalidArgument);
    assert!(flat.message().contains(message), "{}", flat.message());
    assert_eq!(flat.message(), fused.message());
}

#[tokio::test]
async fn public_contexts_refuse_options_their_route_cannot_serve() {
    let coordinator = CoordinatorServiceImpl::new(Vec::new());
    for request in [
        Bm25SearchRequest {
            fields: vec![crate::pb::QueryField {
                field: "body".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        Bm25SearchRequest {
            phrase: Some(Default::default()),
            ..Default::default()
        },
    ] {
        assert_eq!(
            FlatLexicalQuery::new(&request, 0, &[], None)
                .err()
                .expect("fused input")
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    for request in [
        Bm25SearchRequest {
            stats_fields: vec!["price".into()],
            ..Default::default()
        },
        Bm25SearchRequest {
            cardinality_fields: vec!["price".into()],
            ..Default::default()
        },
    ] {
        let query = FlatLexicalQuery::new(&request, 0, &[], None).unwrap();
        assert_eq!(
            coordinator
                .fanout_bm25_context(query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    for request in [
        Bm25SearchRequest {
            analysis: Some(Default::default()),
            ..Default::default()
        },
        Bm25SearchRequest {
            phrase: Some(Default::default()),
            ..Default::default()
        },
        Bm25SearchRequest {
            prefixes: vec![Default::default()],
            ..Default::default()
        },
        Bm25SearchRequest {
            score_stages: vec![Default::default()],
            ..Default::default()
        },
        Bm25SearchRequest {
            projections: vec![Default::default()],
            ..Default::default()
        },
        Bm25SearchRequest {
            stats_fields: vec!["price".into()],
            ..Default::default()
        },
        Bm25SearchRequest {
            cardinality_fields: vec!["price".into()],
            ..Default::default()
        },
    ] {
        assert_eq!(
            FusedLexicalQuery::new(&request, 0, None)
                .err()
                .expect("flat-only input")
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn contexts_preserve_positive_infinity_and_normalize_ignored_fused_inputs() {
    let request = Bm25SearchRequest {
        min_score: f32::INFINITY,
        pre_analyzed_terms: vec!["ignored".into()],
        synonyms: vec![Default::default()],
        synonyms_off: true,
        ..Default::default()
    };
    let flat = FlatLexicalQuery::new(&request, 3, &[], None).unwrap();
    assert_eq!(flat.options().request().min_score, f32::INFINITY);
    let fused = FusedLexicalQuery::new(&request, 3, None).unwrap();
    let expected = Bm25QueryRequest {
        k: 3,
        min_score: f32::INFINITY,
        ..Default::default()
    };
    assert_eq!(fused.options().request(), expected);
}

#[test]
fn body_phrase_context_preserves_field_analysis_and_expansion_inputs() {
    let request = Bm25SearchRequest {
        text: "compound query".into(),
        analysis: Some(Default::default()),
        phrase: Some(Default::default()),
        prefixes: vec![Default::default()],
        synonyms: vec![Default::default()],
        synonyms_off: true,
        pre_analyzed_terms: vec!["ignored in positioned query".into()],
        ..Default::default()
    };
    let query = FusedLexicalQuery::body_phrase(&request, 4, None).unwrap();
    assert_eq!(query.fields().len(), 1);
    let field = &query.fields()[0];
    assert_eq!(field.field, "body");
    assert_eq!(field.weight, 1.0);
    assert_eq!(field.analysis, request.analysis);
    assert_eq!(field.phrase, request.phrase);
    assert_eq!(field.prefixes, request.prefixes);
    assert_eq!(field.synonyms, request.synonyms);
    assert!(field.synonyms_off);
    assert_eq!(query.options().k(), 4);
}
