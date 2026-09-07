mod common;
use pipestream_search::{pb, relay::merge_bm25_responses};

#[test]
fn relay_requires_map_context_even_when_the_map_key_is_empty() {
    let request = pb::Bm25QueryRequest {
        map_facet_fields: vec![pb::MapFacetField {
            column: "meta".into(),
            key: String::new(),
        }],
        ..Default::default()
    };
    // This response could describe a plain facet; the empty key alone cannot
    // prove that the child evaluated the requested map entry.
    let response = pb::Bm25QueryResponse {
        facets: vec![pb::FacetFieldCounts {
            field: "meta".into(),
            key: String::new(),
            known: true,
            counts: vec![pb::FacetCount {
                value: "x".into(),
                count: 1,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        merge_bm25_responses(&request, vec![response])
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[test]
fn facet_merging_checks_names_keys_unknown_counts_and_overflow() {
    let req = pb::Bm25QueryRequest {
        map_facet_fields: vec![pb::MapFacetField {
            column: "meta".into(),
            key: String::new(),
        }],
        ..Default::default()
    };
    let good = pb::FacetFieldCounts {
        field: "meta".into(),
        key: String::new(),
        map_key: Some(String::new()),
        known: true,
        counts: vec![pb::FacetCount {
            value: String::new(),
            count: 1,
        }],
    };
    let wrap = |facet| pb::Bm25QueryResponse {
        facets: vec![facet],
        ..Default::default()
    };
    let mut wrong_name = good.clone();
    wrong_name.field = "different".into();
    let mut wrong_key = good.clone();
    wrong_key.map_key = Some("different".into());
    let mut wrong_legacy_key = good.clone();
    wrong_legacy_key.key = "different".into();
    let mut unknown_counts = good.clone();
    unknown_counts.known = false;
    for bad in [wrong_name, wrong_key, wrong_legacy_key, unknown_counts] {
        assert_eq!(
            merge_bm25_responses(&req, vec![wrap(bad)])
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    let mut unknown = good.clone();
    unknown.known = false;
    unknown.counts.clear();
    let merged = merge_bm25_responses(&req, vec![wrap(unknown), wrap(good.clone())]).unwrap();
    assert_eq!(merged.facets, vec![good.clone()]);
    let mut huge = good.clone();
    huge.counts[0].count = u64::MAX;
    assert_eq!(
        merge_bm25_responses(&req, vec![wrap(huge), wrap(good)])
            .unwrap_err()
            .code(),
        tonic::Code::OutOfRange
    );
}

fn map_range() -> pb::RangeFacetField {
    use pb::filter_bound::Value;
    pb::RangeFacetField {
        column: "metrics".into(),
        map: Some(pb::MapRangeFacet {
            key: String::new(),
            typed_edges: vec![
                Value::Int(-1),
                Value::Uint(0),
                Value::Int(3),
                Value::Num(6.0),
            ]
            .into_iter()
            .map(|value| pb::FilterBound {
                value: Some(value),
                exclusive: false,
            })
            .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn explicit_map_ranges_preserve_typed_edges_and_refuse_lost_context() {
    use pipestream_search::{
        node::{Bm25Shard, NodeConfig, NodeServiceImpl},
        pb::node_service_server::NodeService,
        postings::{AnalyzedDoc, Bm25Store},
    };
    let mut data = Bm25Store::new().with_map_numerics(&["metrics"]);
    for (row, value) in [Some(-1.0), Some(0.0), Some(2.0), Some(3.0), Some(6.0), None]
        .into_iter()
        .enumerate()
    {
        data.add_document(
            row as u32,
            "word".into(),
            AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
        );
        if let Some(value) = value {
            data.set_map_numeric(0, row as u32, "", value);
        }
    }
    let node = NodeServiceImpl::new(
        None,
        NodeConfig {
            map_numeric_fields: vec!["metrics".into()],
            ..Default::default()
        },
    )
    .with_bm25(Some(Bm25Shard::Building(data)));
    let range = map_range();
    let req = pb::Bm25QueryRequest {
        terms: vec!["word".into()],
        global_doc_count: 6,
        global_doc_frequencies: vec![6],
        global_total_doc_length: 6,
        range_facet_fields: vec![range.clone()],
        ..Default::default()
    };
    let response = node
        .bm25_query(tonic::Request::new(req.clone()))
        .await
        .unwrap()
        .into_inner();
    let counts = &response.range_facets[0];
    assert_eq!(counts.map_key.as_deref(), Some(""));
    assert_eq!(counts.key, "");
    assert!(counts.known);
    assert_eq!(
        counts.buckets.iter().map(|b| b.count).collect::<Vec<_>>(),
        vec![1, 2, 1]
    );
    let edges = &range.map.as_ref().unwrap().typed_edges;
    for (i, bucket) in counts.buckets.iter().enumerate() {
        assert_eq!(bucket.typed_from.as_ref(), Some(&edges[i]));
        assert_eq!(bucket.typed_to.as_ref(), Some(&edges[i + 1]));
    }
    let merged = merge_bm25_responses(&req, vec![response.clone()]).unwrap();
    assert_eq!(merged.range_facets, response.range_facets);
    for known in [true, false] {
        let mut bad = response.clone();
        bad.range_facets[0].map_key = None;
        bad.range_facets[0].known = known;
        if !known {
            bad.range_facets[0].buckets.clear();
        }
        assert_eq!(
            merge_bm25_responses(&req, vec![bad]).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
    // The legacy empty-key form still addresses a plain numeric column.
    let plain = node
        .bm25_query(tonic::Request::new(pb::Bm25QueryRequest {
            range_facet_fields: vec![pb::RangeFacetField {
                column: "metrics".into(),
                edges: vec![-1.0, 0.0, 3.0, 6.0],
                ..Default::default()
            }],
            ..req
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!plain.range_facets[0].known);
    assert!(plain.range_facets[0].map_key.is_none());
}

#[tokio::test]
async fn map_range_validation_rejects_ambiguous_shapes_and_old_decoding() {
    use pipestream_search::{
        node::{NodeConfig, NodeServiceImpl},
        pb::node_service_server::NodeService,
    };
    use prost::Message;
    #[derive(Clone, PartialEq, Message)]
    struct OldRange {
        #[prost(string, tag = "1")]
        column: String,
        #[prost(string, tag = "2")]
        key: String,
        #[prost(double, repeated, tag = "3")]
        edges: Vec<f64>,
        #[prost(message, repeated, tag = "4")]
        typed_edges: Vec<pb::FilterBound>,
    }
    let request = map_range();
    let old = OldRange::decode(request.encode_to_vec().as_slice()).unwrap();
    assert!(old.edges.is_empty() && old.typed_edges.is_empty());
    let discarded = pb::RangeFacetField::decode(old.encode_to_vec().as_slice()).unwrap();
    let mut bad = vec![discarded];
    let mut key = request.clone();
    key.key = "legacy".into();
    bad.push(key);
    let mut edges = request.clone();
    edges.edges = vec![0.0, 1.0];
    bad.push(edges);
    let mut both = request.clone();
    both.map.as_mut().unwrap().edges = vec![0.0, 1.0];
    bad.push(both);
    let mut empty = request.clone();
    empty.map.as_mut().unwrap().typed_edges.clear();
    bad.push(empty);
    let mut unordered = request.clone();
    unordered.map.as_mut().unwrap().typed_edges.reverse();
    bad.push(unordered);
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    for range in bad {
        let err = node
            .bm25_query(tonic::Request::new(pb::Bm25QueryRequest {
                range_facet_fields: vec![range],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("range facet"));
    }
}
