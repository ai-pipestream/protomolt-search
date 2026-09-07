mod common;

use pipestream_search::{
    cel::compile_filter,
    filter::Tri,
    pb,
    placement::{eval_document, DocColumns},
};

#[test]
fn empty_map_key_never_aliases_a_plain_column_in_placement() {
    let doc = pb::AddDocumentsRequest {
        facets: vec![pb::FacetValue {
            field: "meta".into(),
            value: "a".into(),
        }],
        map_facets: vec![pb::MapFacetEntry {
            field: "meta".into(),
            key: String::new(),
            value: "map".into(),
        }],
        ..Default::default()
    };
    let columns = DocColumns::of(&doc).unwrap();
    for (source, expected) in [
        ("meta[''] >= 'm'", Tri::True),
        ("meta[''] < 'm'", Tri::False),
        ("meta[''].startsWith('m')", Tri::True),
        ("meta >= 'm'", Tri::False),
        ("meta.startsWith('a')", Tri::True),
        ("!(meta['absent'] >= 'm')", Tri::Unknown),
    ] {
        let compiled = compile_filter(source).unwrap().unwrap();
        assert_eq!(eval_document(&compiled, &columns), expected, "{source}");
    }
}

#[test]
fn map_predicates_are_not_pruned_or_removed_using_plain_column_bounds() {
    use pipestream_search::placement::{
        implied_under, impossible_under, without_leaves, ColumnBounds,
    };
    let bounds = ColumnBounds::of_conjunction(&[compile_filter("meta == 'a'").unwrap().unwrap()]);
    for source in ["meta[''] >= 'm'", "meta[''].startsWith('m')"] {
        let predicate = compile_filter(source).unwrap().unwrap();
        assert_eq!(impossible_under(&predicate, &bounds), None);
        assert!(implied_under(&predicate, &bounds).is_empty());
        let combined = compile_filter(&format!("{source} && meta == 'a'"))
            .unwrap()
            .unwrap();
        assert_eq!(implied_under(&combined, &bounds), vec![1]);
        assert_eq!(without_leaves(&combined, &[1]), Some(predicate));
        let impossible = compile_filter(&format!("{source} && meta == 'z'"))
            .unwrap()
            .unwrap();
        assert_eq!(impossible_under(&impossible, &bounds), Some(vec![1]));
    }
}

#[test]
fn older_filter_schema_cannot_reinterpret_explicit_map_context() {
    use prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage};
    let mut set = prost_types::FileDescriptorSet::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/search_descriptor.bin")).as_slice(),
    )
    .unwrap();
    let filter = set
        .file
        .iter_mut()
        .filter(|file| file.package.as_deref() == Some("ai.protomolt.search.v1"))
        .flat_map(|file| &mut file.message_type)
        .find(|message| message.name.as_deref() == Some("FilterExpr"))
        .unwrap();
    // Reproduce the preceding schema, which has exactly tags 1 through 12.
    filter.field.retain(|field| field.number.unwrap() <= 12);
    assert_eq!(filter.field.len(), 12);
    let pool = DescriptorPool::from_file_descriptor_set(set).unwrap();
    let descriptor = pool
        .get_message_by_name("ai.protomolt.search.v1.FilterExpr")
        .unwrap();
    for source in ["meta[''] >= 'm'", "meta[''].startsWith('m')"] {
        let compiled = compile_filter(source).unwrap().unwrap();
        let mut old =
            DynamicMessage::decode(descriptor.clone(), compiled.encode_to_vec().as_slice())
                .unwrap();
        assert_eq!(
            old.fields().count(),
            0,
            "old schema must not select any legacy predicate"
        );
        assert_eq!(old.take_unknown_fields().count(), 1);
        // Prost's old generated decoder drops the unknown variant. The
        // existing required-expression validation refuses that empty node.
        let discarded = pb::FilterExpr::decode(old.encode_to_vec().as_slice()).unwrap();
        assert!(discarded.expr.is_none());
        assert_eq!(
            pipestream_search::filter::validate_filter(&discarded)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        for expr in [
            pb::filter_expr::Expr::Not(Box::new(discarded.clone())),
            pb::filter_expr::Expr::Or(pb::FilterList {
                exprs: vec![
                    compile_filter("meta == 'a'").unwrap().unwrap(),
                    discarded.clone(),
                ],
            }),
        ] {
            assert!(pipestream_search::filter::validate_filter(&pb::FilterExpr {
                expr: Some(expr)
            })
            .is_err());
        }
    }
    // Ordinary scalar and nonempty map selectors keep the established wire.
    for source in [
        "meta >= 'm'",
        "meta['key'] >= 'm'",
        "meta['key'].startsWith('m')",
    ] {
        let compiled = compile_filter(source).unwrap().unwrap();
        let old = DynamicMessage::decode(descriptor.clone(), compiled.encode_to_vec().as_slice())
            .unwrap();
        assert_eq!(old.unknown_fields().count(), 0);
        assert_eq!(old.fields().count(), 1);
    }
}

#[tokio::test]
async fn empty_key_ranges_and_prefixes_survive_node_and_relay_wire() {
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        coordinator::CoordinatorServiceImpl,
        harness::start_relay,
        node::{Bm25Shard, NodeConfig, NodeServiceImpl},
        postings::{AnalyzedDoc, Bm25Store},
    };
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-selector-rpc-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for mapped in [false, true] {
        let mut addresses = Vec::new();
        let mut handles = Vec::new();
        for leaf in 0..2 {
            // Low-level storage already supports empty keys. Public ingestion
            // remains gated until score/count selectors can represent them.
            let mut store = Bm25Store::new().with_map_facets(&["meta"]);
            for (row, value) in [Some("map"), Some("a"), Some(""), None].iter().enumerate() {
                store.add_document(
                    row as u32,
                    "word".into(),
                    AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
                );
                if let Some(value) = value {
                    store.set_map_facet(0, row as u32, "", value);
                }
            }
            let path = root.join(format!("{mapped}-{leaf}.bm25"));
            let shard = if mapped {
                store.save(&path).unwrap();
                Bm25Shard::open(&path).unwrap()
            } else {
                Bm25Shard::Building(store)
            };
            let service = NodeServiceImpl::new(
                None,
                NodeConfig {
                    slot_offset: leaf * 4,
                    map_facet_fields: vec!["meta".into()],
                    analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                    ..Default::default()
                },
            )
            .with_bm25(Some(shard));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(format!("http://{}", listener.local_addr().unwrap()));
            handles.push(tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
            ));
        }
        let (relay, _, relay_handle) = start_relay(addresses.clone()).await;
        let (top, _, top_handle) = start_relay(vec![relay.clone()]).await;
        for children in [addresses, vec![relay], vec![top]] {
            let coordinator = CoordinatorServiceImpl::new(children)
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(coordinator.into_server(pipestream_search::MAX_MESSAGE_BYTES))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
            );
            let mut client = pb::search_service_client::SearchServiceClient::connect(address)
                .await
                .unwrap();
            for (filter, expected) in [
                ("meta[''] >= 'm'", vec![0, 4]),
                ("'m' <= meta['']", vec![0, 4]),
                ("meta[''] < 'm'", vec![1, 2, 5, 6]),
                ("meta[''].startsWith('m')", vec![0, 4]),
                ("!(meta[''].startsWith('m'))", vec![1, 2, 5, 6]),
                ("meta[''] >= ''", vec![0, 1, 2, 4, 5, 6]),
                ("meta[''] >= 'm' || !('' in meta)", vec![0, 3, 4, 7]),
            ] {
                let response = client
                    .bm25_search(pb::Bm25SearchRequest {
                        text: "word".into(),
                        k: 10,
                        filter: filter.into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                let mut ids: Vec<_> = response.hits.iter().map(|hit| hit.doc_id).collect();
                ids.sort_unstable();
                assert_eq!(ids, expected, "mapped={mapped} filter={filter}");
            }
            let missing = client
                .bm25_search(pb::Bm25SearchRequest {
                    text: "word".into(),
                    k: 10,
                    filter: "meta['absent'] >= 'm'".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })
                .await
                .unwrap_err();
            assert!(missing.message().contains("absent"));
            drop(client);
            server.abort();
            let _ = server.await;
        }
        relay_handle.abort();
        top_handle.abort();
        let _ = relay_handle.await;
        let _ = top_handle.await;
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
