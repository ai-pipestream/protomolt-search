mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{
        self, node_service_client::NodeServiceClient, search_service_client::SearchServiceClient,
    },
    postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder},
};

#[test]
fn empty_map_values_remain_present_in_both_writers_and_readers() {
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-value-storage-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut heap = Bm25Store::new().with_map_facets(&["meta"]);
    let mut spill = SpillBuilder::create(&root.join("spill"))
        .unwrap()
        .with_map_facet_fields(&["meta"]);
    for row in 0..3 {
        let analyzed = AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1);
        heap.add_document(row, "word".into(), analyzed.clone());
        spill
            .add_document_with_lineage(row, "word".into(), analyzed, None)
            .unwrap();
    }
    for (row, value) in [(0, ""), (2, "value")] {
        heap.set_map_facet(0, row, "key", value);
        spill.set_map_facet(0, row, "key", value);
    }
    let path = root.join("heap.bm25");
    heap.save(&path).unwrap();
    let spilled = root.join("spill.bm25");
    spill.finish(&spilled).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(spilled).unwrap()
    );
    let loaded = Bm25Store::load(&path).unwrap();
    let mapped = Bm25Reader::open(&path).unwrap();
    mapped.verify_integrity().unwrap();
    let ci = mapped.map_facet_index("meta").unwrap();
    let key = mapped.map_facet_key_ord(ci, "key").unwrap();
    for (row, expected) in [(0, Some("")), (1, None), (2, Some("value"))] {
        assert_eq!(
            mapped
                .map_facet_value_ord(ci, key, row)
                .map(|v| mapped.map_facet_value(ci, v)),
            expected
        );
        let ci = loaded.map_facet_index("meta").unwrap();
        let key = loaded.map_facet_key_ord(ci, "key").unwrap();
        assert_eq!(
            loaded
                .map_facet_value_ord(ci, key, row)
                .map(|v| loaded.map_facet_value(ci, v)),
            expected
        );
    }
    drop(mapped);
    std::fs::remove_dir_all(root).unwrap();
}

async fn verify(addresses: Vec<String>, removed: &[u32]) {
    let coordinator = CoordinatorServiceImpl::new(addresses)
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CoordinatorServiceImpl::into_server(
                coordinator,
                pipestream_search::MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let mut client = SearchServiceClient::connect(address).await.unwrap();
    // Empty is present; a missing key remains unknown for equality and ordering.
    for (filter, mut expected) in [
        ("", vec![0, 1, 2, 3, 4, 5]),
        ("meta['key'] == ''", vec![0, 3]),
        ("meta['key'] != ''", vec![2, 5]),
        ("meta['key'] in ['']", vec![0, 3]),
        ("'key' in meta", vec![0, 2, 3, 5]),
        ("!('key' in meta)", vec![1, 4]),
        ("meta['key'] >= ''", vec![0, 2, 3, 5]),
        ("meta['key'] < 'value'", vec![0, 3]),
        ("meta['key'].startsWith('v')", vec![2, 5]),
    ] {
        expected.retain(|key| !removed.contains(key));
        let response = client
            .bm25_search(pb::Bm25SearchRequest {
                text: "word".into(),
                k: 10,
                filter: filter.into(),
                analysis: Some(body_spec()),
                projections: vec![
                    pb::NamedProjection {
                        name: "key".into(),
                        expression: "id".into(),
                    },
                    pb::NamedProjection {
                        name: "value".into(),
                        expression: "meta['key']".into(),
                    },
                ],
                map_facet_fields: vec![pb::MapFacetField {
                    column: "meta".into(),
                    key: "key".into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let mut keys = Vec::new();
        for hit in response.hits {
            let Some(pb::projected_value::Value::StringValue(key)) = &hit.projected[0].value else {
                panic!("missing logical key projection")
            };
            let key = key.parse::<u32>().unwrap();
            let expected_value = match key % 3 {
                0 => Some(pb::projected_value::Value::StringValue(String::new())),
                1 => None,
                _ => Some(pb::projected_value::Value::StringValue("value".into())),
            };
            assert_eq!(
                hit.projected[1].value, expected_value,
                "key {key}, {filter}"
            );
            keys.push(key);
        }
        keys.sort_unstable();
        assert_eq!(keys, expected, "{filter}");
        let mut expected_counts = std::collections::BTreeMap::new();
        for key in &expected {
            if key % 3 != 1 {
                *expected_counts
                    .entry(if key % 3 == 0 { "" } else { "value" })
                    .or_insert(0u64) += 1;
            }
        }
        let counts: std::collections::BTreeMap<_, _> = response.facets[0]
            .counts
            .iter()
            .map(|c| (c.value.as_str(), c.count))
            .collect();
        assert_eq!(counts, expected_counts, "{filter}");
    }
    drop(client);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn empty_value_and_absent_key_stay_distinct_through_rpc_flush_reopen_and_compaction() {
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-value-rpc-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        for shards in [1, 2] {
            let mut configs = Vec::new();
            let mut addresses = Vec::new();
            let mut servers = Vec::new();
            for shard in 0..shards {
                let config = NodeConfig {
                    index_path: Some(root.join(format!("{layout:?}-{shards}-{shard}.tv"))),
                    layout,
                    wal: true,
                    slot_offset: (shard * 3) as u64,
                    map_facet_fields: vec!["meta".into()],
                    facet_fields: vec!["id".into()],
                    analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                    ..Default::default()
                };
                let (address, server) = common::start_empty_node(config.clone()).await;
                let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
                let rows = if shards == 1 { 6 } else { 3 };
                // Compaction currently requires a locked provider configuration.
                let (shift, scale) =
                    common::fit_calibration(16, 4, &common::unit_vectors(64, 16, 134));
                client
                    .set_calibration(pb::SetCalibrationRequest {
                        dim: 16,
                        bit_width: 4,
                        shift,
                        scale,
                    })
                    .await
                    .unwrap();
                let requests = (0..rows)
                    .map(|row| {
                        let key = (shard * 3 + row).to_string();
                        pb::AddDocumentsRequest {
                            text: "word".into(),
                            analysis: Some(body_spec()),
                            facets: vec![pb::FacetValue {
                                field: "id".into(),
                                value: key.clone(),
                            }],
                            original_source: Some(common::protobuf_source("word", &key)),
                            identity: Some(pb::DocumentIdentity {
                                document_key: key.into_bytes(),
                                version: 1,
                                chunk_ordinal: None,
                            }),
                            map_facets: if row % 3 == 1 {
                                Vec::new()
                            } else {
                                vec![pb::MapFacetEntry {
                                    field: "meta".into(),
                                    key: "key".into(),
                                    value: if row % 3 == 0 { "" } else { "value" }.into(),
                                }]
                            },
                            ..Default::default()
                        }
                    })
                    .collect::<Vec<_>>();
                let documents = client
                    .add_documents(tokio_stream::iter(requests))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(documents.first_id, config.slot_offset);
                let vectors = client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        vectors: common::unit_vectors(rows, 16, 135),
                        dim: 16,
                    }]))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(vectors.first_id, documents.first_id);
                assert_eq!(vectors.added, documents.added);
                configs.push(config);
                addresses.push(address);
                servers.push(server);
            }
            // Ordering uses sorted dictionaries; flush also tests the empty
            // string's ordinal rather than treating ordinal zero as missing.
            for address in &addresses {
                NodeServiceClient::connect(address.clone())
                    .await
                    .unwrap()
                    .flush(pb::FlushRequest {})
                    .await
                    .unwrap();
            }
            verify(addresses.clone(), &[]).await;
            for server in servers.drain(..) {
                server.abort();
                let _ = server.await;
            }
            addresses.clear();
            for config in &configs {
                let (address, server) = common::start_opened_node(config.clone()).await;
                addresses.push(address);
                servers.push(server);
            }
            verify(addresses.clone(), &[]).await;
            // Remove a middle row so compaction moves a present-empty value
            // into a different physical slot. Query by the retained logical key.
            NodeServiceClient::connect(addresses[0].clone())
                .await
                .unwrap()
                .delete_documents(pb::DeleteDocumentsRequest {
                    doc_ids: vec![2],
                    ..Default::default()
                })
                .await
                .unwrap();
            for (shard, address) in addresses.iter().enumerate() {
                NodeServiceClient::connect(address.clone())
                    .await
                    .unwrap()
                    .compact_shard(pb::CompactShardRequest {
                        work_dir: root
                            .join(format!("compact-{layout:?}-{shards}-{shard}"))
                            .display()
                            .to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }
            verify(addresses, &[2]).await;
            for server in servers {
                server.abort();
                let _ = server.await;
            }
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
