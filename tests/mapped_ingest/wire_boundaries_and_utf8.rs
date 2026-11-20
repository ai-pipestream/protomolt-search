//! Invalid UTF-8 refusal, 10-byte varint boundaries, and omitted empty secondary text.

use super::*;

fn boundary_case_set(syntax: &str) -> Vec<u8> {
    let mut set = FileDescriptorSet::decode(&case_set()[..]).unwrap();
    let case_file = &mut set.file[1];
    case_file.syntax = Some(syntax.into());
    let case = &mut case_file.message_type[0];
    case.field
        .push(scalar("number", 11, Type::Uint64, Label::Optional));
    case.field
        .push(scalar("signed_number", 12, Type::Int64, Label::Optional));
    set.encode_to_vec()
}

fn boundary_bind(syntax: &str) -> MappedBind {
    let descriptor_set = boundary_case_set(syntax);
    let plan = derive_plan(&descriptor_set, "law.v1.Case").unwrap();
    MappedBind {
        descriptor_set,
        expected_fingerprint: plan.fingerprint,
        ..bind()
    }
}

fn boundary_config(analysis: String) -> NodeConfig {
    let mut config = case_node_config(analysis);
    config.unsigned_integer_fields.push("number".into());
    config.integer_fields.push("signed_number".into());
    config
}

fn boundary_document(i: usize, suffix: &[u8]) -> Vec<u8> {
    let mut document = doc(i % N_DOCS).encode();
    document.extend_from_slice(suffix);
    document
}

async fn mapped_counts(addr: &str) -> (u64, u64) {
    let health = NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    (health.document_slots, health.num_vectors)
}

async fn boundary_hits(addr: &str, analysis: &str) -> Vec<pipestream_search::pb::Bm25Hit> {
    CoordinatorServiceImpl::new(vec![addr.to_string()])
        .with_bm25(Some(analysis.to_string()), Default::default())
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: 16,
            projections: ["id", "number", "signed_number"]
                .into_iter()
                .map(|name| NamedProjection {
                    name: name.into(),
                    expression: name.into(),
                })
                .collect(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .hits
}

#[tokio::test]
async fn mapped_ingest_refuses_an_invalid_utf8_document_and_preserves_the_seed() {
    let (analysis, _mock) = start_mock_analysis().await;
    for syntax in ["proto2", "proto3"] {
        let (addr, _node) = start_empty_node(boundary_config(analysis.clone())).await;
        seed_calibration(&addr).await;
        assert_eq!(
            send(
                &addr,
                boundary_bind(syntax),
                vec![boundary_document(0, &[])]
            )
            .await
            .unwrap()
            .added,
            1
        );
        assert_eq!(mapped_counts(&addr).await, (1, 1));

        let invalid_title = boundary_document(2, &[0x12, 0x02, 0xc3, 0x28]);
        let error = send(&addr, boundary_bind(syntax), vec![invalid_title])
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{syntax}: {error}"
        );
        assert!(error.message().contains("title"), "{syntax}: {error}");
        assert!(error.message().contains("UTF-8"), "{syntax}: {error}");
        assert_eq!(mapped_counts(&addr).await, (1, 1), "{syntax}");
        let hits = boundary_hits(&addr, &analysis).await;
        assert_eq!(hits.len(), 1, "{syntax}");
        assert_eq!(hits[0].doc_id, 0, "{syntax}");
        assert_eq!(
            hits[0].projected[0].value,
            Some(projected_value::Value::StringValue("case-0".into())),
            "{syntax}"
        );
    }
}

#[tokio::test]
async fn mapped_ingest_accepts_ten_byte_boundaries_and_refuses_overwide_varints() {
    let (analysis, _mock) = start_mock_analysis().await;
    let ten_byte_zero = [
        0x58, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
    ];
    let u64_max = [
        0x58, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
    ];
    let int64_min = [
        0x60, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
    ];
    let negative_one = [
        0x60, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
    ];
    let overwide_u64 = [
        0x58, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02,
    ];

    for syntax in ["proto2", "proto3"] {
        let (addr, _node) = start_empty_node(boundary_config(analysis.clone())).await;
        seed_calibration(&addr).await;
        send(
            &addr,
            boundary_bind(syntax),
            vec![boundary_document(0, &[])],
        )
        .await
        .unwrap();
        let controls = [ten_byte_zero, u64_max, int64_min, negative_one]
            .iter()
            .enumerate()
            .map(|(i, wire)| boundary_document(i + 1, wire))
            .collect();
        let accepted = send(&addr, boundary_bind(syntax), controls).await.unwrap();
        assert_eq!(accepted.added, 4, "{syntax}");
        assert_eq!(mapped_counts(&addr).await, (5, 5), "{syntax}");
        let mut hits = boundary_hits(&addr, &analysis).await;
        hits.sort_unstable_by_key(|hit| hit.doc_id);
        assert_eq!(hits.len(), 5, "{syntax}");
        use projected_value::Value::{IntValue, UintValue};
        assert_eq!(hits[1].projected[1].value, Some(UintValue(0)), "{syntax}");
        assert_eq!(
            hits[2].projected[1].value,
            Some(UintValue(u64::MAX)),
            "{syntax}"
        );
        assert_eq!(
            hits[3].projected[2].value,
            Some(IntValue(i64::MIN)),
            "{syntax}"
        );
        assert_eq!(hits[4].projected[2].value, Some(IntValue(-1)), "{syntax}");
        let expected_values: Vec<_> = hits
            .iter()
            .map(|hit| (hit.doc_id, hit.projected.clone()))
            .collect();

        let error = send(
            &addr,
            boundary_bind(syntax),
            vec![boundary_document(5, &overwide_u64)],
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{syntax}: {error}"
        );
        assert!(error.message().contains("number"), "{syntax}: {error}");
        assert!(error.message().contains("varint"), "{syntax}: {error}");
        assert_eq!(mapped_counts(&addr).await, (5, 5), "{syntax}");
        let mut hits_after = boundary_hits(&addr, &analysis).await;
        hits_after.sort_unstable_by_key(|hit| hit.doc_id);
        assert_eq!(
            hits_after
                .iter()
                .map(|hit| (hit.doc_id, hit.projected.clone()))
                .collect::<Vec<_>>(),
            expected_values,
            "{syntax}"
        );
    }
}

#[tokio::test]
async fn empty_secondary_text_is_omitted_while_body_and_source_survive_reopen() {
    let (analysis, _mock) = start_mock_analysis().await;
    for syntax in ["proto2", "proto3"] {
        let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "tvmapped_empty_secondary_{syntax}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let index_path = root.join("shard.tv");
        let config = NodeConfig {
            index_path: Some(index_path.clone()),
            ..boundary_config(analysis.clone())
        };
        let (addr, node) = start_empty_node(config.clone()).await;
        seed_calibration(&addr).await;

        let absent_author = boundary_document(1, &[]);
        let explicitly_empty_author = boundary_document(3, &[0x3a, 0x02, 0x0a, 0x00]);
        let original_documents = vec![absent_author, explicitly_empty_author];
        let accepted = send(&addr, boundary_bind(syntax), original_documents.clone())
            .await
            .unwrap();
        assert_eq!((accepted.added, accepted.total), (2, 2), "{syntax}");
        NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .flush(pipestream_search::pb::FlushRequest {})
            .await
            .unwrap();
        let mut hits = boundary_hits(&addr, &analysis).await;
        hits.sort_unstable_by_key(|hit| hit.doc_id);
        assert_eq!(hits.len(), 2, "{syntax}");
        assert_eq!(
            hits.iter()
                .map(|hit| hit.projected[0].value.clone())
                .collect::<Vec<_>>(),
            vec![
                Some(projected_value::Value::StringValue("case-1".into())),
                Some(projected_value::Value::StringValue("case-3".into())),
            ],
            "{syntax}"
        );
        let expected_ids: Vec<_> = hits
            .iter()
            .map(|hit| (hit.doc_id, hit.projected[0].value.clone()))
            .collect();

        node.abort();
        let _ = node.await;

        let bm25_path = common::catalog::segment_bm25(&index_path, 0);
        let store = pipestream_search::postings::Bm25Store::load(&bm25_path).unwrap();
        for (row, payload) in original_documents.iter().enumerate() {
            assert_eq!(
                store.protobuf_source(row as u32).unwrap(),
                Some((
                    ProtobufSource {
                        descriptor_set: boundary_case_set(syntax),
                        message_type: "law.v1.Case".into(),
                        payload: payload.clone(),
                    },
                    None,
                )),
                "{syntax} row {row}"
            );
        }
        drop(store);

        let (reopened, handle) = start_opened_node(config).await;
        let mut reopened_hits = boundary_hits(&reopened, &analysis).await;
        reopened_hits.sort_unstable_by_key(|hit| hit.doc_id);
        assert_eq!(reopened_hits.len(), 2, "{syntax}");
        assert_eq!(
            reopened_hits
                .iter()
                .map(|hit| (hit.doc_id, hit.projected[0].value.clone()))
                .collect::<Vec<_>>(),
            expected_ids,
            "{syntax}"
        );
        assert_eq!(mapped_counts(&reopened).await, (2, 2), "{syntax}");
        handle.abort();
        let _ = handle.await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
