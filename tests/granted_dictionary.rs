mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    node::{Layout, NodeConfig},
    pb::{node_service_client::NodeServiceClient, *},
    visibility::VisibilityScope,
};
use tonic::transport::Channel;

fn visibility(column: &str) -> DocumentVisibility {
    DocumentVisibility {
        filter: Some(FilterExpr {
            expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                column: column.into(),
                values: vec!["public".into()],
            })),
        }),
    }
}

async fn scan(
    client: &mut NodeServiceClient<Channel>,
    field: &str,
    cap: u64,
) -> SuggestTermsResponse {
    let response = client
        .suggest_terms(SuggestTermsRequest {
            field: field.into(),
            prefix: "a".into(),
            max_scan: cap,
            visibility: Some(visibility("audience")),
        })
        .await
        .unwrap()
        .into_inner();
    VisibilityScope::new(Some(&visibility("audience")))
        .unwrap()
        .validate_echo(
            &response.visibility_fingerprint,
            &response.visibility_columns_known,
        )
        .unwrap();
    assert_eq!(response.visibility_columns_known, vec![true]);
    assert_eq!(response.tombstoned_rows, 0);
    response
}

#[tokio::test]
async fn visible_dictionaries_survive_segments_deletes_compaction_and_reopen() {
    let rows = [
        ("alpha amber", "public"),
        ("alpha algebra alien secret", "private"),
        ("alpha azure", "public"),
        ("alpha", "public"),
        ("alpha", "public"),
    ];
    const DIM: usize = 16;
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "granted-dictionary-{layout:?}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = NodeConfig {
            index_path: Some(dir.join("shard.tv")),
            layout,
            seal_tail_docs: 2,
            wal: true,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            bm25_fields: vec!["body".into(), "title".into()],
            facet_fields: vec!["audience".into()],
            ..Default::default()
        };
        let vectors = common::unit_vectors(rows.len(), DIM, 9036);
        let (shift, scale) = common::fit_calibration(DIM, 4, &vectors);
        let (addr, handle) = common::start_empty_node(cfg.clone()).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift,
                scale,
            })
            .await
            .unwrap();
        for (i, batch) in rows.chunks(2).enumerate() {
            client
                .add_documents(tokio_stream::iter(
                    batch
                        .iter()
                        .map(|(text, audience)| AddDocumentsRequest {
                            text: (*text).into(),
                            analysis: Some(body_spec()),
                            fields: vec![DocumentField {
                                field: "title".into(),
                                text: (*text).into(),
                                analysis: Some(body_spec()),
                            }],
                            facets: vec![FacetValue {
                                field: "audience".into(),
                                value: (*audience).into(),
                            }],
                            ..Default::default()
                        })
                        .collect::<Vec<_>>(),
                ))
                .await
                .unwrap();
            client
                .add_vectors(tokio_stream::iter([AddVectorsRequest {
                    vectors: vectors[i * 2 * DIM..(i * 2 + batch.len()) * DIM].to_vec(),
                    dim: DIM as u32,
                }]))
                .await
                .unwrap();
        }
        if matches!(layout, Layout::SingleImage) {
            // A single-image bulk builder becomes searchable only at Flush.
            client.flush(FlushRequest {}).await.unwrap();
        }
        // The segmented case still includes sealed parts and a live tail.
        for field in ["body", "title"] {
            let full = scan(&mut client, field, 3).await;
            assert_eq!(
                full.entries
                    .iter()
                    .map(|e| (e.term.as_str(), e.df))
                    .collect::<Vec<_>>(),
                vec![("alpha", 4), ("amber", 1), ("azur", 1)]
            );
            let over = scan(&mut client, field, 2).await;
            assert_eq!(over.count, 3);
            assert!(over.entries.is_empty());
            let prefix = client
                .expand_term_prefix(ExpandTermPrefixRequest {
                    field: field.into(),
                    prefix: "a".into(),
                    cap: 2,
                    visibility: Some(visibility("audience")),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(prefix.count, 3);
            assert!(prefix.terms.is_empty());
            VisibilityScope::new(Some(&visibility("audience")))
                .unwrap()
                .validate_echo(
                    &prefix.visibility_fingerprint,
                    &prefix.visibility_columns_known,
                )
                .unwrap();
        }
        client.flush(FlushRequest {}).await.unwrap();
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![0],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        let after = scan(&mut client, "body", 3).await;
        assert_eq!(
            after
                .entries
                .iter()
                .map(|e| (e.term.as_str(), e.df))
                .collect::<Vec<_>>(),
            vec![("alpha", 3), ("azur", 1)]
        );
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![1],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        assert_eq!(scan(&mut client, "body", 3).await, after);
        client.flush(FlushRequest {}).await.unwrap();
        assert_eq!(scan(&mut client, "body", 3).await, after);
        let compacted = client
            .compact_shard(CompactShardRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(compacted.tombstones_reclaimed, 2);
        assert_eq!(scan(&mut client, "body", 3).await, after);
        client.flush(FlushRequest {}).await.unwrap();
        drop(client);
        handle.abort();
        let _ = handle.await;
        let (addr, handle) = common::start_opened_node(cfg).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        assert_eq!(scan(&mut client, "body", 3).await, after);
        assert_eq!(scan(&mut client, "title", 3).await, after);
        let unknown = scan(&mut client, "not_indexed", 3).await;
        assert!(!unknown.known);
        assert!(unknown.entries.is_empty());
        let malformed = client
            .suggest_terms(SuggestTermsRequest {
                field: "body".into(),
                prefix: "a".into(),
                max_scan: 3,
                visibility: Some(DocumentVisibility::default()),
            })
            .await
            .unwrap_err();
        assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
        let missing = client
            .suggest_terms(SuggestTermsRequest {
                field: "body".into(),
                prefix: "a".into(),
                max_scan: 3,
                visibility: Some(visibility("absent_grant_column")),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(missing.visibility_columns_known, vec![false]);
        assert!(missing.entries.is_empty());
        handle.abort();
        let _ = handle.await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
