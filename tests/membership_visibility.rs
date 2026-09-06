mod common;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::*;
use pipestream_search::visibility::VisibilityScope;
use std::collections::BTreeSet;
use tonic::transport::Channel;

fn view(predicate: &str) -> DocumentVisibility {
    DocumentVisibility {
        filter: pipestream_search::cel::compile_filter(predicate).unwrap(),
    }
}
fn ids(base: u64, count: u64, bits: &[u8]) -> BTreeSet<u64> {
    assert_eq!(bits.len(), count.div_ceil(8) as usize);
    (0..count)
        .filter(|i| bits[*i as usize / 8] & (1 << (*i % 8)) != 0)
        .map(|i| base + i)
        .collect()
}
async fn seed(client: &mut NodeServiceClient<Channel>, rows: usize) {
    let vectors = common::unit_vectors(rows, 16, 9916);
    let (shift, scale) = common::fit_calibration(16, 4, &vectors);
    client
        .set_calibration(SetCalibrationRequest {
            dim: 16,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    for (i, vector) in vectors.chunks(16).enumerate() {
        client
            .add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: "alpha".into(),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "audience".into(),
                    value: if i % 2 == 0 { "public" } else { "private" }.into(),
                }],
                ..Default::default()
            }]))
            .await
            .unwrap();
        client
            .add_vectors(tokio_stream::iter([AddVectorsRequest {
                dim: 16,
                vectors: vector.to_vec(),
            }]))
            .await
            .unwrap();
    }
}
async fn membership(
    client: &mut NodeServiceClient<Channel>,
    visibility: Option<DocumentVisibility>,
    filter: Option<FilterExpr>,
) -> (BTreeSet<u64>, MembershipBitmapResponse) {
    let scope = VisibilityScope::new(visibility.as_ref()).unwrap();
    let filter_response = client
        .resolve_filter_bitmap(FilterBitmapRequest {
            visibility: visibility.clone(),
            filter,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    scope
        .validate_echo(
            &filter_response.visibility_fingerprint,
            &filter_response.visibility_columns_known,
        )
        .unwrap();
    assert!(filter_response.stats_epoch > 0);
    assert_eq!(filter_response.stats_incarnation.len(), 32);
    let lexical = client
        .resolve_lexical_bitmap(LexicalBitmapRequest {
            terms: vec!["alpha".into()],
            analysis_fingerprint: pipestream_search::analyzer::analysis_fingerprint(Some(
                &body_spec(),
            )),
            visibility: visibility.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let vector = client
        .resolve_vector_bitmap(VectorBitmapRequest { visibility })
        .await
        .unwrap()
        .into_inner();
    for response in [&lexical, &vector] {
        scope
            .validate_echo(
                &response.visibility_fingerprint,
                &response.visibility_columns_known,
            )
            .unwrap();
        assert_eq!(response.stats_epoch, filter_response.stats_epoch);
        assert_eq!(
            response.stats_incarnation,
            filter_response.stats_incarnation
        );
    }
    let lexical_ids = ids(lexical.base_label, lexical.label_count, &lexical.bits);
    assert_eq!(
        lexical_ids,
        ids(vector.base_label, vector.label_count, &vector.bits)
    );
    (
        ids(
            filter_response.base_label,
            filter_response.label_count,
            &filter_response.bits,
        ),
        lexical,
    )
}
fn config() -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        facet_fields: vec!["audience".into()],
        ..Default::default()
    }
}

#[tokio::test]
async fn membership_intersects_user_predicates_and_keeps_views_separate() {
    let (addr, server) = common::start_empty_node(config()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    seed(&mut client, 4).await;
    for (predicate, expected) in [
        ("audience == 'public'", vec![0, 2]),
        ("audience == 'private'", vec![1, 3]),
        ("audience == 'nobody'", vec![]),
    ] {
        let (all, lexical) = membership(&mut client, Some(view(predicate)), None).await;
        assert_eq!(all, expected.into_iter().collect());
        assert_eq!(
            all,
            ids(lexical.base_label, lexical.label_count, &lexical.bits)
        );
    }
    let (all, _) = membership(&mut client, None, None).await;
    assert_eq!(all, [0, 1, 2, 3].into_iter().collect());
    for user in ["audience == 'private'", "!(audience == 'public')"] {
        let (filtered, _) = membership(
            &mut client,
            Some(view("audience == 'public'")),
            pipestream_search::cel::compile_filter(user).unwrap(),
        )
        .await;
        assert!(filtered.is_empty());
    }
    let (filtered, _) = membership(
        &mut client,
        Some(view("audience == 'public'")),
        pipestream_search::cel::compile_filter("audience == 'private' || audience == 'public'")
            .unwrap(),
    )
    .await;
    assert_eq!(filtered, [0, 2].into_iter().collect());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn scoped_membership_survives_delete_compaction_and_reopen() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("membership-{layout:?}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = NodeConfig {
            index_path: Some(dir.join("shard.tv")),
            layout,
            seal_tail_docs: 2,
            wal: true,
            ..config()
        };
        let (addr, server) = common::start_empty_node(cfg.clone()).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        seed(&mut client, 4).await;
        if layout == Layout::SingleImage {
            client.flush(FlushRequest {}).await.unwrap();
        }
        let (initial, before) =
            membership(&mut client, Some(view("audience == 'public'")), None).await;
        assert_eq!(initial, [0, 2].into_iter().collect());
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![0],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        let (deleted, after) =
            membership(&mut client, Some(view("audience == 'public'")), None).await;
        assert_eq!(deleted, [2].into_iter().collect());
        assert!(after.stats_epoch > before.stats_epoch);
        client.flush(FlushRequest {}).await.unwrap();
        client
            .compact_shard(CompactShardRequest::default())
            .await
            .unwrap();
        let (compacted, compact) =
            membership(&mut client, Some(view("audience == 'public'")), None).await;
        assert_eq!(compacted.len(), 1);
        assert_eq!(
            compacted,
            ids(compact.base_label, compact.label_count, &compact.bits)
        );
        assert!(compact.stats_epoch > after.stats_epoch);
        client.flush(FlushRequest {}).await.unwrap();
        drop(client);
        server.abort();
        let _ = server.await;
        let (addr, server) = common::start_opened_node(cfg).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (reopened, current) =
            membership(&mut client, Some(view("audience == 'public'")), None).await;
        assert_eq!(reopened, compacted);
        assert_ne!(current.stats_incarnation, compact.stats_incarnation);
        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn vector_only_rows_have_no_authorized_document_membership() {
    let (addr, server) = common::start_empty_node(config()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let vectors = common::unit_vectors(3, 16, 9917);
    let (shift, scale) = common::fit_calibration(16, 4, &vectors);
    client
        .set_calibration(SetCalibrationRequest {
            dim: 16,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    client
        .add_vectors(tokio_stream::iter([AddVectorsRequest { dim: 16, vectors }]))
        .await
        .unwrap();
    let all = client
        .resolve_vector_bitmap(VectorBitmapRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(all.base_label, all.label_count, &all.bits).len(), 3);
    let restricted = client
        .resolve_vector_bitmap(VectorBitmapRequest {
            visibility: Some(view("!has(audience)")),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ids(
        restricted.base_label,
        restricted.label_count,
        &restricted.bits
    )
    .is_empty());
    assert_eq!(restricted.stats_epoch, all.stats_epoch);
    assert_eq!(restricted.stats_incarnation, all.stats_incarnation);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn malformed_views_refuse_on_all_membership_routes_even_when_empty() {
    let (addr, server) = common::start_empty_node(config()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    for visibility in [
        DocumentVisibility::default(),
        DocumentVisibility {
            filter: Some(FilterExpr::default()),
        },
    ] {
        assert_eq!(
            client
                .resolve_filter_bitmap(FilterBitmapRequest {
                    visibility: Some(visibility.clone()),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            client
                .resolve_lexical_bitmap(LexicalBitmapRequest {
                    visibility: Some(visibility.clone()),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            client
                .resolve_vector_bitmap(VectorBitmapRequest {
                    visibility: Some(visibility)
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    let (filtered, lexical) =
        membership(&mut client, Some(view("missing == 'public'")), None).await;
    assert!(filtered.is_empty());
    assert_eq!(lexical.visibility_columns_known, vec![false]);
    server.abort();
    let _ = server.await;
}
