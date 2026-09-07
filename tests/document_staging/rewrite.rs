use super::*;
use pipestream_search::segments::OpenedSegmentSet;

pub(super) fn request(rows: u32) -> CompactDocumentIndexRequest {
    CompactDocumentIndexRequest {
        index_key: b"books-index".to_vec(),
        batch_rows: rows,
        batch_bytes: 1024 * 1024,
        max_staged_bytes: 32 * 1024 * 1024,
        proof_batch_rows: 2,
    }
}
pub(super) async fn publish_key(
    f: &Fixture,
    node: Arc<NodeServiceImpl>,
    key: &[u8],
    version: u64,
    rows: Option<usize>,
) -> DocumentProjectionActivation {
    let receipt = f
        .catalog
        .accept(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: key.to_vec(),
            operation_id: [key, &version.to_be_bytes()].concat(),
            expected_version: Some(version - 1),
            mutation: Some(
                rows.map_or(accept_document_request::Mutation::Delete(true), |rows| {
                    accept_document_request::Mutation::Source(source(rows, false))
                }),
            ),
            ..Default::default()
        })
        .unwrap();
    let mut stage = f.request(&receipt);
    stage.projection.as_mut().unwrap().document_key = key.to_vec();
    if rows.is_none() {
        stage.field_analysis.clear();
        stage
            .projection
            .as_mut()
            .unwrap()
            .expected_plan_fingerprint
            .clear();
    }
    let candidate = node
        .stage_document_projection(f.catalog.clone(), stage)
        .await
        .unwrap();
    publish_candidate(node, f.catalog.clone(), candidate)
        .await
        .unwrap()
}
pub(super) async fn compact(
    f: &Fixture,
    node: Arc<NodeServiceImpl>,
    request: CompactDocumentIndexRequest,
) -> Result<DocumentMaintenanceActivation, tonic::Status> {
    let source = f.catalog.clone();
    tokio::task::spawn_blocking(move || node.compact_document_index_blocking(&source, request))
        .await
        .unwrap()
}
pub(super) fn opened(f: &Fixture) -> OpenedSegmentSet {
    OpenedSegmentSet::open(segments_root(f.config.index_path.as_ref().unwrap())).unwrap()
}
fn scratch_count(f: &Fixture) -> usize {
    std::fs::read_dir(&f.root)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".document-rewrite-")
        })
        .count()
}

#[tokio::test]
async fn rebuild_merges_sources_then_reclaims_partial_segments_without_changing_identity() {
    let f = Fixture::new();
    let node = Arc::new(NodeServiceImpl::open(f.config.clone(), None, false).unwrap());
    publish_key(&f, node.clone(), b"a", 1, Some(3)).await;
    publish_key(&f, node.clone(), b"b", 1, Some(2)).await;
    let original = opened(&f);
    assert_eq!(original.len(), 2);
    let first = compact(&f, node.clone(), request(10)).await.unwrap();
    assert_eq!(first.live_rows, 5);
    let merged = opened(&f);
    assert_eq!(merged.len(), 1);
    original
        .verify_source_rewrite(&merged, &f.root.join("first-proof"), 2)
        .unwrap();
    publish_key(&f, node.clone(), b"a", 2, Some(1)).await;
    let partial = opened(&f);
    assert_eq!(partial.live_docs(0).deleted_count(), 3);
    let second = compact(&f, node.clone(), request(2)).await.unwrap();
    assert_eq!(second.live_rows, 3);
    let rebuilt = opened(&f);
    assert_eq!(rebuilt.len(), 2);
    assert_eq!(
        rebuilt
            .manifest()
            .segments
            .iter()
            .map(|s| s.rows)
            .sum::<u64>(),
        3
    );
    partial
        .verify_source_rewrite(&rebuilt, &f.root.join("second-proof"), 1)
        .unwrap();
    let identities: Vec<_> = (0..rebuilt.len())
        .flat_map(|i| (0..rebuilt.metadata(i).rows).map(move |row| (i, row)))
        .map(|(i, row)| rebuilt.bm25(i).document_identity(row as u32).unwrap())
        .collect();
    assert_eq!(
        identities
            .iter()
            .filter(|i| i.document_key == b"b" && i.version == 1)
            .count(),
        2
    );
    assert_eq!(
        identities
            .iter()
            .filter(|i| i.document_key == b"a" && i.version == 2)
            .count(),
        1
    );
    assert_eq!(scratch_count(&f), 0);
    publish_key(&f, node.clone(), b"b", 2, None).await;
    publish_key(&f, node.clone(), b"a", 3, None).await;
    assert_eq!(
        compact(&f, node.clone(), request(2))
            .await
            .unwrap()
            .live_rows,
        0
    );
    let empty = opened(&f);
    assert!(empty.is_empty());
    assert_eq!(
        empty.generation_declaration(),
        rebuilt.generation_declaration()
    );
    publish_key(&f, node.clone(), b"a", 4, Some(2)).await;
    assert_eq!(
        compact(&f, node.clone(), request(1))
            .await
            .unwrap()
            .live_rows,
        2
    );
    publish_key(&f, node.clone(), b"a", 5, Some(0)).await;
    assert_eq!(compact(&f, node, request(2)).await.unwrap().live_rows, 0);
    assert!(f.catalog.get(b"a", Some(5)).unwrap().unwrap().1.is_some());
}

#[tokio::test]
async fn rebuild_budget_failures_leave_source_and_serving_state_unchanged() {
    let f = Fixture::new();
    let node = Arc::new(NodeServiceImpl::open(f.config.clone(), None, false).unwrap());
    let active = publish_key(&f, node.clone(), KEY, 1, Some(3)).await;
    let manifest = f.manifest();
    let receipt = f.catalog.get(KEY, Some(1)).unwrap();
    let mut tiny = request(10);
    tiny.batch_bytes = 1;
    assert_eq!(
        compact(&f, node.clone(), tiny).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let mut disk = request(1);
    disk.max_staged_bytes = 1;
    assert_eq!(
        compact(&f, node.clone(), disk).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(f.manifest(), manifest);
    assert_eq!(f.catalog.get(KEY, Some(1)).unwrap(), receipt);
    assert!(node.ingest_fence().is_none());
    assert_eq!(scratch_count(&f), 0);
    assert_active_rows(&node, &active, &[0, 1, 2]).await;
    publish_key(&f, node.clone(), KEY, 2, Some(20)).await;
    let before = opened(&f);
    let mut adaptive = request(100);
    // Fits a small row but forces the declared input range to shrink.
    adaptive.batch_bytes = 64 * 1024;
    compact(&f, node, adaptive).await.unwrap();
    let after = opened(&f);
    assert!(after.len() > 1);
    before
        .verify_source_rewrite(&after, &f.root.join("adaptive-proof"), 2)
        .unwrap();
    assert_eq!(scratch_count(&f), 0);
}
