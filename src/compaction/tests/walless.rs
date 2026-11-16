use super::walless_fixture::{
    bound_walless_catalog, compact_by_num, segment_tree_bytes, served_texts, walless_fixture,
};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn walless_compaction_catches_up_writes_and_deletes_made_after_the_build() {
    let (index_path, node) = walless_fixture("tail", 120);
    // A tombstone the build must reclaim.
    {
        let mut guard = crate::node::write_shard(&node.state);
        node.delete_documents_locked(&mut guard, &[3], None)
            .unwrap();
    }
    // After the outputs are staged and before the tail catch-up, one
    // more row lands in the tail and one sealed row is deleted. The
    // cutover must carry the row and the tombstone.
    segment_staging_hook::install(index_path.clone(), |node| {
        let spec = body_spec();
        let text = "late arrival common".to_string();
        let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
        let mut guard = crate::node::write_shard(&node.state);
        node.apply_document_locked(
            &mut guard,
            crate::node::MaterializedDocument {
                doc: AddDocumentsRequest {
                    text,
                    analysis: Some(spec),
                    integers: vec![crate::pb::IntegerValue {
                        field: "num".into(),
                        value: 10_000,
                    }],
                    ..Default::default()
                },
                analyzed,
                vector: Some(crate::harness::unit_vectors(1, 64, 99)),
                stable_routing_key: None,
            },
            &mut 0,
            &mut 0,
        )
        .map_err(|status| status.message().to_string())?;
        let deleted = node
            .delete_documents_locked(&mut guard, &[5], None)
            .map_err(|status| status.message().to_string())?;
        assert_eq!(deleted.deleted, 1);
        Ok(())
    });
    let response = compact_by_num(&node).await.unwrap();
    assert_eq!(response.rows_before, 120);
    assert_eq!(response.tombstones_reclaimed, 1);
    assert_eq!(
        response.tail_records_applied, 1,
        "the late row was caught up"
    );
    // The dense partitions plus the caught-up row; the tombstone the
    // hook wrote is a physical row the overlay marks.
    assert_eq!(response.rows_after, 120);
    let texts = served_texts(&node);
    assert_eq!(texts.len(), 119);
    assert_eq!(
        texts.iter().filter(|t| *t == "late arrival common").count(),
        1,
        "the late row appears exactly once"
    );
    for gone in ["row3 common red", "row5 common blue"] {
        assert!(!texts.iter().any(|t| t == gone), "{gone} is tombstoned");
    }
    assert_eq!(texts.iter().filter(|t| *t == "row0 common red").count(), 1);
    drop(node);
    std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_walless_build_leaves_the_serving_catalog_untouched() {
    let (index_path, node) = walless_fixture("interrupted", 120);
    let manifest_before = std::fs::read(SegmentCatalog::manifest_path(
        &crate::node::segments_root(&index_path),
    ))
    .unwrap();
    segment_staging_hook::install(index_path.clone(), |_| Err("injected kill".to_string()));
    let status = compact_by_num(&node).await.unwrap_err();
    assert!(status.message().contains("injected kill"), "{status}");
    // The serving catalog is untouched, and no staged output survived.
    assert_eq!(served_texts(&node).len(), 120);
    assert_eq!(
        std::fs::read(SegmentCatalog::manifest_path(&crate::node::segments_root(
            &index_path
        )))
        .unwrap(),
        manifest_before
    );
    let segments_dir = crate::node::segments_root(&index_path).join("segments");
    assert!(
        !std::fs::read_dir(&segments_dir).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("cmp-")),
        "the failed build's staging was removed"
    );
    // A retry from clean staging succeeds, serves the same rows, and
    // writes byte-identical outputs to an uninterrupted run's.
    std::fs::remove_dir_all(default_work_dir(&index_path)).unwrap();
    let response = compact_by_num(&node).await.unwrap();
    assert_eq!(response.rows_before, 120);
    assert_eq!(response.rows_after, 120);
    assert_eq!(served_texts(&node).len(), 120);
    let (clean_path, clean_node) = walless_fixture("uninterrupted", 120);
    let clean_response = compact_by_num(&clean_node).await.unwrap();
    assert_eq!(clean_response.rows_after, 120);
    assert_eq!(
        segment_tree_bytes(&index_path),
        segment_tree_bytes(&clean_path)
    );
    drop(node);
    drop(clean_node);
    std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
    std::fs::remove_dir_all(clean_path.parent().unwrap()).unwrap();
}

fn derived_declaration(name: &str) -> Arc<crate::derived::Declaration> {
    use crate::pb::{DerivedColumn, DerivedColumns, DerivedDisclosure, MaterializeKind};
    Arc::new(
        crate::derived::Declaration::compile(&DerivedColumns {
            columns: vec![DerivedColumn {
                name: name.into(),
                expression: "num + 1".into(),
                kind: MaterializeKind::I64 as i32,
                disclosure: DerivedDisclosure::Inputs as i32,
            }],
        })
        .unwrap(),
    )
}

/// Thirty document-only rows numbered by their integer column.
fn append_numbered_rows(node: &NodeServiceImpl) {
    let spec = body_spec();
    for i in 0..30 {
        let text = format!("row{i} common");
        let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
        let mut guard = crate::node::write_shard(&node.state);
        node.apply_document_locked(
            &mut guard,
            crate::node::MaterializedDocument {
                doc: AddDocumentsRequest {
                    text,
                    analysis: Some(spec.clone()),
                    integers: vec![crate::pb::IntegerValue {
                        field: "num".into(),
                        value: i as i64,
                    }],
                    ..Default::default()
                },
                analyzed,
                vector: None,
                stable_routing_key: None,
            },
            &mut 0,
            &mut 0,
        )
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn walless_compaction_under_a_changed_declaration_refuses_naming_both() {
    let declaration = derived_declaration;
    let index_path = bound_walless_catalog("derived");
    let mut node = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path.clone()),
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            wal: false,
            integer_fields: vec!["num".into()],
            derived: Some(declaration("num_d")),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    append_numbered_rows(&node);
    node.flush_index().unwrap();
    // The node now declares a changed declaration; the catalog's rows
    // carry the old one's values. The refusal names both.
    node.config.derived = Some(declaration("num_e"));
    let status = node
        .compact_shard(&CompactShardRequest {
            partition_column: "num".into(),
            tail_bound: 16,
            ..Default::default()
        })
        .unwrap_err();
    let message = status.message();
    assert!(
        message.contains("was written under derived-column declaration"),
        "{message}"
    );
    assert!(
        message.contains(&declaration("num_d").fingerprint().to_string()),
        "{message}"
    );
    assert!(
        message.contains(&declaration("num_e").fingerprint().to_string()),
        "{message}"
    );
    drop(node);
    std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
}
