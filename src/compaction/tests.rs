use super::*;
use crate::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use crate::node::NodeConfig;
use crate::vector::EMBEDDED_TURBOVEC;
use prost::Message;
use std::sync::Arc;

#[tokio::test]
async fn a_reserved_cutover_yields_writers_without_blocking_reads() {
    use crate::pb::node_service_server::NodeService;
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let reservation = node.mutation_gate.clone().write_owned().await;
    let writer_node = node.clone();
    let (started, entered) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        started.send(()).unwrap();
        writer_node
            .delete_documents(tonic::Request::new(
                crate::pb::DeleteDocumentsRequest::default(),
            ))
            .await
    });
    entered.await.unwrap();
    tokio::task::yield_now().await;
    assert!(
        !writer.is_finished(),
        "write bypassed the cutover reservation"
    );
    node.health(tonic::Request::new(crate::pb::HealthRequest {}))
        .await
        .unwrap();
    drop(reservation);
    let deleted = writer.await.unwrap().unwrap().into_inner();
    assert_eq!(deleted.deleted, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cutover_analysis_allows_reads_and_includes_writes_during_preparation() {
    cutover_with_writes_during_analysis(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cutover_refuses_persistent_writes_without_replacing_the_live_generation() {
    cutover_with_writes_during_analysis(true).await;
}

async fn cutover_with_writes_during_analysis(always_advance: bool) {
    tokio::task::spawn_blocking(move || {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("psearch-cutover-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sample = crate::harness::unit_vectors(32, 64, 19);
        let config = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, 64, 4, &sample).unwrap();
        let index = VectorIndex::from_backend_config(64, &config).unwrap();
        let node = NodeServiceImpl::new(
            Some(index),
            NodeConfig {
                index_path: Some(dir.join("shard.vector")),
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                wal: true,
                ..Default::default()
            },
        );
        let handle = tokio::runtime::Handle::current();
        let spec = body_spec();
        let texts = ["seed", "first tail", "second tail", "late tail"];
        let analyzed = texts
            .iter()
            .map(|text| {
                handle
                    .block_on(crate::analyzer::analyze_document(
                        NATIVE_ANALYSIS_BACKEND,
                        text,
                        Some(&spec),
                    ))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let append = |i: usize| {
            let mut guard = node.state.write().unwrap();
            node.apply_document_locked(
                &mut guard,
                AddDocumentsRequest {
                    text: texts[i].into(),
                    analysis: Some(spec.clone()),
                    ..Default::default()
                },
                analyzed[i].clone(),
                Some(sample[..64].to_vec()),
                None,
                &mut 0,
                &mut 0,
            )
            .unwrap();
        };
        append(0);
        node.flush_index().unwrap();
        let preflight = node
            .preflight_at_row_boundary(&CompactShardRequest::default())
            .unwrap();
        let PreflightKind::Wal(pre) = preflight else {
            panic!("a WAL-backed shard preflights to the log path")
        };
        std::fs::create_dir_all(&pre.work_dir).unwrap();
        let mut calls = 0;
        let mut analyze = |docs: &[(
            &str,
            Option<&crate::pb::AnalysisSpec>,
            crate::analyzer::SessionLayers,
        )]| {
            assert!(
                node.state.try_read().is_ok(),
                "analysis ran while the live shard was write-locked"
            );
            calls += 1;
            // Build, catch-up, and the first cutover preparation each
            // receive another committed row. The final fence must retry.
            if always_advance || calls <= 3 {
                append(calls.min(3));
            }
            handle
                .block_on(crate::analyzer::analyze_batch_streams(
                    NATIVE_ANALYSIS_BACKEND,
                    docs,
                    2,
                ))
                .map_err(|error| error.to_string())
        };
        let result = node.build_and_cut_over(&pre, 256, &mut analyze);
        let expected = if always_advance {
            let error = result.unwrap_err();
            assert_eq!(error.code(), tonic::Code::ResourceExhausted);
            assert!(error.message().contains("writes outpace compaction"));
            assert_eq!(calls, 2 + CUTOVER_RETRIES);
            let live = node.state.read().unwrap();
            assert_eq!(live.wal.as_ref().unwrap().generation(), 0);
            assert!(!marker_path(&pre.index_path).exists());
            (0..=calls).map(|i| texts[i.min(3)]).collect::<Vec<_>>()
        } else {
            let response = result.unwrap();
            assert_eq!(calls, 4);
            assert_eq!(response.rows_after, 4);
            assert_eq!(response.locked_tail_records, 0);
            texts.to_vec()
        };
        let fetched = handle
            .block_on(crate::pb::node_service_server::NodeService::get_documents(
                &node,
                tonic::Request::new(crate::pb::GetDocumentsRequest {
                    doc_ids: (0..expected.len() as u64).collect(),
                }),
            ))
            .unwrap()
            .into_inner();
        let actual = fetched
            .documents
            .into_iter()
            .map(|doc| doc.text)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        drop(node);
        std::fs::remove_dir_all(dir).unwrap();
    })
    .await
    .unwrap();
}

/// A bound, WAL-less, segmented catalog served in-process: the shape
/// the children of a re-placement split come in
/// (docs/replay-from-segments.md).
fn walless_fixture(label: &str, rows: usize) -> (PathBuf, NodeServiceImpl) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "psearch-walless-{label}-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");
    let plan = crate::mapping::derive_plan(
        include_bytes!("../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    let binding = crate::postings::StoredBinding {
        plan_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    };
    SegmentCatalog::open(crate::node::segments_root(&index_path))
        .unwrap()
        .publish_binding(&binding)
        .unwrap();
    let node = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path.clone()),
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            wal: false,
            layout: crate::node::Layout::Segments,
            integer_fields: vec!["num".into()],
            ..Default::default()
        },
        None,
        false,
    )
    .unwrap();
    let sample = crate::harness::unit_vectors(rows.max(64), 64, 7);
    let config =
        VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, 64, 4, &sample[..64 * 64]).unwrap();
    {
        let mut guard = crate::node::write_shard(&node.state);
        let index = VectorIndex::from_backend_config(64, &config).unwrap();
        guard.index = Some(NodeServiceImpl::adopt_layout(guard.bm25.as_ref(), index).unwrap());
    }
    let spec = body_spec();
    for i in 0..rows {
        let text = format!("row{i} common {}", ["red", "green", "blue"][i % 3]);
        let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
        let mut guard = crate::node::write_shard(&node.state);
        node.apply_document_locked(
            &mut guard,
            AddDocumentsRequest {
                text,
                analysis: Some(spec.clone()),
                integers: vec![crate::pb::IntegerValue {
                    field: "num".into(),
                    value: (i / 3) as i64,
                }],
                position_fields: Vec::new(),
                ..Default::default()
            },
            analyzed,
            Some(sample[i * 64..(i + 1) * 64].to_vec()),
            None,
            &mut 0,
            &mut 0,
        )
        .unwrap();
    }
    node.flush_index().unwrap();
    (index_path, node)
}

/// The texts the shard serves, in label order.
fn served_texts(node: &NodeServiceImpl) -> Vec<String> {
    let guard = crate::node::read_shard(&node.state);
    let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
        panic!("a segmented fixture")
    };
    let rows = crate::node::physical_rows(&guard);
    (0..rows)
        .filter(|&row| !guard.live_docs.is_deleted(row as usize))
        .map(|row| {
            shard
                .text(row as u32)
                .unwrap_or_else(|| panic!("row {row} has no text"))
        })
        .collect()
}

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
            AddDocumentsRequest {
                text,
                analysis: Some(spec),
                integers: vec![crate::pb::IntegerValue {
                    field: "num".into(),
                    value: 10_000,
                }],
                ..Default::default()
            },
            analyzed,
            Some(crate::harness::unit_vectors(1, 64, 99)),
            None,
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
    let worker = node.clone();
    let response = tokio::task::spawn_blocking(move || {
        worker.compact_shard(&CompactShardRequest {
            partition_column: "num".into(),
            tail_bound: 16,
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap();
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
    let worker = node.clone();
    let status = tokio::task::spawn_blocking(move || {
        worker.compact_shard(&CompactShardRequest {
            partition_column: "num".into(),
            tail_bound: 16,
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap_err();
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
    let worker = node.clone();
    let response = tokio::task::spawn_blocking(move || {
        worker.compact_shard(&CompactShardRequest {
            partition_column: "num".into(),
            tail_bound: 16,
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.rows_before, 120);
    assert_eq!(response.rows_after, 120);
    assert_eq!(served_texts(&node).len(), 120);
    let (clean_path, clean_node) = walless_fixture("uninterrupted", 120);
    let clean = clean_node.clone();
    let clean_response = tokio::task::spawn_blocking(move || {
        clean.compact_shard(&CompactShardRequest {
            partition_column: "num".into(),
            tail_bound: 16,
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(clean_response.rows_after, 120);
    let bytes_of = |index_path: &Path| {
        let root = crate::node::segments_root(index_path);
        let mut out = BTreeMap::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    stack.push(entry.path());
                } else {
                    let relative = entry
                        .path()
                        .strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    out.insert(relative, std::fs::read(entry.path()).unwrap());
                }
            }
        }
        out
    };
    assert_eq!(bytes_of(&index_path), bytes_of(&clean_path));
    drop(node);
    drop(clean_node);
    std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
    std::fs::remove_dir_all(clean_path.parent().unwrap()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn walless_compaction_under_a_changed_declaration_refuses_naming_both() {
    use crate::pb::{DerivedColumn, DerivedColumns, DerivedDisclosure, MaterializeKind};
    let declaration = |name: &str| {
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
    };
    let (index_path, mut node) = {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "psearch-walless-derived-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let index_path = dir.join("shard.tv");
        let plan = crate::mapping::derive_plan(
            include_bytes!("../../tests/fixtures/vector-binding/descriptor.bin"),
            "vector_binding.Named",
        )
        .unwrap();
        let binding = crate::postings::StoredBinding {
            plan_fingerprint: plan.fingerprint,
            body_path: "body".into(),
            vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
            ..Default::default()
        };
        SegmentCatalog::open(crate::node::segments_root(&index_path))
            .unwrap()
            .publish_binding(&binding)
            .unwrap();
        let node = NodeServiceImpl::open(
            NodeConfig {
                index_path: Some(index_path.clone()),
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                wal: false,
                layout: crate::node::Layout::Segments,
                integer_fields: vec!["num".into()],
                derived: Some(declaration("num_d")),
                ..Default::default()
            },
            None,
            false,
        )
        .unwrap();
        (index_path, node)
    };
    let spec = body_spec();
    for i in 0..30 {
        let text = format!("row{i} common");
        let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
        let mut guard = crate::node::write_shard(&node.state);
        node.apply_document_locked(
            &mut guard,
            AddDocumentsRequest {
                text,
                analysis: Some(spec.clone()),
                integers: vec![crate::pb::IntegerValue {
                    field: "num".into(),
                    value: i as i64,
                }],
                ..Default::default()
            },
            analyzed,
            None,
            None,
            &mut 0,
            &mut 0,
        )
        .unwrap();
    }
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
