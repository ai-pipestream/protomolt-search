use super::*;

/// A fresh shard directory whose catalog carries the vector binding; returns
/// the index path inside it.
pub(super) fn bound_walless_catalog(label: &str) -> PathBuf {
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
        include_bytes!("../../../tests/fixtures/vector-binding/descriptor.bin"),
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
    index_path
}

/// Bound, WAL-less, segmented catalog served in-process (the shape produced for
/// re-placement split children).
pub(super) fn walless_fixture(label: &str, rows: usize) -> (PathBuf, NodeServiceImpl) {
    let index_path = bound_walless_catalog(label);
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
pub(super) fn served_texts(node: &NodeServiceImpl) -> Vec<String> {
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

/// Run the partitioned from-segments compaction on a blocking thread.
pub(super) async fn compact_by_num(node: &NodeServiceImpl) -> Result<CompactShardResponse, Status> {
    let worker = node.clone();
    tokio::task::spawn_blocking(move || {
        worker.compact_shard(&CompactShardRequest {
            partition_column: "num".into(),
            tail_bound: 16,
            ..Default::default()
        })
    })
    .await
    .unwrap()
}

/// Every file under the catalog root, by relative path.
pub(super) fn segment_tree_bytes(index_path: &Path) -> BTreeMap<String, Vec<u8>> {
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
}
