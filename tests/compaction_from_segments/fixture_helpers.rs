use super::*;

/// The on-disk state a crash leaves around a walless cutover publish.
pub(super) struct RollbackFixture {
    pub(super) dir: super::scratch::ScratchPath,
    pub(super) index_path: PathBuf,
    pub(super) root: PathBuf,
    pub(super) manifest: SegmentSetManifest,
    pub(super) manifest_bytes: Vec<u8>,
    pub(super) backup_path: PathBuf,
    pub(super) marker_path: PathBuf,
    pub(super) staged_dir: PathBuf,
}

pub(super) fn rollback_fixture() -> RollbackFixture {
    let dir = scratch_path(&format!("walless-rollback-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");
    let root = segments_root(&index_path);
    let catalog = SegmentCatalog::open(&root).unwrap();
    // One real segment to stand for the serving set.
    let work = dir.join("work-segment");
    std::fs::create_dir_all(&work).unwrap();
    let bm25_path = work.join("documents.bm25");
    let live_path = work.join("live.bin");
    let mut store = pipestream_search::postings::Bm25Store::with_text_fields(&["body"]);
    store.add_document(
        0,
        "document".into(),
        pipestream_search::analyzer::analyze_document_native("document", Some(&body_spec()))
            .unwrap(),
    );
    store.save(&bm25_path).unwrap();
    LiveDocs::default().write(&live_path, 1).unwrap();
    catalog
        .append(pipestream_search::segments::SegmentSource {
            multi_res_path: None,
            segment_id: "seg-00000001",
            generation: 1,
            base_label: 0,
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
            parents_colocated: false,
        })
        .unwrap();
    let manifest = catalog.snapshot().manifest().clone();
    let manifest_bytes = std::fs::read(SegmentCatalog::manifest_path(&root)).unwrap();
    let backup_path = {
        let mut name = SegmentCatalog::manifest_path(&root).as_os_str().to_owned();
        name.push(".pre-compact");
        PathBuf::from(name)
    };
    let marker_path = pipestream_search::compaction::marker_path(&index_path);
    let staged_dir = SegmentCatalog::segment_dir(&root, "cmp-000007-0000");
    RollbackFixture {
        dir,
        index_path,
        root,
        manifest,
        manifest_bytes,
        backup_path,
        marker_path,
        staged_dir,
    }
}

/// Write the interrupted-cutover marker naming the staged and replaced
/// segment ids.
pub(super) fn write_rollback_marker(fixture: &RollbackFixture) {
    std::fs::write(
        &fixture.marker_path,
        serde_json::json!({
            "format": 1,
            "layout": "segments",
            "old_wal_generation": 0,
            "new_wal_generation": 0,
            "walless": true,
            "work_dir": fixture.dir.join("work"),
            "previous_snapshot": false,
            "legacy_files": [],
            "staged_segments": ["cmp-000007-0000"],
            "replaced_segments": ["seg-00000001"],
        })
        .to_string(),
    )
    .unwrap();
}

/// Append one 3-row lattice source segment (positions, integer column,
/// per-document sparse scales, row 1 tombstoned) to the catalog.
pub(super) fn append_lattice_source(
    catalog: &SegmentCatalog,
    dir: &std::path::Path,
    binding: &StoredBinding,
    backend: &pipestream_search::vector::VectorBackendConfig,
    segment: usize,
) {
    use pipestream_search::exact_vectors::ExactVectorStore;
    use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};
    use pipestream_search::segments::SegmentSource;
    use pipestream_search::vector::VectorIndex;
    let source = dir.join(format!("source-{segment}"));
    std::fs::create_dir(&source).unwrap();
    let mut store = Bm25Store::with_text_fields(&["body"])
        .with_positions(&["body"])
        .with_columns(pipestream_search::postings::ColumnKind::Integer, &["num"]);
    store.set_binding(Some(binding.clone()));
    let mut vectors = Vec::new();
    let mut live = LiveDocs::default();
    for row in 0..3 {
        let id = segment * 3 + row;
        store.add_document(
            row as u32,
            format!("row{id}"),
            AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![
                        ("joined".into(), 1, vec![(0, 3)]),
                        ("word".into(), 1, vec![(4, 8)]),
                    ],
                    length: 3,
                    positions: Some(vec![vec![0], vec![2]]),
                    pos_lengths: Some(vec![vec![2], vec![1]]),
                    splade: vec![("neural".into(), 65535)],
                    splade_scale_bits: (0.00001_f32 * (id + 1) as f32).to_bits(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        store.set_integer(0, row as u32, (5 - id) as i64);
        vectors.extend((0..8).map(|d| (id as f32 + d as f32) / 8.0));
        if id == 1 {
            live.delete(row);
        }
    }
    let bp = source.join("bm25");
    store.save(&bp).unwrap();
    let vp = source.join("vector");
    let mut vector = VectorIndex::from_backend_config(8, backend).unwrap();
    vector.add(&vectors, 8).unwrap();
    vector.prepare().unwrap();
    vector.write(&vp).unwrap();
    let ep = source.join("exact");
    ExactVectorStore::from_values(8, vectors)
        .unwrap()
        .write(&ep)
        .unwrap();
    let lp = source.join("live");
    live.write(&lp, 3).unwrap();
    catalog
        .append(SegmentSource {
            multi_res_path: None,
            segment_id: &format!("source-{segment}"),
            generation: 1,
            base_label: (segment * 3) as u64,
            backend_kind: pipestream_search::vector::EMBEDDED_TURBOVEC,
            vector_path: Some(&vp),
            exact_vector_path: Some(&ep),
            bm25_path: &bp,
            live_docs_path: &lp,
            partition_column: None,
            parents_colocated: false,
        })
        .unwrap();
}
