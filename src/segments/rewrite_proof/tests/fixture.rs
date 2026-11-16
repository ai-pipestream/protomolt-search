use super::*;
use crate::vector::VectorBackendConfig;

pub(super) struct Fixture(pub(super) PathBuf, pub(super) usize);

/// Whether a row carries the one changed value a set was built with: only
/// document 1 changes, and only in the named family.
#[derive(Clone, Copy)]
struct Variant<'a> {
    change: &'a str,
    mutate: bool,
}

impl Variant<'_> {
    fn is(self, name: &str) -> bool {
        self.mutate && self.change == name
    }
}

/// The rows of one segment under construction.
struct Rows {
    store: Bm25Store,
    vectors: Vec<f32>,
    exact_vectors: Vec<f32>,
    live: LiveDocs,
    count: usize,
}

impl Fixture {
    pub(super) fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "rewrite-proof-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root, 8)
    }

    pub(super) fn set(
        &self,
        name: &str,
        ids: &[u32],
        deleted: &[usize],
        change: &str,
        splits: usize,
    ) -> OpenedSegmentSet {
        self.set_with_quantized_rows(name, ids, deleted, change, splits, false)
    }

    pub(super) fn set_with_quantized_rows(
        &self,
        name: &str,
        ids: &[u32],
        deleted: &[usize],
        change: &str,
        splits: usize,
        perturb_quantized: bool,
    ) -> OpenedSegmentSet {
        let root = self.0.join(name);
        std::fs::create_dir(&root).unwrap();
        let catalog = SegmentCatalog::open(&root).unwrap();
        let dim = self.1;
        let sample: Vec<f32> = (0..8 * dim).map(|i| (i as f32 * 0.3).sin()).collect();
        let backend =
            VectorIndex::fit_backend_config(crate::vector::EMBEDDED_TURBOVEC, dim, 4, &sample)
                .unwrap();
        let mut base = 0;
        for (segment, ids) in ids.chunks(splits).enumerate() {
            let dir = self.0.join(format!("{name}-part-{segment}"));
            std::fs::create_dir(&dir).unwrap();
            let mut rows = Rows {
                store: declared_store(change),
                vectors: Vec::new(),
                exact_vectors: Vec::new(),
                live: LiveDocs::default(),
                count: ids.len(),
            };
            for (row, &id) in ids.iter().enumerate() {
                let variant = Variant {
                    change,
                    mutate: id == 1,
                };
                add_row(&mut rows.store, row as u32, id, variant);
                push_vectors(&mut rows, id, dim, variant, perturb_quantized);
                if deleted.contains(&(base + row)) {
                    rows.live.delete(row);
                }
            }
            self.append_segment(&catalog, &backend, &dir, rows, (segment, base));
            base += ids.len();
        }
        owned_set(&catalog, root)
    }

    /// Write one segment's images under `dir` and append them at `base`.
    fn append_segment(
        &self,
        catalog: &SegmentCatalog,
        backend: &VectorBackendConfig,
        dir: &Path,
        rows: Rows,
        (segment, base): (usize, usize),
    ) {
        let dim = self.1;
        let mut vector = VectorIndex::from_backend_config(dim, backend).unwrap();
        vector.add(&rows.vectors, dim).unwrap();
        vector.prepare().unwrap();
        let vp = dir.join("vector");
        vector.write(&vp).unwrap();
        let ep = dir.join("exact");
        ExactVectorStore::from_values(dim, rows.exact_vectors)
            .unwrap()
            .write(&ep)
            .unwrap();
        let bp = dir.join("bm25");
        rows.store.save(&bp).unwrap();
        let lp = dir.join("live");
        rows.live.write(&lp, rows.count as u64).unwrap();
        catalog
            .append(SegmentSource {
                multi_res_path: None,
                segment_id: &format!("part-{segment}"),
                generation: 1,
                base_label: base as u64,
                backend_kind: crate::vector::EMBEDDED_TURBOVEC,
                vector_path: Some(&vp),
                exact_vector_path: Some(&ep),
                bm25_path: &bp,
                live_docs_path: &lp,
                partition_column: None,
                parents_colocated: false,
            })
            .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The catalog's published set, reopened as a source-owned generation.
fn owned_set(catalog: &SegmentCatalog, root: PathBuf) -> OpenedSegmentSet {
    let mut manifest = catalog.snapshot().published_manifest();
    manifest.format = 3;
    manifest.source_owner = Some(
        SegmentSourceOwner::encode(&crate::pb::storage::SourceIndexOwner {
            format_version: 1,
            history_id: vec![1; 16],
            index_key: b"index".to_vec(),
            collection: "books".into(),
        })
        .unwrap(),
    );
    OpenedSegmentSet::open_manifest(root, manifest, Default::default()).unwrap()
}

fn declared_store(change: &str) -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body", "name"])
        .with_positions(&["body"])
        .with_sentences(&["body"])
        .with_facets(if change == "schema" {
            &["facet"][..]
        } else {
            &["facet", "empty_facet"][..]
        })
        .with_numerics(&["double"])
        .with_integers(&["signed"])
        .with_unsigned_integers(&["unsigned"])
        .with_geos(&["point"])
        .with_map_facets(&["labels"])
        .with_map_numerics(&["double_map"])
        .with_map_integers(&["signed_map"])
        .with_map_unsigned_integers(&["unsigned_map"]);
    store
        .set_analysis_fingerprint(0, if change == "analyzer" { 12 } else { 11 })
        .unwrap();
    store.set_analysis_fingerprint(1, 22).unwrap();
    store
}

fn body_field(id: u32, variant: Variant<'_>) -> AnalyzedField {
    AnalyzedField {
        terms: vec![(
            if variant.is("term") {
                "changed"
            } else {
                "token"
            }
            .into(),
            if variant.is("tf") { 2 } else { 1 },
            vec![(0, if variant.is("span") { 4 } else { 5 })],
        )],
        length: if variant.is("length") { 3 } else { 2 },
        positions: Some(vec![vec![if variant.is("position") { 1 } else { 0 }]]),
        sentences: Some(vec![(0, if variant.is("sentence") { 8 } else { 9 })]),
        pos_lengths: Some(vec![vec![if variant.is("pos_length") { 1 } else { 2 }]]),
        splade: if variant.is("missing_splade") {
            vec![]
        } else {
            vec![(
                "neural".into(),
                if variant.is("splade_impact") {
                    199
                } else {
                    200
                },
            )]
        },
        splade_scale_bits: (if variant.is("splade_scale") {
            0.9_f32
        } else {
            0.01_f32 * (id + 1) as f32
        })
        .to_bits(),
    }
}

/// Add document `id` at `row` with its source, identity and every column.
fn add_row(store: &mut Bm25Store, row: u32, id: u32, variant: Variant<'_>) {
    store.add_document_with_lineage(
        row,
        if variant.is("text") {
            "different text".into()
        } else {
            format!("text {id}")
        },
        AnalyzedDoc {
            fields: vec![
                body_field(id, variant),
                AnalyzedField {
                    terms: vec![("name".into(), 1, vec![(0, 4)])],
                    length: 1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        Some(crate::postings::DocLineage {
            parent_id: if variant.is("lineage") { 8 } else { 7 },
            group_id: 6,
            span_start: 0,
            span_end: 9,
        }),
    );
    attach_source(store, row, id, variant);
    set_columns(store, row, variant);
    set_map_columns(store, row, variant);
}

fn attach_source(store: &mut Bm25Store, row: u32, id: u32, variant: Variant<'_>) {
    let mut payload = vec![9];
    payload.extend_from_slice(&(id as u64).to_le_bytes());
    payload.extend_from_slice(if variant.is("source") {
        &[0xa0, 6, 1]
    } else {
        &[0xa0, 6, 0x81, 0]
    });
    let source = ProtobufSource {
        descriptor_set: include_bytes!(
            "../../../../tests/fixtures/unsigned-mapping/descriptor.bin"
        )
        .to_vec(),
        message_type: "unsigned_mapping.Parent".into(),
        payload,
    };
    if variant.is("missing_identity") {
        return;
    }
    let identity = DocumentIdentity {
        document_key: if variant.is("key") {
            b"changed".to_vec()
        } else {
            format!("key-{id}").into_bytes()
        },
        version: if variant.is("version") { 2 } else { 1 },
        chunk_ordinal: if variant.is("ordinal") { None } else { Some(0) },
    };
    store
        .source_archive_mut()
        .attach_source_with_identity(row, &source, identity.chunk_ordinal, Some(&identity))
        .unwrap();
}

fn set_columns(store: &mut Bm25Store, row: u32, variant: Variant<'_>) {
    store.set_facet(
        0,
        row,
        if variant.is("facet") {
            "other"
        } else {
            "value"
        },
    );
    store.set_numeric(0, row, if variant.is("double") { 0.0 } else { -0.0 });
    store.set_integer(
        0,
        row,
        if variant.is("signed") {
            i64::MIN + 1
        } else {
            i64::MIN
        },
    );
    if !variant.is("presence") {
        store.set_unsigned_integer(
            0,
            row,
            if variant.is("unsigned") {
                u64::MAX - 1
            } else {
                u64::MAX
            },
        );
    }
    store.set_geo(0, row, if variant.is("geo") { 1.0 } else { 0.0 }, 2.0);
}

fn set_map_columns(store: &mut Bm25Store, row: u32, variant: Variant<'_>) {
    store.set_map_facet(
        0,
        row,
        "label",
        if variant.is("map_facet") {
            "other"
        } else {
            "value"
        },
    );
    store.set_map_numeric(
        0,
        row,
        "n",
        if variant.is("map_double") { 0.0 } else { -0.0 },
    );
    store
        .set_map_integer(
            0,
            row,
            "n",
            if variant.is("map_signed") {
                i64::MIN + 1
            } else {
                i64::MIN
            },
        )
        .unwrap();
    store
        .set_map_unsigned_integer(
            0,
            row,
            "n",
            if variant.is("map_unsigned") {
                u64::MAX - 1
            } else {
                u64::MAX
            },
        )
        .unwrap();
}

/// Append document `id`'s exact row, and the row the provider encodes:
/// the same values unless `perturb_quantized` shifts document 1's.
fn push_vectors(
    rows: &mut Rows,
    id: u32,
    dim: usize,
    variant: Variant<'_>,
    perturb_quantized: bool,
) {
    let exact_row: Vec<_> = (0..dim)
        .map(|i| (id as f32 + i as f32) / 8.0 + if variant.is("vector") { 0.1 } else { 0.0 })
        .collect();
    rows.vectors.extend(exact_row.iter().map(|value| {
        *value
            + if perturb_quantized && id == 1 {
                0.25
            } else {
                0.0
            }
    }));
    rows.exact_vectors.extend(exact_row);
}
