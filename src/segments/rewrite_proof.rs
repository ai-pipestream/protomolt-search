//! Compare logical rows without a whole-field transpose or an in-memory key set.
use super::*;
use crate::pb::storage::SourceRewriteCertificate;
use crate::reshard::{reconstruct_document, segment_tables, SourceTables};
use crate::sha256::Sha256;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

const BEFORE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("before");
const AFTER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("after");
const MAX_BATCH_ROWS: usize = 1_048_576;

fn err(error: impl std::fmt::Display) -> String {
    format!("source rewrite proof: {error}")
}
fn part(hash: &mut Sha256, bytes: &[u8]) {
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}
fn extend(previous: &[u8; 32], bytes: &[u8]) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(previous);
    part(&mut hash, bytes);
    hash
}
fn names(hash: &mut Sha256, values: &[String]) {
    hash.update(&(values.len() as u64).to_le_bytes());
    for value in values {
        part(hash, value.as_bytes());
    }
}

#[derive(PartialEq, Eq)]
struct Schema {
    tables: SourceTables,
    derived: Option<crate::postings::StoredDerived>,
}
fn schema(set: &OpenedSegmentSet) -> Result<Option<Schema>, String> {
    let mut schema = None;
    for i in 0..set.len() {
        let reader = set.bm25(i);
        let this = Schema {
            tables: segment_tables(reader),
            derived: reader.derived().cloned(),
        };
        if schema.as_ref().is_some_and(|expected| expected != &this) {
            return Err(err(
                "segment schema or derived declaration differs within a catalog",
            ));
        }
        schema = Some(this);
    }
    Ok(schema)
}
fn schema_hash(set: &OpenedSegmentSet, schema: Option<&Schema>) -> [u8; 32] {
    let mut hash = Sha256::new();
    part(&mut hash, b"protomolt.source-rewrite.schema.v1");
    if let Some(binding) = &set.manifest().binding {
        part(&mut hash, &binding.protobuf);
    } else {
        part(&mut hash, &[]);
    }
    hash.update(&[u8::from(schema.is_some())]);
    if let Some(schema) = schema {
        let t = &schema.tables;
        names(&mut hash, &t.fields);
        for value in &t.fingerprints {
            hash.update(&value.to_le_bytes());
        }
        names(&mut hash, &t.position_fields);
        names(&mut hash, &t.sentence_fields);
        let c = &t.columns;
        for names_ in [
            &c.facets,
            &c.numerics,
            &c.integers,
            &c.unsigned_integers,
            &c.geo,
            &c.map_facets,
            &c.map_numerics,
            &c.map_integers,
            &c.map_unsigned_integers,
        ] {
            names(&mut hash, names_);
        }
        hash.update(&[u8::from(schema.derived.is_some())]);
        if let Some(derived) = &schema.derived {
            part(&mut hash, &derived.declaration);
            part(&mut hash, derived.fingerprint.as_bytes());
        }
    }
    hash.finalize()
}
fn backend(
    set: &OpenedSegmentSet,
) -> Result<Option<(usize, crate::vector::VectorBackendConfig)>, String> {
    let mut expected = None;
    for i in 0..set.len() {
        if let Some(vector) = set.vector(i) {
            let this = (
                vector
                    .dim_opt()
                    .ok_or_else(|| err("vector dimension is absent"))?,
                vector.backend_config().map_err(err)?,
            );
            if expected.as_ref().is_some_and(|expected| expected != &this) {
                return Err(err("vector backend states differ within a catalog"));
            }
            if set.exact_vectors(i).is_none() {
                return Err(err("vector segment has no exact FP32 rows"));
            }
            expected = Some(this);
        } else if set.exact_vectors(i).is_some() {
            return Err(err("orphan exact-vector rows"));
        }
    }
    Ok(expected)
}

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl OpenedSegmentSet {
    /// Compare every live document/chunk and its stored query content. Physical
    /// slots and segment cuts may differ. Duplicate identities refuse, even if
    /// their values match. The result is not permission to publish either view.
    ///
    /// `scratch` must not exist; only this method's temporary directory is
    /// removed. Digests use at most 32 bytes per batch row (1..=1,048,576), plus
    /// one reconstructed row/posting and an 8 MiB disk-table cache. No whole
    /// source corpus, vocabulary transpose or identity set is materialized.
    pub fn verify_source_rewrite(
        &self,
        after: &OpenedSegmentSet,
        scratch: &Path,
        batch_rows: usize,
    ) -> Result<SourceRewriteCertificate, String> {
        if !(1..=MAX_BATCH_ROWS).contains(&batch_rows) {
            return Err(err("batch_rows must be 1..=1048576"));
        }
        let owner = self
            .manifest()
            .source_owner
            .as_ref()
            .ok_or_else(|| err("before catalog has no source owner"))?;
        if after.manifest().source_owner.as_ref() != Some(owner)
            || self.binding() != after.binding()
        {
            return Err(err("owner or mapped binding changed"));
        }
        let before_schema = schema(self)?;
        let after_schema = schema(after)?;
        if before_schema != after_schema {
            return Err(err(
                "rewrite changes schema or drops empty-column declarations",
            ));
        }
        let before_backend = backend(self)?;
        let after_backend = backend(after)?;
        if before_backend != after_backend {
            return Err(err("rewrite changes or drops vector backend state"));
        }
        std::fs::create_dir(scratch).map_err(err)?;
        let scratch = Scratch(scratch.to_path_buf());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&scratch.0, std::fs::Permissions::from_mode(0o700))
                .map_err(err)?;
        }
        let mut builder = Database::builder();
        builder.set_cache_size(8 * 1024 * 1024);
        let database = builder.create(scratch.0.join("rows.redb")).map_err(err)?;
        let rows = fill(&database, BEFORE, self, batch_rows)?;
        if rows != fill(&database, AFTER, after, batch_rows)? {
            return Err(err("live row count differs"));
        }
        let tx = database.begin_read().map_err(err)?;
        let before = tx.open_table(BEFORE).map_err(err)?;
        let after = tx.open_table(AFTER).map_err(err)?;
        let mut hash = Sha256::new();
        part(&mut hash, b"protomolt.source-rewrite.rows.v1");
        for (left, right) in before.iter().map_err(err)?.zip(after.iter().map_err(err)?) {
            let (lk, lv) = left.map_err(err)?;
            let (rk, rv) = right.map_err(err)?;
            if lk.value() != rk.value() || lv.value() != rv.value() {
                return Err(err("document identity or stored query content differs"));
            }
            part(&mut hash, lk.value());
            part(&mut hash, lv.value());
        }
        let mut schema_digest = Sha256::new();
        schema_digest.update(&schema_hash(self, before_schema.as_ref()));
        if let Some((dim, config)) = before_backend {
            schema_digest.update(&(dim as u64).to_le_bytes());
            part(&mut schema_digest, config.backend_kind.as_bytes());
            part(&mut schema_digest, config.config_format.as_bytes());
            part(&mut schema_digest, &config.payload);
        }
        Ok(SourceRewriteCertificate {
            format_version: 1,
            owner: Some(owner.decode()?),
            live_rows: rows,
            schema_sha256: schema_digest.finalize().to_vec(),
            identity_content_sha256: hash.finalize().to_vec(),
        })
    }
}

fn identity(reader: &Bm25Reader, row: u32) -> Result<Vec<u8>, String> {
    let identity = reader
        .document_identity(row)
        .ok_or_else(|| err("live row has no document identity"))?;
    if identity.document_key.is_empty()
        || identity.document_key.len() > 16384
        || identity.version == 0
    {
        return Err(err("live row has an invalid document identity"));
    }
    Ok(identity.encode_to_vec())
}

fn fill(
    database: &Database,
    table: TableDefinition<&[u8], &[u8]>,
    set: &OpenedSegmentSet,
    batch_rows: usize,
) -> Result<u64, String> {
    let tx = database.begin_write().map_err(err)?;
    tx.open_table(table).map_err(err)?;
    tx.commit().map_err(err)?;
    let mut count = 0u64;
    for segment in 0..set.len() {
        let reader = set.bm25(segment);
        let tables = segment_tables(reader);
        let rows = usize::try_from(set.metadata(segment).rows).map_err(err)?;
        u32::try_from(rows).map_err(|_| err("segment row count exceeds stored document ids"))?;
        let live = set.live_docs(segment);
        for start in (0..rows).step_by(batch_rows) {
            let end = rows.min(start + batch_rows);
            let mut digests = vec![[0u8; 32]; end - start];
            for row in start..end {
                if live.is_deleted(row) {
                    continue;
                }
                let doc = reconstruct_document(reader, row as u32, &tables, true)?;
                if doc.original_source.is_none() {
                    return Err(err("live source row has no original protobuf"));
                }
                let mut hash = Sha256::new();
                part(&mut hash, b"protomolt.source-rewrite.row.v1");
                part(&mut hash, &doc.encode_to_vec());
                // Protobuf scalar default elision normalizes -0. Preserve its
                // stored bits too, just as the FP32 vector transcript does.
                for value in &doc.numerics {
                    hash.update(&value.value.to_bits().to_le_bytes());
                }
                for value in &doc.map_numerics {
                    hash.update(&value.value.to_bits().to_le_bytes());
                }
                for value in &doc.geo_points {
                    hash.update(&value.lat.to_bits().to_le_bytes());
                    hash.update(&value.lon.to_bits().to_le_bytes());
                }
                if let Some(exact) = set.exact_vectors(segment) {
                    hash.update(&[1]);
                    for value in exact.row_values(row, row + 1).map_err(err)? {
                        hash.update(&value.to_bits().to_le_bytes());
                    }
                } else {
                    hash.update(&[0]);
                }
                digests[row - start] = hash.finalize();
            }
            for fi in 0..reader.field_count() {
                let field = reader.field(fi);
                for row in start..end {
                    if live.is_deleted(row) {
                        continue;
                    }
                    let mut hash = extend(&digests[row - start], b"field");
                    hash.update(&(fi as u64).to_le_bytes());
                    hash.update(&field.doc_length(row as u32).to_le_bytes());
                    hash.update(&[u8::from(reader.field_has_positions(fi))]);
                    let sentences = reader.field_doc_sentences(fi, row as u32);
                    hash.update(&[u8::from(sentences.is_some())]);
                    if let Some(sentences) = sentences {
                        hash.update(&(sentences.len() as u64).to_le_bytes());
                        for (start, end) in sentences {
                            hash.update(&start.to_le_bytes());
                            hash.update(&end.to_le_bytes());
                        }
                    }
                    digests[row - start] = hash.finalize();
                }
                for term in field.prefix_iter("") {
                    let mut cursor = field
                        .file_impacts(&term)
                        .ok_or_else(|| err("rewrite proof requires posting skip runs"))?;
                    cursor.advance_shallow(start as u32);
                    while !cursor.exhausted() && (cursor.doc_id() as usize) < end {
                        let row = cursor.doc_id() as usize;
                        if row >= start && !live.is_deleted(row) {
                            let mut hash = extend(&digests[row - start], b"posting");
                            part(&mut hash, term.as_bytes());
                            hash.update(&cursor.tf().to_le_bytes());
                            let offsets = cursor.offsets();
                            hash.update(&(offsets.len() as u64).to_le_bytes());
                            for (start, end) in offsets {
                                hash.update(&start.to_le_bytes());
                                hash.update(&end.to_le_bytes());
                            }
                            let positions = field.posting_positions(&term, row as u32);
                            hash.update(&[u8::from(positions.is_some())]);
                            if let Some(positions) = positions {
                                hash.update(&(positions.len() as u64).to_le_bytes());
                                for pos in positions {
                                    hash.update(&pos.to_le_bytes());
                                }
                            }
                            digests[row - start] = hash.finalize();
                        }
                        cursor.next_posting();
                    }
                }
            }
            let mut tx = database.begin_write().map_err(err)?;
            tx.set_durability(Durability::None).map_err(err)?;
            {
                let mut output = tx.open_table(table).map_err(err)?;
                for row in start..end {
                    if live.is_deleted(row) {
                        continue;
                    }
                    let key = identity(reader, row as u32)?;
                    if output
                        .insert(key.as_slice(), digests[row - start].as_slice())
                        .map_err(err)?
                        .is_some()
                    {
                        return Err(err("duplicate live document/chunk identity"));
                    }
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| err("live row count overflow"))?;
                }
            }
            tx.commit().map_err(err)?;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{DocumentIdentity, ProtobufSource};
    use crate::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "rewrite-proof-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            Self(root)
        }
        fn set(
            &self,
            name: &str,
            ids: &[u32],
            deleted: &[usize],
            change: &str,
            splits: usize,
        ) -> OpenedSegmentSet {
            let root = self.0.join(name);
            std::fs::create_dir(&root).unwrap();
            let catalog = SegmentCatalog::open(&root).unwrap();
            let sample: Vec<f32> = (0..64).map(|i| (i as f32 * 0.3).sin()).collect();
            let backend =
                VectorIndex::fit_backend_config(crate::vector::EMBEDDED_TURBOVEC, 8, 4, &sample)
                    .unwrap();
            let mut base = 0;
            for (segment, ids) in ids.chunks(splits).enumerate() {
                let dir = self.0.join(format!("{name}-part-{segment}"));
                std::fs::create_dir(&dir).unwrap();
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
                let mut vectors = Vec::new();
                let mut live = LiveDocs::default();
                for (row, &id) in ids.iter().enumerate() {
                    let mutate = id == 1;
                    let variant = |name: &str| mutate && change == name;
                    let field = AnalyzedField {
                        terms: vec![(
                            if variant("term") { "changed" } else { "token" }.into(),
                            if variant("tf") { 2 } else { 1 },
                            vec![(0, if variant("span") { 4 } else { 5 })],
                        )],
                        length: if variant("length") { 3 } else { 2 },
                        positions: Some(vec![vec![if variant("position") { 1 } else { 0 }]]),
                        sentences: Some(vec![(0, if variant("sentence") { 8 } else { 9 })]),
                    };
                    store.add_document_with_lineage(
                        row as u32,
                        if variant("text") {
                            "different text".into()
                        } else {
                            format!("text {id}")
                        },
                        AnalyzedDoc {
                            fields: vec![
                                field,
                                AnalyzedField {
                                    terms: vec![("name".into(), 1, vec![(0, 4)])],
                                    length: 1,
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        },
                        Some(crate::postings::DocLineage {
                            parent_id: if variant("lineage") { 8 } else { 7 },
                            group_id: 6,
                            span_start: 0,
                            span_end: 9,
                        }),
                    );
                    let mut payload = vec![9];
                    payload.extend_from_slice(&(id as u64).to_le_bytes());
                    payload.extend_from_slice(if variant("source") {
                        &[0xa0, 6, 1]
                    } else {
                        &[0xa0, 6, 0x81, 0]
                    });
                    let source = ProtobufSource {
                        descriptor_set: include_bytes!(
                            "../../tests/fixtures/unsigned-mapping/descriptor.bin"
                        )
                        .to_vec(),
                        message_type: "unsigned_mapping.Parent".into(),
                        payload,
                    };
                    if !variant("missing_identity") {
                        let identity = DocumentIdentity {
                            document_key: if variant("key") {
                                b"changed".to_vec()
                            } else {
                                format!("key-{id}").into_bytes()
                            },
                            version: if variant("version") { 2 } else { 1 },
                            chunk_ordinal: if variant("ordinal") { None } else { Some(0) },
                        };
                        store
                            .source_archive_mut()
                            .attach_source_with_identity(
                                row as u32,
                                &source,
                                identity.chunk_ordinal,
                                Some(&identity),
                            )
                            .unwrap();
                    }
                    store.set_facet(
                        0,
                        row as u32,
                        if variant("facet") { "other" } else { "value" },
                    );
                    store.set_numeric(0, row as u32, if variant("double") { 0.0 } else { -0.0 });
                    store.set_integer(
                        0,
                        row as u32,
                        if variant("signed") {
                            i64::MIN + 1
                        } else {
                            i64::MIN
                        },
                    );
                    if !variant("presence") {
                        store.set_unsigned_integer(
                            0,
                            row as u32,
                            if variant("unsigned") {
                                u64::MAX - 1
                            } else {
                                u64::MAX
                            },
                        );
                    }
                    store.set_geo(0, row as u32, if variant("geo") { 1.0 } else { 0.0 }, 2.0);
                    store.set_map_facet(
                        0,
                        row as u32,
                        "label",
                        if variant("map_facet") {
                            "other"
                        } else {
                            "value"
                        },
                    );
                    store.set_map_numeric(
                        0,
                        row as u32,
                        "n",
                        if variant("map_double") { 0.0 } else { -0.0 },
                    );
                    store
                        .set_map_integer(
                            0,
                            row as u32,
                            "n",
                            if variant("map_signed") {
                                i64::MIN + 1
                            } else {
                                i64::MIN
                            },
                        )
                        .unwrap();
                    store
                        .set_map_unsigned_integer(
                            0,
                            row as u32,
                            "n",
                            if variant("map_unsigned") {
                                u64::MAX - 1
                            } else {
                                u64::MAX
                            },
                        )
                        .unwrap();
                    vectors.extend((0..8).map(|i| {
                        (id as f32 + i as f32) / 8.0 + if variant("vector") { 0.1 } else { 0.0 }
                    }));
                    if deleted.contains(&(base + row)) {
                        live.delete(row);
                    }
                }
                let mut vector = VectorIndex::from_backend_config(8, &backend).unwrap();
                vector.add(&vectors, 8).unwrap();
                vector.prepare().unwrap();
                let vp = dir.join("vector");
                vector.write(&vp).unwrap();
                let ep = dir.join("exact");
                ExactVectorStore::from_values(8, vectors)
                    .unwrap()
                    .write(&ep)
                    .unwrap();
                let bp = dir.join("bm25");
                store.save(&bp).unwrap();
                let lp = dir.join("live");
                live.write(&lp, ids.len() as u64).unwrap();
                catalog
                    .append(SegmentSource {
                        segment_id: &format!("part-{segment}"),
                        generation: 1,
                        base_label: base as u64,
                        backend_kind: crate::vector::EMBEDDED_TURBOVEC,
                        vector_path: Some(&vp),
                        exact_vector_path: Some(&ep),
                        bm25_path: &bp,
                        live_docs_path: &lp,
                        partition_column: None,
                    })
                    .unwrap();
                base += ids.len();
            }
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
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn rewrite_proof_preserves_identity_across_physical_reordering_and_tombstone_reclamation() {
        let fixture = Fixture::new();
        let before = fixture.set("before", &[0, 1, 2, 3], &[0], "", 2);
        let after = fixture.set("after", &[3, 1, 2], &[], "", 3);
        let mut expected = None;
        for batch in [1, 2, 1024] {
            let scratch = fixture.0.join(format!("scratch-{batch}"));
            let proof = before
                .verify_source_rewrite(&after, &scratch, batch)
                .unwrap();
            assert_eq!(proof.live_rows, 3);
            if let Some(expected) = &expected {
                assert_eq!(expected, &proof);
            } else {
                expected = Some(proof);
            }
            assert!(!scratch.exists());
        }
    }

    #[test]
    fn rewrite_proof_rejects_changed_source_identity_and_every_stored_value_family() {
        let fixture = Fixture::new();
        let before = fixture.set("before", &[1, 2], &[], "", 2);
        for change in [
            "key",
            "version",
            "ordinal",
            "text",
            "lineage",
            "source",
            "missing_identity",
            "term",
            "tf",
            "span",
            "length",
            "position",
            "sentence",
            "facet",
            "double",
            "signed",
            "unsigned",
            "presence",
            "geo",
            "map_facet",
            "map_double",
            "map_signed",
            "map_unsigned",
            "vector",
            "analyzer",
            "schema",
        ] {
            let after = fixture.set(change, &[1, 2], &[], change, 1);
            let scratch = fixture.0.join("proof");
            assert!(
                before.verify_source_rewrite(&after, &scratch, 1).is_err(),
                "accepted changed {change}"
            );
            assert!(!scratch.exists());
        }
    }

    #[test]
    fn rewrite_proof_refuses_duplicate_missing_or_unowned_rows_and_preserves_existing_scratch() {
        let fixture = Fixture::new();
        let before = fixture.set("before", &[1, 2], &[], "", 2);
        let duplicate = fixture.set("duplicate", &[1, 1], &[], "", 1);
        let missing = fixture.set("missing", &[1], &[], "", 1);
        let scratch = fixture.0.join("scratch");
        assert!(before
            .verify_source_rewrite(&duplicate, &scratch, 1)
            .unwrap_err()
            .contains("duplicate"));
        assert!(before
            .verify_source_rewrite(&missing, &scratch, 1)
            .unwrap_err()
            .contains("count"));
        assert!(before.verify_source_rewrite(&before, &scratch, 0).is_err());
        let mut manifest = before.published_manifest();
        manifest.source_owner = None;
        manifest.format = 1;
        let unowned = OpenedSegmentSet::open_manifest(
            before.root().to_path_buf(),
            manifest,
            Default::default(),
        )
        .unwrap();
        assert!(before.verify_source_rewrite(&unowned, &scratch, 1).is_err());
        std::fs::create_dir(&scratch).unwrap();
        std::fs::write(scratch.join("keep"), b"user data").unwrap();
        assert!(before.verify_source_rewrite(&before, &scratch, 1).is_err());
        assert_eq!(std::fs::read(scratch.join("keep")).unwrap(), b"user data");
    }
    #[test]
    fn rewrite_proof_batches_cross_posting_skip_boundaries() {
        let fixture = Fixture::new();
        let ids: Vec<u32> = (0..280).collect();
        let deleted: Vec<usize> = (0..280).step_by(5).collect();
        let survivors: Vec<u32> = ids.iter().copied().filter(|id| id % 5 != 0).rev().collect();
        let before = fixture.set("before", &ids, &deleted, "", 280);
        let after = fixture.set("after", &survivors, &[], "", 97);
        let mut expected = None;
        for batch in [13, 128, 1024] {
            let proof = before
                .verify_source_rewrite(&after, &fixture.0.join("scratch"), batch)
                .unwrap();
            assert_eq!(proof.live_rows, survivors.len() as u64);
            if let Some(expected) = &expected {
                assert_eq!(expected, &proof);
            } else {
                expected = Some(proof);
            }
        }
    }
}
