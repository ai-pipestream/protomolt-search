//! Compare logical rows without a whole-field transpose or an in-memory key set.
use super::*;
use crate::pb::storage::SourceRewriteCertificate;
use crate::reshard::{reconstruct_document, segment_tables, SourceTables};
use crate::sha256::Sha256;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

const BEFORE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("before");
const AFTER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("after");
/// The certificate this module writes. Format 1 covered stored content and
/// exact FP32 rows; format 2 adds the provider's own encoded rows.
pub const CERTIFICATE_FORMAT: u32 = 2;
/// Digest bytes held per batch row.
pub const BATCH_ROW_BYTES: usize = 32;
/// The largest batch: 32 MiB of digests.
pub const MAX_BATCH_ROWS: usize = 1_048_576;
/// The batch a caller gets by asking for none (`proof_batch_rows == 0`): the
/// largest, because a smaller batch buys nothing but repeated vocabulary
/// passes (`proof_batch_rows_for_budget`).
pub const DEFAULT_PROOF_BATCH_ROWS: usize = MAX_BATCH_ROWS;

/// The batch size a digest budget of `bytes` affords, capped at
/// `MAX_BATCH_ROWS`. Fewer than 32 bytes cannot hold one row and refuse.
/// The proof walks every term of every field once per
/// batch of each segment, so the number of vocabulary passes is
/// `sum over segments of ceil(rows / batch)` per field: the batch bounds the
/// digest memory and nothing else, and the result is identical at any size.
pub fn proof_batch_rows_for_budget(bytes: u64) -> Result<usize, String> {
    if bytes < BATCH_ROW_BYTES as u64 {
        return Err(err("digest budget cannot hold one 32-byte proof row"));
    }
    Ok(usize::try_from(bytes / BATCH_ROW_BYTES as u64)
        .unwrap_or(MAX_BATCH_ROWS)
        .min(MAX_BATCH_ROWS))
}

#[cfg(test)]
type ScratchObserver = Box<dyn FnOnce(&Path)>;
#[cfg(test)]
thread_local! {
    /// Vocabulary passes the current thread's proofs have made: one per
    /// (segment, field, batch) posting walk.
    pub(crate) static VOCABULARY_PASSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Observes the private scratch after its directory and database exist.
    pub(crate) static AFTER_SCRATCH_CREATED: std::cell::RefCell<Option<ScratchObserver>> =
        const { std::cell::RefCell::new(None) };
}

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
    let mut schema = set
        .generation_declaration()
        .map(|declaration| {
            let columns = declaration
                .columns
                .as_ref()
                .expect("validated generation columns");
            Ok::<_, String>(Schema {
                tables: SourceTables {
                    fields: declaration
                        .text_fields
                        .iter()
                        .map(|f| f.name.clone())
                        .collect(),
                    fingerprints: declaration
                        .text_fields
                        .iter()
                        .map(|f| f.analysis_fingerprint.unwrap_or(0))
                        .collect(),
                    position_fields: declaration
                        .text_fields
                        .iter()
                        .filter(|f| f.positions)
                        .map(|f| f.name.clone())
                        .collect(),
                    sentence_fields: declaration
                        .text_fields
                        .iter()
                        .filter(|f| f.sentences)
                        .map(|f| f.name.clone())
                        .collect(),
                    columns: crate::reshard::ColumnTables {
                        facets: columns.facets.clone(),
                        numerics: columns.numerics.clone(),
                        integers: columns.integers.clone(),
                        unsigned_integers: columns.unsigned_integers.clone(),
                        geo: columns.geo.clone(),
                        map_facets: columns.map_facets.clone(),
                        map_numerics: columns.map_numerics.clone(),
                        map_integers: columns.map_integers.clone(),
                        map_unsigned_integers: columns.map_unsigned_integers.clone(),
                    },
                },
                derived: declaration
                    .derived
                    .as_ref()
                    .map(|d| {
                        crate::derived::Declaration::compile(d)
                            .map(|d| crate::postings::StoredDerived::of(&d))
                    })
                    .transpose()?,
            })
        })
        .transpose()?;
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
    let mut expected = set.generation_declaration().and_then(generation::backend);
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
    /// Compare every live document/chunk, its stored query content, its exact
    /// FP32 row and the vector provider's own encoding of it. Physical slots
    /// and segment cuts may differ. Duplicate identities refuse, even if their
    /// values match. The result is not permission to publish either view.
    ///
    /// `scratch` must not exist; it is created private (0700, its database
    /// 0600) and only this method's directory is removed. Digests use
    /// [`BATCH_ROW_BYTES`] per batch row (`1..=MAX_BATCH_ROWS`), plus one
    /// reconstructed row/posting, bounded provider transcript pieces and an
    /// 8 MiB disk-table cache. Mapped vector images stay unmaterialized. No whole source
    /// corpus, vocabulary transpose or identity set is materialized. The
    /// vocabulary of every field is walked once per batch of each segment.
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
        // Private from the first syscall: the directory is created 0700 and
        // the database file 0600, so a permissive umask never exposes
        // document identities through the scratch.
        let mut directory = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(scratch).map_err(err)?;
        let scratch = Scratch(scratch.to_path_buf());
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(scratch.0.join("rows.redb")).map_err(err)?;
        let mut builder = Database::builder();
        builder.set_cache_size(8 * 1024 * 1024);
        let database = builder.create_file(file).map_err(err)?;
        #[cfg(test)]
        if let Some(observe) = AFTER_SCRATCH_CREATED.with(|hook| hook.borrow_mut().take()) {
            observe(&scratch.0);
        }
        let rows = fill(&database, BEFORE, self, batch_rows)?;
        if rows != fill(&database, AFTER, after, batch_rows)? {
            return Err(err("live row count differs"));
        }
        let tx = database.begin_read().map_err(err)?;
        let before = tx.open_table(BEFORE).map_err(err)?;
        let after = tx.open_table(AFTER).map_err(err)?;
        let mut hash = Sha256::new();
        part(&mut hash, b"protomolt.source-rewrite.rows.v2");
        for (left, right) in before.iter().map_err(err)?.zip(after.iter().map_err(err)?) {
            let (lk, lv) = left.map_err(err)?;
            let (rk, rv) = right.map_err(err)?;
            if lk.value() != rk.value() || lv.value() != rv.value() {
                return Err(err(
                    "document identity, stored query content, exact vector or encoded dense \
                     row differs",
                ));
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
            format_version: CERTIFICATE_FORMAT,
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
            let digest_row = |row: usize, transcript: Option<&[u8]>| -> Result<[u8; 32], String> {
                let doc = reconstruct_document(reader, row as u32, &tables, true)?;
                if doc.original_source.is_none() {
                    return Err(err("live source row has no original protobuf"));
                }
                let mut hash = Sha256::new();
                part(&mut hash, b"protomolt.source-rewrite.row.v2");
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
                // The provider's stored encoding of the row, not a recomputation
                // from the FP32 source: two images that agree on every exact row
                // and differ in what their scorer reads are different indexes.
                match transcript {
                    Some(transcript) => {
                        hash.update(&[1]);
                        part(&mut hash, transcript);
                    }
                    None => hash.update(&[0]),
                }
                Ok(hash.finalize())
            };
            match set.vector(segment) {
                Some(vector) => {
                    // Transcripts arrive in pieces of TRANSCRIPT_BLOCK_ROWS: the
                    // provider converts its own layout one block at a time and
                    // keeps no materialized copy, so a large image is read
                    // inside the batch's budget.
                    let mut block = start;
                    while block < end {
                        let block_end = end.min(block + crate::vector::TRANSCRIPT_BLOCK_ROWS);
                        let mut failure = None;
                        vector
                            .row_transcripts(block..block_end, &mut |row, transcript| {
                                if failure.is_some() || live.is_deleted(row) {
                                    return;
                                }
                                match digest_row(row, Some(transcript)) {
                                    Ok(digest) => digests[row - start] = digest,
                                    Err(error) => failure = Some(error),
                                }
                            })
                            .map_err(err)?;
                        if let Some(error) = failure {
                            return Err(error);
                        }
                        block = block_end;
                    }
                }
                None => {
                    for row in start..end {
                        if live.is_deleted(row) {
                            continue;
                        }
                        digests[row - start] = digest_row(row, None)?;
                    }
                }
            }
            for fi in 0..reader.field_count() {
                #[cfg(test)]
                VOCABULARY_PASSES.with(|passes| passes.set(passes.get() + 1));
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
    struct Fixture(PathBuf, usize);
    impl Fixture {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "rewrite-proof-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            Self(root, 8)
        }
        fn set(
            &self,
            name: &str,
            ids: &[u32],
            deleted: &[usize],
            change: &str,
            splits: usize,
        ) -> OpenedSegmentSet {
            self.set_with_quantized_rows(name, ids, deleted, change, splits, false)
        }
        fn set_with_quantized_rows(
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
                let mut exact_vectors = Vec::new();
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
                    let exact_row: Vec<_> = (0..dim)
                        .map(|i| {
                            (id as f32 + i as f32) / 8.0 + if variant("vector") { 0.1 } else { 0.0 }
                        })
                        .collect();
                    vectors.extend(exact_row.iter().map(|value| {
                        *value
                            + if perturb_quantized && id == 1 {
                                0.25
                            } else {
                                0.0
                            }
                    }));
                    exact_vectors.extend(exact_row);
                    if deleted.contains(&(base + row)) {
                        live.delete(row);
                    }
                }
                let mut vector = VectorIndex::from_backend_config(dim, &backend).unwrap();
                vector.add(&vectors, dim).unwrap();
                vector.prepare().unwrap();
                let vp = dir.join("vector");
                vector.write(&vp).unwrap();
                let ep = dir.join("exact");
                ExactVectorStore::from_values(dim, exact_vectors)
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
    fn final_segment_can_be_reclaimed_only_with_identical_generation_metadata() {
        let fixture = Fixture::new();
        let before = fixture.set("all-deleted", &[0, 1], &[0, 1], "", 2);
        let declaration =
            generation::from_reader(before.bm25(0), backend(&before).unwrap()).unwrap();
        let expected = before
            .verify_source_rewrite(&before, &fixture.0.join("self-proof"), 1)
            .unwrap();
        let mut manifest = before.published_manifest();
        manifest.format = 4;
        manifest.generation_declaration =
            Some(SegmentGenerationDeclaration::encode(&declaration).unwrap());
        manifest.segments.clear();
        manifest.epoch += 1;
        let empty = OpenedSegmentSet::open_manifest(
            before.root().to_path_buf(),
            manifest.clone(),
            Default::default(),
        )
        .unwrap();
        let proof = before
            .verify_source_rewrite(&empty, &fixture.0.join("empty-proof"), 1)
            .unwrap();
        assert_eq!(proof, expected);
        assert_eq!(proof.live_rows, 0);
        for change in 0..3 {
            let mut altered = declaration.clone();
            match change {
                0 => altered.columns.as_mut().unwrap().facets.pop(),
                1 => {
                    altered.text_fields[0].analysis_fingerprint = Some(12);
                    None
                }
                2 => {
                    altered.vector = None;
                    None
                }
                _ => unreachable!(),
            };
            manifest.generation_declaration =
                Some(SegmentGenerationDeclaration::encode(&altered).unwrap());
            let mut populated = manifest.clone();
            populated.segments = before.manifest().segments.clone();
            assert!(OpenedSegmentSet::open_manifest(
                before.root().to_path_buf(),
                populated,
                Default::default()
            )
            .is_err());
            let changed = OpenedSegmentSet::open_manifest(
                before.root().to_path_buf(),
                manifest.clone(),
                Default::default(),
            )
            .unwrap();
            assert!(before
                .verify_source_rewrite(&changed, &fixture.0.join("bad-proof"), 1)
                .is_err());
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
    /// Equal exact rows and equal configuration do not certify a dense image
    /// whose stored codes came from other vectors. The same rows encoded
    /// unchanged, or in another order and cut, still certify.
    fn rewrite_proof_rejects_quantized_image_built_from_different_rows() {
        let fixture = Fixture::new();
        let before = fixture.set("before", &[0, 1], &[], "", 2);
        let after = fixture.set_with_quantized_rows("after", &[0, 1], &[], "", 2, true);
        let scratch = fixture.0.join("proof");
        let error = before
            .verify_source_rewrite(&after, &scratch, 1)
            .unwrap_err();
        assert!(error.contains("encoded dense row"), "{error}");
        assert!(!scratch.exists());

        let same = fixture.set_with_quantized_rows("same", &[0, 1], &[], "", 2, false);
        let proof = before.verify_source_rewrite(&same, &scratch, 1).unwrap();
        assert_eq!(proof.format_version, CERTIFICATE_FORMAT);
        assert_eq!(proof.live_rows, 2);
        let reordered = fixture.set_with_quantized_rows("reordered", &[1, 0], &[], "", 1, false);
        assert_eq!(
            before
                .verify_source_rewrite(&reordered, &scratch, 1)
                .unwrap(),
            proof
        );
        let perturbed_reordered =
            fixture.set_with_quantized_rows("perturbed-reordered", &[1, 0], &[], "", 1, true);
        assert!(before
            .verify_source_rewrite(&perturbed_reordered, &scratch, 1)
            .is_err());
    }

    /// The batch bounds digest memory and nothing else: the certificate is
    /// identical at every size, and the vocabulary is walked once per
    /// (segment, field, batch), so passes = fields * sum(ceil(rows / batch)).
    #[test]
    fn vocabulary_passes_scale_with_batches_and_the_certificate_does_not() {
        let fixture = Fixture::new();
        let ids: Vec<u32> = (0..280).collect();
        let before = fixture.set("before", &ids, &[], "", 280);
        let after = fixture.set("after", &ids, &[], "", 100);
        let fields = 2u64;
        let segments = |cut: u64| {
            (0..280u64)
                .step_by(cut as usize)
                .map(move |s| (280 - s).min(cut))
        };
        let expected_passes = |batch: u64| -> u64 {
            let before: u64 = segments(280).map(|rows| rows.div_ceil(batch)).sum();
            let after: u64 = segments(100).map(|rows| rows.div_ceil(batch)).sum();
            fields * (before + after)
        };
        let mut certificate = None;
        let mut measured = Vec::new();
        for batch in [1u64, 7, 64, 280, 1024, MAX_BATCH_ROWS as u64] {
            VOCABULARY_PASSES.with(|passes| passes.set(0));
            let proof = before
                .verify_source_rewrite(&after, &fixture.0.join("scratch"), batch as usize)
                .unwrap();
            let passes = VOCABULARY_PASSES.with(|passes| passes.get());
            assert_eq!(passes, expected_passes(batch), "batch {batch}");
            measured.push((batch, passes));
            match &certificate {
                Some(expected) => assert_eq!(expected, &proof, "batch {batch}"),
                None => certificate = Some(proof),
            }
        }
        // Recorded in docs/source-index-maintenance.md.
        eprintln!("vocabulary passes by batch (280 rows, 2 fields, 1 + 3 segments): {measured:?}");
        assert!(proof_batch_rows_for_budget(0).is_err());
        assert!(proof_batch_rows_for_budget(31).is_err());
        assert_eq!(proof_batch_rows_for_budget(32).unwrap(), 1);
        assert_eq!(proof_batch_rows_for_budget(63).unwrap(), 1);
        assert_eq!(proof_batch_rows_for_budget(64).unwrap(), 2);
        assert_eq!(proof_batch_rows_for_budget(32 * 1000).unwrap(), 1000);
        assert_eq!(
            proof_batch_rows_for_budget(u64::MAX).unwrap(),
            MAX_BATCH_ROWS
        );
        assert_eq!(DEFAULT_PROOF_BATCH_ROWS, MAX_BATCH_ROWS);
    }

    /// The scratch is private from creation. The check runs in a child
    /// process whose umask permits everything, so the modes observed are
    /// the ones requested at creation, not the process default.
    #[cfg(unix)]
    #[test]
    fn scratch_is_private_under_a_permissive_umask() {
        use std::os::unix::fs::PermissionsExt;
        const CHILD: &str = "REWRITE_PROOF_UMASK_CHILD";
        if std::env::var_os(CHILD).is_some() {
            // SAFETY: the child process is single-purpose; nothing else in it
            // depends on the creation mask.
            unsafe { libc::umask(0) };
            let fixture = Fixture::new();
            let before = fixture.set("before", &[0, 1], &[], "", 2);
            let observed = std::rc::Rc::new(std::cell::Cell::new(false));
            let seen = observed.clone();
            AFTER_SCRATCH_CREATED.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |scratch: &Path| {
                    let mode = |path: &Path| {
                        std::fs::symlink_metadata(path)
                            .unwrap()
                            .permissions()
                            .mode()
                            & 0o7777
                    };
                    assert_eq!(mode(scratch), 0o700, "scratch directory mode");
                    assert_eq!(
                        mode(&scratch.join("rows.redb")),
                        0o600,
                        "scratch database mode"
                    );
                    seen.set(true);
                }));
            });
            before
                .verify_source_rewrite(&before, &fixture.0.join("proof"), 1)
                .unwrap();
            assert!(observed.get(), "the scratch observer did not run");
            return;
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "segments::rewrite_proof::tests::scratch_is_private_under_a_permissive_umask",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "child exited with {status}");
    }

    /// Net allocation on the calling thread, for the proof's own thread:
    /// the proof allocates and frees on that thread, and the engine's
    /// per-block conversions stay below its parallel threshold.
    mod allocation {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;
        thread_local! {
            static LIVE: Cell<i64> = const { Cell::new(0) };
            static PEAK: Cell<i64> = const { Cell::new(0) };
        }
        struct Counting;
        fn record(delta: i64) {
            let _ = LIVE.try_with(|live| {
                let now = live.get() + delta;
                live.set(now);
                let _ = PEAK.try_with(|peak| {
                    if now > peak.get() {
                        peak.set(now);
                    }
                });
            });
        }
        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                let pointer = System.alloc(layout);
                if !pointer.is_null() {
                    record(layout.size() as i64);
                }
                pointer
            }
            unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
                System.dealloc(pointer, layout);
                record(-(layout.size() as i64));
            }
            unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                let moved = System.realloc(pointer, layout, new_size);
                if !moved.is_null() {
                    record(new_size as i64 - layout.size() as i64);
                }
                moved
            }
        }
        #[global_allocator]
        static GLOBAL: Counting = Counting;
        pub fn reset() {
            LIVE.with(|live| live.set(0));
            PEAK.with(|peak| peak.set(0));
        }
        pub fn peak() -> i64 {
            PEAK.with(|peak| peak.get())
        }
    }

    fn materialized(set: &OpenedSegmentSet) -> Vec<Option<bool>> {
        (0..set.len())
            .map(|i| set.vector(i).and_then(|v| v.representation_materialized()))
            .collect()
    }

    /// A proof over mapped images (the default open mode) reads them in
    /// bounded pieces: afterwards no segment of either set holds a
    /// materialized packed-row copy, and the certificate equals the one the
    /// heap-loaded form produces.
    #[test]
    fn mapped_proof_leaves_packed_codes_unmaterialized() {
        let fixture = Fixture::new();
        let before = fixture.set("before", &[0, 1, 2, 3], &[0], "", 2);
        let after = fixture.set("after", &[3, 1, 2], &[], "", 3);
        assert_eq!(materialized(&before), vec![Some(false); 2]);
        assert_eq!(materialized(&after), vec![Some(false); 1]);
        let proof = before
            .verify_source_rewrite(&after, &fixture.0.join("proof"), 2)
            .unwrap();
        assert_eq!(proof.live_rows, 3);
        assert_eq!(
            materialized(&before),
            vec![Some(false); 2],
            "before materialized"
        );
        assert_eq!(
            materialized(&after),
            vec![Some(false); 1],
            "after materialized"
        );
        let heap = OpenedSegmentSet::open_manifest(
            after.root().to_path_buf(),
            after.published_manifest(),
            VectorLoad::Heap,
        )
        .unwrap();
        assert_eq!(
            before
                .verify_source_rewrite(&heap, &fixture.0.join("heap-proof"), 2)
                .unwrap(),
            proof
        );
        assert_eq!(
            materialized(&heap),
            vec![Some(false); 1],
            "heap image materialized"
        );
    }

    /// Proof memory at a fixed batch does not grow with the vector image:
    /// the same rows at 8 and at 512 dimensions, a 64x larger image, allocate
    /// within one transcript piece of each other on the proof's thread. A
    /// materialized image at 512 dimensions would be 16,384 rows x 256 bytes
    /// = 4 MiB of packed rows per set, plus a transient of the same size.
    #[test]
    fn proof_allocation_is_independent_of_vector_image_size() {
        let rows: Vec<u32> = (0..16_384).collect();
        let mut peaks = Vec::new();
        for dim in [8usize, 512] {
            let mut fixture = Fixture::new();
            fixture.1 = dim;
            let before = fixture.set("before", &rows, &[], "", 4096);
            let after = fixture.set("after", &rows, &[], "", 4096);
            allocation::reset();
            let proof = before
                .verify_source_rewrite(&after, &fixture.0.join("proof"), 1024)
                .unwrap();
            let peak = allocation::peak();
            assert_eq!(proof.live_rows, rows.len() as u64);
            assert_eq!(materialized(&before), vec![Some(false); 4]);
            assert_eq!(materialized(&after), vec![Some(false); 4]);
            eprintln!("proof peak allocation at {dim} dimensions, batch 1024: {peak} bytes");
            peaks.push(peak);
        }
        assert!(
            peaks[1] - peaks[0] < 1 << 20,
            "proof memory grew with the image: {peaks:?}"
        );
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
