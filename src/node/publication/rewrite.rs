//! Bounded source-owned row rebuilding. Publication stays in the journaled path.
use super::*;
use crate::pb::{CompactDocumentIndexRequest, DocumentMaintenanceActivation};
use crate::postings::{AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Store};
use crate::reshard::{reconstruct_document, segment_tables, ColumnTables};
use crate::segments::generation;
use prost::Message;

#[cfg(test)]
mod hooks {
    use super::*;
    type Callback = Box<dyn FnOnce() + Send>;
    struct Hooks {
        built: Option<Callback>,
        finished: Callback,
    }
    static HOOKS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, Hooks>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());
    pub(crate) fn install(root: PathBuf, built: Callback, finished: Callback) {
        assert!(HOOKS
            .lock()
            .unwrap()
            .insert(
                root,
                Hooks {
                    built: Some(built),
                    finished
                }
            )
            .is_none());
    }
    pub(super) fn run(root: &Path, finished: bool) {
        let callback = {
            let mut hooks = HOOKS.lock().unwrap();
            if finished {
                hooks.remove(root).map(|h| h.finished)
            } else {
                hooks.get_mut(root).and_then(|h| h.built.take())
            }
        };
        if let Some(callback) = callback {
            callback();
        }
    }
}
#[cfg(test)]
pub(crate) use hooks::install as set_rewrite_test_hooks;

struct Directory(PathBuf);
impl Directory {
    fn create(parent: &Path) -> Result<Self, Status> {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).map_err(failure)?;
        let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let path = parent.join(format!(".document-rewrite-{suffix}"));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).map_err(failure)?;
        Ok(Self(path))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn failure(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("source row rebuild: {error}"))
}
fn charge(used: &mut usize, bytes: usize, limit: usize) -> Result<(), Status> {
    *used = used.checked_add(bytes).ok_or_else(|| {
        Status::resource_exhausted("source row rebuild batch accounting overflow")
    })?;
    if *used > limit {
        return Err(Status::resource_exhausted(
            "source row rebuild batch_bytes exceeded",
        ));
    }
    Ok(())
}
struct Row {
    document: crate::pb::AddDocumentsRequest,
    fields: Vec<AnalyzedField>,
    vector: Option<Vec<f32>>,
    bytes: usize,
}

// Scans only posting skip runs intersecting this row interval. The budget is
// charged before retaining each row/term; no whole-field transpose is built.
fn read_batch(
    before: &OpenedSegmentSet,
    segment: usize,
    start: u32,
    end: u32,
    limit: usize,
) -> Result<Vec<Option<Row>>, Status> {
    let reader = before.bm25(segment);
    let tables = segment_tables(reader);
    let mut used = 0;
    charge(
        &mut used,
        (end - start) as usize * std::mem::size_of::<Option<Row>>(),
        limit,
    )?;
    let mut rows = Vec::with_capacity((end - start) as usize);
    for row in start..end {
        if before.live_docs(segment).is_deleted(row as usize) {
            rows.push(None);
            continue;
        }
        let document = reconstruct_document(reader, row, &tables, true).map_err(failure)?;
        if document.original_source.is_none() || document.identity.is_none() {
            return Err(failure(
                "a live source row lacks its original source or identity",
            ));
        }
        let vector = before
            .exact_vectors(segment)
            .map(|exact| {
                exact
                    .row_values(row as usize, row as usize + 1)
                    .map_err(failure)
            })
            .transpose()?;
        let mut bytes = std::mem::size_of::<Row>();
        charge(&mut bytes, document.encoded_len(), limit)?;
        charge(
            &mut bytes,
            reader.field_count() * std::mem::size_of::<AnalyzedField>(),
            limit,
        )?;
        if let Some(vector) = &vector {
            charge(&mut bytes, vector.len() * std::mem::size_of::<f32>(), limit)?;
        }
        charge(&mut used, bytes, limit)?;
        rows.push(Some(Row {
            document,
            fields: vec![AnalyzedField::default(); reader.field_count()],
            vector,
            bytes,
        }));
    }
    for fi in 0..reader.field_count() {
        let field = reader.field(fi);
        for (ordinal, row) in rows.iter_mut().enumerate() {
            let Some(row) = row else { continue };
            let original = start + ordinal as u32;
            let sentences = reader.field_doc_sentences(fi, original);
            let bytes = sentences.as_ref().map_or(0, |s| s.len() * 8);
            charge(&mut used, bytes, limit)?;
            charge(&mut row.bytes, bytes, limit)?;
            row.fields[fi] = AnalyzedField {
                terms: Vec::new(),
                length: field.doc_length(original),
                positions: reader.field_has_positions(fi).then(Vec::new),
                sentences,
            };
        }
        for term in field.prefix_iter("") {
            let mut cursor = field
                .file_impacts(&term)
                .ok_or_else(|| failure("stored field has no posting skip runs"))?;
            cursor.advance_shallow(start);
            while !cursor.exhausted() && cursor.doc_id() < end {
                let original = cursor.doc_id();
                if original >= start {
                    if let Some(row) = &mut rows[(original - start) as usize] {
                        let offsets = cursor.offsets();
                        let positions = if reader.field_has_positions(fi) {
                            Some(field.posting_positions(&term, original).ok_or_else(|| {
                                failure("declared positional posting has no ordinals")
                            })?)
                        } else {
                            None
                        };
                        let bytes = term.len()
                            + std::mem::size_of::<(String, u32, Vec<(u32, u32)>)>()
                            + offsets.len() * 8
                            + positions
                                .as_ref()
                                .map_or(0, |p| std::mem::size_of::<Vec<u32>>() + p.len() * 4);
                        charge(&mut used, bytes, limit)?;
                        charge(&mut row.bytes, bytes, limit)?;
                        row.fields[fi]
                            .terms
                            .push((term.clone(), cursor.tf(), offsets));
                        if let Some(positions) = positions {
                            row.fields[fi].positions.as_mut().unwrap().push(positions);
                        }
                    }
                }
                cursor.next_posting();
            }
        }
    }
    Ok(rows)
}

fn restore_values(
    store: &mut Bm25Store,
    row: u32,
    doc: &crate::pb::AddDocumentsRequest,
    tables: &ColumnTables,
) -> Result<(), Status> {
    fn column(table: &[String], name: &str) -> Result<usize, Status> {
        table
            .iter()
            .position(|n| n == name)
            .ok_or_else(|| failure(format!("undeclared column {name:?}")))
    }
    for v in &doc.facets {
        store.set_facet(column(&tables.facets, &v.field)?, row, &v.value);
    }
    for v in &doc.numerics {
        store.set_numeric(column(&tables.numerics, &v.field)?, row, v.value);
    }
    for v in &doc.integers {
        store.set_integer(column(&tables.integers, &v.field)?, row, v.value);
    }
    for v in &doc.unsigned_integers {
        store.set_unsigned_integer(column(&tables.unsigned_integers, &v.field)?, row, v.value);
    }
    for v in &doc.geo_points {
        store.set_geo(column(&tables.geo, &v.field)?, row, v.lat, v.lon);
    }
    for v in &doc.map_facets {
        store.set_map_facet(column(&tables.map_facets, &v.field)?, row, &v.key, &v.value);
    }
    for v in &doc.map_numerics {
        store.set_map_numeric(
            column(&tables.map_numerics, &v.field)?,
            row,
            &v.key,
            v.value,
        );
    }
    for v in &doc.map_integers {
        store
            .set_map_integer(
                column(&tables.map_integers, &v.field)?,
                row,
                &v.key,
                v.value,
            )
            .map_err(failure)?;
    }
    for v in &doc.map_unsigned_integers {
        store
            .set_map_unsigned_integer(
                column(&tables.map_unsigned_integers, &v.field)?,
                row,
                &v.key,
                v.value,
            )
            .map_err(failure)?;
    }
    Ok(())
}

struct Output {
    id: String,
    base: u64,
    directory: PathBuf,
    vector: bool,
}
struct Builder<'a> {
    node: &'a NodeServiceImpl,
    before: &'a OpenedSegmentSet,
    directory: Directory,
    outputs: Vec<Output>,
    pending: Vec<Row>,
    bytes: usize,
    written: u64,
    next_row: u64,
    request: &'a CompactDocumentIndexRequest,
}
impl Builder<'_> {
    fn flush(&mut self) -> Result<(), Status> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let declaration = self
            .before
            .generation_declaration()
            .ok_or_else(|| failure("generation declaration missing"))?;
        let mut store = heap_store(&self.node.config).map_err(failure)?;
        generation::restore_tail(declaration, &mut store).map_err(failure)?;
        store.set_binding(self.before.binding().cloned());
        let rows = self.pending.len();
        let has_vectors = self.pending[0].vector.is_some();
        let tables = if self.before.is_empty() {
            return Err(failure("nonempty rewrite has no source segments"));
        } else {
            segment_tables(self.before.bm25(0)).columns
        };
        let mut vectors = Vec::new();
        for (id, row) in self.pending.drain(..).enumerate() {
            let id = id as u32;
            if row.vector.is_some() != has_vectors {
                return Err(failure("mixed vector presence in a cut"));
            }
            if let Some(vector) = row.vector {
                vectors.extend(vector);
            }
            let mut doc = row.document;
            store
                .source_archive_mut()
                .attach_source_with_identity(
                    id,
                    doc.original_source.as_ref().unwrap(),
                    doc.source_chunk_ordinal,
                    doc.identity.as_ref(),
                )
                .map_err(failure)?;
            for field in &row.fields {
                field.check_positions().map_err(failure)?;
                field.check_sentences().map_err(failure)?;
            }
            store.add_document_with_lineage(
                id,
                std::mem::take(&mut doc.text),
                AnalyzedDoc {
                    fields: row.fields,
                    ..Default::default()
                },
                doc.lineage.take().map(|l| crate::postings::DocLineage {
                    parent_id: l.parent_id,
                    group_id: l.group_id,
                    span_start: l.span_start,
                    span_end: l.span_end,
                }),
            );
            restore_values(&mut store, id, &doc, &tables)?;
        }
        let id = format!(
            "rewrite-{}-{}",
            self.directory
                .0
                .file_name()
                .unwrap()
                .to_string_lossy()
                .trim_start_matches(".document-rewrite-"),
            self.outputs.len()
        );
        let directory = self.directory.0.join(&id);
        std::fs::create_dir(&directory).map_err(failure)?;
        store
            .save(&directory.join("documents.bm25"))
            .map_err(failure)?;
        drop(store);
        if has_vectors {
            let (dim, backend) = generation::backend(declaration)
                .ok_or_else(|| failure("vector declaration missing"))?;
            if vectors.len()
                != rows
                    .checked_mul(dim)
                    .ok_or_else(|| failure("vector dimension overflow"))?
            {
                return Err(failure("vector rows do not match the declared dimension"));
            }
            let mut provider = VectorIndex::from_backend_config(dim, &backend).map_err(failure)?;
            provider.add(&vectors, dim).map_err(failure)?;
            provider.prepare().map_err(failure)?;
            provider
                .write(&directory.join("vector.index"))
                .map_err(failure)?;
            drop(provider);
            ExactVectorStore::from_values(dim, vectors)
                .map_err(failure)?
                .write(&directory.join("vectors.f32"))
                .map_err(failure)?;
        }
        LiveDocs::default()
            .write(&directory.join("live-docs.bin"), rows as u64)
            .map_err(failure)?;
        for file in std::fs::read_dir(&directory).map_err(failure)? {
            let bytes = file.map_err(failure)?.metadata().map_err(failure)?.len();
            self.written = self
                .written
                .checked_add(bytes)
                .ok_or_else(|| Status::resource_exhausted("rewrite staging size overflow"))?;
        }
        if self.written > self.request.max_staged_bytes {
            return Err(Status::resource_exhausted(
                "source row rebuild max_staged_bytes exceeded",
            ));
        }
        self.outputs.push(Output {
            id,
            base: self.next_row,
            directory,
            vector: has_vectors,
        });
        self.next_row = self
            .next_row
            .checked_add(rows as u64)
            .ok_or_else(|| failure("rewrite row count overflow"))?;
        self.bytes = 0;
        Ok(())
    }
    fn push(&mut self, row: Row) -> Result<(), Status> {
        if !self.pending.is_empty()
            && (self.pending.len() >= self.request.batch_rows as usize
                || self.bytes + row.bytes > self.request.batch_bytes as usize
                || self.pending[0].vector.is_some() != row.vector.is_some())
        {
            self.flush()?;
        }
        charge(
            &mut self.bytes,
            row.bytes,
            self.request.batch_bytes as usize,
        )?;
        self.pending.push(row);
        Ok(())
    }
    fn build(&mut self) -> Result<(), Status> {
        for segment in 0..self.before.len() {
            let rows = u32::try_from(self.before.metadata(segment).rows).map_err(failure)?;
            let mut start = 0;
            while start < rows {
                let mut end = rows.min(start.saturating_add(self.request.batch_rows));
                let batch = loop {
                    match read_batch(
                        self.before,
                        segment,
                        start,
                        end,
                        self.request.batch_bytes as usize,
                    ) {
                        Ok(batch) => break batch,
                        Err(error)
                            if error.code() == tonic::Code::ResourceExhausted
                                && end - start > 1 =>
                        {
                            end = start + (end - start) / 2;
                        }
                        Err(error) => return Err(error),
                    }
                };
                for row in batch.into_iter().flatten() {
                    self.push(row)?;
                }
                start = end;
            }
        }
        self.flush()
    }
}

impl NodeServiceImpl {
    /// Rebuild live stored rows in private bounded batches, then use the source
    /// journal and serving fence for publication. Call on a blocking worker.
    /// This owner operation does not re-analyze or re-evaluate derived values.
    /// A concurrent source publication causes the finished rewrite to refuse.
    pub fn compact_document_index_blocking(
        &self,
        source: &DocumentCatalog,
        request: CompactDocumentIndexRequest,
    ) -> Result<DocumentMaintenanceActivation, Status> {
        if !(1..=65536).contains(&request.batch_rows)
            || !(1..=64 * 1024 * 1024).contains(&request.batch_bytes)
            || request.max_staged_bytes == 0
            || request.proof_batch_rows as usize > crate::segments::rewrite_proof::MAX_BATCH_ROWS
        {
            return Err(Status::invalid_argument("source compaction requires batch_rows 1..65536, batch_bytes 1..64MiB, positive max_staged_bytes and proof_batch_rows 0..1048576 (0 selects the 1048576-row default)"));
        }
        // Zero asks for the default: the largest batch, 32 MiB of digests. A
        // smaller batch only repeats vocabulary passes (rewrite_proof.rs).
        let proof_batch_rows = match request.proof_batch_rows as usize {
            0 => crate::segments::rewrite_proof::DEFAULT_PROOF_BATCH_ROWS,
            rows => rows,
        };
        if source.collection()? != self.config.collection {
            return Err(failure("source and target collections differ"));
        }
        let owner = source.index_owner(&request.index_key)?;
        let (before, catalog, claim) = {
            let guard = read_shard(&self.state);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(failure("source compaction requires a segmented index"));
            };
            let before = shard.snapshot().clone();
            let claim = StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
            self.check_segment_publication(&guard, claim, before.epoch())?;
            check_source_live_view(&before, &guard.live_docs)?;
            if before
                .manifest()
                .source_owner
                .as_ref()
                .map(|o| o.decode())
                .transpose()
                .map_err(failure)?
                .as_ref()
                != Some(&owner)
                || before.generation_declaration().is_none()
            {
                return Err(failure(
                    "source compaction requires the exact owned, declared generation",
                ));
            }
            (before, shard.catalog().clone(), claim)
        };
        source
            .current_index_publication_decision(&request.index_key, &catalog)?
            .ok_or_else(|| failure("source compaction requires a committed source projection"))?;
        let directory = Directory::create(
            before
                .root()
                .parent()
                .ok_or_else(|| failure("catalog root has no parent"))?,
        )?;
        let mut builder = Builder {
            node: self,
            before: &before,
            directory,
            outputs: Vec::new(),
            pending: Vec::new(),
            bytes: 0,
            written: 0,
            next_row: 0,
            request: &request,
        };
        builder.build()?;
        #[cfg(test)]
        hooks::run(before.root(), false);
        let paths: Vec<_> = builder
            .outputs
            .iter()
            .map(|o| {
                [
                    o.directory.join("vector.index"),
                    o.directory.join("vectors.f32"),
                    o.directory.join("documents.bm25"),
                    o.directory.join("live-docs.bin"),
                ]
            })
            .collect();
        let next_epoch = before
            .epoch()
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("maintenance catalog epoch exhausted"))?;
        let sources = builder
            .outputs
            .iter()
            .zip(&paths)
            .map(|(output, paths)| SegmentSource {
                segment_id: &output.id,
                generation: next_epoch,
                base_label: output.base,
                backend_kind: &self.config.vector_backend,
                vector_path: output.vector.then_some(paths[0].as_path()),
                exact_vector_path: output.vector.then_some(paths[1].as_path()),
                bm25_path: &paths[2],
                live_docs_path: &paths[3],
                partition_column: before.manifest().partition_key.as_deref(),
            })
            .collect();
        let result = self.publish_document_maintenance_blocking(
            source,
            &request.index_key,
            claim,
            before.epoch(),
            sources,
            &builder.directory.0.join("proof"),
            proof_batch_rows,
        );
        drop(builder);
        #[cfg(test)]
        hooks::run(before.root(), true);
        result
    }
}

#[cfg(test)]
mod tests;
