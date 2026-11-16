//! Bounded cuts of rebuilt rows, each written as one staged segment.
use super::*;

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

pub(super) struct Output {
    pub(super) id: String,
    pub(super) base: u64,
    pub(super) directory: PathBuf,
    pub(super) vector: bool,
    pub(super) retained: bool,
}
pub(super) struct Builder<'a> {
    pub(super) node: &'a NodeServiceImpl,
    pub(super) before: &'a OpenedSegmentSet,
    pub(super) directory: Directory,
    pub(super) outputs: Vec<Output>,
    pub(super) pending: Vec<Row>,
    pub(super) bytes: usize,
    pub(super) written: u64,
    pub(super) next_row: u64,
    pub(super) request: &'a CompactDocumentIndexRequest,
}

/// One cut's rows gathered into a heap store and its vector columns.
struct Cut {
    store: Bm25Store,
    vectors: Vec<f32>,
    retained: crate::multi_res::MultiResVectors,
    has_vectors: bool,
}

impl Cut {
    fn add(&mut self, id: u32, row: Row, tables: &ColumnTables) -> Result<(), Status> {
        if let Some(materialized) = row.materialized {
            self.retained.insert(
                id,
                crate::multi_res::decode(&materialized).map_err(failure)?,
            );
        }
        if row.vector.is_some() != self.has_vectors {
            return Err(failure("mixed vector presence in a cut"));
        }
        if let Some(vector) = row.vector {
            self.vectors.extend(vector);
        }
        let mut doc = row.document;
        self.store
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
        self.store.add_document_with_lineage(
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
        restore_values(&mut self.store, id, &doc, tables)
    }

    /// Write the cut's segment files into `directory`. Returns whether a
    /// retained-vector file was written.
    fn write(
        self,
        declaration: &crate::pb::storage::IndexGenerationDeclaration,
        rows: usize,
        directory: &Path,
    ) -> Result<bool, Status> {
        let Cut {
            store,
            vectors,
            retained,
            has_vectors,
        } = self;
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
        if !retained.is_empty() {
            retained
                .write(&directory.join(crate::multi_res::FILE_NAME), rows as u64)
                .map_err(failure)?;
        }
        LiveDocs::default()
            .write(&directory.join("live-docs.bin"), rows as u64)
            .map_err(failure)?;
        Ok(!retained.is_empty())
    }
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
        let mut cut = Cut {
            store,
            vectors: Vec::new(),
            retained: crate::multi_res::MultiResVectors::default(),
            has_vectors,
        };
        for (id, row) in self.pending.drain(..).enumerate() {
            cut.add(id as u32, row, &tables)?;
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
        let retained = cut.write(declaration, rows, &directory)?;
        self.charge_staged(&directory)?;
        self.outputs.push(Output {
            id,
            base: self.next_row,
            directory,
            vector: has_vectors,
            retained,
        });
        self.next_row = self
            .next_row
            .checked_add(rows as u64)
            .ok_or_else(|| failure("rewrite row count overflow"))?;
        self.bytes = 0;
        Ok(())
    }

    /// Count a written cut's files against the staging limit.
    fn charge_staged(&mut self, directory: &Path) -> Result<(), Status> {
        for file in std::fs::read_dir(directory).map_err(failure)? {
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
    pub(super) fn build(&mut self) -> Result<(), Status> {
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
