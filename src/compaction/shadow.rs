//! Building the shadow generation and tailing the live log into it.

use super::*;

impl NodeServiceImpl {
    pub(super) fn build_and_cut_over(
        &self,
        pre: &Preflight,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
    ) -> Result<CompactShardResponse, Status> {
        let slot_offset = self.config.slot_offset;
        // The rewritten generation: the source manifest one generation
        // on, full history (nothing preexisting), same geometry.
        let mut new_manifest = pre.manifest.clone();
        new_manifest.generation = pre.cutoff_generation + 1;
        new_manifest.preexisting_vectors = 0;
        new_manifest.preexisting_documents = 0;
        let wal_stage = pre.work_dir.join("wal");
        let mut new_wal = WalWriter::create(&wal_stage, new_manifest)
            .map_err(|e| Status::internal(format!("create the rewritten WAL generation: {e}")))?;
        let build_dir = pre.work_dir.join("build");
        // The binding goes first in the rewritten log (a replica applies
        // it to an empty shard only), so it is read and logged before any
        // row is emitted.
        let bound_first = crate::reshard::read_generation_binding(&pre.gen_dir)
            .map_err(Status::failed_precondition)?;
        if let Some(binding) = &bound_first {
            new_wal
                .append(wal_record::Op::Bind(LoggedBinding {
                    plan_fingerprint: binding.plan_fingerprint.clone(),
                    body_path: binding.body_path.clone(),
                    materialize_sha: binding.materialize_sha.clone(),
                    analysis_sha: binding.analysis_sha.clone(),
                    analysis_contract: binding.analysis_contract.clone(),
                    vector_binding: binding.vector_binding.clone(),
                    index_contract: binding.index_contract.clone(),
                }))
                .map_err(|e| Status::internal(format!("rewrite binding record: {e}")))?;
        }
        let names: Option<Vec<String>> = pre.fields.as_ref().map(|(n, _)| n.clone());
        let pins: Option<Vec<u64>> = pre.fields.as_ref().map(|(_, p)| p.clone());
        let build = {
            let mut sink = |row: crate::reshard::CompactedRow<'_>| -> Result<(), String> {
                let first_id = slot_offset + row.new_local;
                let keys: Vec<Vec<u8>> = row.stable_key.map(<[u8]>::to_vec).into_iter().collect();
                // Document before vector, the order mapped ingest logs
                // them in, so a replica applying this generation lands
                // both legs at the same id.
                if let Some(document) = row.document {
                    new_wal
                        .append(wal_record::Op::AddDocuments(LoggedAddDocuments {
                            first_id,
                            documents: vec![document.clone()],
                            stable_routing_keys: keys.clone(),
                            source_references: Vec::new(),
                        }))
                        .map_err(|e| format!("rewrite document record: {e}"))?;
                }
                if let Some(vector) = row.vector {
                    new_wal
                        .append(wal_record::Op::AddVectors(LoggedAddVectors {
                            first_id,
                            batch: Some(AddVectorsRequest {
                                vectors: vector.to_vec(),
                                dim: pre.manifest.dim,
                            }),
                            stable_routing_keys: keys,
                        }))
                        .map_err(|e| format!("rewrite vector record: {e}"))?;
                }
                Ok(())
            };
            let derived = crate::node::stored_derived(&self.config);
            match pre.partition.as_deref() {
                Some(column) => crate::reshard::compact_log_partitioned(
                    &pre.gen_dir,
                    pre.cutoff_clock,
                    &build_dir,
                    crate::reshard::PartitionSpec {
                        column,
                        bound: tail_bound,
                    },
                    names.as_deref(),
                    pins.as_deref(),
                    pre.columns.as_ref(),
                    derived.as_ref(),
                    analyze,
                    &mut sink,
                ),
                None => crate::reshard::compact_log(
                    &pre.gen_dir,
                    pre.cutoff_clock,
                    &build_dir,
                    pre.segmented,
                    names.as_deref(),
                    pins.as_deref(),
                    pre.columns.as_ref(),
                    derived.as_ref(),
                    analyze,
                    &mut sink,
                ),
            }
            .map_err(|e| Status::failed_precondition(format!("compaction build: {e}")))?
        };
        if build.binding != bound_first {
            return Err(Status::internal(
                "the rewritten generation's binding differs from the replay's; the log carries \
                 contradictory bindings",
            ));
        }
        new_wal
            .flush()
            .map_err(|e| Status::internal(format!("fsync the rewritten WAL generation: {e}")))?;

        let mut shadow = if pre.segmented {
            self.open_segmented_shadow(pre, &build, new_wal)?
        } else {
            self.open_single_image_shadow(pre, &build, new_wal)?
        };
        shadow.id_map = build.id_map;
        let dense_rows = build.rows_before - build.tombstones;
        let outcome = self.tail_and_cut_over(pre, &mut shadow, tail_bound, analyze);
        match outcome {
            Ok((locked_records, write_lock_ms, tail_passes)) => {
                let closing = Instant::now();
                self.closing_flush()?;
                let closing_flush_ms = closing.elapsed().as_millis() as u64;
                let (rows_after, stats_epoch) = {
                    let guard = crate::node::read_shard(&self.state);
                    (crate::node::physical_rows(&guard), guard.stats_epoch)
                };
                Ok(CompactShardResponse {
                    rows_before: build.rows_before,
                    rows_after,
                    tombstones_reclaimed: build.tombstones,
                    tail_records_applied: shadow.tail_records,
                    locked_tail_records: locked_records,
                    write_lock_ms,
                    wal_generation: pre.cutoff_generation + 1,
                    cutoff_clock: pre.cutoff_clock,
                    layout: layout_name(pre.segmented).to_string(),
                    dry_run: false,
                    closing_flush_ms,
                    tail_passes,
                    stats_epoch,
                    partition_column: pre.partition.clone().unwrap_or_default(),
                })
            }
            Err(status) => {
                // Nothing was swapped: the live shard is untouched. The
                // staged segments are the one thing outside the work
                // directory, and they go.
                let _ = dense_rows;
                if pre.segmented {
                    let root = crate::node::segments_root(&pre.index_path);
                    crate::segments::remove_segment_dirs(&root, &shadow.staged);
                }
                Err(status)
            }
        }
    }

    /// The flush that completes a cutover. On the segment layout a flush
    /// that meets a legacy two-RPC append mid-row (documents one ahead of
    /// vectors) refuses to seal, by the layout's own rule; the row
    /// completes with the client's next call, so the closing flush waits
    /// that out for a bounded time rather than reporting a committed
    /// cutover as a failure. Past the bound it returns the seal's refusal
    /// and names the pending cutover, which the next Flush completes.
    pub(super) fn closing_flush(&self) -> Result<(), Status> {
        const ATTEMPTS: usize = 200;
        const PAUSE: std::time::Duration = std::time::Duration::from_millis(10);
        for attempt in 1..=ATTEMPTS {
            match self.flush_index() {
                Ok(_) => return Ok(()),
                Err(status)
                    if attempt < ATTEMPTS
                        && status.code() == tonic::Code::FailedPrecondition
                        && status
                            .message()
                            .contains("a segment's artifacts cover the same rows") =>
                {
                    std::thread::sleep(PAUSE);
                }
                Err(status) => {
                    return Err(Status::new(
                        status.code(),
                        format!(
                            "the compaction cut over and is pending its closing flush, which \
                             refused: {}; the next Flush completes it",
                            status.message()
                        ),
                    ))
                }
            }
        }
        unreachable!("the last attempt returns")
    }

    /// The single-image shadow: the built image laid out as a generation
    /// directory under the work dir, opened the way a snapshot install
    /// opens one.
    fn open_single_image_shadow(
        &self,
        pre: &Preflight,
        build: &crate::reshard::CompactionBuild,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let [image] = build.images.as_slice() else {
            return Err(Status::internal(format!(
                "a single-image compaction built {} images",
                build.images.len()
            )));
        };
        let gen = pre.work_dir.join("generation");
        std::fs::create_dir_all(&gen)
            .map_err(|e| Status::internal(format!("mkdir {}: {e}", gen.display())))?;
        let mv = |from: &Path, to: &Path| {
            std::fs::rename(from, to).map_err(|e| {
                Status::internal(format!("move {} -> {}: {e}", from.display(), to.display()))
            })
        };
        let vector_path = crate::node::generation_vector(&gen);
        let exact_path = crate::node::generation_exact_vectors(&gen);
        let bm25_path = crate::node::generation_bm25(&gen);
        let live_path = crate::node::generation_live_docs(&gen);
        let rows = image.row_parent_ids.len() as u64;
        let has_vectors = image.num_vectors > 0;
        if has_vectors {
            mv(&image.vector_path, &vector_path)?;
            mv(&image.exact_vector_path, &exact_path)?;
        }
        if let Some(path) = &image.bm25_path {
            mv(path, &bm25_path)?;
        }
        LiveDocs::default()
            .write(&live_path, rows)
            .map_err(|e| Status::internal(format!("write {}: {e}", live_path.display())))?;
        crate::postings::fsync_parent(&live_path)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", gen.display())))?;

        let index = if !has_vectors {
            // No vectors through the cutoff: keep the provider state the
            // log locked, so a vector the tail brings lands in the same
            // scoring space (and calibration is not silently lost).
            self.empty_configured_index(pre)?
        } else {
            let mut loaded = VectorIndex::load(&self.config.vector_backend, &vector_path)
                .map_err(|e| Status::internal(format!("load {}: {e}", vector_path.display())))?;
            loaded
                .prepare()
                .map_err(|e| Status::internal(format!("prepare {}: {e}", vector_path.display())))?;
            let d = loaded.descriptor();
            if d.backend_kind != pre.backend_kind
                || d.scoring_fingerprint != pre.scoring_fingerprint
            {
                return Err(Status::failed_precondition(format!(
                    "the compacted image scores under {}/{} but the shard serves {}/{}; the WAL \
                     manifest's provider state does not reproduce the live generation",
                    d.backend_kind,
                    d.scoring_fingerprint,
                    pre.backend_kind,
                    pre.scoring_fingerprint
                )));
            }
            Some(loaded)
        };
        let exact_vectors = if has_vectors {
            Some(
                ExactVectorStore::open(&exact_path)
                    .map_err(|e| Status::internal(format!("open {}: {e}", exact_path.display())))?,
            )
        } else {
            index
                .as_ref()
                .and_then(VectorIndex::dim_opt)
                .map(|dim| ExactVectorStore::spilling(&exact_path, Some(dim)))
                .transpose()
                .map_err(|e| {
                    Status::internal(format!(
                        "exact-vector builder {}: {e}",
                        exact_path.display()
                    ))
                })?
        };
        let bm25 = if image.bm25_path.is_some() {
            let shard = Bm25Shard::open(&bm25_path)
                .map_err(|e| Status::internal(format!("open {}: {e}", bm25_path.display())))?;
            self.check_shadow_fingerprints(pre.fields.as_ref(), &shard)?;
            Some(shard)
        } else {
            // No documents through the cutoff. A document the tail brings
            // goes into a heap store rather than a spill builder, whose
            // directory would live under the generation directory the
            // cutover renames away.
            let mut store =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            store.set_binding(
                crate::reshard::read_generation_binding(&pre.gen_dir)
                    .map_err(Status::failed_precondition)?,
            );
            Some(Bm25Shard::Building(store))
        };
        let live_docs = LiveDocs::open(&live_path)
            .map_err(|e| Status::internal(format!("open {}: {e}", live_path.display())))?;
        let mapped_binding = bm25.as_ref().and_then(|b| b.binding().cloned());
        Ok(Shadow {
            state: ShardState {
                index,
                exact_vectors,
                bm25,
                live_docs,
                generation: Some(gen),
                wal: Some(new_wal),
                parents: None,
                mapped_binding,
                stats_epoch: 0,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: None,
            },
            id_map: BTreeMap::new(),
            staged: Vec::new(),
            replaced: Vec::new(),
            tail_records: 0,
            epoch_at_open: 0,
        })
    }

    /// The segmented shadow: the built images staged as sealed segments
    /// under the live catalog root (unpublished), a staged catalog over
    /// them with a fresh tail, the dense FP32 sidecar assembled from
    /// their rows, and no tombstones.
    fn open_segmented_shadow(
        &self,
        pre: &Preflight,
        build: &crate::reshard::CompactionBuild,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let live_manifest = SegmentCatalog::read_manifest(&root)
            .map_err(Status::internal)?
            .unwrap_or_default();
        let replaced: Vec<String> = live_manifest
            .segments
            .iter()
            .map(|s| s.segment_id.clone())
            .collect();
        let epoch_at_open = {
            let guard = crate::node::read_shard(&self.state);
            match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(shard)) => shard.snapshot().epoch(),
                _ => live_manifest.epoch,
            }
        };
        let generation = epoch_at_open + 1;
        let mut live_paths = Vec::with_capacity(build.images.len());
        for image in &build.images {
            if image.num_vectors != 0 && image.num_vectors as usize != image.row_parent_ids.len() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has {} vectors over {} rows; a segment's artifacts \
                     cover the same rows",
                    image.vector_path.display(),
                    image.num_vectors,
                    image.row_parent_ids.len()
                )));
            }
            if image.bm25_path.is_none() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has no BM25 image; the segment layout seals documents",
                    image.vector_path.display()
                )));
            }
            let path = crate::node::live_docs_sidecar_path(&image.vector_path);
            LiveDocs::default()
                .write(&path, image.row_parent_ids.len() as u64)
                .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
            live_paths.push(path);
        }
        let ids: Vec<String> = (0..build.images.len())
            .map(|i| format!("cmp-{:06}-{i:04}", pre.cutoff_generation + 1))
            .collect();
        let sources: Vec<SegmentSource<'_>> = build
            .images
            .iter()
            .zip(&ids)
            .zip(&live_paths)
            .map(|((image, id), live)| SegmentSource {
                segment_id: id,
                generation,
                base_label: image.slot_offset,
                backend_kind: &pre.backend_kind,
                vector_path: (image.num_vectors > 0).then_some(image.vector_path.as_path()),
                exact_vector_path: (image.num_vectors > 0)
                    .then_some(image.exact_vector_path.as_path()),
                bm25_path: image.bm25_path.as_deref().expect("checked above"),
                live_docs_path: live,
                partition_column: pre.partition.as_deref(),
            })
            .collect();
        let staged = crate::segments::stage_segments(&root, sources)
            .map_err(|e| Status::internal(format!("stage compacted segments: {e}")))?;
        let cleanup =
            |staged: &[SegmentMetadata]| crate::segments::remove_segment_dirs(&root, staged);
        let opened = (|| -> Result<Shadow, Status> {
            // The staged set carries the epoch it was cut from; the
            // cutover commits it one past whatever the live set reached.
            let manifest = SegmentSetManifest {
                epoch: epoch_at_open,
                segments: staged.clone(),
                partition_key: pre.partition.clone(),
                ..Default::default()
            }
            .with_binding(
                crate::reshard::read_generation_binding(&pre.gen_dir)
                    .map_err(Status::failed_precondition)?
                    .as_ref(),
            )
            .map_err(Status::failed_precondition)?;
            let catalog = SegmentCatalog::open_staged(&root, manifest, self.config.vector_load())
                .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
            let tail =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            let shard = crate::segmented::SegmentedShard::open_catalog(catalog, tail)
                .map_err(|e| Status::internal(format!("open the compacted shard: {e}")))?;
            let set = shard.snapshot().clone();
            let bm25 = Bm25Shard::Segmented(shard);
            self.check_shadow_fingerprints(pre.fields.as_ref(), &bm25)?;
            let mut index = None;
            let mut exact_vectors = None;
            if let Some(empty) = self.empty_configured_index(pre)? {
                // Even an empty compacted generation replaces the old FP32
                // sidecar at the closing flush. Leaving this absent retains
                // the retired generation's rows on disk.
                exact_vectors = Some(ExactVectorStore::empty(empty.dim_opt()));
                let provider =
                    crate::segmented_vectors::SegmentedProvider::open(set.clone(), empty)
                        .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
                index = Some(VectorIndex::from_provider(provider));
            }
            if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                let d = first.descriptor();
                if d.backend_kind != pre.backend_kind
                    || d.scoring_fingerprint != pre.scoring_fingerprint
                {
                    return Err(Status::failed_precondition(format!(
                        "the compacted segments score under {}/{} but the shard serves {}/{}; \
                         the WAL manifest's provider state does not reproduce the live generation",
                        d.backend_kind,
                        d.scoring_fingerprint,
                        pre.backend_kind,
                        pre.scoring_fingerprint
                    )));
                }
                let backend = first
                    .backend_config()
                    .map_err(|e| Status::internal(format!("segment vector backend: {e}")))?;
                let dim = first
                    .dim_opt()
                    .ok_or_else(|| Status::internal("segment vector image has no dimension"))?;
                let tail_image = VectorIndex::from_backend_config(dim, &backend)
                    .map_err(|e| Status::internal(format!("segment tail image: {e}")))?;
                let provider =
                    crate::segmented_vectors::SegmentedProvider::open(set.clone(), tail_image)
                        .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
                index = Some(VectorIndex::from_provider(provider));
                exact_vectors =
                    Some(ExactVectorStore::from_segments(&set, dim).map_err(|error| {
                        Status::internal(format!("compacted exact-vector view: {error}"))
                    })?);
            }
            let mapped_binding = bm25.binding().cloned();
            Ok(Shadow {
                state: ShardState {
                    index,
                    exact_vectors,
                    bm25: Some(bm25),
                    live_docs: LiveDocs::default(),
                    generation: None,
                    wal: Some(new_wal),
                    parents: None,
                    mapped_binding,
                    stats_epoch: 0,
                    files_current: false,
                    stats_incarnation: Default::default(),
                    pending_compaction: None,
                },
                id_map: BTreeMap::new(),
                staged: staged.clone(),
                replaced,
                tail_records: 0,
                epoch_at_open,
            })
        })();
        match opened {
            Ok(shadow) => Ok(shadow),
            Err(status) => {
                cleanup(&staged);
                Err(status)
            }
        }
    }

    /// An empty index under the provider state the WAL manifest locked,
    /// for a shadow whose dense image holds no vectors; `None` when the
    /// log never locked one.
    fn empty_configured_index(&self, pre: &Preflight) -> Result<Option<VectorIndex>, Status> {
        let dim = pre.manifest.dim as usize;
        if dim == 0 {
            return Ok(None);
        }
        let Ok(config) = pre.manifest.backend_config() else {
            return Ok(None);
        };
        VectorIndex::from_backend_config(dim, &config)
            .map(Some)
            .map_err(|e| Status::internal(format!("construct the shadow's empty index: {e}")))
    }

    /// The compacted store's field table and analyzer fingerprints must
    /// be the live shard's: same names, and the same fingerprint wherever
    /// both record one (a field whose every document was tombstoned has
    /// none in the dense image).
    pub(super) fn check_shadow_fingerprints(
        &self,
        fields: Option<&(Vec<String>, Vec<u64>)>,
        built: &Bm25Shard,
    ) -> Result<(), Status> {
        let Some((names, fingerprints)) = fields else {
            return Ok(());
        };
        if built.field_count() != names.len() {
            return Err(Status::failed_precondition(format!(
                "the compacted store has {} fields but the shard has {}",
                built.field_count(),
                names.len()
            )));
        }
        for (f, (name, fingerprint)) in names.iter().zip(fingerprints).enumerate() {
            if built.field_name(f) != name {
                return Err(Status::failed_precondition(format!(
                    "compacted field {f} is {:?} but the shard's is {name:?}",
                    built.field_name(f)
                )));
            }
            let got = built.analysis_fingerprint(f);
            if got != 0 && *fingerprint != 0 && got != *fingerprint {
                return Err(Status::failed_precondition(format!(
                    "field {name:?}: the replay analyzed under fingerprint {got:#x} but the \
                     shard holds {fingerprint:#x}; the analysis backend does not reproduce the \
                     shard's term identity",
                )));
            }
        }
        Ok(())
    }

    /// Apply one pass of tailed records to the shadow, in clock order.
    /// Every document of the pass is analyzed in one batch first (one
    /// session per spec, as ingest opens), then the records apply.
    pub(super) fn apply_pass(
        &self,
        shadow: &mut Shadow,
        records: Vec<crate::pb::wal::WalRecord>,
        analyze: &mut Analyze<'_>,
    ) -> Result<(), Status> {
        let docs: Vec<&AddDocumentsRequest> = records
            .iter()
            .filter_map(|record| match &record.op {
                Some(wal_record::Op::AddDocuments(add)) => Some(add.documents.iter()),
                _ => None,
            })
            .flatten()
            .collect();
        let mut analyzed = self.analyze_records(&docs, analyze)?.into_iter();
        for record in records {
            self.apply_to_shadow(shadow, record, &mut analyzed)?;
        }
        if analyzed.next().is_some() {
            return Err(Status::internal(
                "the tail analyzed more documents than it applied",
            ));
        }
        Ok(())
    }

    /// Apply one tailed record to the shadow. Appends get shadow ids and
    /// extend the id map; deletes and replacements map through it, and
    /// an id it does not know is an error; a Bind applies; a Flush marker
    /// is nothing; a Snapshot marker aborts by name.
    fn apply_to_shadow(
        &self,
        shadow: &mut Shadow,
        record: crate::pb::wal::WalRecord,
        analyzed: &mut std::vec::IntoIter<crate::postings::AnalyzedDoc>,
    ) -> Result<(), Status> {
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => {
                let batch = add
                    .batch
                    .ok_or_else(|| Status::internal("WAL vector record has no batch"))?;
                let dim = batch.dim as usize;
                if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
                    return Err(Status::internal("WAL vector record has invalid dimensions"));
                }
                let rows = (batch.vectors.len() / dim) as u64;
                let key = add.stable_routing_keys.into_iter().next();
                let (added, first) = self.apply_batch_locked(&mut shadow.state, batch, key)?;
                if added != rows {
                    return Err(Status::internal(format!(
                        "the shadow applied {added} of {rows} tailed vectors"
                    )));
                }
                for i in 0..rows {
                    map_tailed(&mut shadow.id_map, add.first_id + i, first + i)?;
                }
            }
            Some(wal_record::Op::AddDocuments(add)) => {
                if add.stable_routing_keys.len() > add.documents.len() {
                    return Err(Status::internal(
                        "WAL document record carries more stable keys than documents",
                    ));
                }
                let mut keys = add.stable_routing_keys.into_iter();
                for (i, doc) in add.documents.into_iter().enumerate() {
                    let analyzed = analyzed.next().ok_or_else(|| {
                        Status::internal("the tail applied more documents than it analyzed")
                    })?;
                    let key = keys.next();
                    let (doc, analyzed) =
                        self.materialize_document(doc, analyzed, key.as_deref())?;
                    let mut added = 0u64;
                    let mut first = 0u64;
                    self.apply_document_locked(
                        &mut shadow.state,
                        doc,
                        analyzed,
                        None,
                        key,
                        &mut added,
                        &mut first,
                    )?;
                    map_tailed(&mut shadow.id_map, add.first_id + i as u64, first)?;
                }
            }
            Some(wal_record::Op::DeleteDocument(delete)) => {
                let new_id = mapped(&shadow.id_map, delete.doc_id)?;
                let response = self.delete_documents_locked(&mut shadow.state, &[new_id], None)?;
                if response.deleted != 1 {
                    return Err(Status::internal(format!(
                        "tailed delete of source id {} (shadow id {new_id}) hit a row the shadow \
                         already tombstoned; the log and the shadow disagree",
                        delete.doc_id
                    )));
                }
            }
            Some(wal_record::Op::Replacement(replacement)) => {
                let old = mapped(&shadow.id_map, replacement.old_doc_id)?;
                let new = mapped(&shadow.id_map, replacement.new_doc_id)?;
                let response = self.commit_replacements_locked(
                    &mut shadow.state,
                    &[crate::pb::Replacement {
                        old_doc_id: old,
                        new_doc_id: new,
                    }],
                    None,
                )?;
                if response.committed != 1 {
                    return Err(Status::internal(format!(
                        "tailed replacement of source id {} hit a row the shadow already \
                         tombstoned; the log and the shadow disagree",
                        replacement.old_doc_id
                    )));
                }
            }
            Some(wal_record::Op::Bind(bind)) => {
                Self::apply_binding_locked(
                    &mut shadow.state,
                    crate::postings::StoredBinding {
                        plan_fingerprint: bind.plan_fingerprint,
                        body_path: bind.body_path,
                        materialize_sha: bind.materialize_sha,
                        analysis_sha: bind.analysis_sha,
                        analysis_contract: bind.analysis_contract,
                        vector_binding: bind.vector_binding,
                        index_contract: bind.index_contract,
                    },
                )?;
            }
            Some(wal_record::Op::Snapshot(snapshot)) => {
                return Err(Status::aborted(format!(
                    "a snapshot was installed on this shard during compaction (marker at clock \
                     {}, superseding generation {}); the compaction is aborted and the live \
                     shard is untouched",
                    record.clock, snapshot.source_generation
                )));
            }
            Some(wal_record::Op::Flush(_)) => return Ok(()),
            None => return Err(Status::internal("WAL record without an operation")),
        }
        shadow.tail_records += 1;
        if shadow.state.wal.is_none() {
            return Err(Status::internal(
                "the rewritten WAL generation failed to append and was retired; the compaction \
                 is aborted",
            ));
        }
        Ok(())
    }

    /// Analyze logged documents the way ingest did: body and extra
    /// fields through the node's backend, with the layers each record
    /// names, one batch for the lot, assembled positionally per document.
    fn analyze_records(
        &self,
        docs: &[&AddDocumentsRequest],
        analyze: &mut Analyze<'_>,
    ) -> Result<Vec<crate::postings::AnalyzedDoc>, Status> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch: Vec<(
            &str,
            Option<&crate::pb::AnalysisSpec>,
            crate::analyzer::SessionLayers,
        )> = Vec::new();
        let mut slots: Vec<Vec<usize>> = Vec::with_capacity(docs.len());
        for doc in docs {
            batch.push((
                doc.text.as_str(),
                doc.analysis.as_ref(),
                crate::analyzer::SessionLayers {
                    sentences: !doc.sentence_fields.is_empty(),
                    dual_cased: !doc.cased_field.is_empty(),
                    ..Default::default()
                },
            ));
            let mut own = Vec::with_capacity(doc.fields.len());
            for field in &doc.fields {
                let fi = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|n| *n == field.field)
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "logged document names field {:?}, which this node's table {:?} \
                             lacks",
                            field.field, self.config.bm25_fields
                        ))
                    })?;
                own.push(fi);
                batch.push((
                    field.text.as_str(),
                    field.analysis.as_ref(),
                    crate::analyzer::SessionLayers {
                        sentences: doc.sentence_fields.iter().any(|n| n == &field.field),
                        ..Default::default()
                    },
                ));
            }
            slots.push(own);
        }
        let results = analyze(&batch)
            .map_err(|e| Status::unavailable(format!("analyze tailed documents: {e}")))?;
        if results.len() != batch.len() {
            return Err(Status::internal(format!(
                "analysis returned {} results for {} texts",
                results.len(),
                batch.len()
            )));
        }
        let mut results = results.into_iter();
        let mut out = Vec::with_capacity(docs.len());
        for (doc, own) in docs.iter().zip(slots) {
            let body = results.next().expect("counted above");
            let mut extras = Vec::with_capacity(own.len());
            for fi in own {
                let analyzed = results.next().expect("counted above");
                extras.push((fi, Some(analyzed.into_body())));
            }
            let cased =
                crate::node::cased_field_index(&self.config, self.phrase_index.as_deref(), doc)?;
            out.push(crate::node::join_fields(body, extras, cased)?);
        }
        Ok(out)
    }

    /// Fsync the live log and return its high watermark: the bound of
    /// one tail pass. Brief, under the write lock.
    pub(super) fn flush_live_wal(&self, pre: &Preflight) -> Result<u64, Status> {
        let mut guard = crate::node::write_shard(&self.state);
        Self::flush_live_wal_locked(&mut guard, pre)
    }

    /// [`Self::flush_live_wal`] on a guard the caller holds. A snapshot
    /// install rotates the log to a new generation (its marker lands in
    /// THAT generation), so the rotation itself is the abort signal.
    pub(super) fn flush_live_wal_locked(
        guard: &mut ShardState,
        pre: &Preflight,
    ) -> Result<u64, Status> {
        let wal = guard.wal.as_mut().ok_or_else(|| {
            Status::failed_precondition("the shard lost its WAL during compaction")
        })?;
        if wal.generation() != pre.cutoff_generation {
            return Err(Status::aborted(format!(
                "a snapshot was installed on this shard during compaction (the WAL rotated from \
                 generation {} to {}); the compaction is aborted and the live shard is untouched",
                pre.cutoff_generation,
                wal.generation()
            )));
        }
        wal.flush()
            .map_err(|e| Status::internal(format!("wal fsync during compaction: {e}")))?;
        Ok(wal.high_watermark())
    }
}
