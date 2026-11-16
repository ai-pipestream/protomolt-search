//! Atomic replacement of compacted input segments with dense outputs.

use super::*;

impl SegmentCatalog {
    /// Atomically replace compacted input segments with one output.
    pub fn replace_for_compaction(
        &self,
        input_ids: &[String],
        source: SegmentSource<'_>,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        self.replace_many_for_compaction(input_ids, vec![source], None)
    }

    /// Atomically replace compacted inputs with dense immutable outputs.
    /// No output is needed when the inputs contain no live rows. Stable product
    /// identity remains in lineage, so outputs may use
    /// fresh non-overlapping positional ranges. Every output belongs to one
    /// newer generation and their combined row count must equal the inputs'
    /// live row count.
    pub fn replace_many_for_compaction(
        &self,
        input_ids: &[String],
        sources: Vec<SegmentSource<'_>>,
        partition_key: Option<String>,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        if input_ids.is_empty() {
            return Err("compaction needs at least one input segment".into());
        }
        let current = self.snapshot();
        let wanted: BTreeSet<&str> = input_ids.iter().map(String::as_str).collect();
        let expected_rows = compaction_input_rows(&current, &wanted, &sources)?;
        let outputs = self.stage_compaction_outputs(sources, expected_rows)?;
        let mut manifest = current
            .published_manifest()
            .with_binding(current.binding())?;
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        manifest
            .segments
            .retain(|segment| !wanted.contains(segment.segment_id.as_str()));
        manifest.segments.extend(outputs.iter().cloned());
        manifest.segments.sort_by_key(|segment| segment.base_label);
        manifest.partition_key = partition_key;
        manifest.update_retained_format();
        let published = self.publish(manifest);
        if published.is_err() {
            cleanup_staged(&self.root, &outputs);
        }
        published
    }

    /// Stage every output, removing them all again unless they are dense and
    /// together hold exactly `expected_rows`.
    fn stage_compaction_outputs(
        &self,
        sources: Vec<SegmentSource<'_>>,
        expected_rows: u64,
    ) -> Result<Vec<SegmentMetadata>, String> {
        let mut outputs = Vec::with_capacity(sources.len());
        for source in sources {
            match stage_segment(&self.root, source) {
                Ok(metadata) => outputs.push(metadata),
                Err(error) => {
                    cleanup_staged(&self.root, &outputs);
                    return Err(error);
                }
            }
        }
        let output_rows = outputs.iter().try_fold(0u64, |total, metadata| {
            if metadata.live_rows != metadata.rows {
                return Err(format!(
                    "compaction output {:?} is not dense: {}/{} live/physical rows",
                    metadata.segment_id, metadata.live_rows, metadata.rows
                ));
            }
            total
                .checked_add(metadata.rows)
                .ok_or_else(|| "compaction output row count overflow".to_string())
        });
        let output_rows = match output_rows {
            Ok(rows) => rows,
            Err(error) => {
                cleanup_staged(&self.root, &outputs);
                return Err(error);
            }
        };
        if output_rows != expected_rows {
            cleanup_staged(&self.root, &outputs);
            return Err(format!(
                "compaction outputs have {output_rows} rows, expected {expected_rows} dense live rows"
            ));
        }
        Ok(outputs)
    }

    /// Rebuild full-history WAL generations into bounded immutable segments
    /// and publish them as one atomic compaction. This blocking method is
    /// intended for a background worker. The work directory must be empty and
    /// remains available for audit or retry after publication.
    pub fn compact_wal_generations(
        &self,
        req: &WalCompactionRequest<'_>,
        analyze: &mut crate::reshard::Analyzer,
    ) -> Result<WalCompactionResult, String> {
        if req.work_dir.exists()
            && std::fs::read_dir(req.work_dir)
                .map_err(|e| format!("read compaction work dir {}: {e}", req.work_dir.display()))?
                .next()
                .is_some()
        {
            return Err(format!(
                "compaction work directory {} is not empty",
                req.work_dir.display()
            ));
        }
        let output = self.build_wal_compaction_segments(req, analyze)?;
        if output.segments.is_empty() {
            return Err("compaction produced no live segments".into());
        }
        let (ids, live_paths) = stage_compacted_segment_live_docs(&output)?;
        let partition_col = req.partition.map(|spec| spec.column);
        let sources = output
            .segments
            .iter()
            .zip(ids.iter())
            .zip(live_paths.iter())
            .map(|((segment, id), live_docs_path)| SegmentSource {
                multi_res_path: segment.image.multi_res_path.as_deref(),
                segment_id: id,
                generation: output.generation,
                base_label: segment.image.slot_offset,
                backend_kind: req.backend_kind,
                vector_path: Some(&segment.image.vector_path),
                exact_vector_path: Some(&segment.image.exact_vector_path),
                bm25_path: segment.image.bm25_path.as_deref().expect("validated above"),
                live_docs_path,
                partition_column: partition_col,
                parents_colocated: segment.image.parents_colocated,
            })
            .collect();
        let snapshot = self.replace_many_for_compaction(
            req.input_ids,
            sources,
            partition_col.map(str::to_string),
        )?;
        Ok(WalCompactionResult { snapshot, output })
    }

    fn build_wal_compaction_segments(
        &self,
        req: &WalCompactionRequest<'_>,
        analyze: &mut crate::reshard::Analyzer,
    ) -> Result<crate::reshard::SegmentedReshardOutput, String> {
        match req.partition {
            None => crate::reshard::split_logs_segmented_with(
                &crate::reshard::SplitRequest::new(req.generations, 1, req.work_dir)
                    .slot_base(req.slot_base)
                    .slot_stride(req.slot_capacity)
                    .bm25_fields(req.bm25_fields),
                analyze,
            ),
            Some(spec) => {
                let [gen] = req.generations else {
                    return Err(format!(
                        "a partitioned compaction takes one WAL generation, not {}",
                        req.generations.len()
                    ));
                };
                let mut sink = |_row: crate::reshard::CompactedRow<'_>| Ok(());
                let manifest = crate::wal::read_manifest(gen)
                    .map_err(|e| format!("read manifest of {}: {e}", gen.display()))?;
                let derived = crate::reshard::manifest_derived(&manifest)?;
                let hnsw_sources = (req.backend_kind == crate::hnsw::HNSW)
                    .then(|| {
                        crate::reshard::HnswCompactionSources::from_segment_set(
                            req.slot_base,
                            &self.snapshot(),
                        )
                    })
                    .flatten();
                let comp_req = crate::reshard::CompactionRequest {
                    bm25_fields: req.bm25_fields,
                    derived: derived.as_ref(),
                    hnsw_sources: hnsw_sources.as_ref(),
                    ..crate::reshard::CompactionRequest::new(gen, u64::MAX, req.work_dir)
                };
                let build =
                    crate::reshard::compact_log_partitioned(&comp_req, spec, analyze, &mut sink)?;
                let mut images = build.images;
                for image in &mut images {
                    image.slot_offset += req.slot_base;
                }
                Ok(crate::reshard::SegmentedReshardOutput::from_images(
                    build.generation,
                    images,
                ))
            }
        }
    }
}

/// The live rows the named inputs hold, once the outputs are known to share
/// one generation newer than every input.
fn compaction_input_rows(
    current: &OpenedSegmentSet,
    wanted: &BTreeSet<&str>,
    sources: &[SegmentSource<'_>],
) -> Result<u64, String> {
    let inputs: Vec<&SegmentMetadata> = current
        .manifest
        .segments
        .iter()
        .filter(|segment| wanted.contains(segment.segment_id.as_str()))
        .collect();
    if inputs.len() != wanted.len() {
        return Err("compaction names an unknown or duplicate input segment".into());
    }
    let expected_rows = inputs.iter().try_fold(0u64, |total, segment| {
        total
            .checked_add(segment.live_rows)
            .ok_or("compaction live-row count overflow")
    })?;
    let newest_generation = inputs
        .iter()
        .map(|segment| segment.generation)
        .max()
        .unwrap();
    let output_generation = sources.first().map(|source| source.generation);
    if let Some(generation) = output_generation {
        if generation <= newest_generation {
            return Err(format!(
                "compaction output generation {generation} is not newer than input generation {newest_generation}"
            ));
        }
    }
    if sources
        .iter()
        .any(|source| Some(source.generation) != output_generation)
    {
        return Err("compaction outputs do not share one generation".into());
    }
    Ok(expected_rows)
}

fn stage_compacted_segment_live_docs(
    output: &crate::reshard::SegmentedReshardOutput,
) -> Result<(Vec<String>, Vec<PathBuf>), String> {
    let mut ids = Vec::with_capacity(output.segments.len());
    let mut live_paths = Vec::with_capacity(output.segments.len());
    for seg in &output.segments {
        if seg.image.num_vectors != seg.image.num_documents {
            return Err(format!(
                "compaction segment {} has {} vectors but {} documents; aligned publication requires one document per vector",
                seg.segment_ordinal, seg.image.num_vectors, seg.image.num_documents
            ));
        }
        if seg.image.bm25_path.is_none() {
            return Err(format!(
                "compaction segment {} has no BM25 image",
                seg.segment_ordinal
            ));
        }
        let path = crate::node::live_docs_sidecar_path(&seg.image.vector_path);
        LiveDocs::default()
            .write(&path, seg.image.num_vectors)
            .map_err(|e| format!("write compacted live docs {}: {e}", path.display()))?;
        ids.push(format!(
            "g{}-s{}-p{}",
            output.generation, seg.logical_shard, seg.segment_ordinal
        ));
        live_paths.push(path);
    }
    Ok((ids, live_paths))
}
