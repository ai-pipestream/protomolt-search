//! Shadow generation build, staging, tail replay, and closing flush.

mod open;
mod tail;

pub(in crate::compaction) use self::open::{
    open_segmented_vectors, stage_compacted_images, verify_compacted_vector_descriptor,
    StageImagesSpec,
};

use std::time::Instant;

use tonic::Status;

use crate::node::NodeServiceImpl;
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors, LoggedBinding};
use crate::pb::{AddVectorsRequest, CompactShardResponse};
use crate::wal::WalWriter;

use super::{layout_name, Analyze, Preflight, Shadow, ANALYSIS_STREAMS};

fn create_rewritten_wal(
    pre: &Preflight,
) -> Result<(WalWriter, Option<crate::postings::StoredBinding>), Status> {
    let mut new_manifest = pre.manifest.clone();
    new_manifest.generation = pre.cutoff_generation + 1;
    new_manifest.preexisting_vectors = 0;
    new_manifest.preexisting_documents = 0;
    let wal_stage = pre.work_dir.join("wal");
    let mut new_wal = WalWriter::create(&wal_stage, new_manifest)
        .map_err(|e| Status::internal(format!("create the rewritten WAL generation: {e}")))?;
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
    Ok((new_wal, bound_first))
}

fn append_compacted_row(
    new_wal: &mut WalWriter,
    slot_offset: u64,
    dim: u32,
    row: crate::reshard::CompactedRow<'_>,
) -> Result<(), String> {
    let first_id = slot_offset + row.new_local;
    let keys: Vec<Vec<u8>> = row.stable_key.map(<[u8]>::to_vec).into_iter().collect();
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
                    materialized_rows: row
                        .materialized_row
                        .map(<[u8]>::to_vec)
                        .into_iter()
                        .collect(),
                    vectors: vector.to_vec(),
                    dim,
                }),
                stable_routing_keys: keys,
            }))
            .map_err(|e| format!("rewrite vector record: {e}"))?;
    }
    Ok(())
}

impl NodeServiceImpl {
    /// Returns the node's configured batch analyzer for WAL replay and tail catch-up.
    pub(in crate::compaction) fn analyzer<'a>(
        &'a self,
        handle: &'a tokio::runtime::Handle,
    ) -> Box<Analyze<'a>> {
        Box::new(move |docs| {
            let addr = self.config.analysis_addr.as_deref().ok_or_else(|| {
                "no analysis backend configured for this shard (analysis_addr)".to_string()
            })?;
            handle
                .block_on(crate::analyzer::analyze_batch_streams(
                    addr,
                    docs,
                    ANALYSIS_STREAMS,
                ))
                .map_err(|status| status.message().to_string())
        })
    }

    pub(in crate::compaction) fn build_and_cut_over(
        &self,
        pre: &Preflight,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
    ) -> Result<CompactShardResponse, Status> {
        self.build_and_cut_over_with_shadow_flush(
            pre,
            tail_bound,
            analyze,
            &mut Self::flush_compaction_shadow,
        )
    }

    pub(in crate::compaction) fn flush_compaction_shadow(
        shadow: &mut Shadow,
    ) -> Result<(), Status> {
        shadow
            .state
            .wal
            .as_mut()
            .expect("checked after every apply")
            .flush()
            .map_err(|e| Status::internal(format!("fsync the rewritten generation: {e}")))
    }

    fn run_wal_compaction_build(
        &self,
        pre: &Preflight,
        tail_bound: usize,
        new_wal: &mut WalWriter,
        analyze: &mut Analyze<'_>,
    ) -> Result<crate::reshard::CompactionBuild, Status> {
        let slot_offset = self.config.slot_offset;
        let dim = pre.manifest.dim;
        let build_dir = pre.work_dir.join("build");
        let names: Option<Vec<String>> = pre.fields.as_ref().map(|(n, _)| n.clone());
        let pins: Option<Vec<u64>> = pre.fields.as_ref().map(|(_, p)| p.clone());
        let mut sink = |row: crate::reshard::CompactedRow<'_>| {
            append_compacted_row(new_wal, slot_offset, dim, row)
        };
        let derived = crate::node::stored_derived(&self.config);
        let built = match pre.partition.as_deref() {
            Some(column) => crate::reshard::compact_log_partitioned_with_sources(
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
                pre.hnsw_sources.as_ref(),
                analyze,
                &mut sink,
            ),
            None => crate::reshard::compact_log_with_sources(
                &pre.gen_dir,
                pre.cutoff_clock,
                &build_dir,
                pre.segmented,
                names.as_deref(),
                pins.as_deref(),
                pre.columns.as_ref(),
                derived.as_ref(),
                pre.hnsw_sources.as_ref(),
                analyze,
                &mut sink,
            ),
        };
        built.map_err(|e| Status::failed_precondition(format!("compaction build: {e}")))
    }

    pub(in crate::compaction) fn build_and_cut_over_with_shadow_flush(
        &self,
        pre: &Preflight,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
        flush_shadow: &mut dyn FnMut(&mut Shadow) -> Result<(), Status>,
    ) -> Result<CompactShardResponse, Status> {
        let (mut new_wal, bound_first) = create_rewritten_wal(pre)?;
        let build = self.run_wal_compaction_build(pre, tail_bound, &mut new_wal, analyze)?;
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
        match self.tail_and_cut_over(pre, &mut shadow, tail_bound, analyze, flush_shadow) {
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
                if pre.segmented {
                    let root = crate::node::segments_root(&pre.index_path);
                    crate::segments::remove_segment_dirs(&root, &shadow.staged);
                }
                Err(status)
            }
        }
    }

    /// Runs the post-cutover flush, briefly polling past mid-row paired appends
    /// before surfacing any remaining seal refusal.
    pub(in crate::compaction) fn closing_flush(&self) -> Result<(), Status> {
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

    /// Verifies that the compacted BM25 store matches the live shard's field table
    /// and non-zero analyzer fingerprints.
    pub(in crate::compaction) fn check_shadow_fingerprints(
        &self,
        fields: Option<&(Vec<String>, Vec<u64>)>,
        built: &crate::node::Bm25Shard,
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
}
