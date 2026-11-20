//! Health and shard-layout merges across children.

use tonic::{Request, Response, Status};

use crate::pb::{HealthRequest, HealthResponse};

use super::{grpc_timeout, RelayService};

/// The span a child's health report covers in the global id space, the
/// same reading the root takes when it derives one interval per target.
pub(super) fn health_span(report: &HealthResponse) -> u64 {
    report.num_vectors.max(report.bm25_docs)
}

/// The children in slot order, refused by name when a child's range
/// overlaps the next or leaves a gap before it.
fn contiguous_order(children: &[String], reports: &[HealthResponse]) -> Result<Vec<usize>, Status> {
    let mut order: Vec<usize> = (0..reports.len()).collect();
    order.sort_by_key(|&i| reports[i].slot_offset);
    for pair in order.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let end = reports[a].slot_offset + health_span(&reports[a]);
        if end != reports[b].slot_offset {
            let relation = if end > reports[b].slot_offset {
                "overlaps"
            } else {
                "leaves a gap before"
            };
            return Err(Status::failed_precondition(format!(
                "relay: child {a} ({}) covers slots {}..{end} and {relation} child {b} ({}) at \
                 slot {}; a relay serves contiguous children only, so the range the parent \
                 derives from one health report is real",
                children.get(a).map_or("", String::as_str),
                reports[a].slot_offset,
                children.get(b).map_or("", String::as_str),
                reports[b].slot_offset
            )));
        }
    }
    Ok(order)
}

/// Merge the children's health into the one shard the parent sees.
/// Refuses by name when the children's slot ranges are not contiguous
/// (a gap or an overlap would make the parent's derived range a lie),
/// when they disagree on the provider identity, or when there are none.
pub fn merge_health(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<HealthResponse, Status> {
    if reports.is_empty() {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let order = contiguous_order(children, reports)?;
    let first = &reports[order[0]];
    let last = &reports[*order.last().expect("non-empty")];
    let mut merged = HealthResponse {
        collection: collection.to_string(),
        slot_offset: first.slot_offset,
        dim: first.dim,
        bits_per_dimension: first.bits_per_dimension,
        vector_backend: first.vector_backend.clone(),
        scoring_fingerprint: first.scoring_fingerprint.clone(),
        quality_contract: first.quality_contract.clone(),
        exact_vectors_available: true,
        exact_vectors_mmap: true,
        ..Default::default()
    };
    for (shard, report) in reports.iter().enumerate() {
        same_score_space(&merged, report, shard, order[0])?;
        if report.dim != 0 && merged.dim != 0 && report.dim != merged.dim {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} has dimension {} while child {} has {}",
                report.dim, order[0], merged.dim
            )));
        }
        if merged.dim == 0 {
            merged.dim = report.dim;
            merged.bits_per_dimension = report.bits_per_dimension;
        }
        merged.num_vectors += report.num_vectors;
        merged.bm25_docs += report.bm25_docs;
        merged.bm25_building |= report.bm25_building;
        merged.ingest_active |= report.ingest_active;
        merged.exact_vectors_available &= report.exact_vectors_available;
        merged.exact_vectors_mmap &= report.exact_vectors_mmap;
        merged.exact_vector_rows += report.exact_vector_rows;
        merged.live_docs += report.live_docs;
        merged.deleted_docs += report.deleted_docs;
    }
    // The replication tip relative to the relay's base: the last child's
    // tip, rebased. A relay has no WAL of its own and no live revision,
    // and says so with zeros and `wal_clocked = false`; it never invents
    // a watermark for a subtree.
    merged.document_slots = (last.slot_offset - first.slot_offset) + last.document_slots;
    merged.live_revision = 0;
    merged.wal_generation = 0;
    merged.wal_high_watermark = 0;
    merged.wal_clocked = false;
    Ok(merged)
}

/// A child must report the first child's provider and score identity.
fn same_score_space(
    merged: &HealthResponse,
    report: &HealthResponse,
    shard: usize,
    first: usize,
) -> Result<(), Status> {
    for (name, mine, theirs) in [
        (
            "vector_backend",
            &merged.vector_backend,
            &report.vector_backend,
        ),
        (
            "scoring_fingerprint",
            &merged.scoring_fingerprint,
            &report.scoring_fingerprint,
        ),
        (
            "quality_contract",
            &merged.quality_contract,
            &report.quality_contract,
        ),
    ] {
        if mine != theirs {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} reports {name} {theirs:?} while child {first} reports \
                 {mine:?}; one relay serves one score space"
            )));
        }
    }
    Ok(())
}

/// The children's layouts as the one shard the root lists: counts
/// summed, the children's segments concatenated with the child index in front of each segment id, the
/// knobs true only when every child has them, one placement code when
/// the children agree and `placement_mixed` when they do not, and a
/// child whose diagnostics are unserved named in `layout`. The root
/// relabels `shard` and `address` with the relay's.
pub fn merge_shard_layouts(
    children: &[String],
    layouts: &[crate::pb::ShardLayoutDiagnostics],
) -> crate::pb::ShardLayoutDiagnostics {
    let mut merged = crate::pb::ShardLayoutDiagnostics {
        layout: format!("relay over {} children", layouts.len()),
        segment_pruning: !layouts.is_empty(),
        floor_sharing: !layouts.is_empty(),
        ..Default::default()
    };
    let mut notes = Vec::new();
    let mut partition: Option<String> = None;
    let mut fingerprint: Option<String> = None;
    let mut placement: Option<(bool, u64)> = None;
    for (child, layout) in layouts.iter().enumerate() {
        let addr = children.get(child).map_or("", String::as_str);
        let served = layout.layout == crate::snapshot_repository::LAYOUT_SEGMENTS
            || layout.layout == crate::node::LAYOUT_MEMORY;
        if !served {
            notes.push(format!(
                "child {child} ({addr}) unserved: {}",
                layout.layout
            ));
        }
        merged.rows = merged.rows.saturating_add(layout.rows);
        merged.live_rows = merged.live_rows.saturating_add(layout.live_rows);
        merged.tombstones = merged.tombstones.saturating_add(layout.tombstones);
        merged.tail_rows = merged.tail_rows.saturating_add(layout.tail_rows);
        merged.catalog_epoch = merged.catalog_epoch.max(layout.catalog_epoch);
        merged.segment_pruning &= layout.segment_pruning;
        merged.floor_sharing &= layout.floor_sharing;
        for segment in &layout.segments {
            let mut segment = segment.clone();
            segment.segment_id = format!("child{child}:{}", segment.segment_id);
            merged.segments.push(segment);
        }
        if served {
            match partition.as_ref() {
                None => partition = Some(layout.partition_key.clone()),
                Some(key) if *key != layout.partition_key => {
                    notes.push(format!(
                        "child {child} ({addr}) partition key {:?} differs from {key:?}",
                        layout.partition_key
                    ));
                }
                _ => {}
            }
            match fingerprint.as_ref() {
                None => fingerprint = Some(layout.scoring_fingerprint.clone()),
                Some(seen) if *seen != layout.scoring_fingerprint => {
                    notes.push(format!(
                        "child {child} ({addr}) scoring fingerprint {} differs from {seen}",
                        layout.scoring_fingerprint
                    ));
                }
                _ => {}
            }
            merged.placement_mixed |= layout.placement_mixed;
            match placement {
                None => placement = Some((layout.has_placement, layout.placement)),
                Some(first) if first != (layout.has_placement, layout.placement) => {
                    merged.placement_mixed = true;
                }
                _ => {}
            }
        }
    }
    merged.partition_key = partition.unwrap_or_default();
    merged.scoring_fingerprint = fingerprint.unwrap_or_default();
    if let Some((has_placement, code)) = placement {
        merged.has_placement = has_placement;
        merged.placement = code;
    }
    if !notes.is_empty() {
        merged.layout = format!("{}; {}", merged.layout, notes.join("; "));
    }
    merged
}

impl RelayService {
    pub(super) async fn handle_health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let (pinned, frozen) = self.pin();
        let reports = self.children_health(&frozen, timeout).await?;
        let merged = merge_health(&self.collection, frozen.node_addresses(), &reports)?;
        self.still_current(&pinned, "Health")?;
        Ok(Response::new(merged))
    }
}
