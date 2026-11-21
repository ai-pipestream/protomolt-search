//! One relayed `SearchShard`'s merged answer: the children's terminal
//! lists, counters, and known flags, and the events that build it.

use tokio::sync::mpsc;
use tonic::Status;

use crate::pb::{
    search_shard_request, search_shard_response, FloorUpdate, SearchShardRequest,
    SearchShardResponse, ShardScanStats,
};

use super::map::MapSnapshot;
use super::merge_util::{add_count, add_count32, KnownFlags};

type ParentTx = mpsc::Sender<Result<SearchShardResponse, Status>>;

#[allow(clippy::large_enum_variant)]
pub(super) enum ShardEvent {
    Child(usize, Result<Option<SearchShardResponse>, Status>),
    Parent(Option<Result<SearchShardRequest, Status>>),
}

pub(super) fn shard_floor(floor: f32) -> SearchShardRequest {
    SearchShardRequest {
        payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
            floor,
        })),
    }
}

/// The scan's merged answer so far.
pub(super) struct ShardScan {
    pub(super) children: Vec<String>,
    pub(super) child_nonempty: Vec<bool>,
    pub(super) exact_fp32: bool,
    pub(super) missing_parts: bool,
    pub(super) hits: Vec<crate::pb::ScoredHit>,
    pub(super) stats: Option<ShardScanStats>,
    pub(super) done: Vec<bool>,
    pub(super) remaining: usize,
    pub(super) known: KnownFlags,
    pub(super) mask: Option<crate::placement::ShardMask>,
}

pub(super) fn clear_part_counts(stats: &mut ShardScanStats) {
    stats.parts_total = 0;
    stats.parts_skipped = 0;
    stats.parts_walked = 0;
    stats.parts_enumerated = 0;
    stats.parts_scanned = 0;
    stats.parts_staged = 0;
}

pub(super) fn check_child_part_stats(
    share: &ShardScanStats,
    child: usize,
    exact_fp32: bool,
) -> Result<(), Status> {
    if share.parts_skipped > share.parts_total {
        return Err(Status::internal(format!(
            "relay: child {child} reported {} skipped parts out of {}",
            share.parts_skipped, share.parts_total
        )));
    }
    let active = share.parts_total - share.parts_skipped;
    let executed = share
        .parts_walked
        .saturating_add(share.parts_enumerated)
        .saturating_add(share.parts_scanned)
        .saturating_add(share.parts_staged);
    if executed > active {
        return Err(Status::internal(format!(
            "relay: child {child} executed {executed} parts across {active} active parts"
        )));
    }
    if exact_fp32
        && share.parts_total > 0
        && ((active == 0 && share.fp32_rows_scored != 0)
            || share.fp32_rows_scored < u64::from(active))
    {
        return Err(Status::internal(format!(
            "relay: child {child} scored {} FP32 rows across {active} active parts",
            share.fp32_rows_scored
        )));
    }
    Ok(())
}

/// Add one child's scan counters into the relay's, each with a check.
pub(super) fn add_scan_stats(
    acc: &mut ShardScanStats,
    share: ShardScanStats,
    child: usize,
) -> Result<(), Status> {
    add_count32(
        &mut acc.chunk_calls,
        share.chunk_calls,
        child,
        "chunk calls",
    )?;
    add_count(
        &mut acc.candidates_collected,
        share.candidates_collected,
        child,
        "candidate count",
    )?;
    add_count(
        &mut acc.floors_published,
        share.floors_published,
        child,
        "published floor count",
    )?;
    add_count(
        &mut acc.floor_updates_applied,
        share.floor_updates_applied,
        child,
        "applied floor count",
    )?;
    add_count(
        &mut acc.floors_offered,
        share.floors_offered,
        child,
        "offered floor count",
    )?;
    add_count32(
        &mut acc.segments_total,
        share.segments_total,
        child,
        "segment count",
    )?;
    add_count32(
        &mut acc.segments_skipped,
        share.segments_skipped,
        child,
        "skipped segment count",
    )?;
    let mut regimes = crate::vector::FilterRegimeCounts::from_proto(acc.filter_regimes.as_ref());
    regimes.add(crate::vector::FilterRegimeCounts::from_proto(
        share.filter_regimes.as_ref(),
    ));
    acc.filter_regimes = regimes.to_proto();
    add_count(
        &mut acc.tail_enumerated_rows,
        share.tail_enumerated_rows,
        child,
        "tail enumerated row count",
    )?;
    add_count32(
        &mut acc.patience_stopped,
        share.patience_stopped,
        child,
        "patience stopped count",
    )?;
    add_count32(
        &mut acc.floor_pruned,
        share.floor_pruned,
        child,
        "floor pruned count",
    )?;
    add_part_and_fp32_stats(acc, &share, child)
}

fn add_part_and_fp32_stats(
    acc: &mut ShardScanStats,
    share: &ShardScanStats,
    child: usize,
) -> Result<(), Status> {
    add_count32(
        &mut acc.parts_total,
        share.parts_total,
        child,
        "parts total",
    )?;
    add_count32(
        &mut acc.parts_skipped,
        share.parts_skipped,
        child,
        "parts skipped",
    )?;
    add_count32(
        &mut acc.parts_walked,
        share.parts_walked,
        child,
        "parts walked",
    )?;
    add_count32(
        &mut acc.parts_enumerated,
        share.parts_enumerated,
        child,
        "parts enumerated",
    )?;
    add_count32(
        &mut acc.parts_scanned,
        share.parts_scanned,
        child,
        "parts scanned",
    )?;
    add_count32(
        &mut acc.parts_staged,
        share.parts_staged,
        child,
        "parts staged",
    )?;
    add_count(
        &mut acc.fp32_rows_scored,
        share.fp32_rows_scored,
        child,
        "fp32 rows scored",
    )?;
    add_count(
        &mut acc.fp32_logical_bytes,
        share.fp32_logical_bytes,
        child,
        "fp32 logical bytes",
    )?;
    add_count(
        &mut acc.fp32_pages_touched,
        share.fp32_pages_touched,
        child,
        "fp32 pages touched",
    )?;
    add_count32(&mut acc.fp32_tasks, share.fp32_tasks, child, "fp32 tasks")
}

impl ShardScan {
    /// A child's terminal list: once per child, its flags and counters
    /// merged, its hits added to the union.
    fn accept_done(
        &mut self,
        child: usize,
        share: crate::pb::SearchShardDone,
    ) -> Result<(), Status> {
        if self.done[child] {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) sent a terminal message twice",
                self.children[child]
            )));
        }
        self.done[child] = true;
        self.remaining -= 1;
        self.known.merge(
            child,
            self.mask.as_ref(),
            &share.geo_columns_known,
            &share.filter_columns_known,
        )?;
        let nonempty = self.child_nonempty[child]
            || !share.hits.is_empty()
            || share.stats.as_ref().is_some_and(|s| {
                s.candidates_collected > 0 || s.chunk_calls > 0 || s.fp32_rows_scored > 0
            });
        if nonempty && share.stats.as_ref().map_or(0, |s| s.parts_total) == 0 {
            self.missing_parts = true;
        }
        if let Some(stats) = share.stats {
            check_child_part_stats(&stats, child, self.exact_fp32)?;
            add_scan_stats(
                self.stats.get_or_insert_with(Default::default),
                stats,
                child,
            )?;
        }
        self.hits.extend(share.hits);
        Ok(())
    }

    pub(super) fn finish_stats(&mut self) -> Option<ShardScanStats> {
        let mut stats = self.stats.take();
        if self.missing_parts {
            if let Some(s) = stats.as_mut() {
                clear_part_counts(s);
            }
        }
        stats
    }
}

/// Act on one event; `Ok(false)` ends the scan without an answer (the
/// parent is gone).
pub(super) async fn on_shard_event(
    event: ShardEvent,
    scan: &mut ShardScan,
    senders: &[Option<mpsc::Sender<SearchShardRequest>>],
    tx: &ParentTx,
) -> Result<bool, Status> {
    use search_shard_request::Payload as Down;
    use search_shard_response::Payload as Up;
    match event {
        ShardEvent::Parent(Some(Ok(SearchShardRequest {
            payload: Some(Down::FloorUpdate(update)),
        }))) => {
            if !update.floor.is_nan() {
                for sender in senders.iter().flatten() {
                    // Floors are monotone: a raise a full channel drops is
                    // superseded by the next, and a closed child is done.
                    let _ = sender.try_send(shard_floor(update.floor));
                }
            }
            Ok(true)
        }
        ShardEvent::Parent(Some(Ok(SearchShardRequest {
            payload: Some(Down::Start(_)),
        }))) => Err(Status::invalid_argument(
            "relay: a stream carries one StartShardSearch; another arrived",
        )),
        ShardEvent::Parent(Some(Ok(_))) | ShardEvent::Parent(None) => Ok(true),
        ShardEvent::Parent(Some(Err(status))) => Err(Status::cancelled(format!(
            "relay: the parent's request leg failed: {}",
            status.message()
        ))),
        ShardEvent::Child(
            _,
            Ok(Some(SearchShardResponse {
                payload: Some(Up::FloorUpdate(update)),
            })),
        ) => {
            // A closed parent: nobody reads what the children still scan for.
            let up = SearchShardResponse {
                payload: Some(Up::FloorUpdate(update)),
            };
            Ok(tx.send(Ok(up)).await.is_ok())
        }
        ShardEvent::Child(
            child,
            Ok(Some(SearchShardResponse {
                payload: Some(Up::Done(share)),
            })),
        ) => {
            scan.accept_done(child, share)?;
            Ok(true)
        }
        ShardEvent::Child(_, Ok(Some(_))) => Ok(true),
        ShardEvent::Child(child, Ok(None)) if !scan.done[child] => Err(Status::data_loss(format!(
            "relay: child {child} ({}) closed its stream before Done",
            scan.children[child]
        ))),
        ShardEvent::Child(_, Ok(None)) => Ok(true),
        ShardEvent::Child(_, Err(status)) => Err(status),
    }
}

pub(super) fn map_moved(pinned: &MapSnapshot, revision: u64) -> Status {
    Status::failed_precondition(format!(
        "relay: the shard map moved from revision {} (generation {}) to revision {revision} \
         during SearchShard; the scan was opened under the older map and is not completed; \
         retry under the current one",
        pinned.control_revision, pinned.topology_generation
    ))
}
