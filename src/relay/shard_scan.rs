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
    pub(super) hits: Vec<crate::pb::ScoredHit>,
    pub(super) stats: Option<ShardScanStats>,
    pub(super) done: Vec<bool>,
    pub(super) remaining: usize,
    pub(super) known: KnownFlags,
    pub(super) mask: Option<crate::placement::ShardMask>,
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
        if let Some(stats) = share.stats {
            add_scan_stats(
                self.stats.get_or_insert_with(Default::default),
                stats,
                child,
            )?;
        }
        self.hits.extend(share.hits);
        Ok(())
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
