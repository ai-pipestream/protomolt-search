//! Floor and finish signaling, finish windows and per-shard floor shares.

use super::*;

impl CoordinatorServiceImpl {
    /// Push a floor raise to every still-open stream of `fanout`: UDP
    /// first (the fast lossy copy), then the reliable stream. Both are
    /// monotone max-folds shard-side, so double delivery and loss are
    /// equally free; a full stream channel just means the next raise
    /// supersedes this one.
    pub(crate) fn push_stream_floor(&self, fanout: &StreamFanout, floor: f32) {
        let update = StreamSearchRequest {
            payload: Some(stream_search_request::Payload::FloorUpdate(FloorUpdate {
                floor,
            })),
        };
        for (si, tx) in fanout.floor_txs.iter().enumerate() {
            let Some(tx) = tx.as_ref() else {
                continue;
            };
            if let (Some(socket), Some((token, target))) =
                (fanout.udp_socket.as_deref(), fanout.udp_lanes[si])
            {
                fanout.send_signal(
                    socket,
                    target,
                    crate::stream_signal::encode_floor(token, floor),
                );
            }
            let _ = tx.try_send(update.clone());
        }
    }

    /// Push a graceful finish request (`FinishStreamSearch` + advisory UDP
    /// `FINISH`) to every shard of `fanout` that has not yet delivered its
    /// scan summary. Unlike `cancel`, finished ANN shards flush buffered hits
    /// `>= floor` and report `completed = true` so `IdentityReady` succeeds.
    pub(crate) async fn push_stream_finish(
        &self,
        fanout: &StreamFanout,
        done: &[Option<StreamSearchSummary>],
        deadline: bool,
    ) {
        let req = StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Finish(
                crate::pb::FinishStreamSearch { deadline },
            )),
        };
        let mut pending = Vec::new();
        for (si, tx) in fanout.floor_txs.iter().enumerate() {
            if done.get(si).is_some_and(Option::is_some) {
                continue;
            }
            let Some(tx) = tx.as_ref() else {
                continue;
            };
            if let (Some(socket), Some((token, target))) =
                (fanout.udp_socket.as_deref(), fanout.udp_lanes[si])
            {
                fanout.send_signal(
                    socket,
                    target,
                    crate::stream_signal::encode_finish(token, deadline),
                );
            }
            match tx.try_send(req.clone()) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(_)) => pending.push(tx.clone()),
            }
        }
        for tx in pending {
            let _ = tokio::time::timeout(Duration::from_millis(50), tx.send(req.clone())).await;
        }
    }

    pub(in crate::coordinator) fn resolve_finish_windows(
        &self,
        filters: &RequestFilters,
    ) -> Result<(Option<Duration>, Option<Duration>), Status> {
        let parse_opt = |name: &str, raw: Option<u32>, fallback: Option<Duration>| match raw {
            Some(us) => {
                if !(1..=crate::dense_policy::MAX_FINISH_SETTLE_US).contains(&us) {
                    return Err(Status::invalid_argument(format!(
                        "{name} {us} is outside 1..={}",
                        crate::dense_policy::MAX_FINISH_SETTLE_US
                    )));
                }
                Ok(Some(Duration::from_micros(u64::from(us))))
            }
            None => Ok(fallback),
        };
        Ok((
            parse_opt(
                "finish_settle_us",
                filters.finish_settle_us,
                self.stream_finish_settle,
            )?,
            parse_opt(
                "finish_deadline_us",
                filters.finish_deadline_us,
                self.stream_finish_deadline,
            )?,
        ))
    }

    /// Best-effort refresh of per-shard row counts (`self.shard_vector_rows`)
    /// when a caller invokes a fan-out helper directly without going through
    /// `SearchService::search` or `resolve_dense_execution`. Errors on
    /// unreachable or unconfigured shards are ignored here so the subsequent
    /// RPC surfaces its canonical failure untouched.
    pub(crate) async fn ensure_shard_vector_rows(&self) {
        self.shard_vector_row_probe().wait().await;
    }

    /// Per-shard proportional share (`0..=1_000_000`) of the parent's
    /// `floor_share_ppm` across active (non-skipped) shards.
    pub(crate) fn shard_floor_share_ppms(
        &self,
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Vec<Option<u32>> {
        let n_nodes = self.node_addrs.len();
        let active: Vec<usize> = (0..n_nodes)
            .filter(|&shard| !mask.is_some_and(|m| m.skipped[shard]))
            .collect();
        let mut out = vec![None; n_nodes];
        if active.is_empty() {
            return out;
        }
        let parent_share = u128::from(filters.parent_floor_share_ppm.unwrap_or(1_000_000));
        let cached: Option<Vec<(usize, u64)>> = {
            let cache = self
                .shard_vector_rows
                .lock()
                .expect("shard vector row cache poisoned");
            active
                .iter()
                .map(|&shard| {
                    cache
                        .get(&self.node_addrs[shard])
                        .map(|&rows| (shard, rows))
                })
                .collect()
        };
        if let Some(rows_by_shard) = cached {
            let total_rows: u128 = rows_by_shard
                .iter()
                .map(|&(_, rows)| u128::from(rows))
                .sum();
            for (shard, rows) in rows_by_shard {
                let share = if total_rows == 0 || rows == 0 {
                    0
                } else {
                    (parent_share * u128::from(rows))
                        .div_ceil(total_rows)
                        .min(1_000_000) as u32
                };
                out[shard] = Some(share);
            }
        } else {
            let uniform = parent_share.div_ceil(active.len() as u128).min(1_000_000) as u32;
            for shard in active {
                out[shard] = Some(uniform);
            }
        }
        out
    }
}
