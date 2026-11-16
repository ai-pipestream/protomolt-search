//! Validated stream-open inputs and the per-shard fan-out setup.

use super::reader::StreamReader;
use super::*;

/// Validated inputs for opening a shard stream fan-out.
pub(crate) struct StreamOpen<'a> {
    request_id: &'a str,
    vector: &'a [f32],
    initial_floor: Option<f32>,
    candidate_depth: Option<u32>,
    collapse_parents: bool,
    filters: &'a RequestFilters,
    identity_limits: Option<crate::pb::StreamIdentityLimits>,
}

impl<'a> StreamOpen<'a> {
    pub(crate) fn new(
        request_id: &'a str,
        vector: &'a [f32],
        initial_floor: Option<f32>,
        candidate_depth: Option<u32>,
        collapse_parents: bool,
        filters: &'a RequestFilters,
        identity_limits: Option<crate::pb::StreamIdentityLimits>,
    ) -> Result<Self, Status> {
        if initial_floor.is_some_and(f32::is_nan) {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }
        if let Some(limits) = &identity_limits {
            crate::query_identity::validate_limits(limits)?;
            if collapse_parents {
                return Err(Status::invalid_argument(
                    "collapse streams do not support identity limits",
                ));
            }
        }
        Ok(Self {
            request_id,
            vector,
            initial_floor,
            candidate_depth,
            collapse_parents,
            filters,
            identity_limits,
        })
    }

    fn start(
        &self,
        read_context: Option<crate::pb::VectorReadContext>,
        floor_token: u64,
        filter: Option<crate::pb::FilterExpr>,
        floor_share_ppm: Option<u32>,
    ) -> StreamSearchRequest {
        let filters = self.filters;
        StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                read_context,
                request_id: self.request_id.to_string(),
                vector: self.vector.to_vec(),
                initial_floor: self.initial_floor,
                candidate_depth: self.candidate_depth,
                enumerate_max_rows: filters.enumerate_max_rows,
                patience_saturation_ppm: filters.patience_saturation_ppm,
                patience_hops: filters.patience_hops,
                floor_token,
                collapse_parents: self.collapse_parents,
                geo_filters: filters.geo.clone(),
                filter,
                identity_limits: self.identity_limits,
                floor_prune: filters.floor_prune,
                floor_k: filters.floor_k.or(self.candidate_depth),
                floor_share_ppm,
                floor_greediness_ppm: filters.floor_greediness_ppm,
                floor_clamp_min: filters.floor_clamp_min,
                include_rerank_candidate: filters.include_rerank_candidate,
            })),
        }
    }
}

impl CoordinatorServiceImpl {
    /// Open one `StreamSearch` per shard: Start flows through a held
    /// sender that later carries floor raises or an authoritative Stop. Each
    /// stream also gets a UDP token so both signals reach the shard first on
    /// the fast lossy lane and then on the gRPC stream.
    pub(crate) fn open_stream_fanout(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        candidate_depth: Option<u32>,
        collapse_parents: bool,
        filters: &RequestFilters,
    ) -> Result<StreamFanout, Status> {
        self.open_validated_stream(StreamOpen::new(
            request_id,
            vector,
            initial_floor,
            candidate_depth,
            collapse_parents,
            filters,
            None,
        )?)
    }

    /// Open a streaming fan-out with identity resolution; `candidate_depth`
    /// sets per-shard candidate walk depth (required by ANN providers).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_stream_fanout_with_identities(
        &self,
        request_id: &str,
        vector: &[f32],
        initial_floor: Option<f32>,
        candidate_depth: Option<u32>,
        collapse_parents: bool,
        filters: &RequestFilters,
        identity_limits: Option<crate::pb::StreamIdentityLimits>,
    ) -> Result<StreamFanout, Status> {
        self.open_stream_context(StreamOpen::new(
            request_id,
            vector,
            initial_floor,
            candidate_depth,
            collapse_parents,
            filters,
            identity_limits,
        )?)
    }

    pub(crate) fn open_stream_context(
        &self,
        query: StreamOpen<'_>,
    ) -> Result<StreamFanout, Status> {
        if query.identity_limits.is_none() {
            return self.open_stream_fanout(
                query.request_id,
                query.vector,
                query.initial_floor,
                query.candidate_depth,
                query.collapse_parents,
                query.filters,
            );
        }
        self.open_validated_stream(query)
    }

    fn open_validated_stream(&self, query: StreamOpen<'_>) -> Result<StreamFanout, Status> {
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        self.check_vector_scan(query.filters, query.collapse_parents)?;
        let admission = self.vector_read_barrier()?;
        let udp_socket = self.floor_socket().cloned();
        let (merged_tx, merged_rx) = mpsc::channel(4 * n_nodes);
        let mut floor_txs = Vec::with_capacity(n_nodes);
        let mut udp_lanes = Vec::with_capacity(n_nodes);
        let mut readers = tokio::task::JoinSet::new();
        let mask = self.vector_scan_mask(query.filters.tree.as_ref());
        let floor_shares = self.shard_floor_share_ppms(query.filters, mask.as_ref());
        for shard in 0..n_nodes {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                floor_txs.push(None);
                udp_lanes.push(None);
                continue;
            }
            let client = self.node_client(&self.node_addrs[shard])?;
            let lane = self
                .floor_target(&self.node_addrs[shard])
                .map(|target| (floor_token(), target));
            let (req_tx, req_rx) = mpsc::channel(64);
            req_tx
                .try_send(
                    query.start(
                        admission
                            .as_ref()
                            .map(|barrier| barrier.context(shard))
                            .transpose()?,
                        lane.map_or(0, |(token, _)| token),
                        Self::shard_filter_tree(query.filters, mask.as_ref(), shard),
                        floor_shares.get(shard).copied().flatten(),
                    ),
                )
                .expect("fresh channel accepts the Start message");
            floor_txs.push(Some(req_tx));
            udp_lanes.push(lane);
            readers.spawn(
                StreamReader {
                    shard,
                    client,
                    requests: req_rx,
                    merged: merged_tx.clone(),
                    admission: admission.clone(),
                    deadline: self.limits().shard_deadline.filter(|_| admission.is_some()),
                }
                .run(),
            );
        }
        Ok(StreamFanout {
            scoped: admission.is_some(),
            readers,
            merged_rx,
            floor_txs,
            udp_lanes,
            udp_socket,
            udp_key: self.udp_hmac_key.clone(),
            udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        })
    }
}
