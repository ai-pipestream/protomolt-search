use super::merge_values::health_span;
use super::*;

/// What one relay token stands for: the exact children and the epoch
/// each reported when the token was allocated. The parent's claim on
/// the token translates into one claim per child, which the child
/// enforces itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenTuple {
    pub collection: String,
    /// The map revision the children were read under.
    pub control_revision: u64,
    pub topology_generation: u64,
    pub children: Vec<String>,
    pub epochs: Vec<StatsClaim>,
}

/// Relay tokens (`docs/relay-coordinators.md`, "The epoch token"): a
/// nonzero allocation bound to a [`TokenTuple`], reused while the tuple
/// repeats so a parent's stats cache keeps hitting, replaced the moment
/// any child's lifetime or epoch moves, and retained for [`RETAINED_TOKENS`]
/// tuples. The relay's separate 32-byte identity fences a restart; the legacy
/// numeric clock prefix below is not sufficient to identify a lifetime.
pub struct TokenRegistry {
    incarnation: u64,
    pub(super) counter: u32,
    pub(super) entries: VecDeque<(u64, TokenTuple)>,
}

impl TokenRegistry {
    pub(super) fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            counter: 0,
            entries: VecDeque::new(),
        }
    }

    pub(super) fn allocate(&mut self, tuple: TokenTuple) -> Result<u64, Status> {
        if let Some(position) = self.entries.iter().position(|(_, t)| *t == tuple) {
            let entry = self.entries.remove(position).expect("position from iter");
            let token = entry.0;
            self.entries.push_back(entry);
            return Ok(token);
        }
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("relay statistics token space exhausted"))?;
        let token = (self.incarnation << 32) | u64::from(self.counter);
        self.entries.push_back((token, tuple));
        while self.entries.len() > RETAINED_TOKENS {
            self.entries.pop_front();
        }
        Ok(token)
    }

    pub(super) fn lookup(&self, token: u64) -> Option<&TokenTuple> {
        self.entries
            .iter()
            .rev()
            .find(|(t, _)| *t == token)
            .map(|(_, tuple)| tuple)
    }
}

/// One immutable reading of a relay's shard map: the children in shard
/// order with their placement codes, and the placement tree of that
/// generation when the map has one.
#[derive(Debug, Clone)]
pub struct RelayMap {
    pub routes: Vec<TopologyRoute>,
    pub placement: Option<Arc<crate::placement::Placement>>,
}

impl RelayMap {
    /// Child addresses in shard order.
    pub fn children(&self) -> Vec<String> {
        self.routes.iter().map(|route| route.addr.clone()).collect()
    }
}

/// A map reading stamped with where it came from. `control_revision` is
/// the number a decision pins; `topology_generation` is what the shards
/// and the parent see on the wire. Under the file-polled source the two
/// are the same number.
#[derive(Debug, Clone)]
pub struct MapSnapshot {
    pub control_revision: u64,
    pub topology_generation: u64,
    pub map: Arc<RelayMap>,
}

/// Where a relay's shard map comes from (`docs/relay-coordinators.md`,
/// "Map interface"). Minimal on purpose: the current reading, and a
/// receiver whose value changes whenever the current revision does.
pub trait MapSource: Send + Sync {
    /// The current reading. Cheap; called once per decision.
    fn current(&self) -> MapSnapshot;
    /// A receiver that changes whenever the current revision changes;
    /// its value is the new control revision.
    fn changes(&self) -> watch::Receiver<u64>;
}

/// The file-polled map as a [`MapSource`]: the coordinator's live
/// topology (or its static shard set), with the topology generation as
/// the control revision and the coordinator's publication watch as the
/// change notification. Every reading is one frozen snapshot, so the
/// children, their codes, and the generation come from one publication.
pub struct CoordinatorMapSource {
    coordinator: Arc<CoordinatorServiceImpl>,
}

impl CoordinatorMapSource {
    pub fn new(coordinator: Arc<CoordinatorServiceImpl>) -> Self {
        Self { coordinator }
    }
}

impl MapSource for CoordinatorMapSource {
    fn current(&self) -> MapSnapshot {
        let frozen = self
            .coordinator
            .request_snapshot()
            .unwrap_or_else(|| self.coordinator.as_ref().clone());
        let generation = frozen.current_topology_generation();
        MapSnapshot {
            control_revision: generation,
            topology_generation: generation,
            map: Arc::new(RelayMap {
                routes: frozen.current_topology_routes(),
                placement: frozen.current_placement(),
            }),
        }
    }

    fn changes(&self) -> watch::Receiver<u64> {
        self.coordinator.topology_changes()
    }
}
/// Sums carried across the children's terminal summaries.
#[derive(Default)]
pub(super) struct StreamTotals {
    emitted: u64,
    blocks_scanned: u64,
    floor_raises_applied: u64,
    segments_total: u32,
    segments_skipped: u32,
}

pub(super) enum StreamEvent {
    Message(Option<(usize, StreamSearchResponse)>),
    Floor(f32),
    Cancel,
    Deadline,
    MapMoved(u64),
}

/// The forwarding loop of one relayed `StreamSearch`: children's batches
/// go up untouched, the parent's floor and cancel go down, and the
/// relay's summary follows the last child's.
#[allow(clippy::too_many_arguments)]
pub(super) async fn relay_stream(
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    mut map_changes: watch::Receiver<u64>,
    start: crate::pb::StartStreamSearch,
    filters: RequestFilters,
    signals: Arc<RelaySignals>,
    tx: mpsc::Sender<Result<StreamSearchResponse, Status>>,
    deadline: Option<tokio::time::Instant>,
    mut input: tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>>,
    identity_ready: Arc<AtomicBool>,
) -> Result<(), Status> {
    // A publication racing the pin: the receiver may already hold a newer
    // value than the reading, which the first poll below reports.
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
    let n = coordinator.node_addresses().len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let identity_limits = start.identity_limits;
    if let Some(limits) = identity_limits.as_ref() {
        crate::query_identity::validate_limits(limits)?;
    }
    let stride = if start.collapse_parents { 20 } else { 12 };
    let mask = coordinator.shard_mask(filters.tree.as_ref());
    let mut fanout: StreamFanout = coordinator.open_stream_fanout_with_identities(
        &start.request_id,
        &start.vector,
        start.initial_floor,
        start.collapse_parents,
        &filters,
        identity_limits,
    )?;
    let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n];
    let mut remaining = n;
    for (shard, summary) in summaries.iter_mut().enumerate() {
        if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
            *summary = Some(StreamSearchSummary {
                completed: true,
                ..Default::default()
            });
            remaining -= 1;
        }
    }
    let mut terminal = summaries.clone();
    let mut ranges: Vec<Option<crate::pb::StreamIdentityRange>> = vec![None; n];
    let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
    let mut geo_known = vec![false; filters.geo.len()];
    let mut tree_known = vec![false; leaves];
    if let Some(mask) = mask.as_ref() {
        for &leaf in &mask.known {
            if let Some(flag) = tree_known.get_mut(leaf) {
                *flag = true;
            }
        }
    }
    let mut totals = StreamTotals::default();
    let mut scoring_fingerprint: Option<String> = None;
    let mut last_floor = start.initial_floor.unwrap_or(f32::NEG_INFINITY);
    let mut floor_rx = signals.floor.subscribe();
    let mut floor_open = true;
    let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));

    while remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = signals.cancel.notified(), if !signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = async { true }, if signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => StreamEvent::Deadline,
            moved = map_changes.changed() => match moved {
                Ok(()) => StreamEvent::MapMoved(*map_changes.borrow_and_update()),
                Err(_) => {
                    // The source is gone with its authority; nothing
                    // will move again, and the pinned reading stands.
                    map_changes = watch::channel(pinned.control_revision).1;
                    continue;
                }
            },
            changed = floor_rx.changed(), if floor_open => match changed {
                Ok(()) => StreamEvent::Floor(*floor_rx.borrow_and_update()),
                Err(_) => {
                    floor_open = false;
                    continue;
                }
            },
            message = fanout.next_message(&terminal) => match message {
                Ok(pair) => StreamEvent::Message(pair),
                Err(status) => return fanout.cancel_with(status).await,
            },
        };
        match event {
            StreamEvent::Cancel => {
                fanout.cancel().await;
                let summary = StreamSearchSummary {
                    completed: false,
                    emitted: totals.emitted,
                    blocks_scanned: totals.blocks_scanned,
                    floor_raises_applied: totals.floor_raises_applied,
                    geo_columns_known: geo_known,
                    filter_columns_known: tree_known,
                    scoring_fingerprint: scoring_fingerprint.unwrap_or_default(),
                    segments_total: totals.segments_total,
                    segments_skipped: totals.segments_skipped,
                };
                let _ = tx
                    .send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(summary)),
                    }))
                    .await;
                return Ok(());
            }
            StreamEvent::Deadline => {
                return fanout
                    .cancel_with(Status::deadline_exceeded(
                        "relay: the parent's deadline passed before every child completed",
                    ))
                    .await;
            }
            StreamEvent::MapMoved(revision) => {
                if revision == pinned.control_revision {
                    continue;
                }
                return fanout
                    .cancel_with(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during StreamSearch; the stream was opened under \
                         the older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )))
                    .await;
            }
            StreamEvent::Floor(floor) => {
                if floor > last_floor {
                    last_floor = floor;
                    coordinator.push_stream_floor(&fanout, floor);
                }
            }
            StreamEvent::Message(None) => continue,
            StreamEvent::Message(Some((shard, message))) => {
                if summaries[shard].is_some() {
                    return fanout
                        .cancel_with(Status::internal(
                            "relay: child message after scan certificate",
                        ))
                        .await;
                }
                let payload = match message.payload {
                    Some(stream_search_response::Payload::IdentityReady(ready))
                        if identity_limits.is_some() =>
                    {
                        let summary = ready.scan.ok_or_else(|| {
                            Status::internal("relay: missing readiness certificate")
                        })?;
                        if let Some(range) = ready.range.as_ref() {
                            if range.first_id > range.last_id
                                || ranges.iter().flatten().any(|other| {
                                    range.first_id <= other.last_id
                                        && other.first_id <= range.last_id
                                })
                            {
                                return fanout
                                    .cancel_with(Status::failed_precondition(
                                        "relay: overlapping or invalid captured identity ranges",
                                    ))
                                    .await;
                            }
                        } else if summary.emitted != 0 {
                            return fanout
                                .cancel_with(Status::failed_precondition(
                                    "relay: candidates without a captured identity range",
                                ))
                                .await;
                        }
                        ranges[shard] = ready.range;
                        Some(stream_search_response::Payload::Summary(summary))
                    }
                    Some(stream_search_response::Payload::Summary(_))
                        if identity_limits.is_some() =>
                    {
                        return fanout
                            .cancel_with(Status::failed_precondition(
                                "relay: child lacks snapshot-bound identity support",
                            ))
                            .await;
                    }
                    payload => payload,
                };
                match payload {
                    Some(stream_search_response::Payload::Batch(batch)) => {
                        if batch.hits.is_empty() || batch.hits.len() % stride != 0 {
                            let status = Status::internal(format!(
                                "relay: child {shard} sent a batch of {} bytes, not a multiple of \
                             the {stride}-byte record",
                                batch.hits.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if tx
                            .send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::Batch(batch)),
                            }))
                            .await
                            .is_err()
                        {
                            // The parent is gone: nobody reads what the
                            // children still scan for.
                            fanout.cancel().await;
                            return Ok(());
                        }
                    }
                    Some(stream_search_response::Payload::Summary(summary)) => {
                        if !summary.completed {
                            let status = Status::internal(format!(
                                "relay: child {shard} stopped before completing its scan"
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if summary.scoring_fingerprint.is_empty() {
                            let status = Status::failed_precondition(format!(
                            "relay: child {shard} completed without a vector scoring fingerprint"
                        ));
                            return fanout.cancel_with(status).await;
                        }
                        match scoring_fingerprint.as_ref() {
                            Some(expected) if expected != &summary.scoring_fingerprint => {
                                let status = Status::failed_precondition(format!(
                                    "relay: child {shard} vector scoring fingerprint {} differs \
                                 from {expected}",
                                    summary.scoring_fingerprint
                                ));
                                return fanout.cancel_with(status).await;
                            }
                            None => scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                            _ => {}
                        }
                        if summary.geo_columns_known.len() != geo_known.len()
                            || summary.filter_columns_known.len() != tree_known.len()
                        {
                            let status = Status::internal(format!(
                                "relay: child {shard} answered {} geo and {} filter flags for {} \
                             and {}",
                                summary.geo_columns_known.len(),
                                summary.filter_columns_known.len(),
                                geo_known.len(),
                                tree_known.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        for (acc, k) in geo_known.iter_mut().zip(&summary.geo_columns_known) {
                            *acc |= *k;
                        }
                        for (acc, k) in tree_known.iter_mut().zip(&summary.filter_columns_known) {
                            *acc |= *k;
                        }
                        totals.emitted += summary.emitted;
                        totals.blocks_scanned += summary.blocks_scanned;
                        totals.floor_raises_applied += summary.floor_raises_applied;
                        totals.segments_total =
                            totals.segments_total.saturating_add(summary.segments_total);
                        totals.segments_skipped = totals
                            .segments_skipped
                            .saturating_add(summary.segments_skipped);
                        summaries[shard] = Some(summary.clone());
                        if identity_limits.is_none() {
                            terminal[shard] = Some(summary);
                            fanout.mark_completed(shard);
                        }
                        remaining -= 1;
                    }
                    _ => {
                        return fanout
                            .cancel_with(Status::internal("relay: unexpected identity message"))
                            .await
                    }
                }
            }
        }
    }
    let summary = StreamSearchSummary {
        completed: true,
        emitted: totals.emitted,
        blocks_scanned: totals.blocks_scanned,
        floor_raises_applied: totals.floor_raises_applied,
        geo_columns_known: geo_known,
        filter_columns_known: tree_known,
        // Every child agreed, or the loop above refused; a relay over
        // masked-out children only has no fingerprint to report and the
        // parent refuses that, as it refuses a node without one.
        scoring_fingerprint: scoring_fingerprint.unwrap_or_default(),
        segments_total: totals.segments_total,
        segments_skipped: totals.segments_skipped,
    };
    if let Some(limits) = identity_limits {
        let range =
            ranges
                .iter()
                .flatten()
                .fold(None::<crate::pb::StreamIdentityRange>, |acc, range| {
                    Some(match acc {
                        None => *range,
                        Some(acc) => crate::pb::StreamIdentityRange {
                            first_id: acc.first_id.min(range.first_id),
                            last_id: acc.last_id.max(range.last_id),
                        },
                    })
                });
        let exchange = async {
            identity_ready.store(true, Ordering::Release);
            tx.send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::IdentityReady(
                    crate::pb::StreamIdentityReady {
                        scan: Some(summary.clone()),
                        range,
                    },
                )),
            }))
            .await
            .map_err(|_| Status::cancelled("relay: parent response closed"))?;
            let selection = (&mut input)
                .await
                .map_err(|e| Status::internal(format!("relay: request task failed: {e}")))??
                .ok_or_else(|| Status::cancelled("relay: selection stopped"))?;
            if selection.vector_ids.len() > limits.max_rows as usize {
                return Err(Status::resource_exhausted(
                    "relay: identity selection exceeds max_rows",
                ));
            }
            let mut seen = std::collections::HashSet::new();
            let mut hits = Vec::with_capacity(selection.vector_ids.len());
            for id in selection.vector_ids {
                if !seen.insert(id) {
                    return Err(Status::invalid_argument(
                        "relay: identity selection repeats an ID",
                    ));
                }
                let shard = ranges
                    .iter()
                    .position(|range| {
                        range
                            .as_ref()
                            .is_some_and(|r| r.first_id <= id && id <= r.last_id)
                    })
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "relay: identity ID is outside captured child ranges",
                        )
                    })?;
                hits.push(crate::merge::MergedHit {
                    shard: shard as u32,
                    vector_id: id,
                    score: 0.0,
                });
            }
            let mut identities = fanout
                .resolve_identities(&hits, &summaries, &mut terminal, &limits)
                .await?;
            let rows = hits
                .into_iter()
                .map(|hit| crate::pb::StreamIdentity {
                    vector_id: hit.vector_id,
                    identity: identities
                        .remove(&(hit.shard, hit.vector_id))
                        .expect("validated child response"),
                })
                .collect();
            let response = StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Identities(
                    crate::pb::StreamIdentities { rows },
                )),
            };
            if prost::Message::encoded_len(&response) > limits.max_response_bytes as usize {
                return Err(Status::resource_exhausted(
                    "relay: identity response exceeds max_response_bytes",
                ));
            }
            tx.send(Ok(response))
                .await
                .map_err(|_| Status::cancelled("relay: parent response closed"))?;
            tx.send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary.clone())),
            }))
            .await
            .map_err(|_| Status::cancelled("relay: parent response closed"))
        };
        let result = tokio::select! {
            _ = signals.cancel.notified() => Err(Status::cancelled("relay: identity selection stopped")),
            _ = async {}, if signals.cancelled.load(Ordering::Acquire) => Err(Status::cancelled("relay: identity selection stopped")),
            _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Err(Status::deadline_exceeded("relay: parent deadline passed during identity selection")),
            _ = async { loop {
                if map_changes.changed().await.is_err() { std::future::pending::<()>().await; }
                if *map_changes.borrow_and_update() != pinned.control_revision { break; }
            }} => Err(Status::failed_precondition("relay: map moved during identity selection")),
            result = tokio::time::timeout(Duration::from_millis(u64::from(limits.timeout_ms)), exchange) =>
                result.unwrap_or_else(|_| Err(Status::deadline_exceeded("relay: identity selection timed out"))),
        };
        if let Err(status) = result {
            fanout.cancel().await;
            if signals.cancelled.load(Ordering::Acquire) {
                let _ = tx
                    .send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(
                            StreamSearchSummary {
                                completed: false,
                                ..summary
                            },
                        )),
                    }))
                    .await;
                return Ok(());
            }
            return Err(status);
        }
    } else {
        let _ = tx
            .send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary)),
            }))
            .await;
    }
    Ok(())
}
/// A child's error as the parent must read it. The stale-epoch prefix
/// stays at the front so the parent's retry rule fires; everything else
/// gets the child named after the code.
pub(super) fn child_error(shard: usize, addr: &str, route: &str, status: Status) -> Status {
    let message = status.message();
    let text = match message.strip_prefix(STALE_STATS_EPOCH) {
        Some(rest) => format!(
            "{STALE_STATS_EPOCH}: relay child {shard} ({addr}) {route}{}",
            rest
        ),
        None => format!("relay child {shard} ({addr}) {route}: {message}"),
    };
    Status::new(status.code(), text)
}
/// Which child holds a global id: the one whose slot range (from its
/// health report) contains it. Contiguity was checked at startup; a gap
/// that appeared since refuses by name here.
pub(super) fn child_of_id(ranges: &[(u64, u64)], id: u64) -> Option<usize> {
    ranges
        .iter()
        .position(|&(offset, span)| id >= offset && id - offset < span)
}
/// The slot range of each child, `(offset, span)`, from the children's
/// health reports, with the contiguity rule applied: a gap or an overlap
/// refuses by name, because an id routed by these ranges, or a bitmap
/// laid out over them, would otherwise be wrong without a word.
pub(super) fn child_ranges(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<Vec<(u64, u64)>, Status> {
    merge_health(collection, children, reports)?;
    Ok(reports
        .iter()
        .map(|r| (r.slot_offset, health_span(r)))
        .collect())
}

/// Sort ids into the child whose slot range holds each. An id in no
/// child's range is another shard's and is dropped, which is what a
/// node does with an id outside its own range (the boolean planner and
/// the exact rerank send every shard every candidate; the cascade routes
/// by shard, and a relay is one shard to it).
pub(super) fn route_ids(ranges: &[(u64, u64)], ids: &[u64]) -> Result<Vec<Vec<u64>>, Status> {
    let mut by_child: Vec<Vec<u64>> = vec![Vec::new(); ranges.len()];
    for &id in ids {
        if let Some(child) = child_of_id(ranges, id) {
            by_child[child].push(id);
        }
    }
    Ok(by_child)
}

/// Overflow-checked `u64` sum for a counter the children report.
pub(super) fn add_count(acc: &mut u64, share: u64, child: usize, what: &str) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// Overflow-checked `u32` sum for a counter the children report.
pub(super) fn add_count32(
    acc: &mut u32,
    share: u32,
    child: usize,
    what: &str,
) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}
/// The children's prefix expansions as one dictionary's: the union in
/// byte order and its exact size when every child stayed within the
/// cap; a child past the cap answers no terms and a count above it, and
/// the relay then answers the largest such count (a lower bound on the
/// subtree's, enough for the root's refusal) and no terms, as a node
/// past the cap does. `known` when any child knows the field.
pub fn merge_prefix_expansions(
    cap: u32,
    shares: &[crate::pb::ExpandTermPrefixResponse],
) -> crate::pb::ExpandTermPrefixResponse {
    let mut union = std::collections::BTreeSet::new();
    let mut known = false;
    let mut past_cap: u64 = 0;
    for share in shares {
        if !share.known {
            continue;
        }
        known = true;
        if share.count > u64::from(cap) {
            past_cap = past_cap.max(share.count);
            continue;
        }
        union.extend(share.terms.iter().cloned());
    }
    if past_cap > 0 {
        return crate::pb::ExpandTermPrefixResponse {
            terms: Vec::new(),
            count: past_cap,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
        };
    }
    let count = union.len() as u64;
    crate::pb::ExpandTermPrefixResponse {
        terms: if count > u64::from(cap) {
            Vec::new()
        } else {
            union.into_iter().collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    }
}

/// The children's dictionary scans as one: entries unioned in byte order
/// with each term's df summed (checked), the tombstone count summed, the
/// exact union size as the count within the bound; a child past the scan
/// bound makes the relay answer the largest such count and no entries,
/// as a node past the bound does.
pub fn merge_suggest_terms(
    max_scan: u64,
    shares: &[crate::pb::SuggestTermsResponse],
) -> Result<crate::pb::SuggestTermsResponse, Status> {
    let mut union: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut known = false;
    let mut tombstoned: u64 = 0;
    let mut past_bound: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        add_count(
            &mut tombstoned,
            share.tombstoned_rows,
            child,
            "tombstone count",
        )?;
        if !share.known {
            continue;
        }
        known = true;
        if share.count > max_scan {
            past_bound = past_bound.max(share.count);
            continue;
        }
        for entry in &share.entries {
            let df = union.entry(entry.term.clone()).or_insert(0);
            add_count(df, entry.df, child, "posting frequency")?;
        }
    }
    if past_bound > 0 {
        return Ok(crate::pb::SuggestTermsResponse {
            entries: Vec::new(),
            count: past_bound,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
            tombstoned_rows: tombstoned,
        });
    }
    let count = union.len() as u64;
    Ok(crate::pb::SuggestTermsResponse {
        entries: if count > max_scan {
            Vec::new()
        } else {
            union
                .into_iter()
                .map(|(term, df)| crate::pb::SuggestTermEntry { term, df })
                .collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
        tombstoned_rows: tombstoned,
    })
}

/// The children's layouts as the one shard the root lists
/// (`docs/diagnostics.md`): counts summed, the children's segments
/// concatenated with the child index in front of each segment id, the
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
        let served = layout.layout == "segments" || layout.layout == "single-image";
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
/// One request per child (`None` asks that child nothing), the answers
/// paired with their child index in child order; a child's error is
/// named by child and address.
pub(super) async fn fan_out<Req, Resp, F, Fut>(
    frozen: &CoordinatorServiceImpl,
    children: &[String],
    requests: Vec<Option<Req>>,
    timeout: Option<Duration>,
    route: &'static str,
    call: F,
) -> Result<Vec<(usize, Resp)>, Status>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    F: Fn(crate::link::NodeLink, Request<Req>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
{
    let mut tasks = Vec::with_capacity(children.len());
    for (shard, child_req) in requests.into_iter().enumerate() {
        let Some(child_req) = child_req else {
            continue;
        };
        let link = frozen.node_client(&children[shard])?;
        let addr = children[shard].clone();
        let call = call.clone();
        tasks.push((
            shard,
            tokio::spawn(async move {
                let mut request = Request::new(child_req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                call(link, request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, route, status))
            }),
        ));
    }
    let mut out = Vec::with_capacity(tasks.len());
    for (shard, task) in tasks {
        let share = task
            .await
            .map_err(|e| Status::internal(format!("relay {route} task: {e}")))??;
        out.push((shard, share));
    }
    Ok(out)
}

/// The children's answers put back in the caller's id order, each
/// occurrence of an id served once, the way a node answers a list that
/// repeats an id. An id no child answered is absent, as a node leaves
/// out an id it does not serve.
pub(super) fn in_caller_order<T>(ids: &[u64], items: Vec<T>, id_of: impl Fn(&T) -> u64) -> Vec<T> {
    let mut by_id: HashMap<u64, VecDeque<T>> = HashMap::new();
    for item in items {
        by_id.entry(id_of(&item)).or_default().push_back(item);
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(item) = by_id.get_mut(id).and_then(VecDeque::pop_front) {
            out.push(item);
        }
    }
    out
}

/// A per-column scalar type the children report beside a known flag:
/// the children that know the column must agree on the type, and a
/// child that does not know it reports `Unspecified`.
pub(super) fn merge_column_types(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<i32>>,
    share: &[i32],
) -> Result<(), Status> {
    match acc {
        None => *acc = Some(share.to_vec()),
        Some(types) => {
            if types.len() != share.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} {what} types while an earlier child \
                     answered {}",
                    share.len(),
                    types.len()
                )));
            }
            let unspecified = crate::pb::ScalarValueType::Unspecified as i32;
            for (column, (held, &reported)) in types.iter_mut().zip(share).enumerate() {
                if reported == unspecified {
                    continue;
                }
                if *held == unspecified {
                    *held = reported;
                } else if *held != reported {
                    return Err(Status::failed_precondition(format!(
                        "relay: child {shard} types {what} column {column} as {reported} while \
                         an earlier child typed it {held}; the column families diverge across \
                         the children"
                    )));
                }
            }
        }
    }
    Ok(())
}
impl RelayService {
    pub(super) async fn browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        crate::metrics::timed(Route::BrowseShard, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.k == 0 {
                return Err(Status::invalid_argument("browse requires k > 0"));
            }
            for sort in &req.sort {
                crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
            }
            let (pinned, frozen, children, claims) = self.open_children(
                "BrowseShard",
                req.visibility.as_ref(),
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            // The boundary is a row's keys and id, which each child
            // applies to its rows as sent.
            let requests = claims
                .iter()
                .map(|claim| {
                    let mut child_req = req.clone();
                    child_req.expected_stats_epoch = claim.epoch;
                    child_req.expected_stats_incarnation = claim.incarnation();
                    Some(child_req)
                })
                .collect();
            let shares: Vec<crate::pb::BrowseShardResponse> = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "browse",
                |mut link, request| async move { link.browse_shard(request).await },
            )
            .await?
            .into_iter()
            .map(|(_, share)| share)
            .collect();
            let mut merged = merge_browse_pages(&req, &shares)?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &shares,
            )?;
            merged.stats_epoch = receipt.stats_epoch;
            merged.stats_incarnation = receipt.stats_incarnation;
            merged.visibility_fingerprint = receipt.visibility_fingerprint;
            merged.visibility_columns_known = receipt.visibility_columns_known;
            Ok(Response::new(merged))
        })
        .await
    }
}
