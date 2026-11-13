use super::map::{add_count, add_count32, child_error, relay_stream};
use super::merge_lexical::merge_known;
use super::*;

/// The children's vector backends as one: the descriptor and the
/// configuration must match child for child (the root's preflight
/// treats a shard's identity as generation-wide, so a relay hiding a
/// mismatch behind one answer would break that contract), rows sum
/// with checked arithmetic, and an unconfigured child refuses by name.
pub fn merge_vector_backend(
    children: &[String],
    reports: &[crate::pb::GetVectorBackendResponse],
) -> Result<crate::pb::GetVectorBackendResponse, Status> {
    let Some(first) = reports.first() else {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    };
    let name = |shard: usize| children.get(shard).map_or("", String::as_str);
    let mut num_vectors: u64 = 0;
    let mut indexed_vectors: Option<u64> = Some(0);
    for (shard, report) in reports.iter().enumerate() {
        if report.descriptor.is_none() {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) has no vector backend configured; a relay answers \
                 for configured children only",
                name(shard)
            )));
        }
        if report.descriptor != first.descriptor {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) advertises a different vector backend descriptor \
                 than child 0 ({}); a relay presents one provider identity",
                name(shard),
                name(0)
            )));
        }
        if report.config != first.config {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) runs a different vector backend configuration \
                 than child 0 ({}); a relay presents one provider identity",
                name(shard),
                name(0)
            )));
        }
        num_vectors = num_vectors.checked_add(report.num_vectors).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: child {shard} ({}) overflows the summed vector count",
                name(shard)
            ))
        })?;
        if report
            .indexed_vectors
            .is_some_and(|indexed| indexed > report.num_vectors)
        {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} ({}) reports more indexed vectors than vectors",
                name(shard)
            )));
        }
        // Bounded by the checked `num_vectors` sum, so this cannot overflow.
        indexed_vectors = indexed_vectors
            .zip(report.indexed_vectors)
            .map(|(sum, indexed)| sum + indexed);
    }
    Ok(crate::pb::GetVectorBackendResponse {
        descriptor: first.descriptor.clone(),
        config: first.config.clone(),
        num_vectors,
        indexed_vectors,
    })
}

/// Sum the children's statistics shares into the parent's view of one
/// shard. Checked arithmetic throughout: a `u32` document frequency or
/// a `u64` total that would overflow refuses by name, because a wrong
/// idf is a wrong ranking and not a warning. Field capabilities must
/// agree across children: `known`, `positions`, and `sentences` are one
/// boolean each on the wire, and a mixed answer has no faithful spelling
/// in one (a phrase needs positions EVERYWHERE, a typo check needs the
/// field SOMEWHERE), so a relay refuses the mixture by name. The epoch is
/// left at 0 for the caller to replace with a token.
#[allow(clippy::large_enum_variant)]
pub(super) enum Bm25Event {
    Child(usize, Result<Option<Bm25QueryStreamResponse>, Status>),
    Parent(Option<Result<Bm25QueryStreamRequest, Status>>),
    Deadline,
    MapMoved(u64),
}

impl RelayService {
    /// One request per child with the parent's claim translated into
    /// that child's, or the stale-epoch refusal when the token is not
    /// one this relay can translate under the current map.
    pub(super) fn child_claims(
        &self,
        token: u64,
        incarnation: &[u8],
        children: usize,
    ) -> Result<Vec<StatsClaim>, Status> {
        self.stats_incarnation.check(token, token, incarnation)?;
        let claims = self.translate_epoch(token)?;
        if claims.len() != children {
            return Err(Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} names {} children and the map has \
                 {children}; refetch TermStats",
                claims.len()
            )));
        }
        Ok(claims)
    }

    /// The forwarding loop of one relayed `Bm25QueryStream`: children's
    /// candidate batches go up untouched, their cutoff raises go up
    /// monotone, the parent's raises and its stop go down, and the
    /// relay's completion follows the last child's.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn relay_bm25_stream(
        &self,
        frozen: CoordinatorServiceImpl,
        pinned: MapSnapshot,
        mut map_changes: watch::Receiver<u64>,
        req: Bm25QueryRequest,
        claims: Vec<StatsClaim>,
        mut inbound: Streaming<Bm25QueryStreamRequest>,
        tx: mpsc::Sender<Result<Bm25QueryStreamResponse, Status>>,
        deadline: Option<tokio::time::Instant>,
        timeout: Option<Duration>,
    ) -> Result<(), Status> {
        map_changes.mark_unchanged();
        if *map_changes.borrow() != pinned.control_revision {
            map_changes.mark_changed();
        }
        let children = frozen.node_addresses().to_vec();
        let n = children.len();
        // One outbound leg per child and one shared inbound lane the
        // children's reader tasks feed, tagged by child.
        let (events_tx, mut events_rx) =
            mpsc::channel::<(usize, Result<Option<Bm25QueryStreamResponse>, Status>)>(64);
        let mut legs: Vec<Option<mpsc::Sender<Bm25QueryStreamRequest>>> = Vec::with_capacity(n);
        let mut readers = Vec::with_capacity(n);
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let (out_tx, out_rx) = mpsc::channel::<Bm25QueryStreamRequest>(8);
            let mut child_req = req.clone();
            child_req.expected_stats_epoch = claims[shard].epoch;
            child_req.expected_stats_incarnation = claims[shard].incarnation();
            out_tx
                .send(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Start(child_req)),
                })
                .await
                .map_err(|_| Status::internal("relay: child request channel closed at start"))?;
            let mut request = Request::new(ReceiverStream::new(out_rx));
            if let Some(timeout) = timeout {
                request.set_timeout(timeout);
            }
            let events = events_tx.clone();
            let addr = addr.clone();
            readers.push(tokio::spawn(async move {
                let mut stream = match link.bm25_query_stream(request).await {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let _ = events
                            .send((shard, Err(child_error(shard, &addr, "bm25 stream", status))))
                            .await;
                        return;
                    }
                };
                loop {
                    let next = stream.message().await;
                    let done = matches!(next, Ok(None) | Err(_));
                    let item =
                        next.map_err(|status| child_error(shard, &addr, "bm25 stream", status));
                    if events.send((shard, item)).await.is_err() || done {
                        return;
                    }
                }
            }));
            legs.push(Some(out_tx));
        }
        drop(events_tx);
        let stop_children = |legs: &mut Vec<Option<mpsc::Sender<Bm25QueryStreamRequest>>>| {
            let stops: Vec<mpsc::Sender<Bm25QueryStreamRequest>> =
                legs.iter_mut().filter_map(Option::take).collect();
            async move {
                for leg in stops {
                    let _ = leg
                        .send(Bm25QueryStreamRequest {
                            payload: Some(bm25_query_stream_request::Payload::Stop(
                                StopBm25Query {},
                            )),
                        })
                        .await;
                }
            }
        };
        let mut completions: Vec<Option<Bm25StreamCompletion>> = vec![None; n];
        let mut remaining = n;
        let mut forwarded: u64 = 0;
        let mut last_up = f32::NEG_INFINITY;
        let mut last_down = f32::NEG_INFINITY;
        let mut fingerprint: Option<String> = None;
        let mut parent_open = true;
        let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));
        let outcome: Result<Option<Bm25StreamCompletion>, Status> = loop {
            if remaining == 0 {
                break Ok(None);
            }
            let event = tokio::select! {
                biased;
                _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Bm25Event::Deadline,
                moved = map_changes.changed() => match moved {
                    Ok(()) => Bm25Event::MapMoved(*map_changes.borrow_and_update()),
                    Err(_) => {
                        map_changes = watch::channel(pinned.control_revision).1;
                        continue;
                    }
                },
                message = inbound.message(), if parent_open => Bm25Event::Parent(message.transpose()),
                event = events_rx.recv() => match event {
                    Some((shard, item)) => Bm25Event::Child(shard, item),
                    None => break Err(Status::internal("relay: every child reader ended before completion")),
                },
            };
            match event {
                Bm25Event::Deadline => {
                    break Err(Status::deadline_exceeded(
                        "relay: the parent's deadline passed before every child completed",
                    ));
                }
                Bm25Event::MapMoved(revision) => {
                    if revision == pinned.control_revision {
                        continue;
                    }
                    break Err(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during Bm25QueryStream; the stream was opened \
                         under the older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )));
                }
                Bm25Event::Parent(Some(Ok(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(u)),
                }))) => {
                    if !u.floor.is_nan() && u.floor > last_down {
                        last_down = u.floor;
                        for leg in legs.iter().flatten() {
                            let _ = leg
                                .send(Bm25QueryStreamRequest {
                                    payload: Some(bm25_query_stream_request::Payload::FloorUpdate(
                                        FloorUpdate { floor: u.floor },
                                    )),
                                })
                                .await;
                        }
                    }
                }
                Bm25Event::Parent(Some(Ok(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Stop(_)),
                }))) => {
                    // The parent's stop: the children are told, and the
                    // relay certifies an incomplete scan without waiting
                    // on children that may hold their answer.
                    stop_children(&mut legs).await;
                    break Ok(Some(Bm25StreamCompletion {
                        completed: false,
                        response: None,
                        scoring_fingerprint: fingerprint.clone().unwrap_or_default(),
                        candidates_emitted: forwarded,
                    }));
                }
                Bm25Event::Parent(Some(Ok(_))) => {}
                Bm25Event::Parent(Some(Err(_))) | Bm25Event::Parent(None) => {
                    // The parent's leg closed: gone, or done sending.
                    // The children finish on their own unless the
                    // response side is closed too, which `tx.closed()`
                    // in the caller turns into a cancel.
                    parent_open = false;
                }
                Bm25Event::Child(_, Err(status)) => break Err(status),
                Bm25Event::Child(shard, Ok(None)) => {
                    if completions[shard].is_none() {
                        break Err(Status::data_loss(format!(
                            "relay child {shard} ({}): BM25 stream ended without a completion \
                             certificate",
                            children[shard]
                        )));
                    }
                }
                Bm25Event::Child(shard, Ok(Some(message))) => {
                    if completions[shard].is_some() {
                        break Err(Status::internal(format!(
                            "relay child {shard} ({}): message after its completion",
                            children[shard]
                        )));
                    }
                    match message.payload {
                        Some(bm25_query_stream_response::Payload::CandidateBatch(batch)) => {
                            if batch.candidates.len() % 12 != 0 {
                                break Err(Status::data_loss(format!(
                                    "relay child {shard} ({}): BM25 candidate batch has {} \
                                     bytes, not 12-byte records",
                                    children[shard],
                                    batch.candidates.len()
                                )));
                            }
                            forwarded += (batch.candidates.len() / 12) as u64;
                            if tx
                                .send(Ok(Bm25QueryStreamResponse {
                                    payload: Some(
                                        bm25_query_stream_response::Payload::CandidateBatch(batch),
                                    ),
                                }))
                                .await
                                .is_err()
                            {
                                break Err(Status::cancelled("relay: parent response closed"));
                            }
                        }
                        Some(bm25_query_stream_response::Payload::FloorUpdate(u)) => {
                            if !u.floor.is_nan() && u.floor > last_up {
                                last_up = u.floor;
                                if tx
                                    .send(Ok(Bm25QueryStreamResponse {
                                        payload: Some(
                                            bm25_query_stream_response::Payload::FloorUpdate(u),
                                        ),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    break Err(Status::cancelled("relay: parent response closed"));
                                }
                            }
                        }
                        Some(bm25_query_stream_response::Payload::Completion(completion)) => {
                            if completion.scoring_fingerprint.is_empty() {
                                break Err(Status::data_loss(format!(
                                    "relay child {shard} ({}): BM25 completion omitted its \
                                     scoring fingerprint",
                                    children[shard]
                                )));
                            }
                            match fingerprint.as_ref() {
                                Some(seen) if seen != &completion.scoring_fingerprint => {
                                    break Err(Status::failed_precondition(format!(
                                        "relay child {shard} ({}): scoring fingerprint {} \
                                         differs from {seen}; one relay serves one score space",
                                        children[shard], completion.scoring_fingerprint
                                    )));
                                }
                                None => fingerprint = Some(completion.scoring_fingerprint.clone()),
                                _ => {}
                            }
                            completions[shard] = Some(completion);
                            legs[shard] = None;
                            remaining -= 1;
                        }
                        Some(bm25_query_stream_response::Payload::Done(_)) => {
                            break Err(Status::failed_precondition(format!(
                                "relay child {shard} ({}): BM25 stream used the obsolete \
                                 uncertified terminal response",
                                children[shard]
                            )));
                        }
                        None => {}
                    }
                }
            }
        };
        let completion = match outcome {
            Ok(Some(incomplete)) => incomplete,
            Ok(None) => {
                self.still_current(&pinned, "Bm25QueryStream")?;
                let mut completed = true;
                let mut emitted: u64 = 0;
                let mut responses = Vec::with_capacity(n);
                for (shard, completion) in completions.into_iter().enumerate() {
                    let completion = completion.expect("remaining reached zero");
                    completed &= completion.completed;
                    emitted = emitted
                        .checked_add(completion.candidates_emitted)
                        .ok_or_else(|| {
                            Status::internal("relay: candidates_emitted overflows u64")
                        })?;
                    match completion.response {
                        Some(response) => responses.push(response),
                        None if completion.completed => {
                            return Err(Status::data_loss(format!(
                                "relay child {shard} ({}): BM25 completion omitted its response",
                                children[shard]
                            )));
                        }
                        None => {}
                    }
                }
                if emitted != forwarded {
                    return Err(Status::data_loss(format!(
                        "relay: children certified {emitted} candidates but {forwarded} were \
                         forwarded"
                    )));
                }
                let response = if completed {
                    Some(merge_bm25_responses(&req, responses)?)
                } else {
                    None
                };
                Bm25StreamCompletion {
                    completed,
                    response,
                    scoring_fingerprint: fingerprint.unwrap_or_default(),
                    candidates_emitted: forwarded,
                }
            }
            Err(status) => {
                stop_children(&mut legs).await;
                for reader in &readers {
                    reader.abort();
                }
                return Err(status);
            }
        };
        for reader in &readers {
            reader.abort();
        }
        tx.send(Ok(Bm25QueryStreamResponse {
            payload: Some(bm25_query_stream_response::Payload::Completion(completion)),
        }))
        .await
        .map_err(|_| Status::cancelled("relay: parent response closed"))
    }
}
// --- The vector-side routes, the bitmap routes, and the dictionaries ---

/// The known-column accumulator of one fan-out at this level: geo flags
/// ORed, filter-leaf flags ORed over the request's tree with each
/// child's answer mapped through the leaves its placement code implied
/// away (`ShardMask::implied`: the child was sent a shorter tree and
/// answers in that order), and the leaves the mask itself resolved
/// marked from the start (`ShardMask::known`).
pub(super) struct KnownFlags {
    pub(super) geo: Vec<bool>,
    pub(super) tree: Vec<bool>,
}

impl KnownFlags {
    pub(super) fn new(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        let mut tree = vec![false; leaves];
        if let Some(mask) = mask {
            for &leaf in &mask.known {
                if let Some(flag) = tree.get_mut(leaf) {
                    *flag = true;
                }
            }
        }
        KnownFlags {
            geo: vec![false; filters.geo.len()],
            tree,
        }
    }

    pub(super) fn merge(
        &mut self,
        child: usize,
        mask: Option<&crate::placement::ShardMask>,
        geo: &[bool],
        tree: &[bool],
    ) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} geo-column flags for {} geo filters",
                geo.len(),
                self.geo.len()
            )));
        }
        for (acc, k) in self.geo.iter_mut().zip(geo) {
            *acc |= *k;
        }
        let dropped: &[usize] = mask
            .and_then(|m| m.implied.get(child))
            .map_or(&[], Vec::as_slice);
        let kept: Vec<usize> = (0..self.tree.len())
            .filter(|index| !dropped.contains(index))
            .collect();
        if tree.len() != kept.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} filter-leaf flags for the {} leaves it was \
                 sent (of {})",
                tree.len(),
                kept.len(),
                self.tree.len()
            )));
        }
        for (&index, k) in kept.iter().zip(tree) {
            self.tree[index] |= *k;
        }
        Ok(())
    }
}
/// The children's stream and the parent's leg of one relayed
/// `SearchShard`, dropped together: a return on any path closes the
/// children's request channels and stops the readers.
pub(super) struct ShardLegs {
    senders: Vec<Option<mpsc::Sender<SearchShardRequest>>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    parent: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ShardLegs {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.abort();
        }
        if let Some(parent) = &self.parent {
            parent.abort();
        }
    }
}

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

/// The forwarding loop of one relayed `SearchShard`: one child stream
/// per child the placement mask keeps, the parent's floor raises
/// forwarded to every child, each child's raises forwarded to the parent
/// (a child's k-th best is a lower bound on the relay's, so the parent's
/// running maximum only tightens), and the children's terminal lists
/// concatenated in score order. The relay keeps no heap: the parent's
/// merge picks the top-k from the union exactly as it does over leaves,
/// and with `tie_complete` the union holds every child's boundary tie
/// group, so the cascade's score-defined pool is the same set.
#[allow(clippy::too_many_arguments)]
async fn relay_shard_search(
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    mut map_changes: watch::Receiver<u64>,
    start: crate::pb::StartShardSearch,
    filters: RequestFilters,
    mut inbound: Streaming<SearchShardRequest>,
    tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    deadline: Option<tokio::time::Instant>,
) -> Result<(), Status> {
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
    let children = coordinator.node_addresses().to_vec();
    let n = children.len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let mask = coordinator.shard_mask(filters.tree.as_ref());
    let mut known = KnownFlags::new(&filters, mask.as_ref());
    let (event_tx, mut events) = mpsc::channel::<ShardEvent>(64);
    let mut legs = ShardLegs {
        senders: vec![None; n],
        readers: Vec::with_capacity(n),
        parent: None,
    };
    let mut remaining = 0;
    for (child, addr) in children.iter().enumerate() {
        if mask.as_ref().is_some_and(|m| m.skipped[child]) {
            continue;
        }
        remaining += 1;
        let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
        let child_start = crate::pb::StartShardSearch {
            read_context: None,
            request_id: start.request_id.clone(),
            k: start.k,
            vector: start.vector.clone(),
            tie_complete: start.tie_complete,
            collapse_parents: start.collapse_parents,
            geo_filters: filters.geo.clone(),
            filter: CoordinatorServiceImpl::shard_filter_tree(&filters, mask.as_ref(), child),
        };
        req_tx
            .try_send(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(child_start)),
            })
            .map_err(|_| Status::internal("relay: child request channel refused the Start"))?;
        legs.senders[child] = Some(req_tx);
        let mut link = coordinator.node_client(addr)?;
        let addr = addr.clone();
        let events = event_tx.clone();
        legs.readers.push(tokio::spawn(async move {
            let mut request = Request::new(ReceiverStream::new(req_rx));
            if let Some(at) = deadline {
                request.set_timeout(at.saturating_duration_since(tokio::time::Instant::now()));
            }
            let mut stream = match link.search_shard(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let _ = events
                        .send(ShardEvent::Child(
                            child,
                            Err(child_error(child, &addr, "shard search", status)),
                        ))
                        .await;
                    return;
                }
            };
            loop {
                let message = stream.message().await;
                let terminal = !matches!(message, Ok(Some(_)))
                    || matches!(
                        message,
                        Ok(Some(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::Done(_)),
                        }))
                    );
                let message =
                    message.map_err(|status| child_error(child, &addr, "shard search", status));
                if events
                    .send(ShardEvent::Child(child, message))
                    .await
                    .is_err()
                    || terminal
                {
                    return;
                }
            }
        }));
    }
    {
        let events = event_tx;
        legs.parent = Some(tokio::spawn(async move {
            loop {
                let message = inbound.message().await;
                let terminal = !matches!(message, Ok(Some(_)));
                let event = match message {
                    Ok(Some(message)) => ShardEvent::Parent(Some(Ok(message))),
                    Ok(None) => ShardEvent::Parent(None),
                    Err(status) => ShardEvent::Parent(Some(Err(status))),
                };
                if events.send(event).await.is_err() || terminal {
                    return;
                }
            }
        }));
    }
    let mut hits: Vec<crate::pb::ScoredHit> = Vec::new();
    let mut stats: Option<crate::pb::ShardScanStats> = None;
    let mut done = vec![false; n];
    let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));
    while remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => {
                return Err(Status::deadline_exceeded(
                    "relay: the parent's deadline passed before every child completed",
                ));
            }
            moved = map_changes.changed() => match moved {
                Ok(()) => {
                    let revision = *map_changes.borrow_and_update();
                    if revision == pinned.control_revision {
                        continue;
                    }
                    return Err(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during SearchShard; the scan was opened under the \
                         older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )));
                }
                Err(_) => {
                    map_changes = watch::channel(pinned.control_revision).1;
                    continue;
                }
            },
            event = events.recv() => match event {
                Some(event) => event,
                None => {
                    return Err(Status::internal(
                        "relay: the child readers ended before every child completed",
                    ))
                }
            },
        };
        match event {
            ShardEvent::Parent(Some(Ok(SearchShardRequest {
                payload: Some(search_shard_request::Payload::FloorUpdate(update)),
            }))) => {
                if update.floor.is_nan() {
                    continue;
                }
                for sender in legs.senders.iter().flatten() {
                    // Floors are monotone: a raise a full channel drops is
                    // superseded by the next, and a closed child is done.
                    let _ = sender.try_send(shard_floor(update.floor));
                }
            }
            ShardEvent::Parent(Some(Ok(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(_)),
            }))) => {
                return Err(Status::invalid_argument(
                    "relay: a stream carries one StartShardSearch; another arrived",
                ));
            }
            ShardEvent::Parent(Some(Ok(_))) | ShardEvent::Parent(None) => {}
            ShardEvent::Parent(Some(Err(status))) => {
                return Err(Status::cancelled(format!(
                    "relay: the parent's request leg failed: {}",
                    status.message()
                )));
            }
            ShardEvent::Child(
                _,
                Ok(Some(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::FloorUpdate(update)),
                })),
            ) => {
                if tx
                    .send(Ok(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::FloorUpdate(update)),
                    }))
                    .await
                    .is_err()
                {
                    // The parent is gone: nobody reads what the children
                    // still scan for.
                    return Ok(());
                }
            }
            ShardEvent::Child(
                child,
                Ok(Some(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::Done(share)),
                })),
            ) => {
                if done[child] {
                    return Err(Status::internal(format!(
                        "relay: child {child} ({}) sent a terminal message twice",
                        children[child]
                    )));
                }
                done[child] = true;
                remaining -= 1;
                known.merge(
                    child,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                if let Some(share) = share.stats {
                    let acc = stats.get_or_insert_with(Default::default);
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
                }
                hits.extend(share.hits);
            }
            ShardEvent::Child(_, Ok(Some(_))) => {}
            ShardEvent::Child(child, Ok(None)) => {
                if !done[child] {
                    return Err(Status::data_loss(format!(
                        "relay: child {child} ({}) closed its stream before Done",
                        children[child]
                    )));
                }
            }
            ShardEvent::Child(_, Err(status)) => return Err(status),
        }
    }
    if *map_changes.borrow() != pinned.control_revision {
        return Err(Status::failed_precondition(format!(
            "relay: the shard map moved from revision {} (generation {}) during SearchShard; \
             retry under the current one",
            pinned.control_revision, pinned.topology_generation
        )));
    }
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.vector_id.cmp(&b.vector_id))
    });
    let _ = tx
        .send(Ok(SearchShardResponse {
            payload: Some(search_shard_response::Payload::Done(
                crate::pb::SearchShardDone {
                    hits,
                    stats,
                    geo_columns_known: known.geo,
                    filter_columns_known: known.tree,
                },
            )),
        }))
        .await;
    Ok(())
}
impl RelayService {
    pub(super) async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<<Self as NodeService>::StreamSearchStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSearch, request, |request| async move {
            let deadline =
                grpc_timeout(request.metadata()).map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let start = match inbound.message().await? {
                Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Start(start)),
                }) => start,
                _ => {
                    return Err(Status::invalid_argument(
                        "first StreamSearchRequest must be StartStreamSearch",
                    ))
                }
            };
            if start.read_context.is_some() {
                return Err(Status::failed_precondition(
                    "relay does not yet compose scoped vector read receipts",
                ));
            }
            if start.initial_floor.is_some_and(f32::is_nan) {
                return Err(Status::invalid_argument("initial_floor must not be NaN"));
            }
            crate::node::validate_geo_filters(&start.geo_filters)?;
            if let Some(f) = start.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: start.geo_filters.clone(),
                tree: start.filter.clone(),
            };
            let signals = Arc::new(RelaySignals::new(
                start.initial_floor.unwrap_or(f32::NEG_INFINITY),
            ));
            let token = (start.floor_token != 0).then_some(start.floor_token);
            if let Some(token) = token {
                self.signals
                    .lock()
                    .expect("relay stream signal registry poisoned")
                    .insert(token, Arc::clone(&signals));
            }
            // The parent's gRPC leg: floor raises and the authoritative
            // Stop. A closed leg before the summary means the parent is
            // gone, and the children are told so.
            let pump = Arc::clone(&signals);
            let identity_enabled = start.identity_limits.is_some();
            let identity_ready = Arc::new(AtomicBool::new(false));
            let pump_ready = Arc::clone(&identity_ready);
            let input = tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                        })) => pump.raise(u.floor),
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::Stop(_)),
                        })) => {
                            pump.cancel();
                            return Ok(None);
                        }
                        Ok(Some(StreamSearchRequest {
                            payload:
                                Some(stream_search_request::Payload::ResolveIdentities(selection)),
                        })) => {
                            if !identity_enabled || !pump_ready.load(Ordering::Acquire) {
                                pump.cancel();
                                return Err(Status::failed_precondition(
                                    "relay: identity selection requires IdentityReady",
                                ));
                            }
                            return Ok(Some(selection));
                        }
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => {
                            if identity_enabled {
                                pump.cancel();
                            }
                            return Ok(None);
                        }
                    }
                }
            });
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let map_changes = self.inner.map.changes();
            let registry = Arc::clone(&self.signals);
            let loop_signals = Arc::clone(&signals);
            let input_abort = input.abort_handle();
            tokio::spawn(async move {
                let work = relay_stream(
                    frozen,
                    pinned,
                    map_changes,
                    start,
                    filters,
                    loop_signals,
                    tx.clone(),
                    deadline,
                    input,
                    identity_ready,
                );
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    _ = async { tokio::time::sleep_until(deadline.expect("guarded")).await }, if deadline.is_some() =>
                        Err(Status::deadline_exceeded("relay: parent deadline passed")),
                    result = work => result,
                };
                input_abort.abort();
                if let Some(token) = token {
                    registry
                        .lock()
                        .expect("relay stream signal registry poisoned")
                        .remove(&token);
                }
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status))).await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<<Self as NodeService>::SearchShardStream>, Status> {
        crate::metrics::timed_stream(Route::SearchShard, request, |request| async move {
            let deadline =
                grpc_timeout(request.metadata()).map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let start = match inbound.message().await? {
                Some(SearchShardRequest {
                    payload: Some(search_shard_request::Payload::Start(start)),
                }) => start,
                _ => {
                    return Err(Status::invalid_argument(
                        "first SearchShardRequest must be StartShardSearch",
                    ))
                }
            };
            if start.read_context.is_some() {
                return Err(Status::failed_precondition(
                    "relay: scoped vector scans require child read receipt composition",
                ));
            }
            crate::node::validate_geo_filters(&start.geo_filters)?;
            if let Some(f) = start.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: start.geo_filters.clone(),
                tree: start.filter.clone(),
            };
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let map_changes = self.inner.map.changes();
            tokio::spawn(async move {
                let work = relay_shard_search(
                    frozen,
                    pinned,
                    map_changes,
                    start,
                    filters,
                    inbound,
                    tx.clone(),
                    deadline,
                );
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    result = work => result,
                };
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status)))
                        .await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn get_vector_backend(
        &self,
        request: Request<crate::pb::GetVectorBackendRequest>,
    ) -> Result<Response<crate::pb::GetVectorBackendResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(crate::pb::GetVectorBackendRequest {});
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.get_vector_backend(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| {
                        Status::new(
                            status.code(),
                            format!(
                                "relay child {shard} ({addr}) vector backend: {}",
                                status.message()
                            ),
                        )
                    })
            }));
        }
        let mut reports = Vec::with_capacity(children.len());
        for task in tasks {
            reports.push(
                task.await
                    .map_err(|e| Status::internal(format!("relay vector backend task: {e}")))??,
            );
        }
        let merged = merge_vector_backend(&children, &reports)?;
        self.still_current(&pinned, "GetVectorBackend")?;
        Ok(Response::new(merged))
    }

    pub(super) async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let read_claims = req
                .read_context
                .as_ref()
                .map(|context| {
                    crate::visibility::VisibilityScope::new(context.visibility.as_ref())?;
                    self.child_claims(
                        context.expected_stats_epoch,
                        &context.expected_stats_incarnation,
                        children.len(),
                    )
                })
                .transpose()?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                if let (Some(context), Some(read_claims)) =
                    (&mut child_req.read_context, &read_claims)
                {
                    context.expected_stats_epoch = read_claims[shard].epoch;
                    context.expected_stats_incarnation = read_claims[shard].incarnation();
                }
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.shard_legs(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "shard legs", status))
                }));
            }
            // Raw per-leg lists: the parent merges them by score across
            // its shards, and the union of the children's lists holds the
            // subtree's top of each leg. Competition ranks at the parent
            // make the order of equal scores immaterial.
            let mut merged = ShardLegsResponse::default();
            let mut geo_known = None;
            let mut filter_known = None;
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            for (shard, task) in tasks.into_iter().enumerate() {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay shard legs task: {e}")))??;
                match (&req.read_context, share.read_receipt.as_ref()) {
                    (Some(context), Some(receipt)) => {
                        crate::vector_read::check_binding(
                            &context.field,
                            receipt.vector_binding.as_ref(),
                            &mut binding,
                        )?;
                        receipts.push(receipt.clone());
                    }
                    (None, None) => {}
                    _ => {
                        return Err(Status::failed_precondition(
                            "relay: hybrid leg read receipt mismatch",
                        ))
                    }
                }
                merge_known(
                    "geo-column",
                    shard,
                    &mut geo_known,
                    &share.geo_columns_known,
                )?;
                merge_known(
                    "filter-leaf",
                    shard,
                    &mut filter_known,
                    &share.filter_columns_known,
                )?;
                merged.vector_hits.extend(share.vector_hits);
                merged.bm25_hits.extend(share.bm25_hits);
            }
            merged.geo_columns_known = geo_known.unwrap_or_default();
            merged.filter_columns_known = filter_known.unwrap_or_default();
            if let Some(context) = &req.read_context {
                let mut receipt = self.read_receipt(
                    &pinned,
                    children,
                    context.visibility.as_ref(),
                    read_claims.as_deref(),
                    &receipts,
                )?;
                receipt.vector_binding = binding.flatten();
                merged.read_receipt = Some(receipt);
            }
            self.still_current(&pinned, "ShardLegs")?;
            Ok(Response::new(merged))
        })
        .await
    }
}
