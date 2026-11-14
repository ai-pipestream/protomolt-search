//! Composed lexical routes and the streamed BM25 relay.

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::CoordinatorServiceImpl;
use crate::metrics::Route;
use crate::node::STALE_STATS_EPOCH;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, Bm25PhraseQueryRequest,
    Bm25QueryRequest, Bm25QueryResponse, Bm25QueryStreamRequest, Bm25QueryStreamResponse,
    Bm25RescoreRequest, Bm25RescoreResponse, Bm25StreamCompletion, FloorUpdate, StopBm25Query,
    TermStatsRequest, TermStatsResponse,
};
use crate::stats_identity::StatsClaim;

use super::map::{MapSnapshot, TokenTuple};
use super::merge_lexical::{
    child_ranges, merge_bm25_responses, merge_known, merge_prefix_expansions, merge_suggest_terms,
    merge_term_stats, refuse_bm25_aggregates, route_ids,
};
use super::stream_search::child_error;
use super::{grpc_timeout, read_metadata, RelayService};

#[allow(clippy::large_enum_variant)]
enum Bm25Event {
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
    async fn relay_bm25_stream(
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

impl RelayService {
    pub(super) async fn handle_term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::validate_stats_request(&req)?;
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.term_stats(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| {
                            Status::new(
                                status.code(),
                                format!(
                                    "relay child {shard} ({addr}) term stats: {}",
                                    status.message()
                                ),
                            )
                        })
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay term stats task: {e}")))??,
                );
            }
            let mut merged = merge_term_stats(&req, &shares)?;
            merged.stats_incarnation = self.stats_incarnation.bytes()?;
            self.still_current(&pinned, "TermStats")?;
            let tuple = TokenTuple {
                collection: self.collection.clone(),
                control_revision: pinned.control_revision,
                topology_generation: pinned.topology_generation,
                children,
                epochs: shares
                    .iter()
                    .map(|s| StatsClaim::required(s.stats_epoch, &s.stats_incarnation))
                    .collect::<Result<_, _>>()?,
            };
            merged.stats_epoch = self
                .tokens
                .lock()
                .expect("relay token registry poisoned")
                .allocate(tuple)?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<<Self as NodeService>::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let deadline = timeout.map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let req = match inbound.message().await? {
                Some(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Start(req)),
                }) => req,
                _ => {
                    return Err(Status::invalid_argument(
                        "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                    ))
                }
            };
            refuse_bm25_aggregates(
                "Bm25QueryStream",
                &req.stats_fields,
                &req.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                frozen.node_addresses().len(),
            )?;
            if frozen.node_addresses().is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let map_changes = self.inner.map.changes();
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            let relay = self.clone();
            tokio::spawn(async move {
                let work = relay.relay_bm25_stream(
                    frozen,
                    pinned,
                    map_changes,
                    req,
                    claims,
                    inbound,
                    tx.clone(),
                    deadline,
                    timeout,
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

    pub(super) async fn handle_expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let child_req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.expand_term_prefix(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "prefix expansion", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay prefix task: {e}")))??,
                );
            }
            for share in &shares {
                scope.validate_echo(
                    &share.visibility_fingerprint,
                    &share.visibility_columns_known,
                )?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&share.visibility_columns_known)
                {
                    *known |= present;
                }
            }
            let mut merged = merge_prefix_expansions(req.cap, &shares);
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = visibility_known;
            self.still_current(&pinned, "ExpandTermPrefix")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let child_req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.suggest_terms(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "dictionary scan", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay suggest task: {e}")))??,
                );
            }
            for share in &shares {
                scope.validate_echo(
                    &share.visibility_fingerprint,
                    &share.visibility_columns_known,
                )?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&share.visibility_columns_known)
                {
                    *known |= present;
                }
            }
            let mut merged = merge_suggest_terms(req.max_scan, &shares)?;
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = visibility_known;
            self.still_current(&pinned, "SuggestTerms")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            refuse_bm25_aggregates("Bm25Query", &req.stats_fields, &req.cardinality_fields)?;
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
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_query(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 query", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay bm25 query task: {e}")))??,
                );
            }
            let merged = merge_bm25_responses(&req, shares)?;
            self.still_current(&pinned, "Bm25Query")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_bm25_phrase_query(
        &self,
        request: Request<Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let query = req
                .query
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Bm25PhraseQuery: query is absent"))?;
            refuse_bm25_aggregates(
                "Bm25PhraseQuery",
                &query.stats_fields,
                &query.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                query.expected_stats_epoch,
                &query.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                if let Some(q) = child_req.query.as_mut() {
                    q.expected_stats_epoch = claims[shard].epoch;
                    q.expected_stats_incarnation = claims[shard].incarnation();
                }
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_phrase_query(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 phrase query", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay bm25 phrase task: {e}")))??,
                );
            }
            let merged = merge_bm25_responses(query, shares)?;
            self.still_current(&pinned, "Bm25PhraseQuery")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn handle_bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            // Each candidate goes to the child whose slot range holds it;
            // the ranges come from the children's health reports. An id
            // in no child's range is another shard's, and is ignored as
            // a node ignores an id outside its own range: the boolean
            // planner sends every shard every candidate.
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let by_child = route_ids(&ranges, &req.candidate_ids)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let requested: std::collections::HashSet<_> = ids.iter().copied().collect();
                let mut link = frozen.node_client(&children[shard])?;
                let mut child_req = req.clone();
                child_req.candidate_ids = ids;
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = children[shard].clone();
                tasks.push((requested, tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 rescore", status))
                })));
            }
            let mut hits = Vec::new();
            let mut stage_known: Option<Vec<bool>> = None;
            let mut receipts = Vec::with_capacity(children.len());
            for (i, (requested, task)) in tasks.into_iter().enumerate() {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay bm25 rescore task: {e}")))??;
                merge_known(
                    "stage-column",
                    i,
                    &mut stage_known,
                    &share.stage_columns_known,
                )?;
                let mut seen = std::collections::HashSet::new();
                for hit in &share.hits {
                    if !requested.contains(&hit.doc_id) || !hit.score.is_finite() || !seen.insert(hit.doc_id) {
                        return Err(Status::failed_precondition("relay: BM25 child returned an unrequested, duplicate or nonfinite score"));
                    }
                }
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            self.still_current(&pinned, "Bm25Rescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            hits.sort_by_key(|hit| hit.doc_id);
            Ok(Response::new(Bm25RescoreResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                hits,
                stage_columns_known: stage_known
                    .unwrap_or_else(|| vec![false; req.score_stages.len()]),
            }))
        })
        .await
    }
}
