//! Query-stream admission, cancellation races, and completion delivery.

use super::*;

type QueryStreamSender = mpsc::Sender<Result<crate::pb::QueryStreamResponse, Status>>;
type QueryStreamReceiver = ReceiverStream<Result<crate::pb::QueryStreamResponse, Status>>;

struct QueryStreamTask {
    service: CoordinatorServiceImpl,
    request: crate::pb::QueryStreamRequest,
    access: Option<crate::pb::AccessDecision>,
    grpc_budget: Option<Duration>,
    request_fingerprint: String,
}

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_query_stream(
        &self,
        request: Request<crate::pb::QueryStreamRequest>,
    ) -> Result<Response<<Self as SearchService>::QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::QueryStream, request, |request| {
            self.open_query_stream(request)
        })
        .await
    }

    async fn open_query_stream(
        &self,
        request: Request<crate::pb::QueryStreamRequest>,
    ) -> Result<Response<QueryStreamReceiver>, Status> {
        self.admit(&request.get_ref().collection)?;
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(SearchService::query_stream(
                &snapshot,
                crate::metrics::nested(request),
            ))
            .await
            .map(|response| response.map(crate::metrics::Timed::into_inner));
        }
        let grpc_budget = crate::relay::grpc_timeout(request.metadata());
        let access = request
            .extensions()
            .get::<crate::pb::AccessDecision>()
            .cloned();
        let mut request = request.into_inner();
        let request_fingerprint = request
            .query
            .as_ref()
            .map(|query| crate::sha256::hex_digest(&prost::Message::encode_to_vec(query)))
            .unwrap_or_default();
        if let Some(query) = request.query.as_mut() {
            self.admit_query_stream_query(query, access.as_ref())?;
        }
        let (tx, rx) = mpsc::channel(8);
        let task = QueryStreamTask {
            service: self.clone(),
            request,
            access,
            grpc_budget,
            request_fingerprint,
        };
        tokio::spawn(task.run(tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    fn admit_query_stream_query(
        &self,
        query: &crate::pb::QueryRequest,
        access: Option<&crate::pb::AccessDecision>,
    ) -> Result<(), Status> {
        self.require_topology_generation(query.required_topology_generation)?;
        // Bind a copy here; execution retains the original cursor for data-version checks.
        self.bind_query_cursor(&mut query.clone(), access)?;
        if query.explain {
            return Err(Status::invalid_argument(
                "explain is served on the unary Query route: a stream's revisions carry                      candidate hits without their trees, and a tree over a revision that a                      later one replaces would explain a score that was never served",
            ));
        }
        Ok(())
    }
}

impl QueryStreamTask {
    async fn run(mut self, tx: QueryStreamSender) {
        let mut revision = 1u64;
        let accepted = query_stream_revision(
            revision,
            crate::pb::QueryStreamPhase::Accepted,
            Vec::new(),
            String::new(),
            crate::pb::QueryStreamIdentityState::Unspecified,
        );
        if tx
            .send(Ok(crate::pb::QueryStreamResponse {
                payload: Some(crate::pb::query_stream_response::Payload::Revision(
                    accepted,
                )),
            }))
            .await
            .is_err()
        {
            return;
        }
        let Some(query) = self.request.query.take() else {
            send_query_completion(
                &tx,
                query_stream_completion(
                    Err(Status::invalid_argument("QueryStream requires query")),
                    revision,
                    Vec::new(),
                ),
            )
            .await;
            return;
        };
        let mut scoring_fingerprints = Vec::new();
        let Some(result) = self
            .attempt(query, &tx, &mut revision, &mut scoring_fingerprints)
            .await
        else {
            return;
        };
        // The attempt future is dropped before completion can await client capacity.
        scoring_fingerprints.sort();
        scoring_fingerprints.dedup();
        send_query_completion(
            &tx,
            query_stream_completion(result, revision, scoring_fingerprints),
        )
        .await;
    }

    async fn attempt(
        self,
        query: crate::pb::QueryRequest,
        tx: &QueryStreamSender,
        revision: &mut u64,
        scoring_fingerprints: &mut Vec<String>,
    ) -> Option<Result<crate::pb::QueryResponse, Status>> {
        let (progress_tx, progress_rx) = watch::channel(None);
        let runner = self
            .service
            .with_stream_search(true)
            .with_bm25_stream(true)
            .with_query_progress(progress_tx);
        let request_timeout =
            (self.request.timeout_ms > 0).then(|| Duration::from_millis(self.request.timeout_ms));
        let timeout = match (request_timeout, self.grpc_budget) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let finish_deadline_us = query_finish_deadline(timeout);
        let deadline = async move {
            match timeout {
                Some(duration) => tokio::time::sleep(duration).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline);
        let attempt = runner.query_stream_attempt(
            query,
            self.access,
            finish_deadline_us,
            progress_rx,
            query_progress::QueryStreamOutput {
                tx,
                revision,
                scoring_fingerprints,
                request_fingerprint: &self.request_fingerprint,
            },
        );
        tokio::pin!(attempt);
        tokio::select! {
            biased;
            _ = tx.closed() => None,
            _ = &mut deadline => Some(Err(Status::deadline_exceeded(format!(
                "QueryStream exceeded its {}ms deadline", self.request.timeout_ms)))),
            result = &mut attempt => Some(result),
        }
    }
}

fn query_finish_deadline(timeout: Option<Duration>) -> u64 {
    timeout.map_or(0, |duration| {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        now_us
            .saturating_add(duration.as_micros().min(u128::from(u64::MAX)) as u64)
            .max(1)
    })
}

fn query_stream_completion(
    result: Result<crate::pb::QueryResponse, Status>,
    revision: u64,
    scoring_fingerprints: Vec<String>,
) -> crate::pb::QueryStreamCompletion {
    match result {
        Ok(response) => crate::pb::QueryStreamCompletion {
            completed: true,
            response: Some(response),
            final_revision: revision,
            scoring_fingerprints,
            error_code: 0,
            error_message: String::new(),
            error_disclosure: None,
        },
        Err(status) => crate::pb::QueryStreamCompletion {
            completed: false,
            response: None,
            final_revision: revision,
            scoring_fingerprints,
            error_code: status.code() as u32,
            error_message: status.message().to_string(),
            error_disclosure: crate::error_disclosure::status_detail(&status),
        },
    }
}

async fn send_query_completion(
    tx: &QueryStreamSender,
    completion: crate::pb::QueryStreamCompletion,
) {
    let _ = tx
        .send(Ok(crate::pb::QueryStreamResponse {
            payload: Some(crate::pb::query_stream_response::Payload::Completion(
                completion,
            )),
        }))
        .await;
}
