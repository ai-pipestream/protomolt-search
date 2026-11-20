//! Per-spec extra-field analysis sessions and their shared event channel.

use crate::node::*;

/// Documents held for ordered apply; bounds this side's memory the way
/// ANALYZE_PIPELINE bounded the unary path.
pub(super) const MAX_PENDING: usize = 32;

/// One event from the extra-field analysis streams.
pub(in crate::node) enum FieldEvent {
    /// A field finished: the submission sequence it was tagged with, and
    /// either its analysis or that ONE field's own failure.
    Result(u64, Result<crate::postings::AnalyzedField, Status>),
    /// The stream itself failed, so every field riding it is lost. Kept
    /// distinct from a per-field error because a dead stream cannot be
    /// attributed to a sequence, and silently dropping it would hang the
    /// apply wavefront on a field that is never coming.
    StreamFailed(Status),
}

/// Persistent per-spec [`crate::analyzer::AnalyzeStream`] sessions for extra
/// document fields, opened lazily per distinct `AnalysisSpec` and reused across
/// the ingest call.
pub(in crate::node) struct FieldStreams {
    pub(in crate::node) addr: String,
    /// Submission handles, one per distinct spec in first-seen order.
    /// Holding the SUBMITTER here rather than the session is what lets
    /// [`finish`](Self::finish) half-close every stream by dropping them;
    /// each session itself is owned by its driver task.
    pub(in crate::node) sessions: Vec<(
        Option<crate::pb::AnalysisSpec>,
        crate::analyzer::AnalyzeSubmit,
    )>,
    pub(in crate::node) events: tokio::sync::mpsc::Receiver<FieldEvent>,
    /// Cloned into each driver as it is spawned. Taking it in `finish` is
    /// what makes `recv` observe `None` once every driver has exited.
    pub(in crate::node) emit: Option<tokio::sync::mpsc::Sender<FieldEvent>>,
    /// Monotonic across every session: the sequence is this side's
    /// routing key, and results are matched by it alone.
    pub(in crate::node) next_sequence: u64,
    /// Submitted minus delivered. `recv` must not be selected on when
    /// this is zero, or it parks on a channel nothing will feed.
    pub(in crate::node) outstanding: usize,
}

impl FieldStreams {
    pub(in crate::node) fn new(addr: &str) -> Self {
        // Deep enough that drivers rarely block on a full channel while
        // the apply wavefront is busy; every item is one analyzed field.
        let (emit, events) = tokio::sync::mpsc::channel(MAX_PENDING * 4);
        Self {
            addr: addr.to_string(),
            sessions: Vec::new(),
            events,
            emit: Some(emit),
            next_sequence: 0,
            outstanding: 0,
        }
    }

    /// Queue one field's text on the session for its spec, opening that
    /// session if this is the spec's first field. Returns the sequence
    /// the result will carry.
    pub(in crate::node) async fn submit(
        &mut self,
        spec: Option<&crate::pb::AnalysisSpec>,
        text: &str,
    ) -> Result<u64, Status> {
        let index = match self.sessions.iter().position(|(s, _)| s.as_ref() == spec) {
            Some(index) => index,
            None => {
                let mut session = crate::analyzer::AnalyzeStream::open(&self.addr, spec)
                    .await
                    .map_err(|status| {
                        if status.code() == tonic::Code::Unimplemented {
                            Status::failed_precondition(format!(
                                "analysis server at {} does not implement AnalyzeStream; \
                                 rebuild turbo-kserve from main. Refusing to analyze fields on the \
                                 removed unary path.",
                                self.addr
                            ))
                        } else {
                            status
                        }
                    })?;
                let submit = session.submitter();
                // Hand the driver a session that holds NO submitter of
                // its own, so the clone kept here is the only one alive
                // and dropping it in `finish` really does half-close the
                // stream. `open` leaves a submitter inside the session;
                // leaving it there means the sidecar never sees the
                // half-close, and a held response never flushes.
                session.finish();
                let emit = self
                    .emit
                    .clone()
                    .ok_or_else(|| Status::internal("field streams already finished"))?;
                tokio::spawn(drive_field_stream(session, emit));
                self.sessions.push((spec.cloned(), submit));
                self.sessions.len() - 1
            }
        };
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.sessions[index].1.submit(sequence, text).await?;
        self.outstanding += 1;
        Ok(sequence)
    }

    /// True when a field result is still owed, and therefore when `recv`
    /// is safe to select on.
    pub(in crate::node) fn pending(&self) -> bool {
        self.outstanding > 0
    }

    /// The next field event. `None` once every driver has exited, which
    /// only happens after [`finish`](Self::finish) drops the last sender.
    pub(in crate::node) async fn recv(&mut self) -> Option<FieldEvent> {
        let event = self.events.recv().await;
        if matches!(event, Some(FieldEvent::Result(..))) {
            self.outstanding -= 1;
        }
        event
    }

    /// Half-close every stream so the sidecar drains what is in flight.
    /// Idempotent, and required before awaiting the final results: a
    /// sidecar may hold a response until more work arrives (the test mock
    /// deliberately does), so the last field of a call only lands once
    /// its stream is closing.
    pub(in crate::node) fn finish(&mut self) {
        self.sessions.clear();
        self.emit = None;
    }
}

/// Pump one field session's results into the shared event channel.
///
/// Runs as its own task so the main ingest loop never has to poll N
/// streams by hand, and so a session whose results the sidecar is holding
/// cannot stall submission on a different session.
async fn drive_field_stream(
    mut session: crate::analyzer::AnalyzeStream,
    emit: tokio::sync::mpsc::Sender<FieldEvent>,
) {
    loop {
        let event = match session.next().await {
            Ok(Some((sequence, result))) => FieldEvent::Result(
                sequence,
                result.map(crate::postings::AnalyzedDoc::into_body),
            ),
            Ok(None) => return,
            Err(status) => FieldEvent::StreamFailed(status),
        };
        let failed = matches!(event, FieldEvent::StreamFailed(_));
        // A closed receiver means the ingest call is already unwinding.
        if emit.send(event).await.is_err() || failed {
            return;
        }
    }
}
