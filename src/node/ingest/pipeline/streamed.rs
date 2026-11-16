//! Bulk ingest over one body AnalyzeStream plus the per-spec field streams.

use super::field_streams::MAX_PENDING;
use super::{FieldEvent, FieldStreams, IngestDoc, IngestSource, PendingDoc};
use crate::node::*;

fn store_result(
    results: &mut std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
    item: Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>,
) -> Result<(), Status> {
    match item {
        Some((sequence, Ok(analyzed))) => {
            results.insert(sequence, analyzed);
            Ok(())
        }
        // One document failing fails the ingest call, exactly as
        // a failed unary analysis did.
        Some((_, Err(status))) => Err(status),
        None => Err(Status::internal(
            "analysis stream completed with documents in flight",
        )),
    }
}

/// Steer one field result into its document's slot. The route
/// table is the only thing that knows which document a sequence
/// belonged to, so an unknown sequence is a wire-level bug, not a
/// recoverable condition.
fn store_field(
    pending: &mut std::collections::BTreeMap<u64, PendingDoc>,
    route: &mut std::collections::HashMap<u64, (u64, usize)>,
    event: Option<FieldEvent>,
) -> Result<(), Status> {
    match event {
        Some(FieldEvent::Result(tag, result)) => {
            let (sequence, slot) = route.remove(&tag).ok_or_else(|| {
                Status::internal(format!("field result {tag} matches no submitted field"))
            })?;
            // One field failing fails the ingest call, exactly as
            // a failed unary field analysis did.
            let analyzed = result?;
            let doc = pending.get_mut(&sequence).ok_or_else(|| {
                Status::internal(format!(
                    "field result for document {sequence}, which is no longer pending"
                ))
            })?;
            if doc.extras[slot].1.replace(analyzed).is_none() {
                doc.outstanding -= 1;
            }
            Ok(())
        }
        Some(FieldEvent::StreamFailed(status)) => Err(status),
        None => Err(Status::internal(
            "field analysis streams ended with fields in flight",
        )),
    }
}

// One Step lives at a time, on the stack of the ingest loop; boxing
// the request to shrink the enum would cost an allocation per
// ingested document for no held memory.
#[allow(clippy::large_enum_variant)]
enum Step {
    Doc(IngestDoc),
    InboundClosed,
    Result(Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>),
    Field(Option<FieldEvent>),
}

/// The session-identity properties of a body analysis session. The quality
/// layers are requested in the session's options message, so a change to
/// them reopens the session for the same reason a spec change does.
struct BodySessionKey {
    spec: Option<crate::pb::AnalysisSpec>,
    quality: Option<crate::pb::QualitySpec>,
    geography: Option<crate::pb::GeographySpec>,
    cased_field: String,
    nlp_layers: bool,
}

impl BodySessionKey {
    fn of(node: &NodeServiceImpl, doc: &AddDocumentsRequest) -> Self {
        Self {
            spec: doc.analysis.clone(),
            quality: doc.quality.clone(),
            geography: doc.geography.clone(),
            cased_field: doc.cased_field.clone(),
            nlp_layers: node.doc_session_layers(doc).nlp_columns,
        }
    }

    fn differs(&self, node: &NodeServiceImpl, doc: &AddDocumentsRequest) -> bool {
        doc.analysis != self.spec
            || doc.quality != self.quality
            || doc.geography != self.geography
            || doc.cased_field != self.cased_field
            || node.doc_session_layers(doc).nlp_columns != self.nlp_layers
    }
}

/// Progress counters the apply wavefront reports back to the RPC handler.
struct Applied<'a> {
    added: &'a mut u64,
    first_id: &'a mut u64,
}

/// One streamed ingest call's in-flight state. Fields drop in declaration
/// order, so the submitter and field streams close before the body session.
struct StreamedIngest<'a> {
    node: &'a NodeServiceImpl,
    addr: &'a str,
    results: std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
    pending: std::collections::BTreeMap<u64, PendingDoc>,
    route: std::collections::HashMap<u64, (u64, usize)>,
    /// The body session covers the BODY only; extra fields ride their
    /// own per-spec sessions, and a document applies once its body and
    /// every one of its fields have landed.
    fields: FieldStreams,
    submit: Option<crate::analyzer::AnalyzeSubmit>,
    key: BodySessionKey,
    session: crate::analyzer::AnalyzeStream,
    next_seq: u64,
    next_apply: u64,
    inbound_open: bool,
}

impl<'a> StreamedIngest<'a> {
    /// Submit the call's first document on the already-open body session.
    async fn start(
        node: &'a NodeServiceImpl,
        session: crate::analyzer::AnalyzeStream,
        first: IngestDoc,
        addr: &'a str,
    ) -> Result<Self, Status> {
        let IngestDoc {
            req: first,
            vector: first_vector,
            stable_routing_key: first_stable_routing_key,
        } = first;
        let mut run = Self {
            node,
            addr,
            results: std::collections::HashMap::new(),
            pending: std::collections::BTreeMap::new(),
            route: std::collections::HashMap::new(),
            fields: FieldStreams::new(addr),
            submit: Some(session.submitter()),
            key: BodySessionKey::of(node, &first),
            session,
            next_seq: 1,
            next_apply: 0,
            inbound_open: true,
        };
        let first_extras = node
            .submit_field_analyses(&first, 0, &mut run.fields, &mut run.route)
            .await?;
        run.submit
            .as_ref()
            .expect("submitter set above")
            .submit(0, &first.text)
            .await?;
        run.pending.insert(
            0,
            PendingDoc {
                doc: first,
                vector: first_vector,
                stable_routing_key: first_stable_routing_key,
                outstanding: first_extras.len(),
                extras: first_extras,
            },
        );
        Ok(run)
    }

    async fn advance_apply(&mut self, applied: &mut Applied<'_>) -> Result<(), Status> {
        self.node
            .advance_apply(
                &mut self.pending,
                &mut self.results,
                &mut self.next_apply,
                applied.added,
                applied.first_id,
            )
            .await
    }

    fn finished(&self) -> bool {
        self.pending.is_empty() && !self.inbound_open
    }

    async fn next_step(&mut self, source: &mut IngestSource<'_>) -> Result<Step, Status> {
        // Guards, so a stream that owes nothing is never polled. The
        // body session owes exactly the pending documents whose body
        // result has not landed; once it is finished and drained it
        // yields `None` forever, which `store_result` would rightly
        // read as truncation.
        let want_body = self.pending.len() > self.results.len();
        let want_field = self.fields.pending();
        // At least one arm is always live: if a document is still
        // pending after `advance_apply`, the one at the wavefront is
        // owed either its body or a field.
        let step = tokio::select! {
            message = source.next(),
                if self.inbound_open && self.pending.len() < MAX_PENDING => match message? {
                    Some(doc) => Step::Doc(doc),
                    None => Step::InboundClosed,
                },
            result = self.session.next(), if want_body => Step::Result(result?),
            event = self.fields.recv(), if want_field => Step::Field(event),
        };
        Ok(step)
    }

    /// Replace the BODY session after a mid-stream spec change (rare): collect
    /// what the current session still owes so nothing is lost when it is
    /// replaced, then open a new one. Dropping the submitter clone is what
    /// lets the old session half-close and drain.
    ///
    /// Only the BODY session is being replaced. Field sessions are per-spec
    /// and outlive this, and draining them here would DEADLOCK: a sidecar may
    /// hold a result until more work arrives on that stream (the test mock
    /// deliberately does), and no more field work is coming until the new
    /// body session is open.
    async fn reopen_body_session(
        &mut self,
        doc: &AddDocumentsRequest,
        applied: &mut Applied<'_>,
    ) -> Result<(), Status> {
        drop(self.submit.take());
        self.session.finish();
        while self.pending.len() > self.results.len() {
            store_result(&mut self.results, self.session.next().await?)?;
        }
        // Every result is in hand; read the server's end
        // so the stream closes without a reset.
        self.session.drain_to_end().await?;
        self.advance_apply(applied).await?;
        self.session = crate::analyzer::AnalyzeStream::open_with_vocab(
            self.addr,
            doc.analysis.as_ref(),
            self.node.vocab.clone(),
            self.node.doc_session_layers(doc),
        )
        .await?;
        self.key = BodySessionKey::of(self.node, doc);
        self.submit = Some(self.session.submitter());
        Ok(())
    }

    async fn accept_doc(
        &mut self,
        doc: IngestDoc,
        applied: &mut Applied<'_>,
    ) -> Result<(), Status> {
        let IngestDoc {
            req: doc,
            vector: doc_vector,
            stable_routing_key,
        } = doc;
        // Extra-field analyses are queued on arrival
        // (validated now, so a bad field fails before the
        // body enters the session).
        let extras = self
            .node
            .submit_field_analyses(&doc, self.next_seq, &mut self.fields, &mut self.route)
            .await?;
        if self.key.differs(self.node, &doc) {
            self.reopen_body_session(&doc, applied).await?;
        }
        self.submit
            .as_ref()
            .expect("stream open while inbound open")
            .submit(self.next_seq, &doc.text)
            .await?;
        self.pending.insert(
            self.next_seq,
            PendingDoc {
                doc,
                vector: doc_vector,
                stable_routing_key,
                outstanding: extras.len(),
                extras,
            },
        );
        self.next_seq += 1;
        Ok(())
    }

    fn close_inbound(&mut self) {
        self.inbound_open = false;
        self.submit = None;
        self.session.finish();
        // Half-close the field streams too. A sidecar may
        // hold a result until more work arrives, so the last
        // field of the call only lands once its stream is
        // closing.
        self.fields.finish();
    }
}

impl NodeServiceImpl {
    /// Bulk ingest over one AnalyzeStream: submissions run ahead of the
    /// apply point as far as the sidecar grants credit, results return
    /// in completion order, and the apply wavefront advances over
    /// consecutive sequences so application stays in arrival order.
    pub(in crate::node) async fn ingest_streamed(
        &self,
        session: crate::analyzer::AnalyzeStream,
        first: IngestDoc,
        source: &mut IngestSource<'_>,
        addr: &str,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        let mut applied = Applied { added, first_id };
        let mut run = StreamedIngest::start(self, session, first, addr).await?;
        loop {
            run.advance_apply(&mut applied).await?;
            // A full tail seals here, between documents, so one long
            // stream never grows a segment past --seal-tail-docs.
            if self.seal_if_due().await? {
                continue;
            }
            if run.finished() {
                break;
            }
            match run.next_step(source).await? {
                Step::Doc(doc) => run.accept_doc(doc, &mut applied).await?,
                Step::InboundClosed => run.close_inbound(),
                Step::Result(item) => store_result(&mut run.results, item)?,
                Step::Field(event) => store_field(&mut run.pending, &mut run.route, event)?,
            }
        }
        Ok(())
    }
}
