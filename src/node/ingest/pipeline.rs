//! Streaming field-analysis coordination and mapped/plain document intake.

use crate::node::*;

impl NodeServiceImpl {
    pub(in crate::node) fn doc_session_layers(
        &self,
        doc: &AddDocumentsRequest,
    ) -> crate::analyzer::SessionLayers {
        let mut layers = session_layers(
            doc,
            self.phrase_index.as_deref(),
            &self.config.sentence_fields,
        );
        layers.nlp_columns = doc.nlp_materialization.is_empty()
            && self
                .nlp_columns
                .as_deref()
                .is_some_and(crate::turbo_bridge::NlpColumnSpec::is_enabled);
        layers
    }
}

/// Documents held for ordered apply; bounds this side's memory the way
/// ANALYZE_PIPELINE bounded the unary path.
const MAX_PENDING: usize = 32;

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

/// One document held between submission and apply: the request itself,
/// plus its extra fields filling in as their results arrive.
pub(in crate::node) struct PendingDoc {
    pub(in crate::node) doc: AddDocumentsRequest,
    /// The document's dense vector, mapped ingest only: it applies in
    /// LOCKSTEP with the document, landing at the same id. `None` on
    /// the ordinary AddDocuments path.
    pub(in crate::node) vector: Option<Vec<f32>>,
    pub(in crate::node) stable_routing_key: Option<Vec<u8>>,
    /// `(field table index, analysis)`, in submission order. `None` until
    /// that field's result lands.
    pub(in crate::node) extras: Vec<(usize, Option<crate::postings::AnalyzedField>)>,
    /// Extras still unfilled. The document is ready to apply when this is
    /// zero AND its body result has arrived.
    pub(in crate::node) outstanding: usize,
}

/// One document entering the shared ingest pipeline: the ordinary
/// request plus, on the mapped path, its vector.
pub(in crate::node) struct IngestDoc {
    pub(in crate::node) req: AddDocumentsRequest,
    pub(in crate::node) vector: Option<Vec<f32>>,
    /// Opaque product identity supplied only by routed mapped ingest.
    pub(in crate::node) stable_routing_key: Option<Vec<u8>>,
}

/// Where the ingest pipeline's documents come from: the ordinary
/// AddDocuments stream verbatim, or the mapped stream decoding each
/// serialized protobuf document against the bound plan. One pipeline,
/// with private accepted-source staging as another input. Each reuses the
/// analysis session, apply wavefront and column validation. Only live ingest
/// writes the target's WAL.
pub(in crate::node) enum IngestSource<'a> {
    Plain {
        stream: &'a mut Streaming<AddDocumentsRequest>,
        stable_routing_key: Option<Vec<u8>>,
        consumed: bool,
    },
    /// Boxed: the extractor's trie dwarfs the plain variant.
    Mapped(Box<MappedSource<'a>>),
    // Only the accepted-source staging path constructs this private iterator.
    Prepared(Box<projection::PreparedSource>),
}

pub(in crate::node) struct MappedSource<'a> {
    pub(in crate::node) stream: &'a mut Streaming<crate::pb::IngestMappedRequest>,
    pub(in crate::node) extractor: crate::mapping::Extractor,
    /// Session properties from the bind, attached to every decoded
    /// document.
    pub(in crate::node) analysis: Option<crate::pb::AnalysisSpec>,
    pub(in crate::node) field_analysis: std::collections::BTreeMap<String, crate::pb::AnalysisSpec>,
    pub(in crate::node) materialize: Option<crate::pb::MaterializeSpec>,
    /// Source documents decoded so far; extraction errors name the
    /// failing position.
    pub(in crate::node) position: u64,
    /// Source documents consumed (== position, kept for the response's
    /// accounting: a chunked document yields as many rows as it has
    /// chunks, including zero).
    pub(in crate::node) parents: u64,
    /// Rows decoded but not yet handed to the pipeline: a chunked
    /// document explodes into one row per chunk.
    pub(in crate::node) rows: std::collections::VecDeque<IngestDoc>,
    pub(in crate::node) original_source: crate::pb::ProtobufSource,
}

impl IngestSource<'_> {
    pub(in crate::node) async fn next(&mut self) -> Result<Option<IngestDoc>, Status> {
        let projected = matches!(self, Self::Mapped(_) | Self::Prepared(_));
        let mut document = match self {
            IngestSource::Plain {
                stream,
                stable_routing_key,
                consumed,
            } => {
                let request = stream.message().await?;
                if request.is_some() && *consumed && stable_routing_key.is_some() {
                    return Err(Status::invalid_argument(
                        "a replication stable-key metadata value may carry exactly one document",
                    ));
                }
                *consumed |= request.is_some();
                Ok(request.map(|req| IngestDoc {
                    req,
                    vector: None,
                    stable_routing_key: stable_routing_key.clone(),
                }))
            }
            IngestSource::Mapped(source) => source.next().await,
            IngestSource::Prepared(source) => source.next(),
        }?;
        if projected {
            if let Some(doc) = &mut document {
                // Extraction and accepted-source projections retain explicit
                // empty-field presence. Only the analyzer input omits empty
                // secondary text; the original protobuf bytes remain intact.
                doc.req.fields.retain(|field| !field.text.is_empty());
            }
        }
        Ok(document)
    }
}

impl MappedSource<'_> {
    pub(in crate::node) async fn next(&mut self) -> Result<Option<IngestDoc>, Status> {
        use crate::pb::ingest_mapped_request::Payload;
        loop {
            if let Some(mut row) = self.rows.pop_front() {
                row.req.original_source = Some(self.original_source.clone());
                return Ok(Some(row));
            }
            match self.stream.message().await? {
                None => return Ok(None),
                Some(message) => match message.payload {
                    // A zero-chunk document decodes to zero rows; loop
                    // for the next message rather than ending the
                    // stream.
                    Some(Payload::Document(bytes)) => self.decode(&bytes, None)?,
                    Some(Payload::RoutedDocument(document)) => {
                        if document.stable_key.is_empty() {
                            return Err(Status::invalid_argument(
                                "routed mapped document has an empty stable key",
                            ));
                        }
                        self.decode(&document.document, Some(document.stable_key))?;
                    }
                    Some(Payload::Bind(_)) => {
                        return Err(Status::invalid_argument(
                            "bind repeats mid-stream; a mapped stream binds exactly once, \
                             first",
                        ))
                    }
                    None => {
                        return Err(Status::invalid_argument(
                            "empty IngestMappedRequest payload",
                        ))
                    }
                },
            }
        }
    }

    pub(in crate::node) fn decode(
        &mut self,
        bytes: &[u8],
        stable_routing_key: Option<Vec<u8>>,
    ) -> Result<(), Status> {
        let position = self.position;
        self.position += 1;
        let rows = self.extractor.extract(bytes).map_err(|status| {
            Status::new(
                status.code(),
                format!("document {position}: {}", status.message()),
            )
        })?;
        self.parents += 1;
        self.original_source.payload = bytes.to_vec();
        let chunked = !self.extractor.plan().chunks_path.is_empty();
        for (ordinal, extracted) in rows.into_iter().enumerate() {
            let mut req = extracted.request;
            req.source_chunk_ordinal = if chunked {
                Some(
                    u32::try_from(ordinal)
                        .map_err(|_| Status::invalid_argument("chunk ordinal exceeds u32"))?,
                )
            } else {
                None
            };
            req.analysis = self.analysis.clone();
            for field in &mut req.fields {
                field.analysis = self.field_analysis.get(&field.field).cloned();
            }
            req.materialize = self.materialize.clone();
            self.rows.push_back(IngestDoc {
                req,
                vector: Some(extracted.vector),
                stable_routing_key: stable_routing_key.clone(),
            });
        }
        Ok(())
    }
}

/// The optional sidecar layers a document's specs ask its analysis
/// session for — the session-identity companion to the reopen
/// condition (a change to either spec reopens the session).
pub(crate) fn session_layers(
    doc: &AddDocumentsRequest,
    phrase_index: Option<&crate::phrases::PhraseIndex>,
    sentence_fields: &[String],
) -> crate::analyzer::SessionLayers {
    crate::analyzer::SessionLayers {
        quality: quality_wanted(doc.quality.as_ref()),
        geography: geography_wanted(doc.geography.as_ref()),
        entities: phrase_index.is_some_and(crate::phrases::PhraseIndex::include_ner),
        // Request sentence spans whenever the node is configured to store them.
        sentences: !sentence_fields.is_empty(),
        dual_cased: !doc.cased_field.is_empty(),
        nlp_columns: false,
    }
}

impl NodeServiceImpl {
    /// Validate a document's extra fields against the shard's configured
    /// field table and queue their analyses on the per-spec field
    /// streams. Validation failures surface BEFORE any store or WAL
    /// effect, and before anything is submitted.
    ///
    /// Every field's sequence is recorded in `route` so its result can be
    /// steered back to `(document sequence, slot in that document's
    /// extras)`; the sequence is the only key the wire carries.
    pub(in crate::node) async fn submit_field_analyses(
        &self,
        doc: &AddDocumentsRequest,
        sequence: u64,
        streams: &mut FieldStreams,
        route: &mut std::collections::HashMap<u64, (u64, usize)>,
    ) -> Result<Vec<(usize, Option<crate::postings::AnalyzedField>)>, Status> {
        // A cased field is validated here, before the body is submitted,
        // so a misnamed one is refused without analyzing anything.
        cased_field_index(&self.config, self.phrase_index.as_deref(), doc)?;
        if doc.fields.is_empty() {
            return Ok(Vec::new());
        }
        // Validate the whole document first: a partially submitted
        // document would leave orphan sequences in flight that nothing
        // ever routes.
        let mut seen: Vec<&str> = Vec::new();
        let mut accepted: Vec<usize> = Vec::with_capacity(doc.fields.len());
        for field in &doc.fields {
            if field.field == "body" {
                return Err(Status::invalid_argument(
                    "\"body\" is the top-level text; DocumentField names extra fields only",
                ));
            }
            if self
                .phrase_index
                .as_ref()
                .is_some_and(|phrases| field.field == phrases.phrase_field())
            {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is derived from the configured phrase glossary; clients must not supply it",
                    field.field
                )));
            }
            if let Some(source) = self
                .config
                .bigram_fields
                .iter()
                .find(|source| crate::proximity::bigram_field_name(source) == field.field)
            {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is the bigram column derived from {source:?}; clients must not supply it",
                    field.field
                )));
            }
            if seen.contains(&field.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in one document",
                    field.field
                )));
            }
            if !doc.cased_field.is_empty() && field.field == doc.cased_field {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is the body's cased identity (cased_field), produced from the \
                     body's analysis; do not supply it as a DocumentField",
                    field.field
                )));
            }
            seen.push(&field.field);
            let Some(fi) = self
                .config
                .bm25_fields
                .iter()
                .position(|n| *n == field.field)
            else {
                return Err(Status::invalid_argument(format!(
                    "unknown field {:?}; this shard indexes {:?}",
                    field.field, self.config.bm25_fields
                )));
            };
            if field.text.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "field {:?} has empty text; omit absent fields instead",
                    field.field
                )));
            }
            accepted.push(fi);
        }
        let mut extras = Vec::with_capacity(accepted.len());
        for (slot, (field, fi)) in doc.fields.iter().zip(accepted).enumerate() {
            let tag = streams.submit(field.analysis.as_ref(), &field.text).await?;
            route.insert(tag, (sequence, slot));
            extras.push((fi, None));
        }
        Ok(extras)
    }

    /// Apply one analyzed document: id assignment, store insert, WAL
    /// append. Must be called in arrival order — both transports
    /// guarantee it.
    pub(in crate::node) async fn apply_analyzed_document(
        &self,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        vector: Option<Vec<f32>>,
        stable_routing_key: Option<Vec<u8>>,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        // Refuse live requests carrying forged derived column values or
        // fingerprints before placement checks and column materialization run.
        if let Some(declaration) = self.config.derived.as_deref() {
            declaration.refuse_carried(&doc)?;
        }
        self.check_placement(&doc, stable_routing_key.as_deref())?;
        let (doc, analyzed) =
            self.materialize_document(doc, analyzed, stable_routing_key.as_deref())?;
        let _mutation = self.mutation_gate.read().await;
        let mut guard = write_shard(&self.state);
        self.apply_document_locked(
            &mut guard,
            doc,
            analyzed,
            vector,
            stable_routing_key,
            added,
            first_id,
        )
    }

    /// Bulk ingest over one AnalyzeStream: submissions run ahead of the
    /// apply point as far as the sidecar grants credit, results return
    /// in completion order, and the apply wavefront advances over
    /// consecutive sequences so application stays in arrival order.
    pub(in crate::node) async fn ingest_streamed(
        &self,
        mut session: crate::analyzer::AnalyzeStream,
        first: IngestDoc,
        source: &mut IngestSource<'_>,
        addr: &str,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
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
        // One Step lives at a time, on the stack of this loop; boxing
        // the request to shrink the enum would cost an allocation per
        // ingested document for no held memory.
        #[allow(clippy::large_enum_variant)]
        enum Step {
            Doc(IngestDoc),
            InboundClosed,
            Result(Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>),
            Field(Option<FieldEvent>),
        }
        let IngestDoc {
            req: first,
            vector: first_vector,
            stable_routing_key: first_stable_routing_key,
        } = first;
        let mut spec = first.analysis.clone();
        let mut quality = first.quality.clone();
        let mut geography = first.geography.clone();
        let mut cased_field = first.cased_field.clone();
        let mut nlp_layers = self.doc_session_layers(&first).nlp_columns;
        let mut submit = Some(session.submitter());
        // The body session covers the BODY only; extra fields ride their
        // own per-spec sessions, and a document applies once its body and
        // every one of its fields have landed.
        let mut fields = FieldStreams::new(addr);
        let mut route: std::collections::HashMap<u64, (u64, usize)> =
            std::collections::HashMap::new();
        let mut pending: std::collections::BTreeMap<u64, PendingDoc> =
            std::collections::BTreeMap::new();
        let mut results: std::collections::HashMap<u64, crate::postings::AnalyzedDoc> =
            std::collections::HashMap::new();
        let first_extras = self
            .submit_field_analyses(&first, 0, &mut fields, &mut route)
            .await?;
        submit
            .as_ref()
            .expect("submitter set above")
            .submit(0, &first.text)
            .await?;
        pending.insert(
            0,
            PendingDoc {
                doc: first,
                vector: first_vector,
                stable_routing_key: first_stable_routing_key,
                outstanding: first_extras.len(),
                extras: first_extras,
            },
        );
        let mut next_seq = 1u64;
        let mut next_apply = 0u64;
        let mut inbound_open = true;
        loop {
            self.advance_apply(&mut pending, &mut results, &mut next_apply, added, first_id)
                .await?;
            // A full tail seals here, between documents, so one long
            // stream never grows a segment past --seal-tail-docs.
            if self.seal_if_due().await? {
                continue;
            }
            if pending.is_empty() && !inbound_open {
                break;
            }
            // Guards, so a stream that owes nothing is never polled. The
            // body session owes exactly the pending documents whose body
            // result has not landed; once it is finished and drained it
            // yields `None` forever, which `store_result` would rightly
            // read as truncation.
            let want_body = pending.len() > results.len();
            let want_field = fields.pending();
            // At least one arm is always live: if a document is still
            // pending after `advance_apply`, the one at the wavefront is
            // owed either its body or a field.
            let step = tokio::select! {
                message = source.next(),
                    if inbound_open && pending.len() < MAX_PENDING => match message? {
                        Some(doc) => Step::Doc(doc),
                        None => Step::InboundClosed,
                    },
                result = session.next(), if want_body => Step::Result(result?),
                event = fields.recv(), if want_field => Step::Field(event),
            };
            match step {
                Step::Doc(doc) => {
                    let IngestDoc {
                        req: doc,
                        vector: doc_vector,
                        stable_routing_key,
                    } = doc;
                    // Extra-field analyses are queued on arrival
                    // (validated now, so a bad field fails before the
                    // body enters the session).
                    let extras = self
                        .submit_field_analyses(&doc, next_seq, &mut fields, &mut route)
                        .await?;
                    // The quality layers are requested in the session's
                    // options message, so a change to them reopens the
                    // session for the same reason a spec change does.
                    if doc.analysis != spec
                        || doc.quality != quality
                        || doc.geography != geography
                        || doc.cased_field != cased_field
                        || self.doc_session_layers(&doc).nlp_columns != nlp_layers
                    {
                        // A mid-stream BODY spec change (rare): collect
                        // what the current session still owes so nothing
                        // is lost when it is replaced, then open a new
                        // one. Dropping the submitter clone is what lets
                        // the old session half-close and drain.
                        //
                        // Only the BODY session is being replaced. Field
                        // sessions are per-spec and outlive this, and
                        // draining them here would DEADLOCK: a sidecar
                        // may hold a result until more work arrives on
                        // that stream (the test mock deliberately does),
                        // and no more field work is coming until the new
                        // body session is open.
                        drop(submit.take());
                        session.finish();
                        while pending.len() > results.len() {
                            store_result(&mut results, session.next().await?)?;
                        }
                        // Every result is in hand; read the server's end
                        // so the stream closes without a reset.
                        session.drain_to_end().await?;
                        self.advance_apply(
                            &mut pending,
                            &mut results,
                            &mut next_apply,
                            added,
                            first_id,
                        )
                        .await?;
                        session = crate::analyzer::AnalyzeStream::open_with_vocab(
                            addr,
                            doc.analysis.as_ref(),
                            self.vocab.clone(),
                            self.doc_session_layers(&doc),
                        )
                        .await?;
                        spec = doc.analysis.clone();
                        quality = doc.quality.clone();
                        geography = doc.geography.clone();
                        cased_field = doc.cased_field.clone();
                        nlp_layers = self.doc_session_layers(&doc).nlp_columns;
                        submit = Some(session.submitter());
                    }
                    submit
                        .as_ref()
                        .expect("stream open while inbound open")
                        .submit(next_seq, &doc.text)
                        .await?;
                    pending.insert(
                        next_seq,
                        PendingDoc {
                            doc,
                            vector: doc_vector,
                            stable_routing_key,
                            outstanding: extras.len(),
                            extras,
                        },
                    );
                    next_seq += 1;
                }
                Step::InboundClosed => {
                    inbound_open = false;
                    submit = None;
                    session.finish();
                    // Half-close the field streams too. A sidecar may
                    // hold a result until more work arrives, so the last
                    // field of the call only lands once its stream is
                    // closing.
                    fields.finish();
                }
                Step::Result(item) => store_result(&mut results, item)?,
                Step::Field(event) => store_field(&mut pending, &mut route, event)?,
            }
        }
        Ok(())
    }

    /// Advance the apply wavefront over every consecutive sequence whose
    /// body AND every extra field have landed, keeping application in
    /// arrival order.
    pub(in crate::node) async fn advance_apply(
        &self,
        pending: &mut std::collections::BTreeMap<u64, PendingDoc>,
        results: &mut std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
        next_apply: &mut u64,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        loop {
            let ready = results.contains_key(next_apply)
                && pending.get(next_apply).is_some_and(|p| p.outstanding == 0);
            if !ready || self.tail_full() {
                return Ok(());
            }
            let analyzed = results.remove(next_apply).expect("readiness just checked");
            let held = pending.remove(next_apply).expect("readiness just checked");
            let cased = cased_field_index(&self.config, self.phrase_index.as_deref(), &held.doc)?;
            let analyzed = join_fields(analyzed, held.extras, cased)?;
            self.apply_analyzed_document(
                held.doc,
                analyzed,
                held.vector,
                held.stable_routing_key,
                added,
                first_id,
            )
            .await?;
            *next_apply += 1;
        }
    }
}
