//! Document sources feeding the ingest pipeline: plain, mapped, and prepared.

use crate::domain_error::DomainError;
use crate::node::*;

/// One document held between submission and apply: the request itself,
/// plus its extra fields filling in as their results arrive.
pub(in crate::node) struct PendingDoc {
    pub(in crate::node) doc: AddDocumentsRequest,
    /// The document's dense vector, mapped ingest only: it applies in
    /// LOCKSTEP with the document, landing at the same id. `None` on
    /// the ordinary AddDocuments path.
    pub(in crate::node) vector: Option<Vec<f32>>,
    pub(in crate::node) stable_routing_key: Option<Vec<u8>>,
    /// True for a document a WAL replay stream carries: it keeps the
    /// derived values its primary logged instead of being refused for them.
    pub(in crate::node) logged: bool,
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
    /// Opaque product identity supplied by routed mapped ingest and by
    /// replication streams.
    pub(in crate::node) stable_routing_key: Option<Vec<u8>>,
    /// True only for a document of a WAL replay stream.
    pub(in crate::node) logged: bool,
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
        keys: StreamKeys,
        /// The stream replays a primary's WAL.
        logged: bool,
        replay_permit: Option<Box<crate::authorization::AccessPermit>>,
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
                keys,
                logged,
                replay_permit,
            } => {
                let message = stream.message().await?;
                if let Some(permit) = replay_permit {
                    permit.check()?;
                }
                match message {
                    Some(req) => Ok(Some(IngestDoc {
                        req,
                        vector: None,
                        stable_routing_key: keys.next_row("document")?,
                        logged: *logged,
                    })),
                    None => {
                        keys.finish("document")?;
                        Ok(None)
                    }
                }
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
        let rows = self
            .extractor
            .extract(bytes)
            .map_err(|error| Status::from(error.prefixed(&format!("document {position}: "))))?;
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
                logged: false,
            });
        }
        Ok(())
    }
}
