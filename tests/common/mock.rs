//! A mock AnalysisService for tests: deterministic, model-free analysis
//! with the same contract as the real sidecar — whitespace tokens with
//! original-text spans, term vectors in first-occurrence order, and a toy
//! stemmer for the SOURCE_STEMS path. Test corpora are ASCII, so byte
//! offsets == char offsets == UTF-16 units.

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::{Error as TransportError, Server};
use tonic::{Request, Response, Status};
use turbovec_search::pb::analysis::analysis_service_server::{
    AnalysisService, AnalysisServiceServer,
};
use turbovec_search::pb::analysis::{
    AnalyzeRequest, AnalyzeResponse, GetCapabilitiesRequest, GetCapabilitiesResponse, Span,
    TermVector, Token,
};
use turbovec_search::MAX_MESSAGE_BYTES;

/// Toy stemmer: strips a trailing "ing" (>5 chars, folding a resulting
/// double consonant, so "running" → "run") or trailing 's' (>3 chars)
/// after lowercasing. Deterministic; enough to prove the STEMS identity
/// path groups surface forms.
pub fn toy_stem(word: &str) -> String {
    let w = word.to_lowercase();
    if w.ends_with("ing") && w.len() > 5 {
        let stem = &w[..w.len() - 3];
        let bytes = stem.as_bytes();
        if bytes.len() >= 2 && bytes[bytes.len() - 1] == bytes[bytes.len() - 2] {
            return stem[..stem.len() - 1].to_string();
        }
        return stem.to_string();
    }
    if w.ends_with('s') && w.len() > 3 {
        return w[..w.len() - 1].to_string();
    }
    w
}

/// The mock service: canned-but-faithful analysis.
#[derive(Default)]
pub struct MockAnalysis;

#[tonic::async_trait]
impl AnalysisService for MockAnalysis {
    async fn analyze(
        &self,
        request: Request<AnalyzeRequest>,
    ) -> Result<Response<AnalyzeResponse>, Status> {
        let req = request.into_inner();
        if req.text.is_empty() {
            return Err(Status::invalid_argument("empty text"));
        }
        let options = req.options.unwrap_or_default();

        // Whitespace tokenize with original-text spans.
        let mut tokens = Vec::new();
        let mut offset = 0usize;
        for word in req.text.split_whitespace() {
            let start = req.text[offset..].find(word).map(|i| offset + i).unwrap();
            let end = start + word.len();
            tokens.push(Token {
                span: Some(Span {
                    start: start as i32,
                    end: end as i32,
                }),
                text: word.to_string(),
                pos: String::new(),
            });
            offset = end;
        }

        let stemming_on = options.stemmer > 1;
        let stems: Vec<String> = if stemming_on {
            tokens.iter().map(|t| toy_stem(&t.text)).collect()
        } else {
            Vec::new()
        };

        let mut term_vectors: Vec<TermVector> = Vec::new();
        if let Some(tv) = options.term_vectors {
            if tv.enabled {
                const SOURCE_STEMS: i32 = 2;
                const MODE_SCORING_ONLY: i32 = 2;
                if tv.source == SOURCE_STEMS && !stemming_on {
                    return Err(Status::invalid_argument("SOURCE_STEMS requires a stemmer"));
                }
                for (i, token) in tokens.iter().enumerate() {
                    let identity = if tv.source == SOURCE_STEMS {
                        stems[i].clone()
                    } else {
                        token.text.to_lowercase()
                    };
                    let span = token.span.unwrap();
                    match term_vectors.iter_mut().find(|t| t.term == identity) {
                        Some(entry) => {
                            entry.frequency += 1;
                            if tv.mode != MODE_SCORING_ONLY {
                                entry.occurrences.push(span);
                            }
                        }
                        None => term_vectors.push(TermVector {
                            term: identity,
                            frequency: 1,
                            occurrences: if tv.mode == MODE_SCORING_ONLY {
                                Vec::new()
                            } else {
                                vec![span]
                            },
                        }),
                    }
                }
            }
        }

        Ok(Response::new(AnalyzeResponse {
            sentences: Vec::new(),
            tokens,
            stems,
            entities: Vec::new(),
            term_vectors,
            embeddings: Vec::new(),
            warnings: Vec::new(),
            lemmas: Vec::new(),
        }))
    }

    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        Ok(Response::new(GetCapabilitiesResponse::default()))
    }
}

/// Start the mock on 127.0.0.1:0; returns its `http://` address.
pub async fn start_mock_analysis() -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .add_service(
                AnalysisServiceServer::new(MockAnalysis)
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(turbovec_search::harness::nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}
