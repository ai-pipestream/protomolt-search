//! End-to-end `#105` Stage-2 calibrated reranking, semantic highlighting,
//! security/priority/deadline propagation, and `TurboVocabularyService`
//! feedback loop integration tests:
//! 1. Stage-2 calibrated `RerankAndHighlight` (`DeterministicCalibrated` and
//!    `Remote` over HTTP/2 gRPC `AnalysisService`) reordering pure `DenseQuery`
//!    and hybrid `CompositeSearchStrategy` (`RrfScore`) shortlists into
//!    calibrated `[0.0, 1.0]` probabilities, populating `QueryHit.snippets` on
//!    pure dense hits with zero lexical matches, and composing
//!    `DocLineage.span_start` parent-document UTF-8 byte offsets.
//! 2. `KserveCallPolicy` bearer auth (`authorization: Bearer ...`), mTLS
//!    material (`MtlsClientMaterial`), priority lanes (`x-turbo-priority: bulk`
//!    vs `interactive`), `finish_deadline_us` -> `grpc-timeout` / expired
//!    `DeadlineExceeded` refusal, and runtime-scoped `RemoteChannelPool`
//!    eviction when a Tokio runtime drops.
//! 3. `VocabularyListener` -> `export_corpus_statistics_to_kserve`
//!    (`ImportCorpusStatistics` -> `TCIDF001` `VocabularyCorpusIdf`) and
//!    `train_voronoi_centroids_on_kserve` (`TrainVoronoiCentroids` ->
//!    `TCENT001` spherical k-means Voronoi routing centroids).

mod common;
#[path = "turboembed_rerank_ops/kserve.rs"]
mod kserve;
#[path = "turboembed_rerank_ops/priority.rs"]
mod priority;
#[path = "turboembed_rerank_ops/stage2.rs"]
mod stage2;
#[path = "turboembed_rerank_ops/vocab_mock.rs"]
mod vocab_mock;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::mock::start_mock_analysis;
use common::{fit_calibration, unit_vectors};
use pipestream_search::analyzer::{body_spec, TURBO_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::storage::VocabChannel;
use pipestream_search::pb::{
    search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, CompositeSearchStrategy, DenseExecutionMode, DocLineage, HighlightSpec,
    LexicalQuery, QueryRequest, RrfScore, SearchQuery, SelectionOperator, SelectionQuery,
    SelectionScoreStrategy,
};
use pipestream_search::placement::{train_voronoi_centroids_on_kserve, VoronoiTrainingSpec};
use pipestream_search::turbo_bridge::rerank::{
    rerank_and_highlight_shortlist, RerankBackend, RerankCandidate, RerankSpec,
};
use pipestream_search::turbo_bridge::{KserveCallPolicy, MtlsClientMaterial};
use pipestream_search::vocab::{export_corpus_statistics_to_kserve, VocabularyListener};
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};
use turbo_client::proto::vocabulary::turbo_vocabulary_service_server::{
    TurboVocabularyService, TurboVocabularyServiceServer,
};
use turbo_client::proto::vocabulary::{
    import_corpus_statistics_request, train_voronoi_centroids_request, BuildSuggestLexiconRequest,
    CorpusStatisticsArtifactDescriptor, DeleteVocabularyArtifactRequest,
    DeleteVocabularyArtifactResponse, DictionaryArtifactDescriptor, DownloadVocabularyRequest,
    ImportCorpusStatisticsRequest, ImportDictionaryRequest, LearnVocabularyRequest,
    ListCorpusStatisticsRequest, ListCorpusStatisticsResponse, ListDictionariesRequest,
    ListDictionariesResponse, ListDictionaryFormatsRequest, ListDictionaryFormatsResponse,
    ListSuggestLexiconsRequest, ListSuggestLexiconsResponse, ListVocabulariesRequest,
    ListVocabulariesResponse, ListVoronoiCentroidsRequest, ListVoronoiCentroidsResponse,
    LookupStemSurfacesRequest, LookupStemSurfacesResponse, SpellcheckQueryRequest,
    SpellcheckQueryResponse, SuggestLexiconArtifactDescriptor, SuggestPrefixRequest,
    SuggestPrefixResponse, TrainVoronoiCentroidsRequest, VocabularyArtifactChunk,
    VocabularyArtifactDescriptor, VoronoiCentroidArtifactDescriptor,
};
use turbo_client::{
    RemoteChannelPool, RemoteClient, RemoteEndpointConfig, RemotePriority, VocabularyCorpusIdf,
};

#[path = "common/dir.rs"]
mod dir;
use dir::tempdir;

const DIM: usize = 32;
