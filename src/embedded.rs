//! Socket-free Protomolt Search runtime for private local shards.
//!
//! This is not a second search engine. Each shard is the ordinary
//! [`NodeServiceImpl`], and the public API delegates to the ordinary
//! [`CoordinatorServiceImpl`]. Tonic's HTTP/2 service machinery runs over
//! [`tokio::io::DuplexStream`] so protobuf framing, streaming completion
//! certificates, ranking, schema planning, and mutation behavior stay on the
//! server code paths without binding or dialing a socket.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Status};

use crate::analyzer::NATIVE_ANALYSIS_BACKEND;
use crate::bm25::Bm25Params;
use crate::coordinator::{
    CoordinatorServiceImpl, FanoutLimits, DEFAULT_MAX_K, DEFAULT_MAX_RERANK_BYTES,
};
use crate::document_catalog::DocumentCatalog;
use crate::link::NodeLink;
use crate::node::{
    bm25_sidecar_path, exact_vector_sidecar_path, generation_dir, live_docs_sidecar_path,
    NodeConfig, NodeServiceImpl,
};
use crate::pb::search_service_server::SearchService;
use crate::pb::*;
use crate::quality::DenseQualityProfile;

/// One private shard in an embedded runtime.
#[derive(Clone, Debug)]
pub struct EmbeddedShardConfig {
    /// The ordinary node configuration. Embedded startup accepts only the
    /// native analyzer (or no value, which is resolved to native).
    pub node: NodeConfig,
    /// Explicit recovery escape hatch matching the server flag. The default
    /// refuses an unfinished BM25 bulk build.
    pub allow_missing_bm25: bool,
}

impl EmbeddedShardConfig {
    /// A purely in-memory shard. [`EmbeddedSearch::flush_all`] reports it as
    /// unwritten, matching `NodeService.Flush`.
    pub fn in_memory(slot_offset: u64) -> Self {
        let node = NodeConfig {
            slot_offset,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            ..NodeConfig::default()
        };
        Self {
            node,
            allow_missing_bm25: false,
        }
    }

    /// A persisted shard with WAL enabled. Opening an absent path creates an
    /// empty generation on first ingest/flush; opening an existing path loads
    /// its active snapshot and sidecars.
    pub fn persistent(path: impl Into<PathBuf>, slot_offset: u64) -> Self {
        let node = NodeConfig {
            slot_offset,
            index_path: Some(path.into()),
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            wal: true,
            ..NodeConfig::default()
        };
        Self {
            node,
            allow_missing_bm25: false,
        }
    }
}

/// Runtime-wide search settings. Shard field tables in
/// [`EmbeddedShardConfig::node`] are the schema consumed by ordinary and
/// descriptor-mapped ingest.
#[derive(Clone, Debug)]
pub struct EmbeddedSearchConfig {
    pub shards: Vec<EmbeddedShardConfig>,
    /// One collection-wide authority, independent of the physical shard paths.
    /// Absent disables logical document acceptance.
    pub document_catalog: Option<EmbeddedDocumentCatalogConfig>,
    pub bm25_params: Bm25Params,
    pub stream_search: bool,
    pub bm25_stream: bool,
    pub max_k: u32,
    pub max_rerank_bytes: u64,
    pub fanout_limits: FanoutLimits,
    pub phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    pub dense_quality_profile: Option<DenseQualityProfile>,
    pub topology_generation: u64,
}

impl EmbeddedSearchConfig {
    pub fn new(shards: Vec<EmbeddedShardConfig>) -> Self {
        Self {
            shards,
            document_catalog: None,
            bm25_params: Bm25Params::default(),
            stream_search: true,
            bm25_stream: true,
            max_k: DEFAULT_MAX_K,
            max_rerank_bytes: DEFAULT_MAX_RERANK_BYTES,
            fanout_limits: FanoutLimits::default(),
            phrase_index: None,
            dense_quality_profile: None,
            topology_generation: 0,
        }
    }

    pub fn single(shard: EmbeddedShardConfig) -> Self {
        Self::new(vec![shard])
    }
}

/// Local source/version storage. A persistent path must have an existing parent
/// directory and must remain stable when shard layouts or index generations change.
#[derive(Clone, Debug)]
pub struct EmbeddedDocumentCatalogConfig {
    pub collection: String,
    /// None explicitly selects volatile storage; receipts report durable=false.
    pub path: Option<PathBuf>,
}

/// Startup/configuration failures are separate from RPC failures so a host
/// bridge can distinguish an unusable local generation from a bad request.
#[derive(Debug)]
pub enum EmbeddedError {
    InvalidConfig(String),
    ExistingData(PathBuf),
    OpenShard { shard: usize, message: String },
    Rpc(Status),
}

impl fmt::Display for EmbeddedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => formatter.write_str(message),
            Self::ExistingData(path) => write!(
                formatter,
                "refusing to create an embedded shard over existing data at {}",
                path.display()
            ),
            Self::OpenShard { shard, message } => {
                write!(formatter, "open embedded shard {shard}: {message}")
            }
            Self::Rpc(status) => write!(formatter, "embedded search RPC: {status}"),
        }
    }
}

impl std::error::Error for EmbeddedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rpc(status) => Some(status),
            _ => None,
        }
    }
}

impl From<Status> for EmbeddedError {
    fn from(status: Status) -> Self {
        Self::Rpc(status)
    }
}

/// The complete local cluster: private shard nodes plus one local
/// coordinator. Dropping it aborts only its in-memory service tasks; it never
/// owns a network listener.
pub struct EmbeddedSearch {
    coordinator: CoordinatorServiceImpl,
    /// The shards, reached in-process through [`NodeLink::Local`]: the
    /// same handlers the network serves, with no HTTP/2 between.
    nodes: Vec<Arc<NodeServiceImpl>>,
    document_catalog: Option<DocumentCatalog>,
}

impl EmbeddedSearch {
    /// Create a new local cluster, refusing any configured path that already
    /// has provider, sidecar, snapshot, or WAL data.
    pub async fn create(config: EmbeddedSearchConfig) -> Result<Self, EmbeddedError> {
        if let Some(path) = config
            .document_catalog
            .as_ref()
            .and_then(|c| c.path.as_ref())
        {
            if path.exists() {
                return Err(EmbeddedError::ExistingData(path.clone()));
            }
        }
        for shard in &config.shards {
            if let Some(path) = shard.node.index_path.as_deref() {
                if let Some(existing) = first_existing_artifact(path) {
                    return Err(EmbeddedError::ExistingData(existing));
                }
            }
        }
        Self::open_inner(config, true).await
    }

    /// Open existing private shards, or start empty shards when their paths do
    /// not exist. All analysis and shard transport is forced in-process.
    pub async fn open(config: EmbeddedSearchConfig) -> Result<Self, EmbeddedError> {
        Self::open_inner(config, false).await
    }

    async fn open_inner(
        mut config: EmbeddedSearchConfig,
        create: bool,
    ) -> Result<Self, EmbeddedError> {
        validate_config(&mut config)?;

        let document_catalog = config
            .document_catalog
            .as_ref()
            .map(|catalog| match &catalog.path {
                Some(path) if create => DocumentCatalog::create(path, &catalog.collection),
                Some(path) => DocumentCatalog::open(path, &catalog.collection),
                None => DocumentCatalog::in_memory(&catalog.collection),
            })
            .transpose()?;

        let mut nodes = Vec::with_capacity(config.shards.len());
        for (shard, shard_config) in config.shards.iter().enumerate() {
            let node = NodeServiceImpl::open(
                shard_config.node.clone(),
                config.phrase_index.clone(),
                shard_config.allow_missing_bm25,
            )
            .map_err(|message| EmbeddedError::OpenShard { shard, message })?;
            nodes.push(Arc::new(node));
        }

        let mut coordinator = CoordinatorServiceImpl::with_local_nodes(nodes.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                config.bm25_params,
            )
            .with_phrase_index(config.phrase_index)
            .with_limits(config.fanout_limits)
            .with_stream_search(config.stream_search)
            .with_bm25_stream(config.bm25_stream)
            .with_max_k(config.max_k)
            .with_max_rerank_bytes(config.max_rerank_bytes)
            .with_topology_generation(config.topology_generation);
        if let Some(profile) = config.dense_quality_profile {
            coordinator = coordinator.with_dense_quality_profile(profile);
        }

        Ok(Self {
            coordinator,
            nodes,
            document_catalog,
        })
    }

    /// Persist the source/version and retry decision for this runtime's collection.
    /// This blocking local transaction does not yet publish an index projection.
    pub fn accept_document(
        &self,
        request: &AcceptDocumentRequest,
    ) -> Result<DocumentWriteReceipt, Status> {
        self.document_catalog()?.accept(request)
    }

    /// Trusted application-local lookup of an accepted source, including history.
    pub fn accepted_document(
        &self,
        key: &[u8],
        version: Option<u64>,
    ) -> Result<Option<(storage::DocumentVersion, Option<ProtobufSource>)>, Status> {
        self.document_catalog()?.get(key, version)
    }

    /// Trusted local source history for projection workers and recovery.
    pub fn read_accepted_documents(
        &self,
        request: &ReadAcceptedDocumentsRequest,
    ) -> Result<ReadAcceptedDocumentsResponse, Status> {
        self.document_catalog()?.read_accepted(request)
    }

    fn document_catalog(&self) -> Result<&DocumentCatalog, Status> {
        self.document_catalog.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "configure a collection-wide document catalog before accepting documents",
            )
        })
    }

    pub fn shard_count(&self) -> usize {
        self.nodes.len()
    }

    /// The exact public service implementation used by the network server.
    /// This exposes every current and future `SearchService` method without an
    /// embedded-only protocol fork.
    pub fn search_service(&self) -> &CoordinatorServiceImpl {
        &self.coordinator
    }

    /// Embedded construction hard-disables TCP fallback, DNS resolution, and
    /// UDP floor hints.
    pub fn allows_network(&self) -> bool {
        self.coordinator.allows_network()
    }

    /// A generated client for the complete shard/admin contract over an
    /// in-memory HTTP/2 stream. This is useful for less common operations such
    /// as snapshot install and encoded-row movement.
    /// The in-process link to one shard: every node RPC, dispatched
    /// straight into the handler.
    pub fn shard_client(&self, shard: usize) -> Result<NodeLink, EmbeddedError> {
        let node = self.nodes.get(shard).ok_or_else(|| {
            EmbeddedError::InvalidConfig(format!(
                "shard {shard} is out of range for {} embedded shards",
                self.nodes.len()
            ))
        })?;
        Ok(NodeLink::local(Arc::clone(node)))
    }

    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, Status> {
        SearchService::query(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn query_stream(
        &self,
        request: QueryStreamRequest,
    ) -> Result<crate::metrics::Timed<ReceiverStream<Result<QueryStreamResponse, Status>>>, Status>
    {
        SearchService::query_stream(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn plan_index(&self, request: PlanIndexRequest) -> Result<PlanIndexResponse, Status> {
        SearchService::plan_index(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn search(&self, request: SearchRequest) -> Result<SearchResponse, Status> {
        SearchService::search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn bm25_search(
        &self,
        request: Bm25SearchRequest,
    ) -> Result<Bm25SearchResponse, Status> {
        SearchService::bm25_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn phrase_search(
        &self,
        request: PhraseSearchRequest,
    ) -> Result<Bm25SearchResponse, Status> {
        SearchService::phrase_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn hybrid_search(
        &self,
        request: HybridSearchRequest,
    ) -> Result<HybridSearchResponse, Status> {
        SearchService::hybrid_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn variant_search(
        &self,
        request: VariantSearchRequest,
    ) -> Result<VariantSearchResponse, Status> {
        SearchService::variant_search(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn aggregate(&self, request: AggregateRequest) -> Result<AggregateResponse, Status> {
        SearchService::aggregate(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn cluster_health(&self) -> Result<ClusterHealthResponse, Status> {
        SearchService::cluster_health(
            &self.coordinator,
            Request::new(ClusterHealthRequest {
                collection: String::new(),
            }),
        )
        .await
        .map(tonic::Response::into_inner)
    }

    pub async fn broadcast_vector_backend(
        &self,
        request: BroadcastVectorBackendRequest,
    ) -> Result<BroadcastVectorBackendResponse, Status> {
        SearchService::broadcast_vector_backend(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn broadcast_calibration(
        &self,
        request: BroadcastCalibrationRequest,
    ) -> Result<BroadcastCalibrationResponse, Status> {
        SearchService::broadcast_calibration(&self.coordinator, Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn configure_vector_backend(
        &self,
        shard: usize,
        request: ConfigureVectorBackendRequest,
    ) -> Result<ConfigureVectorBackendResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .configure_vector_backend(request)
            .await?
            .into_inner())
    }

    pub async fn set_calibration(
        &self,
        shard: usize,
        request: SetCalibrationRequest,
    ) -> Result<SetCalibrationResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .set_calibration(request)
            .await?
            .into_inner())
    }

    pub async fn add_vectors(
        &self,
        shard: usize,
        requests: Vec<AddVectorsRequest>,
    ) -> Result<AddVectorsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .add_vectors(tokio_stream::iter(requests))
            .await?
            .into_inner())
    }

    pub async fn add_documents(
        &self,
        shard: usize,
        requests: Vec<AddDocumentsRequest>,
    ) -> Result<AddDocumentsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .add_documents(tokio_stream::iter(requests))
            .await?
            .into_inner())
    }

    pub async fn ingest_mapped(
        &self,
        shard: usize,
        requests: Vec<IngestMappedRequest>,
    ) -> Result<IngestMappedResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .ingest_mapped(tokio_stream::iter(requests))
            .await?
            .into_inner())
    }

    pub async fn delete_documents(
        &self,
        shard: usize,
        request: DeleteDocumentsRequest,
    ) -> Result<DeleteDocumentsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .delete_documents(request)
            .await?
            .into_inner())
    }

    pub async fn commit_replacements(
        &self,
        shard: usize,
        request: CommitReplacementsRequest,
    ) -> Result<CommitReplacementsResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .commit_replacements(request)
            .await?
            .into_inner())
    }

    pub async fn shard_health(&self, shard: usize) -> Result<HealthResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .health(HealthRequest {})
            .await?
            .into_inner())
    }

    pub async fn flush_shard(&self, shard: usize) -> Result<FlushResponse, EmbeddedError> {
        Ok(self
            .shard_client(shard)?
            .flush(FlushRequest {})
            .await?
            .into_inner())
    }

    pub async fn flush_all(&self) -> Result<Vec<FlushResponse>, EmbeddedError> {
        let mut responses = Vec::with_capacity(self.shard_count());
        for shard in 0..self.shard_count() {
            responses.push(self.flush_shard(shard).await?);
        }
        Ok(responses)
    }
}

impl Drop for EmbeddedSearch {
    fn drop(&mut self) {
        for node in &self.nodes {
            node.snapshot_vocab_on_shutdown();
        }
    }
}

fn validate_config(config: &mut EmbeddedSearchConfig) -> Result<(), EmbeddedError> {
    if config.shards.is_empty() {
        return Err(EmbeddedError::InvalidConfig(
            "embedded search requires at least one shard".to_string(),
        ));
    }
    if config.max_k == 0 {
        return Err(EmbeddedError::InvalidConfig(
            "embedded max_k must be positive".to_string(),
        ));
    }
    if config.max_rerank_bytes == 0 {
        return Err(EmbeddedError::InvalidConfig(
            "embedded max_rerank_bytes must be positive".to_string(),
        ));
    }

    let catalog_path = config
        .document_catalog
        .as_ref()
        .and_then(|c| c.path.as_ref())
        .map(|path| canonical_storage_path(path))
        .transpose()?;
    let mut paths = HashSet::new();
    for (shard, shard_config) in config.shards.iter_mut().enumerate() {
        if let Some(catalog) = &config.document_catalog {
            if shard_config.node.collection != catalog.collection {
                return Err(EmbeddedError::InvalidConfig(format!(
                    "embedded shard {shard} collection differs from the document catalog"
                )));
            }
            if let (Some(path), Some(index)) = (&catalog_path, &shard_config.node.index_path) {
                for artifact in [
                    index.clone(),
                    bm25_sidecar_path(index),
                    crate::node::bm25_build_dir(&bm25_sidecar_path(index)),
                    exact_vector_sidecar_path(index),
                    live_docs_sidecar_path(index),
                    generation_dir(index),
                    crate::node::segments_root(index),
                    crate::compaction::default_work_dir(index),
                    crate::compaction::marker_path(index),
                    crate::wal::wal_dir(index),
                ] {
                    if path.starts_with(canonical_storage_path(&artifact)?) {
                        return Err(EmbeddedError::InvalidConfig(
                            "document catalog path overlaps shard storage".into(),
                        ));
                    }
                }
            }
        }
        match shard_config.node.analysis_addr.as_deref() {
            None | Some("native") | Some("native://") => {
                shard_config.node.analysis_addr = Some(NATIVE_ANALYSIS_BACKEND.to_string());
            }
            Some(address) => {
                return Err(EmbeddedError::InvalidConfig(format!(
                    "embedded shard {shard} analysis backend {address:?} is not local; only native is allowed"
                )));
            }
        }
        if shard_config.node.wal && shard_config.node.index_path.is_none() {
            return Err(EmbeddedError::InvalidConfig(format!(
                "embedded shard {shard} enables WAL without an index path"
            )));
        }
        if let Some(path) = shard_config.node.index_path.as_ref() {
            if !paths.insert(path.clone()) {
                return Err(EmbeddedError::InvalidConfig(format!(
                    "multiple embedded shards use the same index path {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn canonical_storage_path(path: &Path) -> Result<PathBuf, EmbeddedError> {
    let result = if path.exists() {
        path.canonicalize()
    } else {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        parent
            .canonicalize()
            .map(|parent| parent.join(path.file_name().unwrap_or_default()))
    };
    result.map_err(|error| {
        EmbeddedError::InvalidConfig(format!(
            "resolve storage path {} (parent must exist): {error}",
            path.display()
        ))
    })
}

fn first_existing_artifact(index_path: &Path) -> Option<PathBuf> {
    [
        index_path.to_path_buf(),
        bm25_sidecar_path(index_path),
        exact_vector_sidecar_path(index_path),
        live_docs_sidecar_path(index_path),
        generation_dir(index_path),
        crate::wal::wal_dir(index_path),
    ]
    .into_iter()
    .find(|path| path.exists())
}
