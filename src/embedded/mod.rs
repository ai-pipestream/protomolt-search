//! Socket-free Protomolt Search runtime for private local shards.
//!
//! This is not a second search engine. Each shard is the ordinary
//! [`NodeServiceImpl`], and the public API delegates to the ordinary
//! [`CoordinatorServiceImpl`]. Tonic's HTTP/2 service machinery runs over
//! [`tokio::io::DuplexStream`] so protobuf framing, streaming completion
//! certificates, ranking, schema planning, and mutation behavior stay on the
//! server code paths without binding or dialing a socket.

mod config;
mod documents;
mod rpc;
mod validate;

use std::sync::Arc;

use crate::analyzer::NATIVE_ANALYSIS_BACKEND;
use crate::coordinator::CoordinatorServiceImpl;
use crate::document_catalog::DocumentCatalog;
use crate::link::NodeLink;
use crate::node::NodeServiceImpl;

pub use config::{
    EmbeddedDocumentCatalogConfig, EmbeddedError, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use validate::{first_existing_artifact, validate_config};

/// The complete local cluster: private shard nodes plus one local
/// coordinator. Dropping it aborts only its in-memory service tasks; it never
/// owns a network listener.
pub struct EmbeddedSearch {
    coordinator: CoordinatorServiceImpl,
    collections: crate::collections::CollectionSet,
    /// The shards, reached in-process through [`NodeLink::Local`]: the
    /// same handlers the network serves, with no HTTP/2 between.
    nodes: Vec<Arc<NodeServiceImpl>>,
    document_catalog: Option<Arc<DocumentCatalog>>,
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
    /// not exist. A configured persistent document catalog must already exist:
    /// missing authority is refused before shard storage is opened. All analysis
    /// and shard transport is forced in-process.
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
            .transpose()?
            .map(Arc::new);

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

        let collection = config.shards[0].node.collection.clone();
        let mut coordinator = CoordinatorServiceImpl::with_local_nodes(nodes.clone())
            .with_collection(&collection)
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

        let collections = if collection.is_empty() {
            crate::collections::CollectionSet::single(coordinator.clone())
        } else {
            crate::collections::CollectionSet::named(
                vec![(collection.clone(), coordinator.clone())],
                Some(collection),
            )
            .map_err(EmbeddedError::InvalidConfig)?
        };
        Ok(Self {
            coordinator,
            collections,
            nodes,
            document_catalog,
        })
    }

    pub fn shard_count(&self) -> usize {
        self.nodes.len()
    }

    /// The raw coordinator for a trusted owner. It exposes every SearchService
    /// method without authorization. Delegate untrusted requests through
    /// [`Self::authorized_service`] instead.
    pub fn search_service(&self) -> &CoordinatorServiceImpl {
        &self.coordinator
    }

    /// An authenticated public facade over the private local shards. Give
    /// untrusted callers this service rather than the owner/admin methods on
    /// `EmbeddedSearch`. The supplied principals must have an explicit authority.
    pub fn authorized_service(
        &self,
        principals: Arc<crate::security::Principals>,
    ) -> crate::collections::CollectionSet {
        self.collections.clone().with_principals(principals)
    }

    /// Embedded construction hard-disables TCP fallback, DNS resolution, and
    /// UDP floor hints.
    pub fn allows_network(&self) -> bool {
        self.coordinator.allows_network()
    }

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
}

impl Drop for EmbeddedSearch {
    fn drop(&mut self) {
        for node in &self.nodes {
            node.snapshot_vocab_on_shutdown();
        }
    }
}
