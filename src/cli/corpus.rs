//! One dataset's coordinator, built from its configuration.

use std::path::Path;

use pipestream_search::clustered_turbovec::ClusteredTurboVecBackend;
use pipestream_search::config::{
    normalize_addr, ClusteredTurboVecConfig, CollectionConfig, Config, ShardMap,
};
use pipestream_search::control_plane::ClusterControlService;
use pipestream_search::coordinator::{CoordinatorServiceImpl, TopologyRoute};

use super::corpus_tasks;
use super::{Startup, TaskHandle};

/// One dataset's coordinator settings: the unnamed dataset of a
/// pre-collection configuration, or one named collection.
pub(super) struct CorpusSpec<'a> {
    pub(super) name: &'a str,
    pub(super) node_addrs: &'a [String],
    replica_addrs: &'a [Option<String>],
    shard_map: Option<&'a ShardMap>,
    pub(super) shard_map_path: Option<&'a Path>,
    analysis_addr: Option<&'a String>,
    bm25_k1: f32,
    bm25_b: f32,
    dense_quality_profile: Option<&'a Path>,
    synonyms: Option<&'a Path>,
    pub(super) replica_state_path: Option<&'a Path>,
    pub(super) control_state_path: Option<&'a Path>,
    clustered_turbovec: Option<&'a ClusteredTurboVecConfig>,
}

impl<'a> CorpusSpec<'a> {
    /// The unnamed dataset of the top-level configuration, with the
    /// control state the caller serves it under.
    pub(super) fn unnamed(cfg: &'a Config, control_state_path: Option<&'a Path>) -> Self {
        Self {
            name: "",
            node_addrs: &cfg.node_addrs,
            replica_addrs: &cfg.replica_addrs,
            shard_map: cfg.shard_map.as_ref(),
            shard_map_path: cfg.shard_map_path.as_deref(),
            analysis_addr: cfg.analysis_addr.as_ref(),
            bm25_k1: cfg.bm25_k1,
            bm25_b: cfg.bm25_b,
            dense_quality_profile: cfg.dense_quality_profile.as_deref(),
            synonyms: cfg.synonyms.as_deref(),
            replica_state_path: cfg.replica_state_path.as_deref(),
            control_state_path,
            clustered_turbovec: cfg.clustered_turbovec.as_ref(),
        }
    }

    /// One named collection.
    pub(super) fn collection(c: &'a CollectionConfig) -> Self {
        Self {
            name: &c.name,
            node_addrs: &c.node_addrs,
            replica_addrs: &c.replica_addrs,
            shard_map: c.shard_map.as_ref(),
            shard_map_path: c.shard_map_path.as_deref(),
            analysis_addr: c.analysis_addr.as_ref(),
            bm25_k1: c.bm25_k1,
            bm25_b: c.bm25_b,
            dense_quality_profile: c.dense_quality_profile.as_deref(),
            synonyms: c.synonyms.as_deref(),
            replica_state_path: c.replica_state_path.as_deref(),
            control_state_path: c.control_state_path.as_deref(),
            clustered_turbovec: None,
        }
    }
}

pub(super) fn topology_routes(map: &ShardMap) -> Result<Vec<TopologyRoute>, String> {
    map.shards
        .iter()
        .enumerate()
        .map(|(shard, entry)| {
            let hash_range = match (entry.hash_lo, entry.hash_hi) {
                (Some(lo), Some(hi)) => Some((lo, hi)),
                (None, None) => None,
                _ => {
                    return Err(format!(
                        "shard map entry {shard} must provide both hash_lo and hash_hi or neither"
                    ))
                }
            };
            Ok(TopologyRoute {
                addr: normalize_addr(entry.addr.clone()),
                replica: entry.replica.clone().map(normalize_addr),
                hash_range,
                placement: entry.placement.map(|code| code as i64),
            })
        })
        .collect()
}

/// The coordinator over the dataset's nodes with the process-wide fan-out,
/// streaming and transport settings.
fn base_coordinator(startup: &Startup<'_>, dataset: &CorpusSpec<'_>) -> CoordinatorServiceImpl {
    let cfg = startup.cfg;
    let to_duration = |ms: u64| (ms > 0).then(|| std::time::Duration::from_millis(ms));
    let mut coordinator = CoordinatorServiceImpl::new(dataset.node_addrs.to_vec())
        .with_bm25(
            dataset.analysis_addr.cloned(),
            pipestream_search::bm25::Bm25Params {
                k1: f64::from(dataset.bm25_k1),
                b: f64::from(dataset.bm25_b),
            },
        )
        .with_phrase_index(startup.phrase_index.clone())
        .with_limits(pipestream_search::coordinator::FanoutLimits {
            shard_deadline: to_duration(cfg.shard_deadline_ms),
            hedge_delay: to_duration(cfg.hedge_delay_ms),
        })
        .with_replicas(dataset.replica_addrs.to_vec())
        .with_stream_search(cfg.stream_search)
        .with_stream_finish_settle(
            cfg.stream_finish_settle_us
                .map(|us| std::time::Duration::from_micros(u64::from(us))),
        )
        .with_stream_finish_deadline(
            cfg.stream_finish_deadline_us
                .map(|us| std::time::Duration::from_micros(u64::from(us))),
        )
        .with_bm25_stream(cfg.bm25_stream)
        .with_max_k(cfg.max_k)
        .with_signal_batch(cfg.signal_batch)
        .with_shard_pruning(cfg.shard_pruning)
        .with_max_rerank_bytes(cfg.max_rerank_bytes)
        .with_topology_generation(dataset.shard_map.map_or(0, |map| map.generation))
        .with_collection(dataset.name);
    if let Some(tls) = &cfg.client_tls {
        coordinator = coordinator.with_client_tls(tls.clone());
    }
    if let Some(key) = &cfg.udp_hmac_key {
        coordinator = coordinator.with_udp_hmac_key(key.clone());
    }
    coordinator
}

/// `--turbo-one-pass-query` and `--turbo-rerank-endpoint`.
fn with_turbo(
    cfg: &Config,
    mut coordinator: CoordinatorServiceImpl,
) -> Result<CoordinatorServiceImpl, Box<dyn std::error::Error>> {
    if cfg.turbo_one_pass_query {
        let analyzer = turbo_client::OnePassQueryBuilder::new().build()?;
        let nlp_spec = cfg.turbo_nlp_columns.clone().unwrap_or_default();
        coordinator = coordinator.with_one_pass_query(analyzer, nlp_spec);
    }
    if let Some(ref ep) = cfg.turbo_rerank_endpoint {
        let remote_opts = pipestream_search::analyzer::RemoteCallOptions {
            tls: cfg.client_tls.clone(),
            bearer_token: cfg.turbo_bearer_token.clone(),
            interactive: true,
            timeout: None,
            expected_handshake: None,
        };
        let client = pipestream_search::analyzer::remote_client_for(ep, &remote_opts)?;
        let rerank_model = cfg
            .turbo_rerank_model
            .clone()
            .unwrap_or_else(|| "reranker".to_string());
        let spec = pipestream_search::turbo_bridge::rerank::RerankSpec::new(
            pipestream_search::turbo_bridge::rerank::RerankBackend::Remote {
                client: Box::new(client),
                rerank_model,
                embed_model: String::new(),
                qa_model: String::new(),
                temperature: 1.0,
            },
        );
        coordinator = coordinator.with_rerank_spec(spec);
    }
    Ok(coordinator)
}

/// The dataset's measured dense profile and synonym table.
fn with_tuning(
    dataset: &CorpusSpec<'_>,
    mut coordinator: CoordinatorServiceImpl,
) -> Result<CoordinatorServiceImpl, Box<dyn std::error::Error>> {
    if let Some(path) = dataset.dense_quality_profile {
        let profile = pipestream_search::quality::DenseQualityProfile::load(path)?;
        eprintln!(
            "dense quality profile: {} ({} measured queries)",
            path.display(),
            profile.measured_queries()
        );
        coordinator = coordinator.with_dense_quality_profile(profile);
    }
    if let Some(path) = dataset.synonyms {
        let table = pipestream_search::synonyms::SynonymTable::load(path)?;
        eprintln!("synonyms: {} ({} rules)", path.display(), table.len());
        coordinator = coordinator.with_synonyms(table);
    }
    Ok(coordinator)
}

/// The clustered TurboVec backend, in process or external.
fn clustered_backend(
    cfg: &Config,
    clustered: &ClusteredTurboVecConfig,
) -> Result<ClusteredTurboVecBackend, Box<dyn std::error::Error>> {
    Ok(match clustered {
        ClusteredTurboVecConfig::InProcess {
            nodes,
            state,
            allow_ephemeral,
        } => {
            let table = turbovec_grpc::NodeTable::parse(&nodes.join("\n"))?;
            let limits = turbovec_grpc::CoordinatorLimits {
                max_k: cfg.max_k as usize,
                ..Default::default()
            };
            let service = match state {
                Some(path) => turbovec_grpc::CoordinatorService::with_state_file_and_limits(
                    table, path, limits,
                )?,
                None if *allow_ephemeral => {
                    turbovec_grpc::CoordinatorService::with_limits(table, limits)
                }
                None => {
                    unreachable!("configuration requires durable state or explicit ephemeral mode")
                }
            };
            ClusteredTurboVecBackend::in_process(service)
        }
        ClusteredTurboVecConfig::External { endpoint } => {
            ClusteredTurboVecBackend::external(endpoint, cfg.max_message_bytes)?
        }
    })
}

/// The shard map's hash ranges, placement tree and derived-column
/// declaration.
fn with_shard_map(
    dataset: &CorpusSpec<'_>,
    map: &ShardMap,
    mut coordinator: CoordinatorServiceImpl,
) -> Result<CoordinatorServiceImpl, Box<dyn std::error::Error>> {
    let routes = topology_routes(map)?;
    let ranges = routes.iter().map(|route| route.hash_range).collect();
    let placement = map
        .placement
        .clone()
        .map(|tree| (tree, routes.iter().map(|route| route.placement).collect()));
    coordinator = coordinator.with_hot_topology_placed(ranges, placement)?;
    // The map's [[derived]] table is the index's declaration; the
    // coordinator routes and scopes with it for the life of the process.
    if !map.derived.is_empty() {
        let declaration = pipestream_search::derived::spec_from_config(&map.derived)
            .and_then(|spec| pipestream_search::derived::Declaration::compile(&spec))
            .map_err(|e| format!("shard map [[derived]]: {e}"))?;
        eprintln!("derived columns: declaration {}", declaration.fingerprint());
        coordinator = coordinator.with_derived(Some(std::sync::Arc::new(declaration)));
    }
    eprintln!(
        "shard map generation {} ({} shards)",
        map.generation,
        dataset.node_addrs.len()
    );
    Ok(coordinator)
}

/// Build one dataset's coordinator, its background tasks (shard-map
/// reload, replica sync, control reconciliation), and its control plane
/// when configured. A collection set contains one of these per collection.
pub(super) async fn build_corpus(
    startup: &Startup<'_>,
    dataset: CorpusSpec<'_>,
    handles: &mut Vec<TaskHandle>,
) -> Result<(CoordinatorServiceImpl, Option<ClusterControlService>), Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let mut coordinator = with_turbo(cfg, base_coordinator(startup, &dataset))?;
    coordinator = with_tuning(&dataset, coordinator)?;
    if let Some(clustered) = dataset.clustered_turbovec {
        let backend = clustered_backend(cfg, clustered)?;
        eprintln!(
            "vector backend: clustered TurboVec ({} coordinator)",
            backend.transport_name()
        );
        coordinator = coordinator.with_clustered_turbovec(backend);
    }
    if let Some(map) = dataset.shard_map {
        coordinator = with_shard_map(&dataset, map, coordinator)?;
    }
    corpus_tasks::spawn_shard_map_reload(startup, &dataset, &coordinator, handles);
    corpus_tasks::spawn_replica_sync(startup, &dataset, &coordinator, handles)?;
    let control_service = corpus_tasks::control_service(startup, &dataset, &coordinator, handles)?;
    Ok((coordinator, control_service))
}
