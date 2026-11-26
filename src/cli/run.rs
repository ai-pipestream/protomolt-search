//! The serving process: start every configured role, then wait for
//! shutdown.

use std::sync::Arc;

use pipestream_search::config::{Config, Role};
use pipestream_search::harness;
use pipestream_search::node::NodeServiceImpl;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::SearchRequest;
use pipestream_search::phrases::PhraseIndex;

use super::raft_member::RaftSurfaces;
use super::{listeners, membership, node_role, transport, turbo, Startup, TaskHandle};

/// Serve the configured roles until SIGINT or SIGTERM, then flush.
pub(crate) async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    pipestream_search::security::install_client_tls(cfg.client_tls.clone());
    pipestream_search::security::install_client_bearer_token(cfg.turbo_bearer_token.clone());
    let turbo_handshake = turbo::startup_handshake(&cfg);
    turbo::verify_remote_endpoints(&cfg, turbo_handshake.as_ref()).await?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        transport::shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let mut handles: Vec<TaskHandle> = Vec::new();
    // A Raft member starts from its existing durable state before any
    // listener that routes on its committed map.
    let mut raft = RaftSurfaces::start(&cfg).await?;
    let advertise = match &cfg.membership {
        Some(membership) => Some(membership::resolve_advertise(&cfg, membership)?),
        None => None,
    };
    let startup = Startup {
        cfg: &cfg,
        phrase_index: load_phrase_index(&cfg)?,
        turbo_handshake,
        shutdown_rx,
        advertise,
        flush_notify: Arc::new(tokio::sync::Notify::new()),
    };

    let mut shards = node_role::NodeShards::default();
    if matches!(cfg.role, Role::Node | Role::Both) {
        for shard in &cfg.shards {
            node_role::serve_shard(&startup, &raft, shard, &mut shards, &mut handles).await?;
        }
    }
    membership::start_agents(&startup, shards.served, &mut handles);
    let node_services = shards.services;
    listeners::serve_metrics(&startup, &raft, &node_services, &mut handles).await?;
    if cfg.relay {
        listeners::serve_relay(&startup, &raft, &mut handles).await?;
    } else if matches!(cfg.role, Role::Coordinator | Role::Both) {
        listeners::serve_coordinator(&startup, &raft, &node_services, &mut handles).await?;
    }
    // The listeners own their copies now; releasing these lets the
    // member shut down on its last handle once they drain.
    raft.release_listener_handles();

    if cfg.demo_query {
        demo_query(&cfg).await?;
    }
    raft.spawn_member_shutdown(&startup.shutdown_rx, &mut handles);

    for handle in handles {
        handle.await??;
    }
    flush_on_shutdown(&cfg, &node_services);
    Ok(())
}

/// The phrase vocabulary of `--phrase-glossary`, announced on stderr.
fn load_phrase_index(cfg: &Config) -> Result<Option<Arc<PhraseIndex>>, Box<dyn std::error::Error>> {
    let phrase_index = cfg
        .phrase_glossary
        .as_ref()
        .map(|path| {
            PhraseIndex::load_tsv(
                path,
                cfg.phrase_field.clone(),
                cfg.entity_map_field.clone(),
                cfg.phrase_ignore_case,
                cfg.phrase_ner,
            )
            .map(Arc::new)
        })
        .transpose()?;
    if let Some(index) = &phrase_index {
        eprintln!(
            "phrase vocabulary: field {:?}, fingerprint {:016x}, entity map {:?}, NER {}",
            index.phrase_field(),
            index.fingerprint(),
            index.entity_map_field(),
            index.include_ner()
        );
    }
    Ok(phrase_index)
}

/// `--demo-query`: one self-issued search against the coordinator listener.
async fn demo_query(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let query = harness::unit_vectors(1, cfg.query_dim, 0x0E0E_0001);
    let endpoint = format!("http://127.0.0.1:{}", cfg.coord_listen.port());
    let mut client = SearchServiceClient::connect(endpoint)
        .await?
        .max_decoding_message_size(cfg.max_message_bytes)
        .max_encoding_message_size(cfg.max_message_bytes);
    let response = client
        .search(SearchRequest {
            field: String::new(),
            collection: String::new(),
            request_id: String::new(),
            k: 10,
            vector: query,
            collapse_parents: false,
            geo_filters: Vec::new(),
            filter: String::new(),
            enumerate_max_rows: None,
            patience_saturation_ppm: None,
            patience_hops: None,
            finish_settle_us: None,
            finish_deadline_us: None,
            floor_prune: None,
            floor_greediness_ppm: None,
            floor_clamp_min: None,
            include_rerank_candidate: false,
            filter_strategy: 0,
            enumerate_all: false,
            staged_selection: false,
        })
        .await?
        .into_inner();
    println!(
        "demo search ({}): top {} hits",
        response.request_id,
        response.hits.len()
    );
    for hit in &response.hits {
        println!("  id={:<8} score={:.6}", hit.vector_id, hit.score);
    }
    Ok(())
}

/// Flush every shard when `--save-on-shutdown` is set, then seal the
/// vocabulary windows.
fn flush_on_shutdown(cfg: &Config, node_services: &[NodeServiceImpl]) {
    if cfg.save_on_shutdown {
        for node in node_services {
            match node.flush_index() {
                Ok(resp) if resp.written => eprintln!(
                    "shutdown: flushed {} vectors to {}",
                    resp.num_vectors, resp.path
                ),
                Ok(_) => {}
                Err(e) => eprintln!("shutdown: flush failed: {e}"),
            }
        }
    }
    // Vocabulary windows seal on shutdown too, independent of the index
    // flush (analytics, not a ledger — an empty window writes nothing).
    for node in node_services {
        node.snapshot_vocab_on_shutdown();
    }
}
