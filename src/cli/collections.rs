//! The collection set a coordinator serves.

use std::net::SocketAddr;

use pipestream_search::collections::{ClusterControlSet, CollectionSet};

use super::corpus::{build_corpus, CorpusSpec};
use super::{Startup, TaskHandle};

type Collections = (CollectionSet, Option<ClusterControlSet>);

/// The one unnamed dataset of a pre-collection configuration.
async fn unnamed_collection(
    startup: &Startup<'_>,
    handles: &mut Vec<TaskHandle>,
    addr: SocketAddr,
) -> Result<Collections, Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let dataset = CorpusSpec::unnamed(cfg, cfg.control_state_path.as_deref());
    let (coordinator, control) = build_corpus(startup, dataset, handles).await?;
    eprintln!(
        "SearchService listening on {addr} ({} shard nodes)",
        cfg.node_addrs.len()
    );
    let set = CollectionSet::single(coordinator);
    let set = match &cfg.principals {
        Some(p) => set.with_principals(p.clone()),
        None => set,
    };
    Ok((set, control.map(ClusterControlSet::single)))
}

/// Every named collection, each with its own coordinator and control
/// plane.
async fn named_collections(
    startup: &Startup<'_>,
    handles: &mut Vec<TaskHandle>,
    addr: SocketAddr,
) -> Result<Collections, Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let mut members = Vec::with_capacity(cfg.collections.len());
    let mut controls = Vec::new();
    for c in &cfg.collections {
        let (coordinator, control) =
            build_corpus(startup, CorpusSpec::collection(c), handles).await?;
        eprintln!(
            "collection {:?}: {} shard nodes{}",
            c.name,
            c.node_addrs.len(),
            if control.is_some() {
                ", durable control"
            } else {
                ""
            }
        );
        members.push((c.name.clone(), coordinator));
        if let Some(control) = control {
            controls.push((c.name.clone(), control));
        }
    }
    let set = CollectionSet::named(members, cfg.default_collection.clone())?;
    let set = match &cfg.principals {
        Some(p) => set.with_principals(p.clone()),
        None => set,
    };
    let control = if controls.is_empty() {
        None
    } else {
        Some(ClusterControlSet::named(
            controls,
            cfg.default_collection.clone(),
        )?)
    };
    eprintln!(
        "SearchService listening on {addr} (collections {:?}, default {:?})",
        set.names(),
        cfg.default_collection
    );
    Ok((set, control))
}

/// The collection set this coordinator serves: the one unnamed dataset of
/// a pre-collection configuration, or every named collection, each with
/// its own coordinator and control plane. Membership is verified against
/// the nodes that answer; a node that serves another collection refuses
/// startup, an unreachable one is reported and re-checked by cluster
/// health.
pub(super) async fn build_collections(
    startup: &Startup<'_>,
    handles: &mut Vec<TaskHandle>,
    addr: SocketAddr,
) -> Result<Collections, Box<dyn std::error::Error>> {
    let (search_set, control_set) = if startup.cfg.collections.is_empty() {
        unnamed_collection(startup, handles, addr).await?
    } else {
        named_collections(startup, handles, addr).await?
    };
    match search_set.verify_membership().await {
        Ok(()) => {}
        Err(status) if status.code() == tonic::Code::Unavailable => {
            eprintln!(
                "collection membership not verified at start ({}); cluster health re-checks it",
                status.message()
            );
        }
        Err(status) => return Err(format!("collection membership: {}", status.message()).into()),
    }
    Ok((search_set, control_set))
}
