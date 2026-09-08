//! The committed map feed (docs/map-feed.md): one `PublishedMap` per applied
//! control revision, produced from committed rows in one read, and the
//! consumer rules that make a stale or forged frame a named refusal.
use super::capacity::load_committed;
use super::import::{hash, prefix_end, read_headers, CONTROL, NODES, REPLICAS, TOPOLOGIES};
use super::*;
use crate::pb::storage::legacy_control_import_supplement::Placement;

const MAP_DOMAIN: &[u8] = b"protomolt.published-map.v1\0";

pub(super) fn digest(map: &PublishedMap) -> Vec<u8> {
    let mut canonical = map.clone();
    canonical.digest = Vec::new();
    hash(MAP_DOMAIN, &canonical.encode_to_vec())
}

impl SourceAuthorityStore {
    /// The applied control revision, published after every commit. A
    /// consumer waits on it and then reads `published_map`; the value it
    /// reads is at least that revision, never an uncommitted proposal.
    pub fn subscribe_applied(&self) -> watch::Receiver<u64> {
        self.inner.applied.subscribe()
    }

    /// The committed map of one resource at the current applied revision:
    /// routes, codes and tree of the current topology, replicas, nodes and
    /// the resource's logical owners with their committed phases and fences.
    pub fn published_map(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<PublishedMap, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let (header, _) = read_headers(&meta, &self.inner.identity)?;
            let committed = {
                let control = tx.open_table(CONTROL).map_err(storage)?;
                let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
                let nodes = tx.open_table(NODES).map_err(storage)?;
                let replicas = tx.open_table(REPLICAS).map_err(storage)?;
                load_committed(&control, &topologies, &nodes, &replicas, key)?
                    .ok_or_else(|| Status::not_found("resource has no applied control state"))?
            };
            let configuration = committed
                .state
                .configuration
                .as_ref()
                .ok_or_else(|| corrupt("applied control state has no configuration"))?;
            let tree = match configuration.placement.as_ref() {
                Some(Placement::Tree(tree)) => Some(tree.clone()),
                Some(Placement::NoPlacement(true)) => None,
                _ => return Err(corrupt("committed placement configuration is not explicit")),
            };
            let mut nodes = Vec::with_capacity(committed.nodes.len());
            for (node_id, row) in &committed.nodes {
                let node = row.node.as_ref().ok_or_else(|| missing("node"))?;
                nodes.push(PublishedNode {
                    node_id: node_id.clone(),
                    addr: node.addr.clone(),
                    state: node.state,
                    residency: row.residency,
                    failure_domain: node
                        .capacity
                        .as_ref()
                        .map(|c| c.failure_domain.clone())
                        .unwrap_or_default(),
                });
            }
            let mut replicas = Vec::with_capacity(committed.replicas.len());
            for row in &committed.replicas {
                replicas.push(row.replica.clone().ok_or_else(|| missing("replica"))?);
            }
            // Owners of this resource: every LogicalSourceOwner key with the
            // resource's workspace and collection.
            let owners_table = tx.open_table(OWNERS).map_err(storage)?;
            let prefix = LogicalSourceOwner {
                workspace: key.workspace.clone(),
                collection: key.collection.clone(),
                owner_id: Vec::new(),
            }
            .encode_to_vec();
            let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
            let mut owners = Vec::new();
            for entry in owners_table
                .range(prefix.as_slice()..end.as_slice())
                .map_err(storage)?
            {
                let (k, v) = entry.map_err(storage)?;
                let owner_key: LogicalSourceOwner = contract::decode(k.value())?;
                if owner_key.workspace != key.workspace || owner_key.collection != key.collection {
                    continue;
                }
                let owner: PreparedSourceOwner = contract::decode(v.value())?;
                contract::owner(&owner).map_err(corrupt)?;
                let target = owner
                    .target
                    .as_ref()
                    .ok_or_else(|| missing("owner target"))?;
                owners.push(PublishedOwner {
                    owner_id: owner_key.owner_id,
                    node_id: target.node_id.clone(),
                    storage_incarnation: target.storage_incarnation.clone(),
                    history_id: target.history_id.clone(),
                    residency: target.residency,
                    ownership_generation: owner.ownership_generation,
                    phase: owner.phase,
                    write_epoch: owner.activation.as_ref().map_or(0, |a| a.write_epoch),
                });
            }
            let mut map = PublishedMap {
                format_version: 1,
                authority: Some(self.inner.identity.clone()),
                key: Some(key.clone()),
                control_revision: header.source.control_revision,
                topology_generation: committed.state.topology_generation,
                routes: committed.topology.routes.clone(),
                codes_available: committed.topology.codes_available,
                codes: committed.topology.codes.clone(),
                tree,
                history_generations: committed.state.history_generations.clone(),
                replicas,
                nodes,
                owners,
                digest: Vec::new(),
            };
            map.digest = digest(&map);
            Ok(map)
        })
    }
}

/// The consumer side of the feed: validates identity, format and digest,
/// then swaps atomically. Same revision with different content is an error;
/// an older revision is ignored; a topology generation never names two
/// different maps.
#[derive(Debug, Default)]
pub struct MapConsumer {
    held: Option<PublishedMap>,
}

impl MapConsumer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn held(&self) -> Option<&PublishedMap> {
        self.held.as_ref()
    }

    /// Offer one frame. `Ok(true)` when the map was swapped, `Ok(false)`
    /// when the frame is older than the held map and was ignored.
    pub fn offer(&mut self, map: PublishedMap) -> Result<bool, Status> {
        if map.format_version != 1 {
            return Err(Status::failed_precondition(
                "published map requires format 1",
            ));
        }
        let authority = map
            .authority
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("published map names no authority"))?;
        contract::identity(authority)?;
        let key = map
            .key
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("published map names no resource"))?;
        contract::key(key, false)?;
        if map.digest.len() != 32 || digest(&map) != map.digest {
            return Err(Status::failed_precondition(
                "published map digest differs from its content",
            ));
        }
        if map.control_revision == 0 || map.topology_generation == 0 {
            return Err(Status::failed_precondition(
                "published map requires nonzero control revision and topology generation",
            ));
        }
        if map.codes_available && map.codes.len() != map.routes.len() {
            return Err(Status::failed_precondition(
                "published map codes do not cover its routes",
            ));
        }
        if let Some(held) = &self.held {
            if held.authority != map.authority || held.key != map.key {
                return Err(Status::failed_precondition(format!(
                    "published map names another authority or resource than the held {}/{}",
                    held.key.as_ref().map_or("", |k| k.workspace.as_str()),
                    held.key.as_ref().map_or("", |k| k.collection.as_str())
                )));
            }
            if map.control_revision < held.control_revision {
                return Ok(false);
            }
            if map.control_revision == held.control_revision {
                if map.digest != held.digest {
                    return Err(Status::failed_precondition(format!(
                        "published map at revision {} carries different content than the held map",
                        map.control_revision
                    )));
                }
                return Ok(false);
            }
            if map.topology_generation < held.topology_generation {
                return Err(Status::failed_precondition(format!(
                    "published map revision {} carries topology generation {}, behind the held {}",
                    map.control_revision, map.topology_generation, held.topology_generation
                )));
            }
            if map.topology_generation == held.topology_generation
                && (map.routes != held.routes
                    || map.codes != held.codes
                    || map.codes_available != held.codes_available
                    || map.tree != held.tree)
            {
                return Err(Status::failed_precondition(format!(
                    "topology generation {} names a different map at revision {} than at {}",
                    map.topology_generation, map.control_revision, held.control_revision
                )));
            }
        }
        self.held = Some(map);
        Ok(true)
    }
}
