//! The committed control rows of one resource and the planner facts
//! derived from them without repair.
use super::*;

/// The committed control rows of one resource, read in one transaction.
pub(in crate::source_authority) struct Committed {
    pub(in crate::source_authority) state: ControlCollectionState,
    pub(in crate::source_authority) topology: ControlTopology,
    pub(in crate::source_authority) nodes: BTreeMap<String, ControlNode>,
    pub(in crate::source_authority) replicas: Vec<ControlReplica>,
}

pub(in crate::source_authority) fn load_committed(
    control: &impl ReadableTable<&'static [u8], &'static [u8]>,
    topologies: &impl ReadableTable<&'static [u8], &'static [u8]>,
    nodes: &impl ReadableTable<&'static [u8], &'static [u8]>,
    replicas: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &LogicalSourceOwner,
) -> Result<Option<Committed>, Status> {
    let Some(state) = control
        .get(key.encode_to_vec().as_slice())
        .map_err(storage)?
    else {
        return Ok(None);
    };
    let state: ControlCollectionState = contract::decode(state.value())?;
    let topology: ControlTopology = contract::decode(
        topologies
            .get(
                ControlTopologyKey {
                    key: Some(key.clone()),
                    generation: state.topology_generation,
                }
                .encode_to_vec()
                .as_slice(),
            )
            .map_err(storage)?
            .ok_or_else(|| corrupt("current topology row is missing"))?
            .value(),
    )?;
    let prefix = resource_prefix(key);
    let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
    let mut node_rows = BTreeMap::new();
    for entry in nodes
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        let node_key: ControlNodeKey = contract::decode(k.value())?;
        let row: ControlNode = contract::decode(v.value())?;
        node_rows.insert(node_key.node_id, row);
    }
    let mut replica_rows = Vec::new();
    for entry in replicas
        .range(prefix.as_slice()..end.as_slice())
        .map_err(storage)?
    {
        let (_, v) = entry.map_err(storage)?;
        replica_rows.push(contract::decode::<ControlReplica>(v.value())?);
    }
    Ok(Some(Committed {
        state,
        topology,
        nodes: node_rows,
        replicas: replica_rows,
    }))
}

/// The committed facts the planner binds to, derived without repair.
pub(in crate::source_authority) struct Derived {
    pub(in crate::source_authority) view: tiers::CommittedView,
    pub(in crate::source_authority) derived_fingerprint: [u8; 32],
    pub(in crate::source_authority) placement_tree_digest: [u8; 32],
    pub(in crate::source_authority) provider_geometry_digest: [u8; 32],
}

pub(in crate::source_authority) fn derive(
    committed: &Committed,
    key: &LogicalSourceOwner,
) -> Result<Derived, Status> {
    let configuration = committed
        .state
        .configuration
        .as_ref()
        .ok_or_else(|| corrupt("applied control state has no configuration"))?;
    let derived_fingerprint = match configuration.derived.as_ref() {
        Some(DerivedChoice::Declaration(_)) => parse_hex32(&configuration.derived_fingerprint)
            .ok_or_else(|| corrupt("committed derived fingerprint is not 64 hex digits"))?,
        Some(DerivedChoice::NoDerived(true)) => [0u8; 32],
        _ => return Err(corrupt("committed derived configuration is not explicit")),
    };
    let (placement, placement_tree_digest) = committed_placement(configuration)?;
    let provider_geometry_digest = match configuration.provider.as_ref() {
        Some(Provider::Geometry(geometry)) => hash(GEOMETRY_DOMAIN, &geometry.encode_to_vec())
            .try_into()
            .expect("sha256 is 32 bytes"),
        Some(Provider::NoProvider(true)) => [0u8; 32],
        _ => return Err(corrupt("committed provider configuration is not explicit")),
    };
    let shards = committed_shards(committed, placement.as_ref())?;
    Ok(Derived {
        view: tiers::CommittedView {
            workspace: key.workspace.clone(),
            collection: key.collection.clone(),
            derived_fingerprint,
            topology_generation: committed.state.topology_generation,
            shards,
        },
        derived_fingerprint,
        placement_tree_digest,
        provider_geometry_digest,
    })
}

/// The committed placement tree, validated, with its digest; no tree is the
/// zero digest.
fn committed_placement(
    configuration: &ControlConfiguration,
) -> Result<(Option<crate::placement::Placement>, [u8; 32]), Status> {
    match configuration.placement.as_ref() {
        Some(Placement::Tree(tree)) => {
            let placement = crate::placement::Placement::validate(
                &crate::placement::PlacementTreeConfig::from_proto(tree),
            )
            .map_err(|error| corrupt(format!("committed placement tree: {error}")))?;
            let digest: [u8; 32] = hash(TREE_DOMAIN, &tree.encode_to_vec())
                .try_into()
                .expect("sha256 is 32 bytes");
            Ok((Some(placement), digest))
        }
        Some(Placement::NoPlacement(true)) => Ok((None, [0u8; 32])),
        _ => Err(corrupt("committed placement configuration is not explicit")),
    }
}

/// Shards: every committed replica row names its shard; the primary's
/// generation is the shard's source generation. A shard with no primary
/// or several is an impossible committed state, reported by name.
fn committed_shards(
    committed: &Committed,
    placement: Option<&crate::placement::Placement>,
) -> Result<BTreeMap<String, tiers::CommittedShard>, Status> {
    let mut shards: BTreeMap<String, tiers::CommittedShard> = BTreeMap::new();
    let mut by_shard: BTreeMap<String, Vec<&ControlReplicaRecord>> = BTreeMap::new();
    for row in &committed.replicas {
        let replica = row
            .replica
            .as_ref()
            .ok_or_else(|| corrupt("replica row without a replica"))?;
        by_shard
            .entry(replica.shard_id.clone())
            .or_default()
            .push(replica);
    }
    for (shard_id, replicas) in by_shard {
        let primaries: Vec<&&ControlReplicaRecord> = replicas
            .iter()
            .filter(|r| r.role == crate::pb::ShardReplicaRole::Primary as i32)
            .collect();
        let [primary] = primaries.as_slice() else {
            return Err(Status::failed_precondition(format!(
                "planner input: shard {shard_id} has {} primary replicas in committed state; its source generation cannot be named",
                primaries.len()
            )));
        };
        let mut pool: Vec<String> = replicas.iter().map(|r| r.node_id.clone()).collect();
        pool.sort();
        pool.dedup();
        let copies = committed_copies(committed, &replicas);
        let leaves = match placement {
            Some(placement) => shard_leaves(committed, placement, primary, &pool, &copies)?,
            None => BTreeMap::new(),
        };
        shards.insert(
            shard_id,
            tiers::CommittedShard {
                source_generation: primary.generation,
                // No committed ownership transition has happened since the
                // install; the counter starts here and the map feed publishes it.
                ownership_epoch: 0,
                leaves,
            },
        );
    }
    Ok(shards)
}

fn committed_copies(
    committed: &Committed,
    replicas: &[&ControlReplicaRecord],
) -> Vec<tiers::CommittedCopy> {
    let mut copies: Vec<tiers::CommittedCopy> = replicas
        .iter()
        .map(|r| tiers::CommittedCopy {
            node_id: r.node_id.clone(),
            // Installed replicas carry no storage incarnation or coverage
            // evidence: partial evidence, never a complete replica.
            storage_incarnation: [0u8; 16],
            failure_domain: committed
                .nodes
                .get(&r.node_id)
                .and_then(|n| n.node.as_ref())
                .and_then(|n| n.capacity.as_ref())
                .map(|c| c.failure_domain.clone())
                .unwrap_or_default(),
            complete: false,
            coverage_digest: [0u8; 32],
        })
        .collect();
    copies.sort_by(|a, b| {
        (&a.node_id, &a.storage_incarnation).cmp(&(&b.node_id, &b.storage_incarnation))
    });
    copies.dedup();
    copies
}

/// The current topology's route with the primary's hash range names the
/// leaf through its committed placement code.
fn shard_leaves(
    committed: &Committed,
    placement: &crate::placement::Placement,
    primary: &ControlReplicaRecord,
    pool: &[String],
    copies: &[tiers::CommittedCopy],
) -> Result<BTreeMap<String, tiers::LeafCoverage>, Status> {
    let mut leaves = BTreeMap::new();
    for (index, route) in committed.topology.routes.iter().enumerate() {
        if route.hash_lo != primary.hash_lo || route.hash_hi != primary.hash_hi {
            continue;
        }
        let Some(code) = committed.topology.codes.get(index) else {
            continue;
        };
        if !code.has_placement {
            continue;
        }
        let code = i64::try_from(code.placement)
            .map_err(|_| corrupt("committed placement code exceeds i64"))?;
        let leaf = placement
            .leaf_by_code(code)
            .ok_or_else(|| corrupt("committed placement code names no leaf"))?;
        leaves.insert(
            leaf.name.clone(),
            tiers::LeafCoverage {
                pool: pool.to_vec(),
                owner_node: primary.node_id.clone(),
                copies: copies.to_vec(),
            },
        );
    }
    Ok(leaves)
}

pub(super) fn node_records(
    committed: &Committed,
    planning_instant_unix_ms: u64,
) -> Result<BTreeMap<String, tiers::NodeRecord>, Status> {
    let mut out = BTreeMap::new();
    for (node_id, row) in &committed.nodes {
        let node = row.node.as_ref().ok_or_else(|| missing("node"))?;
        let residency = match SourceResidency::try_from(row.residency) {
            Ok(SourceResidency::Server) => tiers::NodeResidency::Server,
            Ok(SourceResidency::DeviceLocal) => tiers::NodeResidency::Device,
            Ok(SourceResidency::Unspecified) => tiers::NodeResidency::Unspecified,
            Err(_) => return Err(corrupt("committed node residency is unknown")),
        };
        // Eligibility is a transcription of committed facts at the planning
        // instant: an active, server-resident node whose committed lease has
        // not expired. Installed leases are observations; this grants nothing.
        let eligible = node.state == crate::pb::ClusterNodeState::Active as i32
            && residency == tiers::NodeResidency::Server
            && node.expires_unix_ms > planning_instant_unix_ms;
        let capacity = node.capacity.clone().unwrap_or_default();
        out.insert(
            node_id.clone(),
            tiers::NodeRecord {
                residency,
                eligible,
                failure_domain: capacity.failure_domain,
                capacity: tiers::NodeCapacity {
                    total_bytes: capacity.disk_bytes,
                    resident_bytes: capacity.used_disk_bytes,
                },
            },
        );
    }
    Ok(out)
}
