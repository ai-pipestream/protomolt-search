//! Lossless, bounded migration input for the legacy single-file authority.
//! This is not the new control state machine and never activates source owners.
use super::*;
use crate::pb::storage as wire;
use prost::Message;
use std::io::Write;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_FIELDS: usize = 262_144;

fn invalid(message: &str) -> Status {
    Status::invalid_argument(format!("legacy control checkpoint: {message}"))
}
fn capacity() -> Status {
    Status::resource_exhausted("legacy control checkpoint exceeds 16MiB or 262144 wire fields")
}

// No Debug: the checkpoint contains lease secrets, unlike ClusterPlan.
pub struct LegacyControlCheckpoint {
    value: wire::LegacyControlImport,
}

impl LegacyControlCheckpoint {
    /// Decode a canonical private migration record. Unknown fields, enum values,
    /// duplicate keys and allocator reuse refuse instead of losing semantics.
    pub fn decode(bytes: &[u8]) -> Result<Self, Status> {
        if bytes.len() > MAX_BYTES {
            return Err(capacity());
        }
        let mut remaining = MAX_FIELDS;
        preflight(bytes, Shape::Import, &mut remaining)?;
        let value =
            wire::LegacyControlImport::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
        if value.encode_to_vec() != bytes {
            return Err(invalid("unknown or noncanonical protobuf fields"));
        }
        validate(&value)?;
        Ok(Self { value })
    }

    /// SHA-256 of the exact canonical bytes, used by retirement preconditions.
    pub fn sha256(&self) -> [u8; 32] {
        crate::sha256::digest(&self.value.encode_to_vec())
    }

    /// Contains privileged state, including lease credentials. Do not expose
    /// these bytes through the public query, diagnostics or map subscription API.
    pub fn encode(&self) -> Vec<u8> {
        self.value.encode_to_vec()
    }

    pub fn state(&self) -> &wire::LegacyControlState {
        self.value.state.as_ref().expect("validated state")
    }

    /// Captured runtime policy; this does not establish historical policy inputs.
    pub fn policy(&self) -> &wire::LegacyControlPolicy {
        self.value.policy.as_ref().expect("validated policy")
    }
}

impl DurableControlPlane {
    /// Privileged local export under the current authority lock. Possession of
    /// the plane is required; no public or node-facing RPC exposes this record.
    /// Keep the old writer stopped throughout an eventual transactional import.
    pub fn checkpoint_for_import(&self) -> Result<Vec<u8>, Status> {
        let state = self.lock_state()?;
        encode(&state, &self.policy)
    }
}

// Count the legacy representation without allocating a second state-sized
// buffer before constructing owned protobuf strings and vectors.
struct JsonBudget(usize);
impl Write for JsonBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
            std::io::Error::other("legacy control JSON exceeds checkpoint capacity")
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn encode(state: &StoredState, policy: &ControlPolicy) -> Result<Vec<u8>, Status> {
    serde_json::to_writer(JsonBudget(MAX_BYTES), state).map_err(|_| capacity())?;
    let value = wire::LegacyControlImport {
        format_version: 1,
        state: Some(to_wire(state)),
        policy: Some(wire::LegacyControlPolicy {
            lease_ms: policy.lease_ms,
            replication_factor: policy.replication_factor as u64,
            split_rows: policy.split_rows,
            merge_rows: policy.merge_rows,
            compact_segments: policy.compact_segments,
            compact_tombstone_ppm: policy.compact_tombstone_ppm,
            history_limit: policy.history_limit as u64,
        }),
    };
    if value.encoded_len() > MAX_BYTES {
        return Err(capacity());
    }
    validate(&value)?;
    let bytes = value.encode_to_vec();
    let mut remaining = MAX_FIELDS;
    preflight(&bytes, Shape::Import, &mut remaining)?;
    Ok(bytes)
}

fn topology(value: &StoredTopology) -> wire::LegacyControlTopology {
    wire::LegacyControlTopology {
        generation: value.generation,
        routes: value
            .routes
            .iter()
            .map(|r| wire::LegacyControlRoute {
                addr: r.addr.clone(),
                replica: r.replica.clone(),
                hash_lo: r.hash_lo,
                hash_hi: r.hash_hi,
            })
            .collect(),
    }
}

fn to_wire(s: &StoredState) -> wire::LegacyControlState {
    wire::LegacyControlState {
        format: s.format,
        collection: s.collection.clone(),
        revision: s.revision,
        next_token: s.next_token,
        next_action: s.next_action,
        topology: Some(topology(&s.topology)),
        history: s.history.iter().map(topology).collect(),
        nodes: s
            .nodes
            .iter()
            .map(|(key, n)| wire::LegacyControlNodeEntry {
                key: key.clone(),
                node: Some(wire::LegacyControlNode {
                    node_id: n.node_id.clone(),
                    addr: n.addr.clone(),
                    state: match n.state {
                        StoredNodeState::Active => ClusterNodeState::Active as i32,
                        StoredNodeState::Draining => ClusterNodeState::Draining as i32,
                        StoredNodeState::Expired => ClusterNodeState::Expired as i32,
                    },
                    lease_token: n.lease_token,
                    expires_unix_ms: n.expires_unix_ms,
                    capacity: Some((&n.capacity).into()),
                }),
            })
            .collect(),
        replicas: s
            .replicas
            .iter()
            .map(|(key, r)| wire::LegacyControlReplicaEntry {
                key: key.clone(),
                replica: Some(wire::LegacyControlReplica {
                    shard_id: r.shard_id.clone(),
                    node_id: r.node_id.clone(),
                    addr: r.addr.clone(),
                    generation: r.generation,
                    hash_lo: r.hash_lo,
                    hash_hi: r.hash_hi,
                    slot_offset: r.slot_offset,
                    rows: r.rows,
                    bytes: r.bytes,
                    role: match r.role {
                        StoredRole::Primary => ShardReplicaRole::Primary as i32,
                        StoredRole::Replica => ShardReplicaRole::Replica as i32,
                    },
                    ready: r.ready,
                    scoring_fingerprint: r.scoring_fingerprint.clone(),
                    analysis_fingerprint: r.analysis_fingerprint.clone(),
                    immutable_segments: r.immutable_segments,
                    tombstones: r.tombstones,
                }),
            })
            .collect(),
        actions: s
            .actions
            .iter()
            .map(|a| wire::LegacyControlAction {
                action_id: a.action_id,
                kind: a.kind,
                shard_id: a.shard_id.clone(),
                peer_shard_id: a.peer_shard_id.clone(),
                peer_source_generation: a.peer_source_generation,
                source_node_id: a.source_node_id.clone(),
                target_node_id: a.target_node_id.clone(),
                source_generation: a.source_generation,
                target_generation: a.target_generation,
                hash_lo: a.hash_lo,
                hash_hi: a.hash_hi,
                reason: a.reason.clone(),
            })
            .collect(),
        completed_actions: s.completed_actions.iter().copied().collect(),
    }
}

fn ordered<'a>(keys: impl Iterator<Item = &'a str>) -> Result<(), Status> {
    let mut previous = None;
    for key in keys {
        if previous.is_some_and(|p| p >= key) {
            return Err(invalid("map keys must be unique and sorted"));
        }
        previous = Some(key);
    }
    Ok(())
}
fn validate(value: &wire::LegacyControlImport) -> Result<(), Status> {
    if value.format_version != 1 {
        return Err(invalid("requires import format 1"));
    }
    let s = value
        .state
        .as_ref()
        .ok_or_else(|| invalid("state is required"))?;
    value
        .policy
        .as_ref()
        .ok_or_else(|| invalid("policy is required"))?;
    validate_state(s)
}
fn validate_state(s: &wire::LegacyControlState) -> Result<(), Status> {
    if s.format != 1 || s.revision == 0 || s.next_token == 0 || s.next_action == 0 {
        return Err(invalid(
            "state requires format 1 and nonzero revision and allocators",
        ));
    }
    let current = s
        .topology
        .as_ref()
        .ok_or_else(|| invalid("topology is required"))?;
    for t in std::iter::once(current).chain(s.history.iter()) {
        for r in &t.routes {
            if r.hash_lo > r.hash_hi {
                return Err(invalid("topology route has reversed hash range"));
            }
        }
    }
    ordered(s.nodes.iter().map(|e| e.key.as_str()))?;
    ordered(s.replicas.iter().map(|e| e.key.as_str()))?;
    let mut lease_tokens = BTreeSet::new();
    for e in &s.nodes {
        let n = e
            .node
            .as_ref()
            .ok_or_else(|| invalid("node entry is missing node"))?;
        if e.key != n.node_id {
            return Err(invalid("node map key differs from node_id"));
        }
        if !matches!(
            ClusterNodeState::try_from(n.state),
            Ok(ClusterNodeState::Active | ClusterNodeState::Draining | ClusterNodeState::Expired)
        ) {
            return Err(invalid("node state is unknown or unspecified"));
        }
        if n.lease_token == 0
            || n.lease_token >= s.next_token
            || !lease_tokens.insert(n.lease_token)
        {
            return Err(invalid(
                "node lease token is zero, reused or reaches next_token",
            ));
        }
        let c = n
            .capacity
            .as_ref()
            .ok_or_else(|| invalid("node capacity is required"))?;
        if NodeResidency::try_from(c.residency).is_err() {
            return Err(invalid("node residency is unknown"));
        }
    }
    for e in &s.replicas {
        let r = e
            .replica
            .as_ref()
            .ok_or_else(|| invalid("replica entry is missing replica"))?;
        if e.key != replica_key(&r.shard_id, &r.node_id) {
            return Err(invalid("replica map key differs from shard/node key"));
        }
        if !matches!(
            ShardReplicaRole::try_from(r.role),
            Ok(ShardReplicaRole::Primary | ShardReplicaRole::Replica)
        ) {
            return Err(invalid("replica role is unknown or unspecified"));
        }
        if r.hash_lo > r.hash_hi || r.tombstones > r.rows {
            return Err(invalid("replica range or tombstone count is invalid"));
        }
    }
    let mut ids = BTreeSet::new();
    let mut prior = None;
    for &id in &s.completed_actions {
        if id == 0 || id >= s.next_action || prior.is_some_and(|p| p >= id) {
            return Err(invalid(
                "completed actions must be sorted unique nonzero IDs below next_action",
            ));
        }
        prior = Some(id);
        ids.insert(id);
    }
    for a in &s.actions {
        if a.action_id == 0 || a.action_id >= s.next_action || !ids.insert(a.action_id) {
            return Err(invalid(
                "pending action ID is reused or reaches next_action",
            ));
        }
        if !matches!(
            PlacementActionKind::try_from(a.kind),
            Ok(PlacementActionKind::CopyReplica
                | PlacementActionKind::DropReplica
                | PlacementActionKind::PromoteReplica
                | PlacementActionKind::SplitShard
                | PlacementActionKind::MergeShards
                | PlacementActionKind::CompactShard)
        ) {
            return Err(invalid("action kind is unknown or unspecified"));
        }
        if a.hash_lo > a.hash_hi {
            return Err(invalid("action has reversed hash range"));
        }
    }
    Ok(())
}

// Test and future importer bridge. Restore every persisted field; do not use the
// lossy ClusterPlan projection. This does not create or open a live authority.
#[cfg(test)]
pub(super) fn restore(s: &wire::LegacyControlState) -> Result<StoredState, Status> {
    validate_state(s)?;
    let topology = |t: &wire::LegacyControlTopology| StoredTopology {
        generation: t.generation,
        routes: t
            .routes
            .iter()
            .map(|r| StoredRoute {
                addr: r.addr.clone(),
                replica: r.replica.clone(),
                hash_lo: r.hash_lo,
                hash_hi: r.hash_hi,
            })
            .collect(),
    };
    Ok(StoredState {
        format: s.format,
        collection: s.collection.clone(),
        revision: s.revision,
        next_token: s.next_token,
        next_action: s.next_action,
        topology: topology(s.topology.as_ref().expect("validated topology")),
        history: s.history.iter().map(topology).collect(),
        nodes: s
            .nodes
            .iter()
            .map(|e| {
                let n = e.node.as_ref().expect("validated node");
                (
                    e.key.clone(),
                    StoredNode {
                        node_id: n.node_id.clone(),
                        addr: n.addr.clone(),
                        state: match ClusterNodeState::try_from(n.state).expect("validated state") {
                            ClusterNodeState::Active => StoredNodeState::Active,
                            ClusterNodeState::Draining => StoredNodeState::Draining,
                            ClusterNodeState::Expired => StoredNodeState::Expired,
                            _ => unreachable!(),
                        },
                        lease_token: n.lease_token,
                        expires_unix_ms: n.expires_unix_ms,
                        capacity: n.capacity.clone().expect("validated capacity").into(),
                    },
                )
            })
            .collect(),
        replicas: s
            .replicas
            .iter()
            .map(|e| {
                let r = e.replica.as_ref().expect("validated replica");
                (
                    e.key.clone(),
                    StoredReplica {
                        shard_id: r.shard_id.clone(),
                        node_id: r.node_id.clone(),
                        addr: r.addr.clone(),
                        generation: r.generation,
                        hash_lo: r.hash_lo,
                        hash_hi: r.hash_hi,
                        slot_offset: r.slot_offset,
                        rows: r.rows,
                        bytes: r.bytes,
                        role: match ShardReplicaRole::try_from(r.role).expect("validated role") {
                            ShardReplicaRole::Primary => StoredRole::Primary,
                            ShardReplicaRole::Replica => StoredRole::Replica,
                            _ => unreachable!(),
                        },
                        ready: r.ready,
                        scoring_fingerprint: r.scoring_fingerprint.clone(),
                        analysis_fingerprint: r.analysis_fingerprint.clone(),
                        immutable_segments: r.immutable_segments,
                        tombstones: r.tombstones,
                    },
                )
            })
            .collect(),
        actions: s
            .actions
            .iter()
            .map(|a| StoredAction {
                action_id: a.action_id,
                kind: a.kind,
                shard_id: a.shard_id.clone(),
                peer_shard_id: a.peer_shard_id.clone(),
                peer_source_generation: a.peer_source_generation,
                source_node_id: a.source_node_id.clone(),
                target_node_id: a.target_node_id.clone(),
                source_generation: a.source_generation,
                target_generation: a.target_generation,
                hash_lo: a.hash_lo,
                hash_hi: a.hash_hi,
                reason: a.reason.clone(),
            })
            .collect(),
        completed_actions: s.completed_actions.iter().copied().collect(),
    })
}

#[derive(Clone, Copy)]
enum Shape {
    Import,
    State,
    Topology,
    Route,
    NodeEntry,
    Node,
    ReplicaEntry,
    Replica,
    Action,
    Policy,
    Capacity,
}
fn child(shape: Shape, tag: u32) -> Option<Shape> {
    match (shape, tag) {
        (Shape::Import, 2) => Some(Shape::State),
        (Shape::Import, 3) => Some(Shape::Policy),
        (Shape::State, 6 | 7) => Some(Shape::Topology),
        (Shape::State, 8) => Some(Shape::NodeEntry),
        (Shape::State, 9) => Some(Shape::ReplicaEntry),
        (Shape::State, 10) => Some(Shape::Action),
        (Shape::Topology, 2) => Some(Shape::Route),
        (Shape::NodeEntry, 2) => Some(Shape::Node),
        (Shape::Node, 6) => Some(Shape::Capacity),
        (Shape::ReplicaEntry, 2) => Some(Shape::Replica),
        _ => None,
    }
}
fn consume(remaining: &mut usize) -> Result<(), Status> {
    *remaining = remaining.checked_sub(1).ok_or_else(capacity)?;
    Ok(())
}
fn varint(bytes: &mut &[u8]) -> Result<u64, Status> {
    prost::encoding::decode_varint(bytes).map_err(|_| invalid("malformed protobuf varint"))
}
fn preflight(mut bytes: &[u8], shape: Shape, remaining: &mut usize) -> Result<(), Status> {
    use prost::encoding::WireType;
    while !bytes.is_empty() {
        consume(remaining)?;
        let (tag, kind) = prost::encoding::decode_key(&mut bytes)
            .map_err(|_| invalid("malformed protobuf key"))?;
        if kind == WireType::LengthDelimited {
            let len = usize::try_from(varint(&mut bytes)?).map_err(|_| capacity())?;
            if len > bytes.len() {
                return Err(invalid("truncated protobuf field"));
            }
            let (body, rest) = bytes.split_at(len);
            bytes = rest;
            if let Some(nested) = child(shape, tag) {
                preflight(body, nested, remaining)?;
            } else if matches!(shape, Shape::State) && tag == 11 {
                let mut packed = body;
                while !packed.is_empty() {
                    consume(remaining)?;
                    varint(&mut packed)?;
                }
            }
        } else {
            match kind {
                WireType::Varint => {
                    varint(&mut bytes)?;
                }
                WireType::ThirtyTwoBit | WireType::SixtyFourBit => {
                    let len = if kind == WireType::ThirtyTwoBit { 4 } else { 8 };
                    bytes = bytes
                        .get(len..)
                        .ok_or_else(|| invalid("truncated protobuf scalar"))?;
                }
                _ => return Err(invalid("protobuf groups are unsupported")),
            }
        }
    }
    Ok(())
}

// JSON objects are maps, but serde's default BTreeMap visitor replaces duplicate
// keys. Private authority records must reject this ambiguity before migration.
pub(super) fn unique_map<'de, D, T>(deserializer: D) -> Result<BTreeMap<String, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct Visitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a control map with unique keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut access: A,
        ) -> Result<Self::Value, A::Error> {
            let mut map = BTreeMap::new();
            while let Some((key, value)) = access.next_entry::<String, T>()? {
                if map.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate control map key"));
                }
            }
            Ok(map)
        }
    }
    deserializer.deserialize_map(Visitor(std::marker::PhantomData))
}
pub(super) fn unique_set<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeSet<u64>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = BTreeSet<u64>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("unique completed control action IDs")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut access: A,
        ) -> Result<Self::Value, A::Error> {
            let mut ids = BTreeSet::new();
            while let Some(id) = access.next_element::<u64>()? {
                if !ids.insert(id) {
                    return Err(serde::de::Error::custom(
                        "duplicate completed control action ID",
                    ));
                }
            }
            Ok(ids)
        }
    }
    deserializer.deserialize_seq(Visitor)
}
