//! The committed, digest-bound view of one resource's control state.
use super::*;

impl SourceAuthorityStore {
    /// The immutable committed view of a resource's control state. Lease
    /// tokens are omitted. Requires current resource Admin.
    pub fn control_snapshot(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<ControlCollectionSnapshot, Status> {
        self.guarded(|| {
            contract::key(key, false)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let (header, _) = read_headers(&meta, &self.inner.identity)?;
            let control = tx.open_table(CONTROL).map_err(storage)?;
            let state: ControlCollectionState = contract::decode(
                control
                    .get(key.encode_to_vec().as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("resource has no installed control state"))?
                    .value(),
            )?;
            let topologies = tx.open_table(TOPOLOGIES).map_err(storage)?;
            let topology = crate::source_authority::capacity::current_topology(
                &topologies,
                key,
                state.topology_generation,
            )?;
            let prefix = resource_prefix(key);
            let end = prefix_end(&prefix).ok_or_else(|| corrupt("resource prefix overflow"))?;
            let range = |definition| range_rows(&tx, definition, &prefix, &end);
            let nodes = snapshot_nodes(range(NODES)?)?;
            let replicas = snapshot_replicas(&range(REPLICAS)?)?;
            let actions = snapshot_actions(&range(ACTIONS)?)?;
            let mut completed_actions = range(COMPLETED)?
                .iter()
                .map(|(k, _)| contract::decode::<ControlActionKey>(k).map(|k| k.action_id))
                .collect::<Result<Vec<_>, _>>()?;
            completed_actions.sort_unstable();
            let mut snapshot = ControlCollectionSnapshot {
                format_version: 1,
                authority: Some(self.inner.identity.clone()),
                key: Some(key.clone()),
                control_revision: header.source.control_revision,
                policy_revision: policy.revision,
                state: Some(state),
                topology: Some(topology),
                nodes,
                replicas,
                actions,
                completed_actions,
                digest: Vec::new(),
            };
            snapshot.digest = hash(SNAPSHOT_DOMAIN, &snapshot.encode_to_vec());
            Ok(snapshot)
        })
    }
}

/// Every row of one table under a resource's key prefix.
fn range_rows(
    tx: &redb::ReadTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    prefix: &[u8],
    end: &[u8],
) -> Result<RawRows, Status> {
    let mut out = Vec::new();
    for entry in tx
        .open_table(definition)
        .map_err(storage)?
        .range(prefix..end)
        .map_err(storage)?
    {
        let (k, v) = entry.map_err(storage)?;
        out.push((k.value().to_vec(), v.value().to_vec()));
    }
    Ok(out)
}

/// Node rows without their lease tokens, in node id order.
fn snapshot_nodes(rows: RawRows) -> Result<Vec<ControlNodeSnapshot>, Status> {
    let mut nodes = Vec::new();
    for (_, value) in rows {
        let row: ControlNode = contract::decode(&value)?;
        let node = row.node.ok_or_else(|| missing("node"))?;
        nodes.push(ControlNodeSnapshot {
            node_id: node.node_id,
            addr: node.addr,
            state: node.state,
            expires_unix_ms: node.expires_unix_ms,
            capacity: node.capacity,
            residency: row.residency,
        });
    }
    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    Ok(nodes)
}

/// Replica rows in decoded replica key order.
fn snapshot_replicas(rows: &RawRows) -> Result<Vec<ControlReplica>, Status> {
    let mut replicas = rows
        .iter()
        .map(|(k, v)| {
            Ok((
                contract::decode::<ControlReplicaKey>(k)?.replica_key,
                contract::decode::<ControlReplica>(v)?,
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    replicas.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(replicas.into_iter().map(|(_, row)| row).collect())
}

/// Pending action rows in action id order.
fn snapshot_actions(rows: &RawRows) -> Result<Vec<ControlAction>, Status> {
    let mut actions = rows
        .iter()
        .map(|(k, v)| {
            Ok((
                contract::decode::<ControlActionKey>(k)?.action_id,
                contract::decode::<ControlAction>(v)?,
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    actions.sort_by_key(|a| a.0);
    Ok(actions.into_iter().map(|(_, row)| row).collect())
}
