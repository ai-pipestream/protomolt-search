//! Node membership options: the identity a node registers with the control plane.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::normalize_addr;
use super::file::FileConfig;
use super::option::Options;
use super::{NodeMembershipConfig, Role};

/// What a registered node says about itself, before `--node-id` decides
/// whether the process registers.
struct NodeIdentity {
    control_addr: Option<String>,
    failure_domain: Option<String>,
    data_dir: Option<String>,
    advertise_addr: Option<String>,
    replica_listen: Option<SocketAddr>,
}

/// Node membership (docs/cluster-control.md "Node lifecycle"): the identity a node registers.
pub(super) fn parse_membership(
    o: Options<'_>,
    file: &FileConfig,
    role: Role,
) -> Result<Option<NodeMembershipConfig>, String> {
    let node_id = o.text("node-id", file.node_id.as_deref());
    let identity = NodeIdentity {
        control_addr: o.text("control-addr", file.control_addr.as_deref()),
        failure_domain: o.text("failure-domain", file.failure_domain.as_deref()),
        data_dir: o.text("data-dir", file.data_dir.as_deref()),
        advertise_addr: o.text("advertise-addr", file.advertise_addr.as_deref()),
        replica_listen: o.value::<SocketAddr>("replica-listen", file_listen(file)?)?,
    };
    let report_ms = o
        .value("node-report-ms", file.node_report_ms)?
        .unwrap_or(10_000);
    let reconcile_ms = o
        .value("node-reconcile-ms", file.node_reconcile_ms)?
        .unwrap_or(2_000);
    let lease_ms = o.value("node-lease-ms", file.node_lease_ms)?.unwrap_or(0);
    let lag_bound = o
        .value("replica-lag-bound", file.replica_lag_bound)?
        .unwrap_or(0);
    let Some(node_id) = node_id else {
        return unregistered(&identity);
    };
    if node_id.trim().is_empty() {
        return Err("--node-id must not be empty".to_string());
    }
    if !matches!(role, Role::Node | Role::Both) {
        return Err("--node-id registers a node: it needs the node or both role".to_string());
    }
    if report_ms == 0 || reconcile_ms == 0 {
        return Err("--node-report-ms and --node-reconcile-ms must be positive".to_string());
    }
    let mut membership = registered(node_id, identity)?;
    membership.report_ms = report_ms;
    membership.reconcile_ms = reconcile_ms;
    membership.lease_ms = lease_ms;
    membership.lag_bound = lag_bound;
    Ok(Some(membership))
}

/// The file's `replica_listen`, parsed as the flag's value is.
fn file_listen(file: &FileConfig) -> Result<Option<SocketAddr>, String> {
    file.replica_listen
        .as_deref()
        .map(|raw| {
            raw.parse::<SocketAddr>()
                .map_err(|error| super::option::invalid("replica-listen", raw, error))
        })
        .transpose()
}

/// Without `--node-id` no identity option may be set.
fn unregistered(identity: &NodeIdentity) -> Result<Option<NodeMembershipConfig>, String> {
    if identity.control_addr.is_some()
        || identity.data_dir.is_some()
        || identity.failure_domain.is_some()
        || identity.advertise_addr.is_some()
        || identity.replica_listen.is_some()
    {
        return Err(
            "--control-addr, --data-dir, --failure-domain, --advertise-addr, and \
             --replica-listen describe a registered node: pass --node-id with them"
                .to_string(),
        );
    }
    Ok(None)
}

/// The membership of `node_id`, with the timing fields left at zero.
fn registered(node_id: String, identity: NodeIdentity) -> Result<NodeMembershipConfig, String> {
    let control_addr = identity.control_addr.ok_or_else(|| {
        "--node-id needs --control-addr (the coordinator's ClusterControl endpoint)".to_string()
    })?;
    let data_dir = identity
        .data_dir
        .ok_or_else(|| "--node-id needs --data-dir (where placed replicas live)".to_string())?;
    if let Some(addr) = &identity.advertise_addr {
        let well_formed = addr
            .rsplit_once(':')
            .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
        if !well_formed {
            return Err(format!("--advertise-addr={addr:?} is not host:port"));
        }
    }
    Ok(NodeMembershipConfig {
        node_id,
        control_addr: normalize_addr(control_addr),
        failure_domain: identity.failure_domain.unwrap_or_default(),
        data_dir: PathBuf::from(data_dir),
        advertise_addr: identity.advertise_addr,
        replica_listen: identity.replica_listen,
        report_ms: 0,
        reconcile_ms: 0,
        lease_ms: 0,
        lag_bound: 0,
    })
}
