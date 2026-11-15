//! Node membership options: the identity a node registers with the control plane.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::normalize_addr;
use super::file::FileConfig;
use super::option::Options;
use super::{NodeMembershipConfig, Role};

/// The membership options as given, before they are checked together.
struct Given {
    node_id: Option<String>,
    control_addr: Option<String>,
    failure_domain: Option<String>,
    data_dir: Option<String>,
    advertise_addr: Option<String>,
    replica_listen: Option<SocketAddr>,
}

fn given(o: Options, file: &FileConfig) -> Result<Given, String> {
    Ok(Given {
        node_id: o.text("node-id", file.node_id.as_deref()),
        control_addr: o.text("control-addr", file.control_addr.as_deref()),
        failure_domain: o.text("failure-domain", file.failure_domain.as_deref()),
        data_dir: o.text("data-dir", file.data_dir.as_deref()),
        advertise_addr: o.text("advertise-addr", file.advertise_addr.as_deref()),
        replica_listen: o.parsed("replica-listen", file.replica_listen.as_deref())?,
    })
}

fn check_advertise(addr: &str) -> Result<(), String> {
    let well_formed = addr
        .rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
    if well_formed {
        Ok(())
    } else {
        Err(format!("--advertise-addr={addr:?} is not host:port"))
    }
}

/// A registering node: its identity, where it reaches the plane, and its timers.
fn member(
    o: Options,
    file: &FileConfig,
    node_id: String,
    given: Given,
) -> Result<NodeMembershipConfig, String> {
    if node_id.trim().is_empty() {
        return Err("--node-id must not be empty".to_string());
    }
    let control_addr = given.control_addr.ok_or_else(|| {
        "--node-id needs --control-addr (the coordinator's ClusterControl endpoint)".to_string()
    })?;
    let data_dir = given
        .data_dir
        .ok_or_else(|| "--node-id needs --data-dir (where placed replicas live)".to_string())?;
    let report_ms = o.value_or("node-report-ms", file.node_report_ms, 10_000)?;
    let reconcile_ms = o.value_or("node-reconcile-ms", file.node_reconcile_ms, 2_000)?;
    if report_ms == 0 || reconcile_ms == 0 {
        return Err("--node-report-ms and --node-reconcile-ms must be positive".to_string());
    }
    if let Some(addr) = &given.advertise_addr {
        check_advertise(addr)?;
    }
    Ok(NodeMembershipConfig {
        node_id,
        control_addr: normalize_addr(control_addr),
        failure_domain: given.failure_domain.unwrap_or_default(),
        data_dir: PathBuf::from(data_dir),
        advertise_addr: given.advertise_addr,
        replica_listen: given.replica_listen,
        report_ms,
        reconcile_ms,
        lease_ms: o.value_or("node-lease-ms", file.node_lease_ms, 0)?,
        lag_bound: o.value_or("replica-lag-bound", file.replica_lag_bound, 0)?,
    })
}

/// Node membership: the identity a node registers. Without `--node-id` the
/// node does not register, and the options that describe a registered node
/// are refused.
pub(super) fn parse_membership(
    o: Options,
    file: &FileConfig,
    role: Role,
) -> Result<Option<NodeMembershipConfig>, String> {
    let mut given = given(o, file)?;
    match given.node_id.take() {
        Some(node_id) => {
            if !matches!(role, Role::Node | Role::Both) && !node_id.trim().is_empty() {
                return Err(
                    "--node-id registers a node: it needs the node or both role".to_string()
                );
            }
            member(o, file, node_id, given).map(Some)
        }
        None if given.control_addr.is_some()
            || given.data_dir.is_some()
            || given.failure_domain.is_some()
            || given.advertise_addr.is_some()
            || given.replica_listen.is_some() =>
        {
            Err(
                "--control-addr, --data-dir, --failure-domain, --advertise-addr, and \
                 --replica-listen describe a registered node: pass --node-id with them"
                    .to_string(),
            )
        }
        None => Ok(None),
    }
}
