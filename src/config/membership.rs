//! Node membership options: the identity a node registers with the control plane.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::{control_u64, normalize_addr, opt};
use super::file::FileConfig;
use super::{NodeMembershipConfig, Role};

/// Node membership (docs/cluster-control.md "Node lifecycle"): the identity a node registers.
pub(super) fn parse_membership(
    args: &[String],
    file: &FileConfig,
    role: Role,
) -> Result<Option<NodeMembershipConfig>, String> {
    let parse_control_u64 = |key: &str, env: &str, file_value: Option<u64>, default: u64| {
        control_u64(args, key, env, file_value, default)
    };
    let node_id = opt(
        args,
        "node-id",
        "PIPESTREAM_SEARCH_NODE_ID",
        file.node_id.as_deref(),
    );
    let control_addr = opt(
        args,
        "control-addr",
        "PIPESTREAM_SEARCH_CONTROL_ADDR",
        file.control_addr.as_deref(),
    );
    let failure_domain = opt(
        args,
        "failure-domain",
        "PIPESTREAM_SEARCH_FAILURE_DOMAIN",
        file.failure_domain.as_deref(),
    );
    let data_dir = opt(
        args,
        "data-dir",
        "PIPESTREAM_SEARCH_DATA_DIR",
        file.data_dir.as_deref(),
    );
    let advertise_addr = opt(
        args,
        "advertise-addr",
        "PIPESTREAM_SEARCH_ADVERTISE_ADDR",
        file.advertise_addr.as_deref(),
    );
    let replica_listen = opt(
        args,
        "replica-listen",
        "PIPESTREAM_SEARCH_REPLICA_LISTEN",
        file.replica_listen.as_deref(),
    )
    .map(|a| {
        a.parse::<SocketAddr>()
            .map_err(|e| format!("invalid --replica-listen: {e}"))
    })
    .transpose()?;
    let node_report_ms = parse_control_u64(
        "node-report-ms",
        "PIPESTREAM_SEARCH_NODE_REPORT_MS",
        file.node_report_ms,
        10_000,
    )?;
    let node_reconcile_ms = parse_control_u64(
        "node-reconcile-ms",
        "PIPESTREAM_SEARCH_NODE_RECONCILE_MS",
        file.node_reconcile_ms,
        2_000,
    )?;
    let node_lease_ms = parse_control_u64(
        "node-lease-ms",
        "PIPESTREAM_SEARCH_NODE_LEASE_MS",
        file.node_lease_ms,
        0,
    )?;
    let replica_lag_bound = parse_control_u64(
        "replica-lag-bound",
        "PIPESTREAM_SEARCH_REPLICA_LAG_BOUND",
        file.replica_lag_bound,
        0,
    )?;
    let membership = match node_id {
        Some(node_id) => {
            if node_id.trim().is_empty() {
                return Err("--node-id must not be empty".to_string());
            }
            if !matches!(role, Role::Node | Role::Both) {
                return Err(
                    "--node-id registers a node: it needs the node or both role".to_string()
                );
            }
            let control_addr = control_addr.ok_or_else(|| {
                "--node-id needs --control-addr (the coordinator's ClusterControl endpoint)"
                    .to_string()
            })?;
            let data_dir = data_dir.ok_or_else(|| {
                "--node-id needs --data-dir (where placed replicas live)".to_string()
            })?;
            if node_report_ms == 0 || node_reconcile_ms == 0 {
                return Err("--node-report-ms and --node-reconcile-ms must be positive".to_string());
            }
            if let Some(addr) = &advertise_addr {
                let well_formed = addr
                    .rsplit_once(':')
                    .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
                if !well_formed {
                    return Err(format!("--advertise-addr={addr:?} is not host:port"));
                }
            }
            Some(NodeMembershipConfig {
                node_id,
                control_addr: normalize_addr(control_addr),
                failure_domain: failure_domain.unwrap_or_default(),
                data_dir: PathBuf::from(data_dir),
                advertise_addr,
                replica_listen,
                report_ms: node_report_ms,
                reconcile_ms: node_reconcile_ms,
                lease_ms: node_lease_ms,
                lag_bound: replica_lag_bound,
            })
        }
        None => {
            if control_addr.is_some()
                || data_dir.is_some()
                || failure_domain.is_some()
                || advertise_addr.is_some()
                || replica_listen.is_some()
            {
                return Err(
                    "--control-addr, --data-dir, --failure-domain, --advertise-addr, and \
                     --replica-listen describe a registered node: pass --node-id with them"
                        .to_string(),
                );
            }
            None
        }
    };
    Ok(membership)
}
