//! The member's start and the services recovered over it.

use std::sync::Arc;

use pipestream_search::config::Config;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::document_write_service::hosted::HostedDocumentWriteService;
use pipestream_search::raft::RaftHost;
use pipestream_search::relay::RelayService;

use crate::cli::raft_ops::hex;

/// The Raft member this process serves as, from `--raft-*`; `None` when
/// none is configured. Serving opens existing state and creates nothing.
pub(super) async fn start_raft_member(
    cfg: &Config,
) -> Result<Option<RaftHost>, Box<dyn std::error::Error>> {
    use pipestream_search::raft::operator;
    let Some(member) = &cfg.raft else {
        return Ok(None);
    };
    let identity = operator::identity(member);
    let host_config = operator::host_config(member)?;
    let transport = operator::cluster_transport(member, cfg.tls.as_ref(), cfg.client_tls.as_ref())?;
    let host = RaftHost::start_member(
        &member.dir,
        &identity,
        member.node_id,
        &host_config,
        transport,
    )
    .await
    .map_err(|status| format!("raft member: {}", status.message()))?;
    eprintln!(
        "raft member {} of group {} listening on {} (peers dial {}), timing {}",
        member.node_id,
        hex(&member.group_id),
        host.listen_addr()
            .map_or_else(String::new, |a| a.to_string()),
        host.advertised_addr().unwrap_or(""),
        host_config.timing_agreement()
    );
    Ok(Some(host))
}

/// The relay over its coordinator's shard set, routing on the committed map
/// of the Raft member when `--raft-map-*` names one, and on the file-polled
/// map otherwise.
pub(super) fn relay_service(
    cfg: &Config,
    host: Option<&RaftHost>,
    coordinator: CoordinatorServiceImpl,
) -> Result<RelayService, Box<dyn std::error::Error>> {
    let base = Arc::new(coordinator);
    let map = cfg.raft.as_ref().and_then(|member| member.map.as_ref());
    match (host, map) {
        (Some(host), Some(map)) => {
            let key = pipestream_search::pb::storage::LogicalSourceOwner {
                workspace: map.workspace.clone(),
                collection: map.collection.clone(),
                owner_id: Vec::new(),
            };
            let source = pipestream_search::source_authority::AuthorityMapSource::attach(
                host.store_handle(),
                &map.principal,
                &key,
            )
            .map_err(|status| format!("raft map source: {}", status.message()))?;
            eprintln!(
                "relay routes on the committed map of {}/{} (revision {})",
                map.workspace,
                map.collection,
                pipestream_search::relay::MapSource::current(&source).control_revision
            );
            Ok(RelayService::with_map(base, Arc::new(source)))
        }
        _ => Ok(RelayService::new(base)),
    }
}

/// The hosted owner-write service of a member whose operator configuration
/// names managed catalogs, recovered at start under the configured actor
/// on the store's applied view (no leader is needed to start). `None` when
/// no catalog is named. Every admission the service later grants comes
/// from the host's lease.
pub(super) fn hosted_write_service(
    cfg: &Config,
    host: Option<Arc<RaftHost>>,
) -> Result<Option<HostedDocumentWriteService>, Box<dyn std::error::Error>> {
    use pipestream_search::document_write_service::hosted::recover_catalogs;
    use pipestream_search::pb::DocumentWriteServiceLimits;
    let Some(member) = &cfg.raft else {
        return Ok(None);
    };
    if member.managed_catalogs.is_empty() {
        return Ok(None);
    }
    let Some(host) = host else {
        return Err("raft managed catalogs need the member's host".into());
    };
    let actor = member
        .managed_principal
        .as_deref()
        .ok_or("--raft-managed-catalog needs --raft-managed-principal")?;
    let Some(principals) = &cfg.principals else {
        return Err(
            "--raft-managed-catalog needs --bearer-tokens, the transport principals".into(),
        );
    };
    let catalogs = recover_catalogs(&host, actor, &member.managed_catalogs)
        .map_err(|status| format!("raft managed catalogs: {}", status.message()))?;
    let count = catalogs.len();
    let limits = DocumentWriteServiceLimits {
        max_request_bytes: member.managed_max_request_bytes,
        max_pending_bytes: member.managed_max_pending_bytes,
        max_in_flight: u32::try_from(member.managed_max_in_flight)
            .map_err(|_| "--raft-managed-max-in-flight must be 1..1024")?,
    };
    let service = HostedDocumentWriteService::new((**principals).clone(), host, catalogs, limits)
        .map_err(|status| format!("hosted write service: {}", status.message()))?;
    let collections: Vec<&str> = member
        .managed_catalogs
        .iter()
        .map(|entry| entry.collection.as_str())
        .collect();
    eprintln!(
        "hosted owner writes for collections [{collections}] as actor {actor} ({count} catalogs)",
        collections = collections.join(", "),
    );
    Ok(Some(service))
}
