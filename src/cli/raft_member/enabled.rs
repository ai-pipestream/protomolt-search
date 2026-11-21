//! The Raft surfaces of a build with Raft support.

use std::sync::Arc;

use pipestream_search::config::{Config, Role};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::document_write_service::hosted::HostedDocumentWriteService;
use pipestream_search::metrics::MemberGaugeProvider;
use pipestream_search::raft::operator_service::RaftOperatorServiceImpl;
use pipestream_search::raft::RaftHost;
use pipestream_search::relay::RelayService;
use tonic::transport::server::Router;

use super::services::{hosted_write_service, relay_service, start_raft_member};
use crate::cli::TaskHandle;

/// The member and the services it adds to this process's listeners.
pub(in crate::cli) struct RaftSurfaces {
    host: Option<Arc<RaftHost>>,
    /// Owner writes for the managed catalogs, on the coordinator or relay
    /// listener.
    hosted_writes: Option<HostedDocumentWriteService>,
    /// Startup reconciliation for the configured managed principal.
    reconcile: Option<(HostedDocumentWriteService, String)>,
    /// The operator service, on every listener of a member.
    operator: Option<RaftOperatorServiceImpl>,
}

impl RaftSurfaces {
    /// Start the configured member, then recover its managed catalogs
    /// before any listener serves them: a rejection stops the process
    /// with its cause, no partial service.
    pub(in crate::cli) async fn start(cfg: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let host = start_raft_member(cfg).await?.map(Arc::new);
        // Managed catalogs are served on the coordinator or relay
        // listener; a node-only process has neither, so it is rejected
        // before any catalog is opened.
        let names_catalogs = cfg
            .raft
            .as_ref()
            .is_some_and(|member| !member.managed_catalogs.is_empty());
        if names_catalogs && !cfg.relay && !matches!(cfg.role, Role::Coordinator | Role::Both) {
            return Err(
                "raft managed catalogs need the coordinator or relay listener; a node-only process serves no write surface"
                    .into(),
            );
        }
        let hosted_writes = hosted_write_service(cfg, host.clone())?;
        // The operator service rides every listener this process opens
        // when it is a Raft member, node listeners included: unlike the
        // hosted writes, it needs no catalog and no coordinator surface.
        let operator = host.clone().map(|host| {
            RaftOperatorServiceImpl::new(host).with_client_cert_required(
                cfg.tls
                    .as_ref()
                    .is_some_and(|tls| tls.client_ca_pem.is_some()),
            )
        });
        let reconcile = hosted_writes.clone().and_then(|service| {
            cfg.raft
                .as_ref()
                .and_then(|member| member.managed_principal.clone())
                .map(|principal| (service, principal))
        });
        Ok(Self {
            host,
            hosted_writes,
            reconcile,
            operator,
        })
    }

    /// A node listener carries the operator service.
    pub(in crate::cli) fn on_node_listener(&self, router: Router) -> Router {
        router.add_optional_service(self.operator.clone().map(|service| service.into_server()))
    }

    /// The coordinator or relay listener carries the hosted owner writes
    /// and the operator service.
    pub(in crate::cli) fn on_write_listener(&self, router: Router) -> Router {
        router
            .add_optional_service(
                self.hosted_writes
                    .clone()
                    .map(|service| service.into_server()),
            )
            .add_optional_service(self.operator.clone().map(|service| service.into_server()))
    }

    pub(in crate::cli) fn member_gauges(&self) -> Vec<MemberGaugeProvider> {
        self.host
            .clone()
            .map(RaftHost::member_gauge_provider)
            .into_iter()
            .collect()
    }

    /// The relay over its coordinator's shard set, routing on the
    /// member's committed map when one is configured.
    pub(in crate::cli) fn relay_service(
        &self,
        cfg: &Config,
        coordinator: CoordinatorServiceImpl,
    ) -> Result<RelayService, Box<dyn std::error::Error>> {
        relay_service(cfg, self.host.as_deref(), coordinator)
    }

    /// Drop this start's copies of the listener services, leaving the
    /// listeners' own.
    pub(in crate::cli) fn release_listener_handles(&mut self) {
        self.hosted_writes = None;
        self.operator = None;
    }

    /// Shut the member down after the shutdown flag is raised.
    pub(in crate::cli) fn spawn_member_shutdown(
        self,
        shutdown_rx: &tokio::sync::watch::Receiver<bool>,
        handles: &mut Vec<TaskHandle>,
    ) {
        if let Some((service, principal)) = self.reconcile {
            handles.push(service.spawn_reconciler(principal, shutdown_rx.clone()));
        }
        let Some(host) = self.host else {
            return;
        };
        let mut shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            let _ = shutdown.wait_for(|v| *v).await;
            // The hosted write service holds the member's other handle
            // while its listener drains; the member shuts down on the
            // last one, once every listener has released it.
            let mut shared = host;
            let owned = loop {
                match Arc::try_unwrap(shared) {
                    Ok(host) => break host,
                    Err(back) => {
                        shared = back;
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            };
            if let Err(error) = owned.shutdown().await {
                eprintln!("raft member shutdown: {}", error.message());
            }
            Ok(())
        }));
    }
}
