//! The Raft surfaces of a build without Raft support.

use pipestream_search::config::Config;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::metrics::MemberGaugeProvider;
use pipestream_search::relay::RelayService;
use tonic::transport::server::Router;

use crate::cli::TaskHandle;

/// No member and no service: this build has no Raft support.
pub(in crate::cli) struct RaftSurfaces;

impl RaftSurfaces {
    /// Refuses a Raft configuration; otherwise there is nothing to start.
    pub(in crate::cli) async fn start(cfg: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        match &cfg.raft {
            Some(_) => {
                Err("this build has no Raft support (features `raft` and `tls` are needed)".into())
            }
            None => Ok(Self),
        }
    }

    pub(in crate::cli) fn on_node_listener(&self, router: Router) -> Router {
        router
    }

    pub(in crate::cli) fn on_write_listener(&self, router: Router) -> Router {
        router
    }

    pub(in crate::cli) fn member_gauges(&self) -> Vec<MemberGaugeProvider> {
        Vec::new()
    }

    /// The relay over its coordinator's shard set, routing on the
    /// file-polled map.
    pub(in crate::cli) fn relay_service(
        &self,
        _cfg: &Config,
        coordinator: CoordinatorServiceImpl,
    ) -> Result<RelayService, Box<dyn std::error::Error>> {
        Ok(RelayService::new(std::sync::Arc::new(coordinator)))
    }

    pub(in crate::cli) fn release_listener_handles(&mut self) {}

    pub(in crate::cli) fn spawn_member_shutdown(
        self,
        _shutdown_rx: &tokio::sync::watch::Receiver<bool>,
        _handles: &mut Vec<TaskHandle>,
    ) {
    }
}
