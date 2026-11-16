//! Listener and channel plumbing shared by every role of the process.

use tokio::net::TcpListener;
use tonic::transport::server::Router;
use tonic::transport::Server;

use pipestream_search::harness;

use super::TaskHandle;

/// Wait for SIGINT or SIGTERM (whichever comes first).
pub(super) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// A server builder under the configured listener TLS: node
/// listeners demand a client certificate, the coordinator listener accepts
/// one and lets cluster control demand it per call.
pub(super) fn secured_server(
    tls: Option<&pipestream_search::security::ServerTls>,
    require_client: bool,
) -> Result<Server, Box<dyn std::error::Error>> {
    match tls {
        None => Ok(Server::builder()),
        #[cfg(feature = "tls")]
        Some(tls) => Ok(Server::builder().tls_config(tls.server_config(require_client))?),
        #[cfg(not(feature = "tls"))]
        Some(_) => {
            let _ = require_client;
            Err("this build has no TLS support (feature `tls` is off)".into())
        }
    }
}

/// A channel to a cluster member under the process-wide client TLS
/// material when installed.
pub(super) async fn secure_channel(
    addr: &str,
) -> Result<tonic::transport::Channel, Box<dyn std::error::Error>> {
    let endpoint = tonic::transport::Endpoint::from_shared(
        pipestream_search::security::process_secure_url(addr),
    )?;
    let endpoint = pipestream_search::security::secure_endpoint(endpoint)?;
    Ok(endpoint.connect().await?)
}

/// A secured server builder with the process's HTTP/2 flow-control windows.
pub(super) fn windowed_server(
    tls: Option<&pipestream_search::security::ServerTls>,
    require_client: bool,
) -> Result<Server, Box<dyn std::error::Error>> {
    Ok(secured_server(tls, require_client)?
        .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
        .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW))
}

/// Serve `router` on `listener` until the shutdown flag is raised.
pub(super) fn spawn_listener(
    router: Router,
    listener: TcpListener,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
) -> TaskHandle {
    let mut shutdown = shutdown_rx.clone();
    tokio::spawn(router.serve_with_incoming_shutdown(
        harness::nodelay_incoming(listener),
        async move {
            let _ = shutdown.wait_for(|v| *v).await;
        },
    ))
}
