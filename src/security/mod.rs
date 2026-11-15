//! The security surface: TLS and mTLS material for
//! the gRPC listeners and cluster-internal channels, bearer principals
//! with quotas for the public search surface, and the HMAC that
//! authenticates the UDP floor and cancel datagrams.
//!
//! Three rules shape it:
//!
//! - **Refuse, never clamp.** A quota that a request exceeds is a named
//!   `RESOURCE_EXHAUSTED`; the request is not trimmed to fit.
//! - **A shared bearer is not membership.** Cluster-internal calls
//!   (coordinator to node, cluster control) are authenticated by client
//!   certificates; the bearer token identifies a public client.
//! - **The UDP key is not the bearer.** A datagram tag is computed with
//!   its own key, so a leaked client token cannot forge a floor.

mod principals;
mod quota;
mod streams;
mod tls;
#[cfg(feature = "net")]
mod tool_client;
mod udp;

pub use principals::{PrincipalConfig, Principals};
pub use quota::{Permit, Principal};
pub use streams::{Guarded, MeteredIngest};
#[cfg(feature = "net")]
pub use tls::{apply_client_tls, secure_endpoint};
pub use tls::{
    install_client_bearer_token, install_client_tls, is_loopback, process_client_bearer_token,
    process_client_tls, process_secure_url, secure_url, ClientTls, ServerTls,
};
#[cfg(feature = "net")]
pub use tool_client::{Bearer, PublicChannel, ToolClient};
pub use udp::{constant_time_eq, hmac_sha256, UdpKey};
