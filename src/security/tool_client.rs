//! The tools' side: flags, endpoints, and the bearer header.

use tonic::Status;

use super::tls::{
    apply_client_tls, install_client_bearer_token, install_client_tls, secure_url, ClientTls,
};

/// What a tool (the verifier, the ingest driver, the console) takes on
/// its command line to reach a fleet on TLS: the cluster CA it trusts
/// (`--tls-ca`), the identity it presents to node listeners
/// (`--tls-client-cert`, `--tls-client-key`), the name to verify server
/// certificates against when it is not the address (`--tls-domain`), and
/// the bearer token the coordinator's public surface asks for
/// (`--bearer-token-file`, or `--bearer-token` for a literal). Without
/// `--tls-ca` the tool speaks plaintext; flags the tool does
/// not know are left to the tool.
#[derive(Clone, Debug, Default)]
pub struct ToolClient {
    tls: Option<ClientTls>,
    bearer: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    raw_token: Option<String>,
}

impl ToolClient {
    /// Read the flags from the process arguments.
    pub fn from_env_args() -> Result<Self, String> {
        Self::from_args(std::env::args())
    }

    /// Read the flags from `args` (each `--key=value`).
    pub fn from_args<I: IntoIterator<Item = String>>(args: I) -> Result<Self, String> {
        let mut ca = None;
        let mut cert = None;
        let mut key = None;
        let mut domain = None;
        let mut token = None;
        for arg in args {
            let Some((k, v)) = arg.strip_prefix("--").and_then(|a| a.split_once('=')) else {
                continue;
            };
            match k {
                "tls-ca" => ca = Some(std::path::PathBuf::from(v)),
                "tls-client-cert" => cert = Some(std::path::PathBuf::from(v)),
                "tls-client-key" => key = Some(std::path::PathBuf::from(v)),
                "tls-domain" => domain = Some(v.to_string()),
                "bearer-token" => token = Some(v.to_string()),
                "bearer-token-file" => {
                    let text = std::fs::read_to_string(v)
                        .map_err(|e| format!("read bearer token file {v}: {e}"))?;
                    token = Some(text.trim().to_string());
                }
                _ => {}
            }
        }
        let tls = match ca {
            Some(ca) => Some(ClientTls::load(
                &ca,
                cert.as_deref(),
                key.as_deref(),
                domain,
            )?),
            None => {
                if cert.is_some() || key.is_some() || domain.is_some() {
                    return Err(
                        "--tls-client-cert / --tls-client-key / --tls-domain need --tls-ca"
                            .to_string(),
                    );
                }
                None
            }
        };
        let (bearer, raw_token) = match token {
            Some(token) => {
                if token.len() < 16 {
                    return Err("a bearer token is at least 16 bytes".to_string());
                }
                let header = format!("Bearer {token}")
                    .parse()
                    .map_err(|e| format!("bearer token is not a header value: {e}"))?;
                (Some(header), Some(token))
            }
            None => (None, None),
        };
        Ok(ToolClient {
            tls,
            bearer,
            raw_token,
        })
    }

    /// Whether the tool dials TLS.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Whether the tool sends a bearer token.
    pub fn has_bearer(&self) -> bool {
        self.bearer.is_some()
    }

    /// Raw bearer token secret (without the `"Bearer "` prefix), if configured.
    pub fn raw_bearer_token(&self) -> Option<&str> {
        self.raw_token.as_deref()
    }

    /// Install the TLS and bearer material process-wide, for the library's own
    /// channels (an in-process coordinator, `turbo-kserve` offload, replica and
    /// snapshot helpers).
    pub fn install(&self) {
        install_client_tls(self.tls.clone());
        install_client_bearer_token(self.raw_token.clone());
    }

    /// `host:port` (or a schemed address) as the URL the tool dials:
    /// `https://` on TLS, `http://` otherwise, whatever scheme was given.
    pub fn url(&self, addr: &str) -> String {
        secure_url(addr, self.tls.as_ref())
    }

    /// An endpoint for `addr` under the tool's TLS material.
    pub fn endpoint(&self, addr: &str) -> Result<tonic::transport::Endpoint, String> {
        let endpoint = tonic::transport::Endpoint::from_shared(self.url(addr))
            .map_err(|e| format!("bad address {addr:?}: {e}"))?;
        apply_client_tls(endpoint, self.tls.as_ref())
    }

    /// A channel to `addr` whose handshake is deferred to the first call.
    pub fn channel(&self, addr: &str) -> Result<tonic::transport::Channel, String> {
        Ok(self.endpoint(addr)?.connect_lazy())
    }

    /// A connected channel to `addr`.
    pub async fn connect(&self, addr: &str) -> Result<tonic::transport::Channel, String> {
        self.endpoint(addr)?
            .connect()
            .await
            .map_err(|e| format!("connect {}: {e}", self.url(addr)))
    }

    /// The interceptor that adds the bearer header to each call (a no-op
    /// without a token), for `Client::with_interceptor`.
    pub fn bearer(&self) -> Bearer {
        Bearer(self.bearer.clone())
    }
}

/// A `tonic` interceptor that sets `authorization: Bearer <token>` on
/// every request; without a token it changes nothing.
#[derive(Clone, Debug, Default)]
pub struct Bearer(Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(value) = &self.0 {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

/// The client type a tool holds for the coordinator's public surface: a
/// channel with the bearer interceptor.
pub type PublicChannel =
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Bearer>;
