//! The security surface (`docs/security.md`): TLS and mTLS material for
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

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use tonic::metadata::MetadataMap;
use tonic::Status;

// ---------------------------------------------------------------------
// TLS material
// ---------------------------------------------------------------------

/// A listener's identity and, for cluster-internal listeners, the CA
/// client certificates must chain to.
#[derive(Clone)]
pub struct ServerTls {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    /// The CA that issues cluster-member client certificates. Node
    /// listeners require a client certificate from it; the coordinator
    /// listener accepts one and lets cluster control demand it.
    pub client_ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls")
            .field("cert_pem", &self.cert_pem.len())
            .field("key_pem", &"<redacted>")
            .field("client_ca_pem", &self.client_ca_pem.as_ref().map(Vec::len))
            .finish()
    }
}

impl ServerTls {
    pub fn load(cert: &Path, key: &Path, client_ca: Option<&Path>) -> Result<Self, String> {
        Ok(ServerTls {
            cert_pem: read_pem(cert, "certificate")?,
            key_pem: read_pem(key, "private key")?,
            client_ca_pem: client_ca
                .map(|path| read_pem(path, "client CA"))
                .transpose()?,
        })
    }

    /// The tonic configuration: with `require_client` the listener
    /// refuses connections without a client certificate from the CA
    /// (node listeners); without it the certificate is accepted when
    /// offered and checked per call by cluster control.
    #[cfg(feature = "tls")]
    pub fn server_config(&self, require_client: bool) -> tonic::transport::ServerTlsConfig {
        let mut config = tonic::transport::ServerTlsConfig::new().identity(
            tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem),
        );
        if let Some(ca) = &self.client_ca_pem {
            config = config
                .client_ca_root(tonic::transport::Certificate::from_pem(ca))
                .client_auth_optional(!require_client);
        }
        config
    }
}

/// What a cluster-internal client presents and trusts.
#[derive(Clone)]
pub struct ClientTls {
    pub ca_pem: Vec<u8>,
    /// The client certificate and key: this process's membership.
    pub identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// The name to verify server certificates against when the address
    /// is an IP literal the certificate names differently.
    pub domain: Option<String>,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTls")
            .field("ca_pem", &self.ca_pem.len())
            .field("identity", &self.identity_pem.is_some())
            .field("domain", &self.domain)
            .finish()
    }
}

impl ClientTls {
    pub fn load(
        ca: &Path,
        cert: Option<&Path>,
        key: Option<&Path>,
        domain: Option<String>,
    ) -> Result<Self, String> {
        let identity_pem = match (cert, key) {
            (Some(cert), Some(key)) => Some((
                read_pem(cert, "client certificate")?,
                read_pem(key, "client private key")?,
            )),
            (None, None) => None,
            _ => {
                return Err(
                    "a client identity needs both --tls-client-cert and --tls-client-key"
                        .to_string(),
                )
            }
        };
        Ok(ClientTls {
            ca_pem: read_pem(ca, "CA")?,
            identity_pem,
            domain,
        })
    }

    #[cfg(feature = "tls")]
    pub fn client_config(&self) -> tonic::transport::ClientTlsConfig {
        let mut config = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(&self.ca_pem));
        if let Some((cert, key)) = &self.identity_pem {
            config = config.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        if let Some(domain) = &self.domain {
            config = config.domain_name(domain.clone());
        }
        config
    }
}

fn read_pem(path: &Path, what: &str) -> Result<Vec<u8>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {what} {}: {e}", path.display()))?;
    if !bytes.windows(10).any(|w| w == b"-----BEGIN") {
        return Err(format!(
            "{what} {} is not PEM (no BEGIN block)",
            path.display()
        ));
    }
    Ok(bytes)
}

/// The process-wide client TLS material for cluster-internal channels
/// opened outside the coordinator (replica sync, snapshot install, the
/// calibration tools). The coordinator carries its own copy
/// explicitly; this is the fallback those helpers consult.
static PROCESS_CLIENT_TLS: std::sync::OnceLock<Option<ClientTls>> = std::sync::OnceLock::new();

/// Install the process-wide client TLS material once (from `main`).
pub fn install_client_tls(tls: Option<ClientTls>) {
    let _ = PROCESS_CLIENT_TLS.set(tls);
}

/// The installed process-wide client TLS material, if any.
pub fn process_client_tls() -> Option<&'static ClientTls> {
    PROCESS_CLIENT_TLS.get().and_then(Option::as_ref)
}

/// Apply the process-wide client TLS material to an endpoint, when
/// installed; a plaintext endpoint otherwise.
pub fn secure_endpoint(
    endpoint: tonic::transport::Endpoint,
) -> Result<tonic::transport::Endpoint, String> {
    apply_client_tls(endpoint, process_client_tls())
}

/// Apply the given client TLS material to an endpoint.
pub fn apply_client_tls(
    endpoint: tonic::transport::Endpoint,
    tls: Option<&ClientTls>,
) -> Result<tonic::transport::Endpoint, String> {
    match tls {
        None => Ok(endpoint),
        #[cfg(feature = "tls")]
        Some(tls) => endpoint
            .tls_config(tls.client_config())
            .map_err(|e| format!("client TLS: {e}")),
        #[cfg(not(feature = "tls"))]
        Some(_) => Err("this build has no TLS support (feature `tls` is off)".to_string()),
    }
}

/// Whether a listen address is loopback: the only place plaintext gRPC
/// and unsigned UDP are accepted without an explicit opt-in.
pub fn is_loopback(addr: &std::net::SocketAddr) -> bool {
    addr.ip().is_loopback()
}

// ---------------------------------------------------------------------
// Bearer principals and quotas
// ---------------------------------------------------------------------

/// One public client, as configured: a name, its bearer token, and its
/// quotas. A quota of 0 means "no limit of that kind".
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PrincipalConfig {
    pub name: String,
    pub token: String,
    /// The largest `k` this principal may ask for; 0 means the
    /// coordinator's own `max_k`.
    pub max_k: u32,
    /// Requests in flight at once; 0 means unlimited.
    pub concurrency: u32,
    /// Documents per second through routed ingest, as a token bucket
    /// with one second of burst; 0 means unlimited.
    pub ingest_docs_per_sec: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PrincipalsFile {
    principals: Vec<PrincipalConfig>,
}

/// A principal's live quota state.
#[derive(Debug)]
pub struct Principal {
    pub name: String,
    pub max_k: u32,
    pub concurrency: u32,
    in_flight: AtomicU64,
    ingest: Option<Mutex<TokenBucket>>,
}

impl Principal {
    fn new(config: &PrincipalConfig) -> Self {
        Principal {
            name: config.name.clone(),
            max_k: config.max_k,
            concurrency: config.concurrency,
            in_flight: AtomicU64::new(0),
            ingest: (config.ingest_docs_per_sec > 0)
                .then(|| Mutex::new(TokenBucket::new(f64::from(config.ingest_docs_per_sec)))),
        }
    }

    /// Check a requested `k` against this principal's cap.
    pub fn admit_k(&self, k: u32) -> Result<(), Status> {
        if self.max_k != 0 && k > self.max_k {
            return Err(Status::resource_exhausted(format!(
                "principal {:?}: k={k} exceeds its max_k={}; lower k",
                self.name, self.max_k
            )));
        }
        Ok(())
    }

    /// The rule for an unset `k`: it means the coordinator's default,
    /// `default_k`, and that resolved value is what the cap judges. The
    /// request keeps its unset `k`; nothing rewrites it to the cap.
    pub fn admit_default_k(&self, default_k: u32) -> Result<(), Status> {
        if self.max_k != 0 && default_k > self.max_k {
            return Err(Status::resource_exhausted(format!(
                "principal {:?}: k is unset, which means the coordinator default k={default_k}, \
                 above its max_k={}; send an explicit k",
                self.name, self.max_k
            )));
        }
        Ok(())
    }

    /// Take one in-flight slot, or refuse by name when the principal is
    /// at its concurrency limit. The permit releases on drop.
    pub fn admit_request(self: &Arc<Self>) -> Result<Permit, Status> {
        if self.concurrency == 0 {
            return Ok(Permit { principal: None });
        }
        let limit = u64::from(self.concurrency);
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return Err(Status::resource_exhausted(format!(
                    "principal {:?}: {} requests in flight, at its concurrency limit of {}",
                    self.name, current, self.concurrency
                )));
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Permit {
                        principal: Some(Arc::clone(self)),
                    })
                }
                Err(seen) => current = seen,
            }
        }
    }

    /// Charge `documents` against the ingest rate, or refuse by name
    /// when the bucket cannot cover them: the request is not trimmed.
    pub fn admit_ingest(&self, documents: u32) -> Result<(), Status> {
        let Some(bucket) = &self.ingest else {
            return Ok(());
        };
        let mut bucket = bucket.lock().expect("token bucket poisoned");
        if bucket.take(f64::from(documents), std::time::Instant::now()) {
            Ok(())
        } else {
            Err(Status::resource_exhausted(format!(
                "principal {:?}: ingest of {documents} document(s) exceeds its rate of {} per \
                 second; slow the stream",
                self.name, bucket.rate
            )))
        }
    }

    /// Requests in flight right now.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// An in-flight slot; dropping it releases the slot.
#[derive(Debug)]
pub struct Permit {
    principal: Option<Arc<Principal>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(p) = &self.principal {
            p.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A token bucket: `rate` tokens per second, one second of burst.
#[derive(Debug)]
struct TokenBucket {
    rate: f64,
    tokens: f64,
    last: std::time::Instant,
}

impl TokenBucket {
    fn new(rate: f64) -> Self {
        TokenBucket {
            rate,
            tokens: rate,
            last: std::time::Instant::now(),
        }
    }

    fn take(&mut self, n: f64, now: std::time::Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.rate);
        if self.tokens + 1e-9 >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }
}

/// The configured principals, looked up by bearer token.
#[derive(Debug, Clone)]
pub struct Principals {
    by_token: HashMap<String, Arc<Principal>>,
}

impl Principals {
    /// Load `[[principals]]` from a TOML file.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read bearer tokens {}: {e}", path.display()))?;
        let file: PrincipalsFile = toml::from_str(&text)
            .map_err(|e| format!("parse bearer tokens {}: {e}", path.display()))?;
        Self::from_configs(&file.principals)
    }

    pub fn from_configs(configs: &[PrincipalConfig]) -> Result<Self, String> {
        if configs.is_empty() {
            return Err("bearer tokens: no [[principals]] declared".to_string());
        }
        let mut by_token = HashMap::with_capacity(configs.len());
        let mut names: Vec<&str> = Vec::with_capacity(configs.len());
        for (i, c) in configs.iter().enumerate() {
            if c.name.is_empty() {
                return Err(format!("principals[{i}]: name is empty"));
            }
            if c.token.len() < 16 {
                return Err(format!(
                    "principals[{i}] ({:?}): token is shorter than 16 bytes",
                    c.name
                ));
            }
            if names.contains(&c.name.as_str()) {
                return Err(format!("principals[{i}]: name {:?} repeats", c.name));
            }
            names.push(&c.name);
            if by_token
                .insert(c.token.clone(), Arc::new(Principal::new(c)))
                .is_some()
            {
                return Err(format!(
                    "principals[{i}] ({:?}): token repeats another principal's",
                    c.name
                ));
            }
        }
        Ok(Principals { by_token })
    }

    /// The principal behind a request's `authorization: Bearer <token>`
    /// header, or `UNAUTHENTICATED` naming what was missing.
    pub fn authenticate(&self, metadata: &MetadataMap) -> Result<Arc<Principal>, Status> {
        let header = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization: Bearer <token>"))?;
        let value = header
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not ASCII"))?;
        let token = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
            .ok_or_else(|| Status::unauthenticated("authorization is not a Bearer token"))?
            .trim();
        // Constant-time over the configured tokens: the comparison cost
        // does not depend on how far a guess matches.
        let mut found: Option<Arc<Principal>> = None;
        for (known, principal) in &self.by_token {
            if constant_time_eq(known.as_bytes(), token.as_bytes()) {
                found = Some(Arc::clone(principal));
            }
        }
        found.ok_or_else(|| Status::unauthenticated("bearer token is not recognized"))
    }

    pub fn len(&self) -> usize {
        self.by_token.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_token.is_empty()
    }
}

/// A response stream that keeps its principal's in-flight slot until
/// the client is done reading it, so a streaming query counts against
/// the concurrency quota for as long as it runs.
pub struct Guarded<S> {
    inner: S,
    _permit: Option<Permit>,
}

impl<S> Guarded<S> {
    pub fn new(inner: S, permit: Option<Permit>) -> Self {
        Guarded {
            inner,
            _permit: permit,
        }
    }
}

impl<S: tokio_stream::Stream + Unpin> tokio_stream::Stream for Guarded<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// A routed-ingest request stream that charges each document against
/// its principal's ingest rate and ends the stream with the named
/// refusal when the rate is exceeded, never trimming the batch.
pub struct MeteredIngest<S> {
    inner: S,
    principal: Option<Arc<Principal>>,
    refused: bool,
}

impl<S> MeteredIngest<S> {
    pub fn new(inner: S, principal: Option<Arc<Principal>>) -> Self {
        MeteredIngest {
            inner,
            principal,
            refused: false,
        }
    }
}

impl<S> tokio_stream::Stream for MeteredIngest<S>
where
    S: tokio_stream::Stream<Item = Result<crate::pb::RoutedIngestMappedRequest, Status>> + Unpin,
{
    type Item = Result<crate::pb::RoutedIngestMappedRequest, Status>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.refused {
            return std::task::Poll::Ready(None);
        }
        let next = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(message))) = &next {
            let is_document = matches!(
                message.payload,
                Some(crate::pb::routed_ingest_mapped_request::Payload::Document(
                    _
                ))
            );
            if is_document {
                if let Some(principal) = &self.principal {
                    if let Err(status) = principal.admit_ingest(1) {
                        self.refused = true;
                        return std::task::Poll::Ready(Some(Err(status)));
                    }
                }
            }
        }
        next
    }
}

/// Byte equality whose running time depends on the lengths only.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // The accumulator is a usize so a length difference survives whole:
    // narrowing it to a byte would let lengths 256 apart compare as one.
    let mut diff: usize = a.len() ^ b.len();
    let n = a.len().min(b.len());
    for i in 0..n {
        diff |= usize::from(a[i] ^ b[i]);
    }
    // Walk the rest of the longer input so length differences cost
    // the same as content differences.
    for &x in a.iter().skip(n).chain(b.iter().skip(n)) {
        std::hint::black_box(x);
    }
    std::hint::black_box(diff) == 0
}

// ---------------------------------------------------------------------
// HMAC-SHA256 for UDP datagrams
// ---------------------------------------------------------------------

/// HMAC-SHA256 over `crate::sha256` (RFC 2104).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&crate::sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = crate::sha256::Sha256::new();
    inner.update(&ipad);
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = crate::sha256::Sha256::new();
    outer.update(&opad);
    outer.update(&inner);
    outer.finalize()
}

/// The shared key that authenticates UDP floor and cancel datagrams.
#[derive(Clone)]
pub struct UdpKey(Vec<u8>);

impl std::fmt::Debug for UdpKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpKey({} bytes)", self.0.len())
    }
}

impl UdpKey {
    /// Read the key from a file: raw bytes, or a hex string (whitespace
    /// ignored). At least 16 bytes of key material.
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes =
            std::fs::read(path).map_err(|e| format!("read UDP key {}: {e}", path.display()))?;
        Self::from_bytes(&bytes).map_err(|e| format!("UDP key {}: {e}", path.display()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let text: Vec<u8> = bytes
            .iter()
            .copied()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        let key = if !text.is_empty()
            && text.len().is_multiple_of(2)
            && text.iter().all(u8::is_ascii_hexdigit)
        {
            text.chunks(2)
                .map(|pair| {
                    let hi = (pair[0] as char).to_digit(16).expect("hex digit") as u8;
                    let lo = (pair[1] as char).to_digit(16).expect("hex digit") as u8;
                    (hi << 4) | lo
                })
                .collect::<Vec<u8>>()
        } else {
            bytes.to_vec()
        };
        if key.len() < 16 {
            return Err(format!(
                "key material is {} bytes; at least 16 are required",
                key.len()
            ));
        }
        Ok(UdpKey(key))
    }

    /// The 16-byte tag over `message`: HMAC-SHA256 truncated.
    pub fn tag(&self, message: &[u8]) -> [u8; 16] {
        let full = hmac_sha256(&self.0, message);
        let mut tag = [0u8; 16];
        tag.copy_from_slice(&full[..16]);
        tag
    }

    pub fn verify(&self, message: &[u8], tag: &[u8]) -> bool {
        tag.len() == 16 && constant_time_eq(&self.tag(message), tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_sha256_matches_rfc_4231_vectors() {
        // Test case 2 of RFC 4231.
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            crate::sha256::to_hex(&mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 1.
        let mac = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            crate::sha256::to_hex(&mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Test case 6: a key longer than the block.
        let mac = hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        assert_eq!(
            crate::sha256::to_hex(&mac),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn udp_keys_load_hex_or_raw_and_tags_verify() {
        let key = UdpKey::from_bytes(b"000102030405060708090a0b0c0d0e0f\n").unwrap();
        assert_eq!(key.0, (0u8..16).collect::<Vec<_>>());
        let raw = UdpKey::from_bytes(&[7u8; 32]).unwrap();
        assert_eq!(raw.0.len(), 32);
        assert!(UdpKey::from_bytes(b"short").is_err());
        let tag = key.tag(b"frame");
        assert!(key.verify(b"frame", &tag));
        assert!(!key.verify(b"framf", &tag));
        assert!(!raw.verify(b"frame", &tag));
        assert!(!key.verify(b"frame", &tag[..15]));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        // Lengths 256 apart, in both directions, with equal prefixes.
        let short = [7u8; 16];
        let long = [7u8; 272];
        assert!(!constant_time_eq(&short, &long));
        assert!(!constant_time_eq(&long, &short));
        assert!(!constant_time_eq(&[], &[0u8; 256]));
        assert!(constant_time_eq(&long, &long[..]));
    }

    #[test]
    fn principals_authenticate_and_refuse_by_name() {
        let principals = Principals::from_configs(&[
            PrincipalConfig {
                name: "console".into(),
                token: "console-token-0123456789".into(),
                max_k: 50,
                concurrency: 1,
                ingest_docs_per_sec: 2,
            },
            PrincipalConfig {
                name: "batch".into(),
                token: "batch-token-0123456789".into(),
                ..Default::default()
            },
        ])
        .unwrap();
        let mut md = MetadataMap::new();
        assert_eq!(
            principals.authenticate(&md).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        md.insert(
            "authorization",
            "Bearer nope-nope-nope-nope".parse().unwrap(),
        );
        assert!(principals
            .authenticate(&md)
            .unwrap_err()
            .message()
            .contains("not recognized"));
        md.insert("authorization", "Basic abc".parse().unwrap());
        assert!(principals
            .authenticate(&md)
            .unwrap_err()
            .message()
            .contains("not a Bearer"));
        md.insert(
            "authorization",
            "Bearer console-token-0123456789".parse().unwrap(),
        );
        let console = principals.authenticate(&md).unwrap();
        assert_eq!(console.name, "console");
        assert!(console.admit_k(50).is_ok());
        let error = console.admit_k(51).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("k=51 exceeds its max_k=50"));
        let first = console.admit_request().unwrap();
        let error = console.admit_request().unwrap_err();
        assert!(error.message().contains("concurrency limit of 1"));
        drop(first);
        assert!(console.admit_request().is_ok());
        assert!(console.admit_ingest(2).is_ok());
        let error = console.admit_ingest(1).unwrap_err();
        assert!(error.message().contains("exceeds its rate of 2"));
        md.insert(
            "authorization",
            "Bearer batch-token-0123456789".parse().unwrap(),
        );
        let batch = principals.authenticate(&md).unwrap();
        assert!(batch.admit_k(1_000_000).is_ok());
        assert!(batch.admit_ingest(1_000_000).is_ok());
        // Configuration refusals name the entry.
        assert!(Principals::from_configs(&[]).is_err());
        assert!(Principals::from_configs(&[PrincipalConfig {
            name: "x".into(),
            token: "short".into(),
            ..Default::default()
        }])
        .unwrap_err()
        .contains("shorter than 16"));
    }

    #[test]
    fn token_buckets_refill_and_refuse() {
        let start = std::time::Instant::now();
        let mut bucket = TokenBucket::new(4.0);
        bucket.last = start;
        assert!(bucket.take(4.0, start));
        assert!(!bucket.take(1.0, start));
        assert!(bucket.take(2.0, start + std::time::Duration::from_millis(500)));
        assert!(!bucket.take(1.0, start + std::time::Duration::from_millis(500)));
        assert!(bucket.take(4.0, start + std::time::Duration::from_secs(5)));
    }
}
