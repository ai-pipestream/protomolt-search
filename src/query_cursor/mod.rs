//! Integrity-protected query cursors bound to the server's authorization context.
use std::sync::{Arc, OnceLock};

use prost::Message;
use tonic::Status;

use crate::pb::{QueryCursorContext, QueryCursorEnvelope, QueryRequest, QueryResponse};

const PREFIX: &str = "pqc1:";
const DOMAIN: &[u8] = b"protomolt.search.query-cursor.v1\0";
const MAX_PAYLOAD: usize = 64 * 1024;

/// Shared by clones of one coordinator. Default keys live for that coordinator's
/// lifetime; a host may supply a retained key to permit reuse across instances.
#[derive(Default)]
pub(crate) struct CursorSigner {
    key: OnceLock<Result<[u8; 32], String>>,
}
impl std::fmt::Debug for CursorSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorSigner { key: <redacted> }")
    }
}
impl CursorSigner {
    pub(crate) fn from_key(key: [u8; 32]) -> Self {
        Self {
            key: OnceLock::from(Ok(key)),
        }
    }
    fn key(&self) -> Result<&[u8; 32], Status> {
        self.key
            .get_or_init(|| {
                let mut key = [0; 32];
                getrandom::fill(&mut key)
                    .map_err(|error| format!("cursor entropy unavailable: {error}"))?;
                Ok(key)
            })
            .as_ref()
            .map_err(|error| Status::internal(error.clone()))
    }
    fn tag(&self, bytes: &[u8]) -> Result<[u8; 32], Status> {
        let mut message = Vec::with_capacity(DOMAIN.len() + bytes.len());
        message.extend_from_slice(DOMAIN);
        message.extend_from_slice(bytes);
        Ok(crate::security::hmac_sha256(self.key()?, &message))
    }
    fn open(&self, token: &str) -> Result<QueryCursorEnvelope, Status> {
        let bad = || Status::invalid_argument("malformed cursor; restart from the first page");
        if token.len() > PREFIX.len() + MAX_PAYLOAD * 2 + 1 + 64 {
            return Err(bad());
        }
        let (payload, tag) = token
            .strip_prefix(PREFIX)
            .and_then(|s| s.split_once(':'))
            .ok_or_else(bad)?;
        let tag = decode_hex(tag)
            .filter(|tag| tag.len() == 32)
            .ok_or_else(bad)?;
        let bytes = decode_hex(payload).ok_or_else(bad)?;
        if !crate::security::constant_time_eq(&tag, &self.tag(&bytes)?) {
            return Err(Status::failed_precondition("cursor integrity check failed or its signing key changed; restart from the first page"));
        }
        let envelope = QueryCursorEnvelope::decode(bytes.as_slice()).map_err(|_| bad())?;
        if envelope.format_version != 2
            || envelope.context_sha256.len() != 32
            || envelope.read_versions_sha256.len() != 32
            || envelope.boundary.is_empty()
            || envelope.encode_to_vec() != bytes
        {
            return Err(bad());
        }
        Ok(envelope)
    }
    fn seal(&self, envelope: QueryCursorEnvelope) -> Result<String, Status> {
        let bytes = envelope.encode_to_vec();
        if bytes.len() > MAX_PAYLOAD {
            return Err(Status::resource_exhausted(
                "query cursor exceeds its 64 KiB payload limit",
            ));
        }
        Ok(format!(
            "{PREFIX}{}:{}",
            encode_hex(&bytes),
            turbo_sha256::to_hex(self.tag(&bytes)?)
        ))
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|&[high, low]| Some((digit(high)? << 4) | digit(low)?))
        .collect()
}

/// Holds the context while the core executes with its private boundary format.
/// Only this layer may convert a public cursor to/from that internal boundary.
pub(crate) struct CursorBinding {
    signer: Arc<CursorSigner>,
    digest: Vec<u8>,
    expected_read_digest: Option<Vec<u8>>,
    read_digest: Option<Vec<u8>>,
}
impl CursorBinding {
    pub(crate) fn prepare(
        signer: Arc<CursorSigner>,
        request: &mut QueryRequest,
        mut context: QueryCursorContext,
    ) -> Result<Self, Status> {
        // CollectionSet already resolved defaults. Direct library hosts use the
        // coordinator's own collection, never a second namespace from the token.
        request.collection = context.collection.clone();
        let mut normalized = request.clone();
        normalized.cursor.clear();
        normalized.request_id.clear();
        normalized.profile = false;
        // The actual served generation is in context, so an explicit equivalent
        // precondition does not change the query. Admission checks it separately.
        normalized.required_topology_generation = 0;
        context.query_sha256 = turbo_sha256::sha256(&normalized.encode_to_vec()).0.to_vec();
        let digest = turbo_sha256::sha256(&context.encode_to_vec()).0.to_vec();
        let mut expected_read_digest = None;
        if !request.cursor.is_empty() {
            let envelope = signer.open(&request.cursor)?;
            if !crate::security::constant_time_eq(&envelope.context_sha256, &digest) {
                return Err(Status::failed_precondition("cursor query, authorization, data or topology context changed; restart from the first page"));
            }
            request.cursor = envelope.boundary;
            expected_read_digest = Some(envelope.read_versions_sha256);
        }
        Ok(Self {
            signer,
            digest,
            expected_read_digest,
            read_digest: None,
        })
    }
    pub(crate) fn bind_read_versions(
        &mut self,
        versions: &[crate::stats_identity::StatsClaim],
    ) -> Result<(), Status> {
        let mut read_set = crate::pb::QueryReadSet::default();
        for version in versions {
            crate::stats_identity::StatsClaim::required(version.epoch, &version.incarnation())?;
            read_set.versions.push(crate::pb::QueryReadVersion {
                epoch: version.epoch,
                incarnation: version.incarnation(),
            });
        }
        let digest = turbo_sha256::sha256(&read_set.encode_to_vec()).0.to_vec();
        if self
            .expected_read_digest
            .as_ref()
            .is_some_and(|expected| !crate::security::constant_time_eq(expected, &digest))
        {
            return Err(Status::failed_precondition(
                "cursor data context changed; restart from the first page",
            ));
        }
        if self
            .read_digest
            .as_ref()
            .is_some_and(|held| held != &digest)
        {
            return Err(Status::failed_precondition(
                "cursor read versions cannot change during execution",
            ));
        }
        self.read_digest = Some(digest);
        Ok(())
    }
    pub(crate) fn finish(&self, response: &mut QueryResponse) -> Result<(), Status> {
        if !response.next_cursor.is_empty() {
            response.next_cursor = self.signer.seal(QueryCursorEnvelope {
                format_version: 2,
                read_versions_sha256: self
                    .read_digest
                    .clone()
                    .ok_or_else(|| Status::internal("cursor data versions were not bound"))?,
                context_sha256: self.digest.clone(),
                boundary: response.next_cursor.clone(),
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
