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
                getrandom::getrandom(&mut key)
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
        if envelope.format_version != 1
            || envelope.context_sha256.len() != 32
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
            crate::sha256::to_hex(&self.tag(&bytes)?)
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
        .chunks_exact(2)
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

/// Holds the context while the core executes with its private boundary format.
/// Only this layer may convert a public cursor to/from that internal boundary.
pub(crate) struct CursorBinding {
    signer: Arc<CursorSigner>,
    digest: Vec<u8>,
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
        context.query_sha256 = crate::sha256::digest(&normalized.encode_to_vec()).to_vec();
        let digest = crate::sha256::digest(&context.encode_to_vec()).to_vec();
        if !request.cursor.is_empty() {
            let envelope = signer.open(&request.cursor)?;
            if !crate::security::constant_time_eq(&envelope.context_sha256, &digest) {
                return Err(Status::failed_precondition("cursor query, authorization or topology context changed; restart from the first page"));
            }
            request.cursor = envelope.boundary;
        }
        Ok(Self { signer, digest })
    }
    pub(crate) fn finish(&self, response: &mut QueryResponse) -> Result<(), Status> {
        if !response.next_cursor.is_empty() {
            response.next_cursor = self.signer.seal(QueryCursorEnvelope {
                format_version: 1,
                context_sha256: self.digest.clone(),
                boundary: response.next_cursor.clone(),
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{AccessAction, AccessDecision, QueryCursorRoute};
    use tonic::Code;

    fn context() -> QueryCursorContext {
        QueryCursorContext {
            collection: "docs".into(),
            topology_generation: 9,
            access: Some(AccessDecision {
                document_visibility: None,
                policy_revision: 1,
                principal: "alice".into(),
                workspace: "work".into(),
                collection: "docs".into(),
                action: AccessAction::Search as i32,
            }),
            routes: vec![QueryCursorRoute {
                address: "node".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
    fn issued(signer: Arc<CursorSigner>) -> String {
        let binding = CursorBinding::prepare(
            signer,
            &mut QueryRequest {
                k: 2,
                ..Default::default()
            },
            context(),
        )
        .unwrap();
        let mut response = QueryResponse {
            next_cursor: "tvq1:2:00000000:7".into(),
            ..Default::default()
        };
        binding.finish(&mut response).unwrap();
        response.next_cursor
    }

    #[test]
    fn boundary_is_integrity_protected_and_metadata_is_not_a_credential() {
        let signer = Arc::new(CursorSigner::from_key([7; 32]));
        let token = issued(signer.clone());
        let mut request = QueryRequest {
            k: 2,
            cursor: token.clone(),
            request_id: "new trace".into(),
            profile: true,
            required_topology_generation: 9,
            ..Default::default()
        };
        CursorBinding::prepare(signer.clone(), &mut request, context()).unwrap();
        assert_eq!(request.cursor, "tvq1:2:00000000:7");
        for position in [PREFIX.len(), token.len() - 1] {
            let mut corrupt = token.clone().into_bytes();
            corrupt[position] = if corrupt[position] == b'0' {
                b'1'
            } else {
                b'0'
            };
            assert_eq!(
                signer
                    .open(std::str::from_utf8(&corrupt).unwrap())
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
        }
        // Possessing the token never supplies the server's decision.
        let mut anonymous = context();
        anonymous.access = None;
        assert!(CursorBinding::prepare(
            signer.clone(),
            &mut QueryRequest {
                k: 2,
                cursor: token.clone(),
                ..Default::default()
            },
            anonymous
        )
        .is_err());
        let other = CursorSigner::from_key([8; 32]);
        assert_eq!(
            other.open(&token).unwrap_err().code(),
            Code::FailedPrecondition
        );
        assert!(!format!("{signer:?}").contains('7'));
    }

    #[test]
    fn every_authority_and_route_component_is_bound() {
        let signer = Arc::new(CursorSigner::from_key([7; 32]));
        let token = issued(signer.clone());
        for change in 0..11 {
            let mut ctx = context();
            match change {
                0 => ctx.collection = "other".into(),
                1 => ctx.topology_generation += 1,
                2 => ctx.access.as_mut().unwrap().principal = "bob".into(),
                3 => ctx.access.as_mut().unwrap().workspace = "other".into(),
                4 => ctx.access.as_mut().unwrap().policy_revision += 1,
                5 => ctx.access.as_mut().unwrap().action = AccessAction::Admin as i32,
                6 => ctx.access.as_mut().unwrap().collection = "other".into(),
                7 => ctx.routes[0].address = "different node".into(),
                8 => ctx.routes[0].replica = Some("replica".into()),
                9 => {
                    ctx.routes[0].hash_start = Some(0);
                    ctx.routes[0].hash_end = Some(u64::MAX);
                }
                10 => ctx.routes[0].placement = Some(-1),
                _ => unreachable!(),
            }
            let result = CursorBinding::prepare(
                signer.clone(),
                &mut QueryRequest {
                    k: 2,
                    cursor: token.clone(),
                    ..Default::default()
                },
                ctx,
            );
            assert!(
                matches!(result, Err(error) if error.code() == Code::FailedPrecondition),
                "context change {change}"
            );
        }
    }

    #[test]
    fn decoder_refuses_old_malformed_oversized_and_noncanonical_tokens() {
        let signer = CursorSigner::from_key([7; 32]);
        for token in [
            "tvq1:1:00000000:0",
            "pqc1:",
            "pqc1:ff:00",
            "pqc1:gg:00000000",
        ] {
            assert_eq!(
                signer.open(token).unwrap_err().code(),
                Code::InvalidArgument
            );
        }
        assert_eq!(
            signer
                .open(&"x".repeat(MAX_PAYLOAD * 2 + 100))
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        let envelope = QueryCursorEnvelope {
            format_version: 1,
            context_sha256: vec![1; 32],
            boundary: "b".into(),
        };
        for change in 0..4 {
            let mut envelope = envelope.clone();
            match change {
                0 => envelope.format_version = 2,
                1 => envelope.context_sha256.pop().map(|_| ()).unwrap(),
                2 => envelope.boundary.clear(),
                _ => {}
            }
            let mut bytes = envelope.encode_to_vec();
            if change == 3 {
                bytes.extend([0x78, 1]);
            } // Unknown field, valid MAC.
            let token = format!(
                "{PREFIX}{}:{}",
                encode_hex(&bytes),
                crate::sha256::to_hex(&signer.tag(&bytes).unwrap())
            );
            assert_eq!(
                signer.open(&token).unwrap_err().code(),
                Code::InvalidArgument
            );
        }
        let oversized = QueryCursorEnvelope {
            boundary: "x".repeat(MAX_PAYLOAD),
            ..envelope
        };
        assert_eq!(
            signer.seal(oversized).unwrap_err().code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn entropy_failure_never_falls_back_to_a_predictable_key() {
        let signer = CursorSigner {
            key: OnceLock::from(Err("entropy test failure".into())),
        };
        assert_eq!(signer.tag(b"test").unwrap_err().code(), Code::Internal);
    }
}
