//! Identity and capability checks for a trusted planner's document view.
//! These checks do not authenticate a caller or authorize a document grant.

use crate::pb::{DocumentVisibility, TermStatsResponse};
use crate::sha256::Sha256;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::sync::OnceLock;
use tonic::Status;

/// Version probes are distinct from empty-term corpus statistics. Keeping this
/// mode explicit prevents a version response from becoming a zero-valued share.
pub fn validate_stats_request(request: &crate::pb::TermStatsRequest) -> Result<(), Status> {
    if request.version_only && (!request.terms.is_empty() || !request.fields.is_empty()) {
        return Err(Status::invalid_argument(
            "a version-only probe cannot request terms or fields",
        ));
    }
    Ok(())
}

pub fn validate_stats_mode(version_only: bool, response: &TermStatsResponse) -> Result<(), Status> {
    if response.version_only != version_only {
        return Err(Status::failed_precondition(
            "statistics response mode mismatch; use matching node and coordinator builds",
        ));
    }
    if version_only
        && (response.doc_count != 0
            || response.total_doc_length != 0
            || !response.doc_frequencies.is_empty()
            || !response.field_stats.is_empty())
    {
        return Err(Status::failed_precondition(
            "a version-only probe returned corpus statistics",
        ));
    }
    Ok(())
}

/// Intersect an authority view with user membership without treating the
/// authority predicate as a field permission granted to the caller.
pub fn intersect_filter(
    view: Option<&DocumentVisibility>,
    user: Option<crate::pb::FilterExpr>,
) -> Result<Option<crate::pb::FilterExpr>, Status> {
    VisibilityScope::new(view)?;
    let result = match view.and_then(|view| view.filter.as_ref()) {
        None => user,
        Some(mandatory) => Some(match user {
            None => mandatory.clone(),
            Some(user) => crate::pb::FilterExpr {
                expr: Some(crate::pb::filter_expr::Expr::And(crate::pb::FilterList {
                    exprs: vec![mandatory.clone(), user],
                })),
            },
        }),
    };
    if let Some(filter) = &result {
        crate::filter::validate_filter(filter)?;
    }
    Ok(result)
}

/// Cache identity derived from a validated protobuf visibility, never from a
/// caller-supplied digest. Default is the unrestricted live document view.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct VisibilityScope {
    fingerprint: Vec<u8>,
    columns: usize,
}

fn descriptor() -> Result<&'static MessageDescriptor, Status> {
    static DESCRIPTOR: OnceLock<Result<MessageDescriptor, &'static str>> = OnceLock::new();
    DESCRIPTOR
        .get_or_init(|| {
            DescriptorPool::decode(
                include_bytes!(concat!(env!("OUT_DIR"), "/search_descriptor.bin")).as_slice(),
            )
            .map_err(|_| "invalid compiled search descriptor")?
            .get_message_by_name("ai.protomolt.search.v1.DocumentVisibility")
            .ok_or("compiled document visibility descriptor is missing")
        })
        .as_ref()
        .map_err(|error| Status::internal(*error))
}

/// Numeric field order at every message level, including the active tag of a
/// oneof. The generated derive encoder instead positions a oneof by its lowest
/// possible tag. Repeated values retain their order; proto3 implicit defaults
/// are omitted. This closed filter graph contains no protobuf maps/extensions.
fn canonical_bytes(visibility: &DocumentVisibility) -> Result<Vec<u8>, Status> {
    let dynamic =
        DynamicMessage::decode(descriptor()?.clone(), visibility.encode_to_vec().as_slice())
            .map_err(|_| {
                Status::internal("compiled visibility descriptor does not match its message")
            })?;
    Ok(dynamic.encode_to_vec())
}

impl VisibilityScope {
    pub fn new(visibility: Option<&DocumentVisibility>) -> Result<Self, Status> {
        let Some(visibility) = visibility else {
            return Ok(Self::default());
        };
        let filter = visibility
            .filter
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("document visibility must contain a filter"))?;
        crate::filter::validate_filter(filter)?;
        let mut hash = Sha256::new();
        hash.update(b"protomolt.search.document-visibility.v1\0");
        hash.update(&canonical_bytes(visibility)?);
        Ok(Self {
            fingerprint: hash.finalize().to_vec(),
            columns: crate::filter::leaf_count(filter),
        })
    }

    pub fn fingerprint(&self) -> &[u8] {
        &self.fingerprint
    }

    pub fn column_count(&self) -> usize {
        self.columns
    }

    /// An epoch alone does not prove that a node applied the requested view.
    /// Verify the echo even for empty results, before merging a response or
    /// populating a cache miss. Missing echoes from old nodes refuse closed.
    pub fn validate_response(&self, response: &TermStatsResponse) -> Result<(), Status> {
        self.validate_echo(
            &response.visibility_fingerprint,
            &response.visibility_columns_known,
        )
    }

    pub fn validate_echo(&self, fingerprint: &[u8], columns_known: &[bool]) -> Result<(), Status> {
        if fingerprint != self.fingerprint {
            return Err(Status::failed_precondition(
                "document visibility mismatch; the node must apply the requested document view",
            ));
        }
        if columns_known.len() != self.columns {
            return Err(Status::failed_precondition(
                "document visibility column handshake has the wrong length",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oneof_active_tag_order_does_not_change_visibility_identity() {
        // NumberPredicate.min with exclusive (tag 3) and unsigned (tag 4).
        // A derive encoder groups tag 4 at the oneof's lowest declared tag 1.
        let encoded = b"\x0a\x13\x2a\x11\x0a\x09tenant_id\x12\x04\x18\x01\x20\x01";
        let view = DocumentVisibility::decode(encoded.as_slice()).unwrap();
        let reordered = b"\x0a\x13\x2a\x11\x0a\x09tenant_id\x12\x04\x20\x01\x18\x01";
        let same_view = DocumentVisibility::decode(reordered.as_slice()).unwrap();
        assert_eq!(view, same_view);
        assert_eq!(canonical_bytes(&view).unwrap(), encoded);
        assert_eq!(canonical_bytes(&same_view).unwrap(), encoded);
        let scope = VisibilityScope::new(Some(&view)).unwrap();
        assert_eq!(
            crate::sha256::to_hex(scope.fingerprint().try_into().unwrap()),
            "833cffd46fe72f11b9b86ff1733f2d3d1ea376b3ccc4f9395b59e905de7c0462"
        );
    }

    #[test]
    fn fingerprint_has_a_language_independent_wire_vector() {
        // Encoded independently: visibility.filter.facet(audience, [public]).
        let encoded = b"\x0a\x14\x22\x12\x0a\x08audience\x12\x06public";
        let view = DocumentVisibility::decode(encoded.as_slice()).unwrap();
        assert_eq!(view.encode_to_vec(), encoded);
        assert_eq!(canonical_bytes(&view).unwrap(), encoded);
        let scope = VisibilityScope::new(Some(&view)).unwrap();
        assert_eq!(
            crate::sha256::to_hex(scope.fingerprint().try_into().unwrap()),
            "e3e13a3fcf1c73bb6ae20f670de952525f283fa0a869215f0ee77278d9e00f6e"
        );
        assert_eq!(scope.column_count(), 1);
        assert_ne!(scope, VisibilityScope::default());
        assert!(VisibilityScope::default().fingerprint().is_empty());
        assert!(scope
            .validate_response(&TermStatsResponse::default())
            .is_err());
    }

    #[test]
    fn visibility_graph_has_no_undefined_map_or_extension_order() {
        let mut pending = vec![descriptor().unwrap().clone()];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(message) = pending.pop() {
            if !visited.insert(message.full_name().to_string()) {
                continue;
            }
            assert_eq!(message.extensions().count(), 0);
            for field in message.fields() {
                assert!(
                    !field.is_map(),
                    "visibility map ordering must be defined before adding {}",
                    field.full_name()
                );
                if let prost_reflect::Kind::Message(child) = field.kind() {
                    pending.push(child);
                }
            }
        }
    }
}
