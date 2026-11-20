//! Whether a row proves its lineage parent's rows share its shard.

use crate::pb::AddDocumentsRequest;

/// A row proves its parent colocated when it names no lineage parent at
/// all, or when it arrived under a stable routing key: routed ingest
/// places every chunk of a source document by that document's key, so
/// all of the parent's rows land on one shard. A lineage parent id lives
/// in the source document's id space, not this shard's positional one,
/// so a parent id that happens to equal the row's global id proves
/// nothing. A row with a lineage parent and no key proves nothing.
pub(crate) fn row_proves_parent_colocated(
    doc: &AddDocumentsRequest,
    _global_id: u64,
    keyed: bool,
) -> bool {
    keyed || doc.lineage.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::DocLineage;

    fn with_parent(parent_id: Option<u64>) -> AddDocumentsRequest {
        AddDocumentsRequest {
            lineage: parent_id.map(|parent_id| DocLineage {
                parent_id,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn self_parented_and_keyed_rows_prove_colocation_and_raw_children_do_not() {
        assert!(row_proves_parent_colocated(&with_parent(None), 7, false));
        assert!(!row_proves_parent_colocated(
            &with_parent(Some(7)),
            7,
            false
        ));
        assert!(!row_proves_parent_colocated(
            &with_parent(Some(3)),
            7,
            false
        ));
        assert!(row_proves_parent_colocated(&with_parent(Some(3)), 7, true));
    }
}
