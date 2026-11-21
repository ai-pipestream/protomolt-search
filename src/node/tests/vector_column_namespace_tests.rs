//! Vector column names against declared and stored columns.

use super::*;
#[test]
fn active_store_columns_are_checked_even_if_the_config_does_not_declare_them() {
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let stores = [
        Bm25Store::with_text_fields(&["body", "embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::Facet, &["embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::Numeric, &["embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::Integer, &["embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::UnsignedInteger, &["embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::MapFacet, &["embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::MapNumeric, &["embedding"]),
        Bm25Store::new().with_columns(crate::postings::ColumnKind::Geo, &["embedding"]),
    ];
    for store in stores {
        let store = Bm25Shard::Building(store);
        assert_eq!(
            node.validate_vector_column_name("embedding", Some(&store))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        node.validate_vector_column_name("distinct", Some(&store))
            .unwrap();
    }
}
