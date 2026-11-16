//! Vector column names against declared and stored columns.

use super::*;
#[test]
fn active_store_columns_are_checked_even_if_the_config_does_not_declare_them() {
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let stores = [
        Bm25Store::with_fields(&["body", "embedding"]),
        Bm25Store::new().with_facets(&["embedding"]),
        Bm25Store::new().with_numerics(&["embedding"]),
        Bm25Store::new().with_integers(&["embedding"]),
        Bm25Store::new().with_unsigned_integers(&["embedding"]),
        Bm25Store::new().with_map_facets(&["embedding"]),
        Bm25Store::new().with_map_numerics(&["embedding"]),
        Bm25Store::new().with_geos(&["embedding"]),
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
