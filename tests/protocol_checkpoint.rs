//! Wire allocations shared by the foundations and placement work.
use prost_reflect::DescriptorPool;

#[test]
fn identity_explain_and_sort_keep_distinct_wire_fields() {
    let pool = DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/search_descriptor.bin")).as_slice(),
    )
    .unwrap();
    for (message, fields) in [
        ("Bm25Hit", vec![("explain", 6), ("identity", 7)]),
        (
            "QueryHit",
            vec![("sort_values", 10), ("explain", 11), ("identity", 12)],
        ),
    ] {
        let descriptor = pool
            .get_message_by_name(&format!("ai.protomolt.search.v1.{message}"))
            .unwrap();
        for (field, number) in fields {
            assert_eq!(
                descriptor.get_field_by_name(field).unwrap().number(),
                number
            );
        }
    }
    for service in ["SearchService", "NodeService", "DiagnosticsService"] {
        assert!(pool
            .get_service_by_name(&format!("ai.protomolt.search.v1.{service}"))
            .is_some());
        assert!(pool
            .get_service_by_name(&format!("ai.pipestream.search.v1.{service}"))
            .is_none());
    }
}
