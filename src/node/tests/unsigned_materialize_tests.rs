//! Shared materialization over unsigned and typed target families.

use super::*;

#[test]
fn shared_materialization_preserves_uint_and_checks_target_family() {
    let doc = AddDocumentsRequest {
        unsigned_integers: vec![crate::pb::UnsignedIntegerValue {
            field: "counter".into(),
            value: u64::MAX,
        }],
        ..Default::default()
    };
    for (expression, value) in [
        ("counter", Some(u64::MAX)),
        ("counter + 1u", None),
        ("counter - 1u", Some(u64::MAX - 1)),
        ("counter > 0u ? counter : 0u", Some(u64::MAX)),
    ] {
        let mut spec = crate::pb::MaterializeSpec {
            columns: vec![crate::pb::MaterializedColumn {
                name: "derived".into(),
                expression: expression.into(),
                kind: crate::pb::MaterializeKind::U64 as i32,
            }],
        };
        let compiled = compile_materialize_spec(&spec).unwrap();
        let result = apply_materialize(doc.clone(), &compiled).unwrap();
        assert_eq!(
            result
                .unsigned_integers
                .iter()
                .find(|v| v.field == "derived")
                .map(|v| v.value),
            value
        );
        if value.is_some() {
            spec.columns[0].kind = crate::pb::MaterializeKind::I64 as i32;
            let error = apply_materialize(doc.clone(), &compile_materialize_spec(&spec).unwrap())
                .unwrap_err();
            assert!(error.message().contains("evaluated uint"), "{error}");
        }
    }
}
