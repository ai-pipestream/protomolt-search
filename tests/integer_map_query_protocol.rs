use pipestream_search::{cel, filter, pb, values};
use prost::Message;

// The old oneof tags: unknown typed operators must not become legacy map reads.
#[derive(Clone, PartialEq, Message)]
struct LegacyFilter {
    #[prost(oneof = "LegacyFilterNode", tags = "7, 8")]
    expr: Option<LegacyFilterNode>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum LegacyFilterNode {
    #[prost(message, tag = "7")]
    Number(pb::MapNumberPredicate),
    #[prost(message, tag = "8")]
    HasKey(pb::MapKeyPredicate),
}
#[derive(Clone, PartialEq, Message)]
struct LegacyValue {
    #[prost(oneof = "LegacyValueNode", tags = "2, 15")]
    expr: Option<LegacyValueNode>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum LegacyValueNode {
    #[prost(message, tag = "2")]
    Map(pb::MapRead),
    #[prost(uint64, tag = "15")]
    Uint(u64),
}

#[test]
fn old_decoders_reject_typed_map_operators_instead_of_reporting_missing_columns() {
    for text in ["unsigned[''] == 18446744073709551615u", "'' in unsigned"] {
        let compiled = cel::compile_filter(text).unwrap().unwrap();
        let legacy = LegacyFilter::decode(compiled.encode_to_vec().as_slice()).unwrap();
        assert!(legacy.expr.is_none(), "{text}");
        let forwarded = pb::FilterExpr::decode(legacy.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            filter::validate_filter(&forwarded).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    let compiled = cel::compile_value("unsigned['']").unwrap();
    let legacy = LegacyValue::decode(compiled.encode_to_vec().as_slice()).unwrap();
    assert!(legacy.expr.is_none());
    let forwarded = pb::ValueExpr::decode(legacy.encode_to_vec().as_slice()).unwrap();
    let names = ["unsigned".into()];
    let columns = values::NumericTypes {
        numerics: &[],
        integers: &[],
        unsigned_integers: &[],
        map_numerics: &[],
        map_integers: &[],
        map_unsigned_integers: &names,
    };
    assert_eq!(
        values::resolve(&forwarded, &columns).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        values::resolve(&compiled, &columns).unwrap().1,
        values::ValueType::Uint
    );
}

#[test]
fn legacy_map_reads_keep_their_string_and_float_scope() {
    let read = pb::MapRead {
        column: "value".into(),
        key: "".into(),
    };
    let legacy = LegacyValue {
        expr: Some(LegacyValueNode::Map(read.clone())),
    };
    let expression = pb::ValueExpr::decode(legacy.encode_to_vec().as_slice()).unwrap();
    assert!(matches!(
        expression.expr,
        Some(pb::value_expr::Expr::Map(_))
    ));
    let names = ["value".into()];
    let mut columns = values::NumericTypes {
        numerics: &[],
        integers: &[],
        unsigned_integers: &[],
        map_numerics: &names,
        map_integers: &[],
        map_unsigned_integers: &[],
    };
    assert_eq!(
        values::resolve(&expression, &columns).unwrap().1,
        values::ValueType::Double
    );
    columns.map_numerics = &[];
    columns.map_unsigned_integers = &names;
    assert_eq!(
        values::resolve(&expression, &columns).unwrap().1,
        values::ValueType::Unknown
    );
    let typed = pb::ValueExpr {
        expr: Some(pb::value_expr::Expr::TypedMap(read)),
    };
    assert_eq!(
        values::resolve(&typed, &columns).unwrap().1,
        values::ValueType::Uint
    );
    columns.map_integers = &names;
    assert!(values::resolve(&typed, &columns)
        .unwrap_err()
        .message()
        .contains("multiple map families"));
}
