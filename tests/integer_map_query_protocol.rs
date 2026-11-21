use pipestream_search::{cel, pb, values};

#[test]
fn map_operators_compile_to_the_single_map_forms() {
    let compiled = cel::compile_filter("unsigned[''] == 18446744073709551615u")
        .unwrap()
        .unwrap();
    assert!(matches!(
        compiled.expr,
        Some(pb::filter_expr::Expr::MapNumber(_))
    ));
    let compiled = cel::compile_filter("'' in unsigned").unwrap().unwrap();
    assert!(matches!(
        compiled.expr,
        Some(pb::filter_expr::Expr::MapHasKey(_))
    ));
    let compiled = cel::compile_value("unsigned['']").unwrap();
    assert!(matches!(compiled.expr, Some(pb::value_expr::Expr::Map(_))));
}

#[test]
fn map_reads_resolve_every_numeric_plane_and_refuse_ambiguity() {
    let expression = pb::ValueExpr {
        expr: Some(pb::value_expr::Expr::Map(pb::MapRead {
            column: "value".into(),
            key: "".into(),
        })),
    };
    let names = ["value".into()];
    let mut columns = values::NumericTypes {
        facets: &[],
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
        values::ValueType::Uint
    );
    columns.map_unsigned_integers = &[];
    columns.map_integers = &names;
    assert_eq!(
        values::resolve(&expression, &columns).unwrap().1,
        values::ValueType::Int
    );
    columns.map_unsigned_integers = &names;
    assert!(values::resolve(&expression, &columns)
        .unwrap_err()
        .message()
        .contains("multiple map families"));
}
