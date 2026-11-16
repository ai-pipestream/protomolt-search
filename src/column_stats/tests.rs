use super::*;
fn collect(ty: Type, values: &[NumBound]) -> ColumnStats {
    let mut c = Collector::new("value", ty);
    for &v in values {
        c.observe(v).unwrap();
    }
    c.finish().unwrap()
}
#[test]
fn wide_signed_and_unsigned_sums_and_empty_declarations() {
    for ty in [Type::Integer, Type::UnsignedInteger, Type::Number] {
        let empty = collect(ty, &[]);
        assert!(empty.known);
        assert_eq!(empty.value_type, ty as i32);
        assert_eq!(empty.count, 0);
        assert_eq!(empty.min, 0.0);
        assert_eq!(empty.max, 0.0);
        assert_eq!(
            merge(&["value".into()], &[vec![empty.clone()]]).unwrap()[0],
            empty
        );
    }
    let signed = collect(
        Type::Integer,
        &[
            NumBound::I(i64::MIN),
            NumBound::I(i64::MIN),
            NumBound::I(i64::MAX),
        ],
    );
    let unsigned = collect(
        Type::UnsignedInteger,
        &[NumBound::U(u64::MAX), NumBound::U(u64::MAX), NumBound::U(1)],
    );
    let Some(ExactInteger::Signed(s)) = &signed.exact_integer else {
        panic!()
    };
    assert_eq!(
        signed_sum(s),
        i128::from(i64::MIN) * 2 + i128::from(i64::MAX)
    );
    let Some(ExactInteger::Unsigned(u)) = &unsigned.exact_integer else {
        panic!()
    };
    assert_eq!(unsigned_sum(u), u128::from(u64::MAX) * 2 + 1);
    for (ty, full) in [(Type::Integer, signed), (Type::UnsignedInteger, unsigned)] {
        let absent = collect(Type::Unspecified, &[]);
        let empty = collect(ty, &[]);
        for shares in [
            vec![
                vec![absent.clone()],
                vec![empty.clone()],
                vec![full.clone()],
            ],
            vec![
                vec![full.clone()],
                vec![empty.clone()],
                vec![absent.clone()],
            ],
        ] {
            let merged = merge(&["value".into()], &shares).unwrap().remove(0);
            assert_eq!(merged.count, full.count);
            assert_eq!(merged.exact_integer, full.exact_integer);
        }
    }
}
#[test]
fn maximum_count_summaries_fit_and_count_overflow_refuses() {
    let count = u64::MAX;
    for ty in [Type::Integer, Type::UnsignedInteger] {
        let value = if ty == Type::Integer {
            NumBound::I(i64::MIN)
        } else {
            NumBound::U(u64::MAX)
        };
        let one = collect(ty, &[value]);
        let mut many = one.clone();
        many.count = count;
        match many.exact_integer.as_mut().unwrap() {
            ExactInteger::Signed(s) => set_signed(s, i128::from(i64::MIN) * i128::from(count)),
            ExactInteger::Unsigned(s) => set_unsigned(s, u128::from(u64::MAX) * u128::from(count)),
        }
        many.sum = one.sum * count as f64;
        use prost::Message;
        let decoded = ColumnStats::decode(many.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, many);
        validate(&many).unwrap();
        assert_eq!(
            merge(&["value".into()], &[vec![many.clone()]]).unwrap()[0].exact_integer,
            many.exact_integer
        );
        assert_eq!(
            merge(&["value".into()], &[vec![many], vec![one]])
                .unwrap_err()
                .code(),
            tonic::Code::OutOfRange
        );
    }
}
fn malformed_partials(full: &ColumnStats) -> Vec<ColumnStats> {
    let mut bad = vec![];
    let mut s = full.clone();
    s.field = "wrong".into();
    bad.push(s);
    let mut s = full.clone();
    s.value_type = 999;
    bad.push(s);
    let mut s = full.clone();
    s.value_type = 0;
    bad.push(s);
    let mut s = full.clone();
    s.known = false;
    bad.push(s);
    let mut s = full.clone();
    s.exact_integer = None;
    bad.push(s);
    let mut s = full.clone();
    s.count = 1;
    bad.push(s);
    let mut s = full.clone();
    s.count = 0;
    bad.push(s);
    let mut s = full.clone();
    s.min = 1.0;
    bad.push(s);
    let mut s = full.clone();
    s.sum = f64::INFINITY;
    bad.push(s);
    let mut s = full.clone();
    let Some(ExactInteger::Unsigned(e)) = s.exact_integer.as_mut() else {
        panic!()
    };
    e.sum_hi = u64::MAX;
    bad.push(s);
    let mut s = full.clone();
    let Some(ExactInteger::Unsigned(e)) = s.exact_integer.as_mut() else {
        panic!()
    };
    e.min = u64::MAX;
    e.max = 0;
    bad.push(s);
    bad
}

#[test]
fn malformed_partials_and_empty_type_conflicts_refuse() {
    let full = collect(
        Type::UnsignedInteger,
        &[NumBound::U(u64::MAX), NumBound::U(0)],
    );
    let bad = malformed_partials(&full);
    for s in bad {
        assert_eq!(
            merge(&["value".into()], &[vec![s]]).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
    for ty in [Type::Integer, Type::Number] {
        assert_eq!(
            merge(
                &["value".into()],
                &[
                    vec![collect(Type::UnsignedInteger, &[])],
                    vec![collect(ty, &[])]
                ]
            )
            .unwrap_err()
            .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert!(merge(&["value".into()], &[vec![]]).is_err());
    let mut c = Collector::new("value", Type::Number);
    c.observe(NumBound::F(f64::MAX)).unwrap();
    assert_eq!(
        c.observe(NumBound::F(f64::MAX)).unwrap_err().code(),
        tonic::Code::OutOfRange
    );
    let a = collect(Type::Number, &[NumBound::F(f64::MAX)]);
    assert_eq!(
        merge(&["value".into()], &[vec![a.clone()], vec![a]])
            .unwrap_err()
            .code(),
        tonic::Code::OutOfRange
    );
}
