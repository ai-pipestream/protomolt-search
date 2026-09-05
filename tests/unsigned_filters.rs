mod common;

use pipestream_search::filter::{cmp_f64_u64, int_range, uint_range, Edge, NumBound};
use std::cmp::Ordering;

// Independent integer-ratio oracle using the IEEE significand/exponent.
fn exact_float_order(x: f64, n: u64) -> Ordering {
    assert!(x.is_finite());
    if x == 0.0 {
        return 0u64.cmp(&n);
    }
    if x.is_sign_negative() {
        return Ordering::Less;
    }
    let bits = x.to_bits();
    let exponent = ((bits >> 52) & 2047) as i32;
    let significand =
        u128::from(bits & ((1u64 << 52) - 1)) + if exponent == 0 { 0 } else { 1u128 << 52 };
    let shift = exponent.max(1) - 1023 - 52;
    if shift >= 0 {
        if shift >= 128 {
            return Ordering::Greater;
        }
        match significand.checked_mul(1u128 << shift) {
            Some(integer) => integer.cmp(&u128::from(n)),
            None => Ordering::Greater,
        }
    } else {
        if n == 0 {
            return Ordering::Greater;
        }
        if -shift >= 128 {
            return Ordering::Less;
        }
        match u128::from(n).checked_mul(1u128 << -shift) {
            Some(integer) => significand.cmp(&integer),
            None => Ordering::Less,
        }
    }
}

#[test]
fn unsigned_bounds_match_exact_integer_ratios_and_domain_edges() {
    let values = [
        0,
        1,
        (1u64 << 53) - 1,
        1u64 << 53,
        (1u64 << 53) + 1,
        i64::MAX as u64,
        1u64 << 63,
        u64::MAX - 1,
        u64::MAX,
    ];
    let mut floats = vec![
        -f64::MAX,
        -1e300,
        -1.0,
        -f64::from_bits(1),
        -0.0,
        0.0,
        f64::from_bits(1),
        0.5,
        1.5,
        1e300,
        f64::MAX,
    ];
    for n in values {
        let x = n as f64;
        floats.extend([x.next_down(), x, x.next_up()]);
    }
    let mut random = 88312845891u64;
    for _ in 0..2000 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let x = f64::from_bits(random);
        if x.is_finite() {
            floats.push(x);
        }
    }
    let mut bounds: Vec<NumBound> = floats.into_iter().map(NumBound::F).collect();
    bounds.extend(values.into_iter().map(NumBound::U));
    bounds.extend([i64::MIN, -1, 0, 1, i64::MAX].into_iter().map(NumBound::I));
    for n in values {
        for bound in &bounds {
            let order = match *bound {
                NumBound::U(v) => n.cmp(&v),
                NumBound::I(v) => i128::from(n).cmp(&i128::from(v)),
                NumBound::F(v) => {
                    let expected = exact_float_order(v, n);
                    assert_eq!(cmp_f64_u64(v, n), expected, "{v:?} versus {n}");
                    expected.reverse()
                }
            };
            for exclusive in [false, true] {
                let edge = Some(Edge {
                    value: *bound,
                    exclusive,
                });
                let (lo, hi) = uint_range(&edge, &None);
                assert_eq!(
                    lo <= n && n <= hi,
                    order == Ordering::Greater || (!exclusive && order == Ordering::Equal),
                    "lower {edge:?}, {n}"
                );
                let (lo, hi) = uint_range(&None, &edge);
                assert_eq!(
                    lo <= n && n <= hi,
                    order == Ordering::Less || (!exclusive && order == Ordering::Equal),
                    "upper {edge:?}, {n}"
                );
            }
        }
    }
    for n in [i64::MIN, -1, 0, 1, i64::MAX] {
        for v in values {
            for exclusive in [false, true] {
                let edge = Some(Edge {
                    value: NumBound::U(v),
                    exclusive,
                });
                let (lo, hi) = int_range(&edge, &None);
                assert_eq!(
                    lo <= n && n <= hi,
                    i128::from(n) >= i128::from(v) + i128::from(exclusive)
                );
                let (lo, hi) = int_range(&None, &edge);
                assert_eq!(
                    lo <= n && n <= hi,
                    i128::from(n) <= i128::from(v) - i128::from(exclusive)
                );
            }
        }
    }
    for x in [1e300, f64::MAX] {
        let edge = Some(Edge {
            value: NumBound::F(x),
            exclusive: true,
        });
        assert_eq!(int_range(&edge, &None), (1, 0));
        assert_eq!(int_range(&None, &edge), (i64::MIN, i64::MAX));
        let edge = Some(Edge {
            value: NumBound::F(-x),
            exclusive: true,
        });
        assert_eq!(int_range(&None, &edge), (1, 0));
        assert_eq!(int_range(&edge, &None), (i64::MIN, i64::MAX));
    }
}

#[tokio::test]
async fn unsigned_filters_agree_across_monolithic_distributed_and_segmented_search() {
    use pb::{node_service_client::NodeServiceClient, search_service_server::SearchService};
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        coordinator::CoordinatorServiceImpl,
        node::{Layout, NodeConfig},
        pb,
    };
    let values = [
        Some(0),
        Some(1),
        Some((1u64 << 53) - 1),
        Some(1u64 << 53),
        Some((1u64 << 53) + 1),
        Some(i64::MAX as u64),
        Some(1u64 << 63),
        Some(u64::MAX - 1),
        Some(u64::MAX),
        None,
    ];
    let signed = [
        Some(i64::MIN),
        Some(-1),
        Some(0),
        Some(1),
        Some((1i64 << 53) + 1),
        Some(i64::MAX),
        Some(i64::MIN),
        Some(i64::MAX),
        Some(0),
        None,
    ];
    let cases: Vec<(&str, Vec<usize>)> = vec![
        ("u == 18446744073709551615u", vec![8]),
        ("u == 0xffffffffffffffffU", vec![8]),
        ("u >= 9223372036854775808u", vec![6, 7, 8]),
        ("u > 18446744073709551615u", vec![]),
        ("u < 0", vec![]),
        ("u >= -1", (0..9).collect()),
        ("!has(u)", vec![9]),
        ("has(empty)", vec![]),
        ("!(u == 0u)", (1..9).collect()),
        (
            "u in [0u, 18446744073709551615u, 9007199254740993u]",
            vec![0, 4, 8],
        ),
        ("u == 18446744073709551616.0", vec![]),
        ("u < 18446744073709551616.0", (0..9).collect()),
        ("u == 9007199254740992.0", vec![3]),
        ("u > 1e300", vec![]),
        ("i > 1e300", vec![]),
        ("i < -1e300", vec![]),
        ("i < 18446744073709551615u", (0..9).collect()),
        ("i == 9223372036854775808u", vec![]),
        ("i <= 0u", vec![0, 1, 2, 6, 8]),
        ("f < 9007199254740993u", vec![0, 1, 2, 3, 4]),
        ("f == 18446744073709551615u", vec![]),
        ("f > 18446744073709551615u", vec![7, 8]),
        ("m[\"x\"] > 18446744073709551615u", vec![7, 8]),
    ];
    for mode in ["heap", "mapped", "segmented"] {
        let segmented = mode == "segmented";
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("unsigned_filters_{mode}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut addresses = Vec::new();
        let mut servers = Vec::new();
        for (part, range) in [0..10, 0..5, 5..10].into_iter().enumerate() {
            let config = NodeConfig {
                index_path: (mode != "heap").then(|| dir.join(format!("shard-{part}.tv"))),
                layout: if segmented {
                    Layout::Segments
                } else {
                    Layout::SingleImage
                },
                seal_tail_docs: 2,
                slot_offset: if part == 2 { 1000 } else { 0 },
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                unsigned_integer_fields: vec!["u".into(), "empty".into()],
                integer_fields: vec!["i".into()],
                numeric_fields: vec!["f".into()],
                map_numeric_fields: vec!["m".into()],
                facet_fields: vec!["key".into()],
                ..Default::default()
            };
            let (mut addr, mut server) = common::start_empty_node(config.clone()).await;
            let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
            let docs: Vec<_> = range
                .clone()
                .map(|row| pb::AddDocumentsRequest {
                    text: "word".into(),
                    analysis: Some(body_spec()),
                    facets: vec![pb::FacetValue {
                        field: "key".into(),
                        value: row.to_string(),
                    }],
                    unsigned_integers: values[row]
                        .map(|value| pb::UnsignedIntegerValue {
                            field: "u".into(),
                            value,
                        })
                        .into_iter()
                        .collect(),
                    integers: signed[row]
                        .map(|value| pb::IntegerValue {
                            field: "i".into(),
                            value,
                        })
                        .into_iter()
                        .collect(),
                    numerics: values[row]
                        .map(|value| pb::NumericValue {
                            field: "f".into(),
                            value: value as f64,
                        })
                        .into_iter()
                        .collect(),
                    map_numerics: values[row]
                        .map(|value| pb::MapNumericEntry {
                            field: "m".into(),
                            key: "x".into(),
                            value: value as f64,
                        })
                        .into_iter()
                        .collect(),
                    ..Default::default()
                })
                .collect();
            for (filter, expected) in &cases {
                let compiled = pipestream_search::cel::compile_filter(filter)
                    .unwrap()
                    .unwrap();
                for (row, doc) in range.clone().zip(&docs) {
                    let columns = pipestream_search::placement::DocColumns::of(doc).unwrap();
                    assert_eq!(
                        pipestream_search::placement::eval_document(&compiled, &columns)
                            == pipestream_search::filter::Tri::True,
                        expected.contains(&row),
                        "placement {filter}, row {row}"
                    );
                }
            }
            client
                .add_documents(tokio_stream::iter(docs))
                .await
                .unwrap();
            client.flush(pb::FlushRequest {}).await.unwrap();
            if mode != "heap" {
                drop(client);
                server.abort();
                let _ = server.await;
                (addr, server) = common::start_opened_node(config).await;
                client = NodeServiceClient::connect(addr.clone()).await.unwrap();
            }
            for (filter, rows) in &cases {
                let result = client
                    .resolve_filter_bitmap(pb::FilterBitmapRequest {
                        filter: pipestream_search::cel::compile_filter(filter).unwrap(),
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                assert!(
                    result.filter_columns_known.iter().all(|known| *known),
                    "unknown column: {filter}, part {part}, mode {mode}"
                );
                let actual: Vec<_> = (0..result.label_count as usize)
                    .filter(|row| result.bits[row / 8] & (1 << (row % 8)) != 0)
                    .map(|local| range.start + local)
                    .collect();
                let expected: Vec<_> = rows
                    .iter()
                    .copied()
                    .filter(|row| range.contains(row))
                    .collect();
                assert_eq!(
                    actual, expected,
                    "bitmap {filter}, part {part}, mode {mode}"
                );
                if segmented && *filter == "has(empty)" {
                    assert!(result.segments_skipped > 0);
                }
            }
            addresses.push(addr);
            servers.push(server);
        }
        let mono = CoordinatorServiceImpl::new(vec![addresses[0].clone()])
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
        let distributed = CoordinatorServiceImpl::new(addresses[1..].to_vec())
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
        for (filter, expected) in &cases {
            let request = pb::Bm25SearchRequest {
                text: "word".into(),
                analysis: Some(body_spec()),
                k: 20,
                filter: filter.to_string(),
                projections: vec![pb::NamedProjection {
                    name: "key".into(),
                    expression: "key".into(),
                }],
                ..Default::default()
            };
            let mut answers = Vec::new();
            for coordinator in [&mono, &distributed] {
                let result = coordinator
                    .bm25_search(tonic::Request::new(request.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let mut actual: Vec<_> = result
                    .hits
                    .into_iter()
                    .map(|hit| {
                        let Some(pb::projected_value::Value::StringValue(key)) =
                            hit.projected[0].value.as_ref()
                        else {
                            panic!("key projection absent")
                        };
                        (key.parse::<usize>().unwrap(), hit.score.to_bits())
                    })
                    .collect();
                actual.sort_unstable();
                assert_eq!(
                    actual.iter().map(|(row, _)| *row).collect::<Vec<_>>(),
                    *expected,
                    "{filter}, mode {mode}"
                );
                answers.push(actual);
            }
            assert_eq!(answers[0], answers[1], "scores differ for {filter}");
        }
        for server in servers {
            server.abort();
            let _ = server.await;
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn unsigned_literals_and_fanout_bounds_keep_their_domain() {
    use pipestream_search::{
        cel::compile_filter,
        pb,
        placement::{impossible_under, ColumnBounds},
    };
    use prost::Message;
    let expr = compile_filter("u == 18446744073709551615u")
        .unwrap()
        .unwrap();
    let expr = pb::FilterExpr::decode(expr.encode_to_vec().as_slice()).unwrap();
    let Some(pb::filter_expr::Expr::Number(number)) = expr.expr else {
        panic!("number predicate")
    };
    assert_eq!(
        number.min.unwrap().value,
        Some(pb::filter_bound::Value::Uint(u64::MAX))
    );
    for (text, reason) in [
        ("u == 18446744073709551616u", "out of u64 range"),
        ("u == 0x10000000000000000u", "out of range"),
        ("u == -1u", "unary minus"),
        ("u == 1.5u", "uint suffix"),
        ("u == 1e2u", "uint suffix"),
    ] {
        let error = compile_filter(text).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains(reason), "{text}: {error}");
    }
    for (left, right, impossible) in [
        ("u == 9007199254740993u", "u == 9007199254740992.0", true),
        ("u == 9007199254740993u", "u > 9007199254740992.0", false),
        (
            "u == 18446744073709551615u",
            "u >= 18446744073709551616.0",
            true,
        ),
        (
            "u == 18446744073709551615u",
            "u < 18446744073709551616.0",
            false,
        ),
        ("u > 9223372036854775807", "u < 9223372036854775808u", false),
        (
            "u >= 9223372036854775808u",
            "u <= 9223372036854775807",
            true,
        ),
        ("u >= 0u", "u < 0", true),
        ("u >= 0u", "u <= 0", false),
    ] {
        // The topology proof bounds real numeric intervals, conservatively:
        // it need not infer that the gap between adjacent integers is empty.
        for (region, query) in [(left, right), (right, left)] {
            let region = compile_filter(region).unwrap().unwrap();
            let query = compile_filter(query).unwrap().unwrap();
            let bounds = ColumnBounds::of_conjunction(&[region]);
            assert_eq!(
                impossible_under(&query, &bounds).is_some(),
                impossible,
                "{left} versus {right}"
            );
        }
    }
}

#[test]
fn unsigned_segment_pruning_requires_known_metadata_and_keeps_boundary_ties() {
    use pipestream_search::{
        filter::{ResolvedFilter, ResolvedLeaf},
        segment_prune::{no_row_can_pass, ColumnNames},
        segments::{SegmentSummary, UintColumnSummary},
    };
    struct Names;
    impl ColumnNames for Names {
        fn integer_name(&self, _: usize) -> Option<&str> {
            None
        }
        fn numeric_name(&self, _: usize) -> Option<&str> {
            None
        }
        fn unsigned_integer_name(&self, index: usize) -> Option<&str> {
            (index == 0).then_some("u")
        }
    }
    let absent = SegmentSummary::default();
    let present = SegmentSummary {
        uint_columns: vec![UintColumnSummary {
            name: "u".into(),
            min: u64::MAX - 1,
            max: u64::MAX,
            present: 2,
        }],
        ..Default::default()
    };
    let empty = SegmentSummary {
        uint_columns: vec![UintColumnSummary {
            name: "u".into(),
            min: u64::MAX,
            max: 0,
            present: 0,
        }],
        ..Default::default()
    };
    for (lo, hi, pruned) in [
        (0, u64::MAX - 2, true),
        (u64::MAX - 1, u64::MAX - 1, false),
        (u64::MAX, u64::MAX, false),
    ] {
        let filter = ResolvedFilter::Leaf(ResolvedLeaf::UintRange { column: 0, lo, hi });
        assert!(!no_row_can_pass(&filter, &absent, &Names));
        assert_eq!(no_row_can_pass(&filter, &present, &Names), pruned);
        assert!(no_row_can_pass(&filter, &empty, &Names));
    }
    let has = ResolvedFilter::Leaf(ResolvedLeaf::Has {
        facet: None,
        numeric: None,
        integer: None,
        unsigned_integer: Some(0),
        geo: None,
    });
    assert!(!no_row_can_pass(&has, &absent, &Names));
    assert!(!no_row_can_pass(&has, &present, &Names));
    assert!(no_row_can_pass(&has, &empty, &Names));
    assert!(!no_row_can_pass(
        &ResolvedFilter::Not(Box::new(has)),
        &empty,
        &Names
    ));
}
