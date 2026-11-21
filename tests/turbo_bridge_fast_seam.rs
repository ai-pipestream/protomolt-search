//! Regression coverage for cached analysis and typed query binding.

use pipestream_search::turbo_bridge::query_plan::bind_one_pass_query;
use pipestream_search::turbo_bridge::{MultiResVectorOutput, NlpColumnSpec};
use turbo_client::{OnePassQueryBuilder, QueryComparisonOp as Op, StructuredFilterPayload as P};

fn columns() -> NlpColumnSpec {
    NlpColumnSpec {
        money_column: "money".into(),
        quantity_column: "qty".into(),
        date_column: "doc_date".into(),
        geo_spine_column: "geo_spine".into(),
        affirmed_entity_column: "entities".into(),
        ..Default::default()
    }
}

#[test]
fn typed_filters_match_cel_for_all_comparisons_and_escaped_keys() {
    let analyzer = OnePassQueryBuilder::new().build().unwrap();
    let original = analyzer
        .analyze("batteries $5000 25 kg in 2024 Berlin ops@acme.example")
        .unwrap();
    assert!(original
        .filters
        .iter()
        .any(|f| matches!(f.payload, P::Money { .. })));
    assert!(original
        .filters
        .iter()
        .any(|f| matches!(f.payload, P::Quantity { .. })));
    assert!(original
        .filters
        .iter()
        .any(|f| matches!(f.payload, P::Temporal(_))));
    assert!(original
        .filters
        .iter()
        .any(|f| matches!(f.payload, P::Place { .. })));
    for op in [Op::Eq, Op::Gt, Op::Gte, Op::Lt, Op::Lte, Op::Range] {
        let mut plan = original.clone();
        for f in &mut plan.filters {
            f.comparison = op;
            if let P::Quantity { si_base_unit, .. } = &mut f.payload {
                *si_base_unit = "a\"b\\c\n\r\t".into();
            }
        }
        for caller in [None, Some("tier == 1 || tier == 2")] {
            let bound = bind_one_pass_query(&plan, &columns(), caller).unwrap();
            let text = match caller {
                Some(caller) => format!("({caller}) && ({})", bound.synthesized_cel),
                None => bound.synthesized_cel.clone(),
            };
            assert_eq!(
                bound.filter_expr,
                pipestream_search::cel::compile_filter(&text).unwrap()
            );
            assert_eq!(bound.residual_terms.len(), plan.residual_terms.len());
        }
    }
}

#[test]
fn numeric_comparisons_have_the_requested_operators() {
    let analyzer = OnePassQueryBuilder::new().build().unwrap();
    let mut plan = analyzer.analyze("batteries $5000").unwrap();
    let money = plan
        .filters
        .iter()
        .find(|f| matches!(f.payload, P::Money { .. }))
        .unwrap()
        .clone();
    for (op, symbol) in [
        (Op::Eq, "=="),
        (Op::Gt, ">"),
        (Op::Gte, ">="),
        (Op::Lt, "<"),
        (Op::Lte, "<="),
        (Op::Range, "=="),
    ] {
        plan.filters = vec![money.clone()];
        plan.filters[0].comparison = op;
        let expected = format!("money[\"USD\"] {symbol} 5000.0");
        let bound = bind_one_pass_query(&plan, &columns(), None).unwrap();
        assert_eq!(bound.synthesized_cel, expected);
        assert_eq!(
            bound.filter_expr,
            pipestream_search::cel::compile_filter(&expected).unwrap()
        );
    }
}

#[test]
fn temporal_endpoints_preserve_half_open_ranges_and_saturation() {
    let analyzer = OnePassQueryBuilder::new().build().unwrap();
    let mut plan = analyzer.analyze("batteries in 2024").unwrap();
    let template = plan
        .filters
        .iter()
        .find(|f| matches!(f.payload, P::Temporal(_)))
        .unwrap()
        .clone();
    for (start, end) in [
        (Some(1), Some(2)),
        (Some(i64::MIN), Some(i64::MAX)),
        (Some(4), None),
        (None, Some(4)),
        (None, None),
        (Some(2), Some(1)),
    ] {
        for op in [Op::Eq, Op::Gt, Op::Gte, Op::Lt, Op::Lte, Op::Range] {
            let mut filter = template.clone();
            filter.comparison = op;
            let P::Temporal(t) = &mut filter.payload else {
                unreachable!()
            };
            t.start_epoch_seconds = start;
            t.end_epoch_seconds = end;
            plan.filters = vec![filter];
            let bound = bind_one_pass_query(&plan, &columns(), None).unwrap();
            assert_eq!(
                bound.filter_expr,
                pipestream_search::cel::compile_filter(&bound.synthesized_cel).unwrap()
            );
            assert_eq!(bound.estimated_selectivity, 0.20);
        }
    }
}

#[test]
fn structured_filters_validate_identifiers_and_combined_limits() {
    let analyzer = OnePassQueryBuilder::new().build().unwrap();
    let mut plan = analyzer.analyze("batteries $5000").unwrap();
    for invalid in ["x || true", "true", "x.y", "9money"] {
        let mut cols = columns();
        cols.money_column = invalid.into();
        assert_eq!(
            bind_one_pass_query(&plan, &cols, None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    let money = plan
        .filters
        .iter()
        .find(|f| matches!(f.payload, P::Money { .. }))
        .unwrap()
        .clone();
    plan.filters = vec![money; 256];
    assert!(bind_one_pass_query(&plan, &columns(), None).is_ok());
    assert_eq!(
        bind_one_pass_query(&plan, &columns(), Some("tier == 1"))
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn vector_digest_matches_canonical_bytes_including_signed_zero() {
    for raw in [vec![3.0, 4.0], vec![-0.0, 0.0], vec![], vec![f32::NAN, 1.0]] {
        let out = MultiResVectorOutput::from_full_and_sentences(&raw, &[1], None, vec![]);
        let bytes: Vec<_> = out.full.iter().flat_map(|v| v.to_le_bytes()).collect();
        assert_eq!(out.sha256, turbo_sha256::sha256(&bytes).0);
    }
}

#[test]
fn sha256_matches_the_fips_abc_vector_in_one_or_multiple_updates() {
    let expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    let mut hash = turbo_sha256::Sha256::new();
    hash.update(b"a");
    hash.update(b"bc");
    assert_eq!(hash.finish().to_hex(), expected);
    assert_eq!(turbo_sha256::sha256(b"abc").to_hex(), expected);
}

#[cfg(feature = "net")]
#[path = "turbo_bridge_fast_seam/remote.rs"]
mod remote;
