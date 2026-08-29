//! First-class CEL values (`docs/cel-values.md`): query-time
//! projections and ingest-time materialized columns, held to the stock
//! CEL reference wherever stock CEL yields a value — the same
//! differential-oracle discipline `tests/cel_filters.rs` applies to
//! predicates. The engine's documented deviations (absence instead of
//! integer-arithmetic errors, Kleene absence for missing inputs) are
//! pinned by their own assertions, never hidden inside the oracle.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    projected_value, search_query, selection_query, AddDocumentsRequest, Bm25SearchRequest,
    FacetValue, IntegerValue, LexicalQuery, MaterializeKind, MaterializeSpec, MaterializedColumn,
    NamedProjection, NumericValue, QueryRequest, SearchQuery, SelectionQuery,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

const SHARD_DOCS: usize = 4;
const N_DOCS: usize = 2 * SHARD_DOCS;

/// Per-doc column values, the single source both the engine and the
/// oracle read. Doc `id`: price = id * 1.25 + 0.5 (absent for id 3),
/// year = 1990 + id (absent for id 5), court = one of three strings.
fn price_of(id: usize) -> Option<f64> {
    (id != 3).then_some(id as f64 * 1.25 + 0.5)
}

fn year_of(id: usize) -> Option<i64> {
    (id != 5).then_some(1990 + id as i64)
}

fn court_of(id: usize) -> &'static str {
    ["scotus", "ca9", "ca2"][id % 3]
}

/// Two shards, every doc matching "document", columns per the fns
/// above. Returns the coordinator plus server handles.
async fn start_cluster(
    materialize: Option<MaterializeSpec>,
    extra_numeric: &[&str],
    extra_integer: &[&str],
) -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let mut handles = vec![mock];
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let mut numeric_fields: Vec<String> = vec!["price".into()];
        numeric_fields.extend(extra_numeric.iter().map(|s| s.to_string()));
        let mut integer_fields: Vec<String> = vec!["year".into()];
        integer_fields.extend(extra_integer.iter().map(|s| s.to_string()));
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            numeric_fields,
            integer_fields,
            facet_fields: vec!["court".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            tx.send(AddDocumentsRequest {
                text: format!("document number {id}"),
                numerics: price_of(id)
                    .map(|v| {
                        vec![NumericValue {
                            field: "price".into(),
                            value: v,
                        }]
                    })
                    .unwrap_or_default(),
                integers: year_of(id)
                    .map(|v| {
                        vec![IntegerValue {
                            field: "year".into(),
                            value: v,
                        }]
                    })
                    .unwrap_or_default(),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court_of(id).into(),
                }],
                materialize: materialize.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    (coordinator, handles)
}

fn projection(name: &str, expression: &str) -> NamedProjection {
    NamedProjection {
        name: name.into(),
        expression: expression.into(),
    }
}

async fn run_projections(
    coordinator: &CoordinatorServiceImpl,
    projections: Vec<NamedProjection>,
) -> Result<Vec<(u64, Vec<Option<projected_value::Value>>)>, tonic::Status> {
    let response = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "document".into(),
            k: N_DOCS as u32,
            projections,
            ..Default::default()
        }))
        .await?
        .into_inner();
    Ok(response
        .hits
        .into_iter()
        .map(|h| (h.doc_id, h.projected.into_iter().map(|p| p.value).collect()))
        .collect())
}

/// The oracle: evaluate `expr` with cel-interpreter over this doc's
/// values. `None` when stock CEL is undefined here (missing input, or
/// an evaluation error like division by zero) — those cases are OURS
/// to pin, not the oracle's.
fn oracle(expr: &str, id: usize) -> Option<projected_value::Value> {
    let program = cel_interpreter::Program::compile(expr).expect("oracle compiles");
    let mut ctx = cel_interpreter::Context::default();
    // Stock CEL errors on an unbound variable, which is exactly the
    // "undefined" half the engine's absence rule covers; bind only
    // what the document holds.
    ctx.add_variable("price", price_of(id)?)
        .expect("bind price");
    ctx.add_variable("year", year_of(id)?).expect("bind year");
    ctx.add_variable("court", court_of(id).to_string())
        .expect("bind court");
    match program.execute(&ctx) {
        Ok(cel_interpreter::Value::Int(v)) => Some(projected_value::Value::IntValue(v)),
        Ok(cel_interpreter::Value::Float(v)) => Some(projected_value::Value::DoubleValue(v)),
        Ok(cel_interpreter::Value::String(v)) => {
            Some(projected_value::Value::StringValue(v.as_ref().clone()))
        }
        Ok(cel_interpreter::Value::Bool(v)) => Some(projected_value::Value::BoolValue(v)),
        Ok(other) => panic!("oracle produced a non-scalar: {other:?}"),
        Err(_) => None,
    }
}

/// Bitwise agreement with stock CEL on every (expression, document)
/// pair where stock CEL yields a value.
#[tokio::test]
async fn projections_agree_with_the_stock_cel_oracle() {
    let (coordinator, handles) = start_cluster(None, &[], &[]).await;
    let expressions = [
        "price",
        "year",
        "court",
        "price * 2.0",
        "price + 0.5",
        "price - 10.25",
        "price / 4.0",
        "-price",
        "year * 100 + 7",
        "year - 2000",
        "year / 3",
        "year % 7",
        "-year",
        "double(year)",
        "double(year) / 2.0",
        "price + double(year)",
        "(price + 1.5) * (price - 0.5)",
        "double(year % 10) * price",
        "2.0 * 3.5",
        "7 * 6",
        "price > 2.0",
        "year % 2 == 0",
        "court == \"ca9\"",
        "court != \"scotus\"",
        "price > 2.0 && year % 2 == 0",
        "price > 100.0 || year >= 1994",
        "!(price > 2.0)",
        "price > 2.0 ? price * 2.0 : price / 2.0",
        "year % 2 == 0 ? year : -year",
        "court == \"ca9\" ? 1 : 0",
        "price > 0.5 == (year > 1990)",
        "(price - price) / 0.0 != (price - price) / 0.0",
        "true ? price : price + 1.0",
        "price < 1.0 ? 0.0 : price < 5.0 ? 1.0 : 2.0",
    ];
    let projections: Vec<NamedProjection> = expressions
        .iter()
        .enumerate()
        .map(|(i, e)| projection(&format!("p{i}"), e))
        .collect();
    let rows = run_projections(&coordinator, projections).await.unwrap();
    assert_eq!(rows.len(), N_DOCS, "every doc matches \"document\"");
    let mut oracle_hits = 0usize;
    for (doc_id, values) in rows {
        let id = doc_id as usize;
        for (expr, engine) in expressions.iter().zip(&values) {
            match oracle(expr, id) {
                None => {}
                Some(expected) => {
                    oracle_hits += 1;
                    let engine = engine.as_ref().unwrap_or_else(|| {
                        panic!("doc {id} {expr:?}: engine absent, oracle {expected:?}")
                    });
                    match (engine, &expected) {
                        (
                            projected_value::Value::DoubleValue(a),
                            projected_value::Value::DoubleValue(b),
                        ) => assert_eq!(a.to_bits(), b.to_bits(), "doc {id} {expr:?}: {a} != {b}"),
                        (a, b) => assert_eq!(a, b, "doc {id} {expr:?}"),
                    }
                }
            }
        }
    }
    assert!(
        oracle_hits > 100,
        "the oracle must actually cover the matrix, covered {oracle_hits}"
    );
    for h in handles {
        h.abort();
    }
}

/// The engine's documented deviations from stock CEL, pinned: a
/// missing input is ABSENT (stock CEL: unbound-variable error), and
/// integer division by zero and overflow are ABSENT (stock CEL:
/// evaluation errors). Absence is the unset oneof, never a fabricated
/// zero.
#[tokio::test]
async fn absence_and_integer_edges_are_absent_not_errors() {
    let (coordinator, handles) = start_cluster(None, &[], &[]).await;
    let rows = run_projections(
        &coordinator,
        vec![
            projection("price2", "price * 2.0"),
            projection("age", "2026 - year"),
            projection("boom", "year / (year - year)"),
            projection("over", "year * year * year * year * year * year * year"),
        ],
    )
    .await
    .unwrap();
    for (doc_id, values) in rows {
        let id = doc_id as usize;
        assert_eq!(
            values[0].is_some(),
            price_of(id).is_some(),
            "doc {id}: price2 presence must track the input"
        );
        assert_eq!(
            values[1].is_some(),
            year_of(id).is_some(),
            "doc {id}: age presence must track the input"
        );
        assert!(
            values[2].is_none(),
            "doc {id}: integer division by zero is absent, not an error"
        );
        assert!(
            values[3].is_none(),
            "doc {id}: i64 overflow is absent, not an error"
        );
    }
    for h in handles {
        h.abort();
    }
}

/// The conditional layer's deviations, pinned: Kleene logic absorbs
/// an absent operand when the present one determines the answer
/// (exactly stock CEL's commutative error-absorbing `&&`/`||`, with
/// absence in the error role), an absent condition makes the ternary
/// absent, `!` of absent stays absent, and a string literal the
/// dictionary lacks compares FALSE against a present value — false,
/// never absent.
#[tokio::test]
async fn kleene_absence_and_dictionary_misses_are_pinned() {
    let (coordinator, handles) = start_cluster(None, &[], &[]).await;
    let rows = run_projections(
        &coordinator,
        vec![
            projection("absorb_or", "price > 0.0 || year > 0"),
            projection("absorb_and", "price > 0.0 && year < 0"),
            projection("open_and", "price < 0.0 && year > 0"),
            projection("cond", "price > 2.0 ? 1 : 0"),
            projection("neg", "!(price > 2.0)"),
            projection("miss", "court == \"nonexistent\""),
        ],
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), N_DOCS);
    for (doc_id, values) in rows {
        let id = doc_id as usize;
        let t = Some(projected_value::Value::BoolValue(true));
        let f = Some(projected_value::Value::BoolValue(false));
        assert_eq!(values[0], t, "doc {id}: a true leg absorbs an absent one");
        if year_of(id).is_none() {
            assert!(
                values[1].is_none(),
                "doc {id}: true && absent stays absent; truth determines nothing for `&&`"
            );
        } else {
            assert_eq!(values[1], f, "doc {id}: a false leg absorbs an absent one");
        }
        if price_of(id).is_none() {
            assert!(
                values[2].is_none(),
                "doc {id}: absent && true is absent, not false"
            );
            assert!(
                values[3].is_none(),
                "doc {id}: absent condition, absent result"
            );
            assert!(values[4].is_none(), "doc {id}: `!` of absent stays absent");
        } else {
            assert!(values[2].is_some(), "doc {id}: both legs present");
            assert!(values[3].is_some(), "doc {id}: present condition");
            assert!(values[4].is_some(), "doc {id}: present operand");
        }
        assert_eq!(
            values[5], f,
            "doc {id}: a dictionary miss compares false against a present value"
        );
    }
    for h in handles {
        h.abort();
    }
}

/// The math vocabulary, pinned by the engine (the oracle crate does
/// not carry the CEL math extension): `math.*` follows the official
/// extension's semantics, `engine.*` is this engine's own, and every
/// result is an IEEE value computed here in the test from the same
/// inputs. Absence propagates through every argument, and
/// math.abs(i64::MIN) is absent where stock CEL errors, the checked
/// arithmetic's own deviation.
#[tokio::test]
async fn math_functions_match_their_pinned_semantics() {
    let spec = MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "lnp".into(),
            expression: "engine.ln(price)".into(),
            kind: MaterializeKind::F64 as i32,
        }],
    };
    let (coordinator, handles) = start_cluster(Some(spec), &["lnp"], &[]).await;
    let rows = run_projections(
        &coordinator,
        vec![
            projection("abs", "math.abs(0.5 - price)"),
            projection("sign", "math.sign(year - 1993)"),
            projection(
                "greatest",
                "math.greatest(price, 2.0, double(year) / 1000.0)",
            ),
            projection("least", "math.least(year, 1993)"),
            projection("round", "math.round(price)"),
            projection("sqrt", "math.sqrt(price)"),
            projection("nan", "math.isNaN((price - price) / 0.0)"),
            projection("inf", "math.isInf(1.0 / (price - price))"),
            projection("pow", "engine.pow(price, 2.0)"),
            projection("log10", "engine.log10(price * price) / 2.0"),
            projection("minabs", "math.abs(-9223372036854775808)"),
            projection("lnp", "lnp"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), N_DOCS);
    for (doc_id, values) in rows {
        let id = doc_id as usize;
        let d = projected_value::Value::DoubleValue;
        let b = projected_value::Value::BoolValue;
        match (price_of(id), year_of(id)) {
            (Some(p), year) => {
                assert_eq!(values[0], Some(d((0.5 - p).abs())), "doc {id}: abs");
                assert_eq!(values[4], Some(d(p.round())), "doc {id}: round");
                assert_eq!(values[5], Some(d(p.sqrt())), "doc {id}: sqrt");
                assert_eq!(values[6], Some(b(true)), "doc {id}: 0/0 is NaN");
                assert_eq!(values[7], Some(b(true)), "doc {id}: 1/0 is inf");
                assert_eq!(values[8], Some(d(p.powf(2.0))), "doc {id}: pow");
                assert_eq!(values[9], Some(d((p * p).log10() / 2.0)), "doc {id}: log10");
                assert_eq!(values[11], Some(d(p.ln())), "doc {id}: materialized ln");
                match year {
                    Some(y) => assert_eq!(
                        values[2],
                        Some(d(p.max(2.0).max(y as f64 / 1000.0))),
                        "doc {id}: greatest"
                    ),
                    None => assert!(values[2].is_none(), "doc {id}: absent leg, absent call"),
                }
            }
            (None, _) => {
                for (i, v) in values.iter().enumerate() {
                    if i == 1 || i == 3 || i == 10 {
                        continue;
                    }
                    assert!(v.is_none(), "doc {id}: projection {i} reads price, absent");
                }
            }
        }
        match year_of(id) {
            Some(y) => {
                assert_eq!(
                    values[1],
                    Some(projected_value::Value::IntValue((y - 1993).signum())),
                    "doc {id}: sign preserves int"
                );
                assert_eq!(
                    values[3],
                    Some(projected_value::Value::IntValue(y.min(1993))),
                    "doc {id}: least preserves int"
                );
            }
            None => {
                assert!(values[1].is_none(), "doc {id}: no year, no sign");
                assert!(values[3].is_none(), "doc {id}: no year, no least");
            }
        }
        assert!(
            values[10].is_none(),
            "doc {id}: math.abs(i64::MIN) is absent, not an error"
        );
    }

    // Resolution-time refusals name the function.
    for (expr, needle) in [
        ("math.ceil(year)", "takes a double"),
        ("math.greatest(price, year)", "mixes int and double"),
        ("math.abs(court)", "takes numbers"),
    ] {
        let status = run_projections(&coordinator, vec![projection("p", expr)])
            .await
            .expect_err(&format!("{expr:?} must refuse"));
        assert!(
            status.message().contains(needle),
            "{expr:?}: wanted {needle:?} in {:?}",
            status.message()
        );
    }
    for h in handles {
        h.abort();
    }
}

/// Refusals, by name: a typo'd column no shard knows, mixed-type
/// arithmetic, predicate constructs, unknown functions, `%` on
/// doubles, duplicate and empty names.
#[tokio::test]
async fn projection_refusals_name_the_problem() {
    let (coordinator, handles) = start_cluster(None, &[], &[]).await;
    let cases: [(&str, &str); 10] = [
        ("pricee * 2.0", "no shard has column pricee"),
        ("price + year", "double()"),
        ("price > year", "mixes an int and a double"),
        ("size(court)", "size()"),
        ("price % 2.0", "integer-only"),
        ("court * 2", "arithmetic"),
        ("has(price)", "has()"),
        ("price ? 1 : 2", "condition is a"),
        ("court < \"m\"", "orders strings"),
        ("court == price", "string column compares only"),
    ];
    for (expr, needle) in cases {
        let status = run_projections(&coordinator, vec![projection("p", expr)])
            .await
            .expect_err(&format!("{expr:?} must refuse"));
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{expr:?}");
        assert!(
            status.message().contains(needle),
            "{expr:?}: wanted {needle:?} in {:?}",
            status.message()
        );
    }
    // Name hygiene.
    let status = run_projections(
        &coordinator,
        vec![projection("p", "price"), projection("p", "year")],
    )
    .await
    .expect_err("duplicate names must refuse");
    assert!(status.message().contains("duplicate projection name"));
    let status = run_projections(&coordinator, vec![projection("", "price")])
        .await
        .expect_err("an empty name must refuse");
    assert!(status.message().contains("non-empty name"));
    for h in handles {
        h.abort();
    }
}

/// Materialized columns are ordinary columns: they filter, they
/// project, and absence propagates from absent inputs.
#[tokio::test]
async fn materialized_columns_are_ordinary_columns() {
    let spec = MaterializeSpec {
        columns: vec![
            MaterializedColumn {
                name: "price2".into(),
                expression: "price * 2.0".into(),
                kind: MaterializeKind::F64 as i32,
            },
            MaterializedColumn {
                name: "decade".into(),
                expression: "year / 10 * 10".into(),
                kind: MaterializeKind::I64 as i32,
            },
        ],
    };
    let (coordinator, handles) = start_cluster(Some(spec), &["price2"], &["decade"]).await;

    // Project the derived columns back out and check the arithmetic
    // AND the absence propagation (doc 3 has no price, doc 5 no year).
    let rows = run_projections(
        &coordinator,
        vec![projection("p2", "price2"), projection("d", "decade")],
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), N_DOCS);
    for (doc_id, values) in rows {
        let id = doc_id as usize;
        match price_of(id) {
            Some(p) => assert_eq!(
                values[0],
                Some(projected_value::Value::DoubleValue(p * 2.0)),
                "doc {id}: price2 must be the ingest-time product"
            ),
            None => assert!(values[0].is_none(), "doc {id}: no price, no price2"),
        }
        match year_of(id) {
            Some(y) => assert_eq!(
                values[1],
                Some(projected_value::Value::IntValue(y / 10 * 10)),
                "doc {id}: decade must be the ingest-time quotient"
            ),
            None => assert!(values[1].is_none(), "doc {id}: no year, no decade"),
        }
    }

    // And they filter like any column, through the ordinary CEL
    // filter route: decade == 1990 selects exactly ids 0..=9 with
    // year 199x — here that is every doc with a year.
    let response = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "document".into(),
            k: N_DOCS as u32,
            filter: "decade == 1990 && price2 > 1.0".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let expect: Vec<u64> = (0..N_DOCS)
        .filter(|&id| {
            year_of(id).is_some_and(|y| y / 10 * 10 == 1990)
                && price_of(id).is_some_and(|p| p * 2.0 > 1.0)
        })
        .map(|id| id as u64)
        .collect();
    let mut got: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    got.sort_unstable();
    assert_eq!(got, expect, "materialized columns must filter exactly");
    for h in handles {
        h.abort();
    }
}

/// A kind mismatch refuses per document, loudly, naming the fix; it is
/// never stored coerced.
#[tokio::test]
async fn materialize_kind_mismatch_refuses_loudly() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        integer_fields: vec!["year".into()],
        numeric_fields: vec!["wrong".into()],
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddDocumentsRequest {
        text: "a document".into(),
        integers: vec![IntegerValue {
            field: "year".into(),
            value: 1999,
        }],
        materialize: Some(MaterializeSpec {
            columns: vec![MaterializedColumn {
                name: "wrong".into(),
                expression: "year + 1".into(),
                kind: MaterializeKind::F64 as i32,
            }],
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    let status = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .expect_err("an int expression into an F64 column must refuse");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("double(...)"),
        "the refusal must name the fix: {}",
        status.message()
    );
    node.abort();
    mock.abort();
}

/// The ternary buckets at ingest: a materialized column computed
/// through the conditional layer stores, filters, and projects like
/// any other, and a BOOL-valued expression refuses naming the ternary
/// as the fix.
#[tokio::test]
async fn ternary_materialization_buckets_and_bool_refuses() {
    let spec = MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "tier".into(),
            expression: "year >= 1994 ? 1 : 0".into(),
            kind: MaterializeKind::I64 as i32,
        }],
    };
    let (coordinator, handles) = start_cluster(Some(spec), &[], &["tier"]).await;
    let rows = run_projections(&coordinator, vec![projection("t", "tier")])
        .await
        .unwrap();
    for (doc_id, values) in rows {
        let id = doc_id as usize;
        match year_of(id) {
            Some(y) => assert_eq!(
                values[0],
                Some(projected_value::Value::IntValue(i64::from(y >= 1994))),
                "doc {id}: tier must be the ingest-time bucket"
            ),
            None => assert!(values[0].is_none(), "doc {id}: no year, no tier"),
        }
    }
    let response = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "document".into(),
            k: N_DOCS as u32,
            filter: "tier == 1".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let expect: Vec<u64> = (0..N_DOCS)
        .filter(|&id| year_of(id).is_some_and(|y| y >= 1994))
        .map(|id| id as u64)
        .collect();
    let mut got: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    got.sort_unstable();
    assert_eq!(got, expect, "the bucket must filter exactly");
    for h in handles {
        h.abort();
    }

    // A bool never stores: the refusal names the ternary as the fix.
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        integer_fields: vec!["year".into(), "flag".into()],
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddDocumentsRequest {
        text: "a document".into(),
        integers: vec![IntegerValue {
            field: "year".into(),
            value: 1999,
        }],
        materialize: Some(MaterializeSpec {
            columns: vec![MaterializedColumn {
                name: "flag".into(),
                expression: "year > 0".into(),
                kind: MaterializeKind::I64 as i32,
            }],
        }),
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    let status = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .expect_err("a bool expression into a stored column must refuse");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("ternary"),
        "the refusal must name the fix: {}",
        status.message()
    );
    node.abort();
    mock.abort();
}

/// The Query adapter carries projections on the lexical shape,
/// bit-identically to the Bm25Search route, and refuses them by name
/// on shapes whose route does not serve them yet.
#[tokio::test]
async fn query_projections_agree_across_shapes() {
    let (coordinator, handles) = start_cluster(None, &[], &[]).await;
    let leaf = SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lex".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "document".into(),
                ..Default::default()
            })),
        })),
    };
    let response = coordinator
        .query(Request::new(QueryRequest {
            k: N_DOCS as u32,
            selection: Some(leaf),
            projections: vec![projection("p2", "price * 2.0"), projection("c", "court")],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let direct = run_projections(
        &coordinator,
        vec![projection("p2", "price * 2.0"), projection("c", "court")],
    )
    .await
    .unwrap();
    let via_query: Vec<(u64, Vec<Option<projected_value::Value>>)> = response
        .hits
        .into_iter()
        .map(|h| (h.doc_id, h.projected.into_iter().map(|p| p.value).collect()))
        .collect();
    assert_eq!(via_query, direct, "the adapter must not fork the values");

    // A browse selection serves projections through the post-selection
    // value fetch, and the values agree with the lexical route's.
    let browse = SelectionQuery {
        node: Some(selection_query::Node::Filter(
            pipestream_search::pb::FilterQuery {
                id: "f".into(),
                predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                    "year >= 1990".into(),
                )),
            },
        )),
    };
    let browsed = coordinator
        .query(Request::new(QueryRequest {
            k: N_DOCS as u32,
            selection: Some(browse),
            projections: vec![projection("p2", "price * 2.0"), projection("c", "court")],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    for h in browsed.hits {
        let values: Vec<Option<projected_value::Value>> =
            h.projected.into_iter().map(|p| p.value).collect();
        let reference = direct
            .iter()
            .find(|(id, _)| *id == h.doc_id)
            .expect("browsed doc missing from the lexical reference");
        assert_eq!(values, reference.1, "doc {}", h.doc_id);
    }
    for h in handles {
        h.abort();
    }
}
