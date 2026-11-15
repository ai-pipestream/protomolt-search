use super::compile_value;
use crate::pb;

fn refusal(text: &str) -> String {
    compile_value(text)
        .expect_err("must refuse")
        .message()
        .to_string()
}

/// Literal-only type conflicts refuse at compile, before any shard
/// is involved.
#[test]
fn literal_type_conflicts_refuse_at_compile() {
    assert!(refusal("1 + 2.0").contains("double()"));
    assert!(refusal("3.5 % 2.0").contains("integer-only"));
    assert!(refusal("2.0 - 1").contains("double()"));
}

/// What stays outside the vocabulary refuses by name in value
/// position; the conditional layer itself now compiles.
#[test]
fn out_of_vocabulary_constructs_refuse_by_name() {
    assert!(refusal("has(a)").contains("has()"));
    assert!(refusal("a.matches(\"x\")").contains("matches()"));
    assert!(refusal("\"lit\"").contains("string literal"));
    assert!(refusal("[1, 2]").contains("list"));
    assert!(refusal("int(a)").contains("int()"));
    assert!(refusal("a in [1, 2]").contains("`in`"));
    assert!(refusal("null").contains("null"));
}

/// The conditional layer's literal-pinned typing refuses at
/// compile: strings order nowhere, booleans join no arithmetic,
/// branches and comparisons agree on a type.
#[test]
fn conditional_typing_refuses_at_compile() {
    assert!(refusal("1 < 2.0").contains("double()"));
    assert!(refusal("court < \"m\"").contains("orders strings"));
    assert!(refusal("true < false").contains("do not order"));
    assert!(refusal("true + 1").contains("no arithmetic"));
    assert!(refusal("-(a > b)").contains("`!`"));
    assert!(refusal("!(a + 1)").contains("Kleene"));
    assert!(refusal("(a > b) && 1").contains("Kleene"));
    assert!(refusal("a > b ? 1 : 2.0").contains("double()"));
    assert!(refusal("1 ? a : b").contains("condition is a"));
    assert!(refusal("a > b ? \"x\" : \"y\"").contains("string literal"));
    assert!(refusal("\"x\" == 1").contains("compares a"));
    assert!(refusal("double(true)").contains("double()"));
}

/// Shapes that must compile: precedence, parens re-entering the
/// value grammar, dotted and map reads, double(), unary minus,
/// and the conditional layer.
#[test]
fn the_value_grammar_compiles_its_vocabulary() {
    for text in [
        "a + b * c",
        "(a + b) * c",
        "meta.price / 2.0",
        "prices[\"usd\"] * 1.1",
        "double(year) + 0.5",
        "-a * 2",
        "-(a + b)",
        "7 % 3",
        "a == b",
        "a + 1 >= b * 2",
        "court == \"scotus\"",
        "\"scotus\" != court",
        "prices[\"usd\"] > 10.0 && a < b || !(c >= d)",
        "a > b ? x + 1 : y - 1",
        "a > b ? c > d : e > f",
        "true",
        "a > b == (c > d)",
        "x > 0 ? a : y > 0 ? b : c",
    ] {
        compile_value(text).unwrap_or_else(|e| panic!("{text:?}: {}", e.message()));
    }
}

/// The math vocabulary compiles at both namespaces, and its typing
/// and arity refuse by name.
#[test]
fn math_functions_compile_and_type_check() {
    for text in [
        "math.abs(a)",
        "math.sign(a - b)",
        "math.greatest(a, b, 2)",
        "math.least(1.5, a)",
        "math.ceil(a)",
        "math.sqrt(2.0)",
        "math.isNaN(a / b)",
        "engine.ln(a)",
        "engine.exp(1.0)",
        "engine.pow(a, 2.0)",
        "engine.log10(a) / 2.0",
        "math.greatest(a, b) > 1.0 ? math.abs(a) : 0.0",
    ] {
        compile_value(text).unwrap_or_else(|e| panic!("{text:?}: {}", e.message()));
    }
    assert!(refusal("math.foo(a)").contains("math.foo()"));
    assert!(refusal("engine.foo(a)").contains("engine.foo()"));
    assert!(refusal("math.abs(a, b)").contains("exactly one argument"));
    assert!(refusal("engine.pow(a)").contains("exactly two arguments"));
    assert!(refusal("math.greatest()").contains("at least one argument"));
    assert!(refusal("math.ceil(1)").contains("double()"));
    assert!(refusal("math.abs(true)").contains("takes numbers"));
    assert!(refusal("math.greatest(1, 2.0)").contains("double()"));
    assert!(refusal("math.abs(\"x\")").contains("takes numbers"));
    assert!(refusal("math.isNaN(a) + 1").contains("no arithmetic"));
}

/// The ternary is right-associative and its condition binds the
/// whole `||` layer, exactly CEL's grammar.
#[test]
fn ternary_shape_matches_cel_grammar() {
    use pb::value_expr::Expr as V;
    let expr = compile_value("x > 0 ? a : y > 0 ? b : c").unwrap();
    let Some(V::Ternary(outer)) = expr.expr else {
        panic!("outer ternary expected");
    };
    let else_branch = outer.else_value.expect("else branch");
    assert!(
        matches!(else_branch.expr, Some(V::Ternary(_))),
        "the second `?` nests in the ELSE branch"
    );
    let expr = compile_value("a > 0 || b > 0 ? 1 : 2").unwrap();
    let Some(V::Ternary(outer)) = expr.expr else {
        panic!("ternary expected");
    };
    let cond = outer.cond.expect("condition");
    assert!(
        matches!(cond.expr, Some(V::Logic(_))),
        "the whole `||` is the condition"
    );
}

/// Unary minus folds literals (so i64::MIN is expressible) and
/// keeps a negate node otherwise.
#[test]
fn unary_minus_folds_literals_only() {
    let min = compile_value("-9223372036854775808").unwrap();
    assert_eq!(min.expr, Some(pb::value_expr::Expr::IntLiteral(i64::MIN)));
    let neg = compile_value("-a").unwrap();
    assert!(matches!(neg.expr, Some(pb::value_expr::Expr::Negate(_))));
}
