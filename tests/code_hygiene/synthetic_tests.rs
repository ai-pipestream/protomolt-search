//! Unit tests feeding synthetic Rust snippets to the lexer and analyzer.
//!
//! Verifies that every hygiene detector catches positive violations and that
//! string literals, raw strings, char literals, lifetimes, nested block
//! comments, and `#[cfg(test)]` scopes produce zero false positives.

use crate::analyzer::analyze_source;

#[test]
fn detects_over_80_line_non_test_function() {
    let mut src = String::new();
    src.push_str("/// Doc lines above the signature are not part of the span.\n");
    src.push_str("#[inline]\n");
    src.push_str("pub fn exact_eighty() {\n");
    for _ in 0..78 {
        src.push_str("    let _x = 1;\n");
    }
    src.push_str("}\n\n");
    src.push_str("pub async fn eighty_one() {\n");
    for _ in 0..79 {
        src.push_str("    let _y = 2;\n");
    }
    src.push_str("}\n");

    let report = analyze_source("src/demo_mod.rs", &src);
    assert_eq!(report.functions.len(), 2);
    assert_eq!(report.functions[0].name, "exact_eighty");
    assert_eq!(report.functions[0].lines, Some(80));
    assert!(!report.functions[0].is_test);
    assert_eq!(report.functions[1].name, "eighty_one");
    assert_eq!(report.functions[1].lines, Some(81));
    assert!(!report.functions[1].is_test);
}

#[test]
fn scopes_cfg_test_without_leaking_to_following_items() {
    let body_90 = "    let _v = 0;\n".repeat(89);
    let src = format!(
        "#[cfg(test)]\nmod tests {{\n    fn helper() {{\n{body_90}    }}\n}}\n\
         #[cfg(all(unix, test))]\nfn unix_test_only() {{\n{body_90}}}\n\
         #[tokio::test]\nasync fn async_case() {{\n{body_90}}}\n\
         #[cfg(test)]\nstruct TestOnlyStruct {{\n    field: usize,\n}}\n\
         #[cfg(not(test))]\nfn production_item() {{\n{body_90}}}\n"
    );

    let report = analyze_source("src/prod.rs", &src);
    assert_eq!(report.functions.len(), 4);
    assert!(report.functions[0].is_test, "mod tests helper must be test");
    assert!(
        report.functions[1].is_test,
        "cfg(all(unix, test)) must be test"
    );
    assert!(report.functions[2].is_test, "tokio::test must be test");
    assert_eq!(report.functions[3].name, "production_item");
    assert!(
        !report.functions[3].is_test,
        "cfg(not(test)) after cfg(test) struct must remain non-test"
    );
    assert!(report.functions[3].lines.unwrap_or(0) > 80);
}

#[test]
fn detects_eight_parameters_and_ignores_nested_commas_and_fn_pointers() {
    let src = r#"
type Callback = fn(u8, u8, u8, u8, u8, u8, u8, u8, u8) -> bool;

pub fn seven_ok<F: Fn(u8, u8) -> Result<u8, u8>>(
    &self,
    map: std::collections::HashMap<String, (u64, u64)>,
    bytes: [u8; 4],
    hook: F,
    d: usize,
    e: usize,
    f: usize,
) -> bool {
    let _ = (map, bytes, hook, d, e, f);
    true
}

pub fn eight_bad(a: u8, b: u8, c: u8, d: u8, e: u8, f: u8, g: u8, h: u8) -> u8 {
    a + b + c + d + e + f + g + h
}
"#;

    let report = analyze_source("src/params.rs", src);
    assert_eq!(report.functions.len(), 2);
    assert_eq!(report.functions[0].name, "seven_ok");
    assert_eq!(report.functions[0].params, 7);
    assert_eq!(report.functions[1].name, "eight_bad");
    assert_eq!(report.functions[1].params, 8);
}

#[test]
fn detects_unconditional_allows_and_permits_conditional_dead_code() {
    let src = r#"
#![allow(dead_code)]

#[cfg_attr(not(feature = "tls"), allow(dead_code))]
fn feature_gated_helper() {}

#[allow(clippy::too_many_arguments)]
fn legacy_wide() {}

#[cfg_attr(not(test), allow(clippy::too_many_arguments))]
fn disguised_wide() {}
"#;

    let report = analyze_source("src/allows.rs", src);
    assert_eq!(report.too_many_args_allows.len(), 2);
    assert_eq!(report.dead_code_allows.len(), 1);
}

#[test]
fn detects_doc_breadcrumbs_slop_narration_and_ai_slop_comments() {
    let ai_slop_comment = concat!("// Reference: docs/", "ai-slop", "/old-bench.md\n");
    let src = format!(
        "// Layout pinned by (docs/foo.md) and `docs/plans/track-1.md`.\n\
         /// Before this change, the cursor used to panic (review finding R2).\n\
         /* outer /* nested inner */ tail citing docs/nested.md */\n\
         {ai_slop_comment}\
         pub fn clean_fn() {{}}\n"
    );

    let report = analyze_source("src/comments.rs", &src);
    let crumbs: Vec<&str> = report
        .comments
        .doc_breadcrumbs
        .iter()
        .map(|(_, c)| c.as_str())
        .collect();
    assert!(crumbs.contains(&"docs/foo.md"), "crumbs={crumbs:?}");
    assert!(
        crumbs.contains(&"docs/plans/track-1.md"),
        "crumbs={crumbs:?}"
    );
    assert!(crumbs.contains(&"docs/nested.md"), "crumbs={crumbs:?}");
    assert!(
        report.comments.slop_phrases.len() >= 3,
        "expected before-this-change + used-to-panic + review-finding, got {:?}",
        report.comments.slop_phrases
    );
    assert_eq!(report.comments.ai_slop_refs.len(), 1);
}

#[test]
fn string_raw_string_char_and_lifetime_tokens_do_not_false_positive() {
    let fake_ai_slop = concat!(
        "\"// docs/foo.md before this change docs/",
        "ai-slop",
        "/bar.md #[allow(clippy::too_many_arguments)] fn big(a,b,c,d,e,f,g,h) {{\""
    );
    let src = format!(
        "pub fn with_lifetimes<'a>(input: &'a str, delim: char) -> &'static str {{\n\
             let _cooked = {fake_ai_slop};\n\
             let _raw = r##\"#[allow(dead_code)] /* docs/bar.md review finding */ {{{{\"##;\n\
             let _bytes = b\"docs/baz.md used to panic\";\n\
             let _chars = ('{{', '}}', '\\'', '\"', b'{{');\n\
             let _ = (input, delim, _cooked, _raw, _bytes, _chars);\n\
             \"ok\"\n\
         }}\n"
    );

    let report = analyze_source("src/strings.rs", &src);
    assert_eq!(report.functions.len(), 1);
    assert_eq!(report.functions[0].name, "with_lifetimes");
    assert_eq!(report.functions[0].params, 2);
    assert!(report.functions[0].lines.unwrap_or(0) <= 15);
    assert!(report.too_many_args_allows.is_empty());
    assert!(report.dead_code_allows.is_empty());
    assert!(report.comments.doc_breadcrumbs.is_empty());
    assert!(report.comments.slop_phrases.is_empty());
    assert!(report.comments.ai_slop_refs.is_empty());
}
