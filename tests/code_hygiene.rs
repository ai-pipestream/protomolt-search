//! Source-code structural and documentation hygiene gate.
//!
//! Enforces structural and documentation invariants over every `.rs` file under
//! `src/` and `crates/` (plus retired-directory hygiene over `docs/`) using a
//! Rust token and comment lexer (`code_hygiene/lexer.rs`):
//!
//! 1. Non-test function length (`signature..=closing_brace`) `<= 80` lines,
//!    and test function length `<= 120` lines.
//! 2. Non-test function parameter count `<= 7` parameters.
//! 3. Zero new unconditional `#[allow(clippy::too_many_arguments)]` or
//!    unconditional `#[allow(dead_code)]` attributes.
//! 4. Zero new `docs/*.md` inline comment breadcrumbs, diff-narration or
//!    war-story comment phrases, or retired research-dump references.
//!
//! Modules above the default ceilings on the stabilization baseline are listed
//! in `code_hygiene/ratchet.rs` keyed by top-level module path (`src/foo.rs`
//! and `src/foo/**` both map to `"src/foo"`), with non-increasing
//! `actual <= limit` checks so modular splits and reductions merge cleanly.

#[path = "code_hygiene/analyzer.rs"]
mod analyzer;
#[path = "code_hygiene/attrs.rs"]
mod attrs;
#[path = "code_hygiene/lexer.rs"]
mod lexer;
#[path = "code_hygiene/ratchet.rs"]
mod ratchet;
#[path = "code_hygiene/scan.rs"]
mod scan;
#[path = "code_hygiene/sig.rs"]
mod sig;
#[path = "code_hygiene/slop.rs"]
mod slop;
#[path = "code_hygiene/synthetic_tests.rs"]
mod synthetic_tests;

use std::path::PathBuf;

use analyzer::analyze_source;
use ratchet::{
    lookup_allowance, DEFAULT_FN_LINES, DEFAULT_MAX_PARAMS, DEFAULT_TEST_FN_LINES, RATCHET,
};
use scan::{
    collect_rs_files, find_any_allow_lines, module_key, preview, read_utf8, relative_path,
    workspace_scan,
};
use slop::collect_docs_retired_violations;

const SELF_MAX_FILE_LINES: usize = 350;
const SELF_MAX_FN_LINES: usize = 75;
const SELF_MAX_FN_PARAMS: usize = 6;

#[test]
fn module_key_collapses_file_and_directory_modules() {
    assert_eq!(module_key("src/analyzer.rs"), "src/analyzer");
    assert_eq!(module_key("src/analyzer/mod.rs"), "src/analyzer");
    assert_eq!(module_key("src/analyzer/pool_tests.rs"), "src/analyzer");
    assert_eq!(module_key("src/bin/console.rs"), "src/bin/console");
    assert_eq!(
        module_key("crates/protomolt-search-embedded/src/mobile.rs"),
        "crates/protomolt-search-embedded/src/mobile"
    );
    assert_eq!(
        module_key("crates/protomolt-search-embedded/src/mobile/ffi.rs"),
        "crates/protomolt-search-embedded/src/mobile"
    );
}

#[test]
fn ratchet_table_is_well_formed() {
    for pair in RATCHET.windows(2) {
        assert!(
            pair[0].module < pair[1].module,
            "RATCHET must be strictly sorted with unique keys: {:?} >= {:?}",
            pair[0].module,
            pair[1].module
        );
    }
    for entry in RATCHET {
        assert!(
            entry.has_any_allowance(),
            "{}: RATCHET entry has only default allowances; remove it",
            entry.module
        );
        assert_eq!(
            entry.over_80_fns == 0,
            entry.max_fn_lines == DEFAULT_FN_LINES,
            "{}: inconsistent fn_lines allowance",
            entry.module
        );
        assert_eq!(
            entry.over_120_test_fns == 0,
            entry.max_test_fn_lines == DEFAULT_TEST_FN_LINES,
            "{}: inconsistent test_fn_lines allowance",
            entry.module
        );
        assert_eq!(
            entry.over_7_params == 0,
            entry.max_params == DEFAULT_MAX_PARAMS,
            "{}: inconsistent params allowance",
            entry.module
        );
    }
}

#[test]
fn non_test_functions_stay_within_line_and_param_ceilings() {
    let mut failures = Vec::new();
    for (module, sum) in &workspace_scan().modules {
        let allow = lookup_allowance(module);
        let over_80 = sum.over_80_fns.len();
        if over_80 > allow.over_80_fns {
            failures.push(format!(
                "{module}: {over_80} non-test fn(s) > {DEFAULT_FN_LINES}L exceeds ceiling {} \
                 [{}]; decompose into focused helpers",
                allow.over_80_fns,
                preview(&sum.over_80_fns)
            ));
        } else if over_80 > 0 && sum.max_fn_lines > allow.max_fn_lines {
            failures.push(format!(
                "{module}: longest non-test fn is {}L, exceeding ceiling {}L [{}]",
                sum.max_fn_lines,
                allow.max_fn_lines,
                preview(&sum.over_80_fns)
            ));
        }
        let over_7 = sum.over_7_params.len();
        if over_7 > allow.over_7_params {
            failures.push(format!(
                "{module}: {over_7} non-test fn(s) > {DEFAULT_MAX_PARAMS} params exceeds \
                 ceiling {} [{}]; pass a context/options struct",
                allow.over_7_params,
                preview(&sum.over_7_params)
            ));
        } else if over_7 > 0 && sum.max_params > allow.max_params {
            failures.push(format!(
                "{module}: widest non-test fn has {} params, exceeding ceiling {} [{}]",
                sum.max_params,
                allow.max_params,
                preview(&sum.over_7_params)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "function size/arity hygiene failures:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn test_functions_stay_within_line_ceiling() {
    let mut failures = Vec::new();
    for (module, sum) in &workspace_scan().modules {
        let allow = lookup_allowance(module);
        let over_120 = sum.over_120_test_fns.len();
        if over_120 > allow.over_120_test_fns {
            failures.push(format!(
                "{module}: {over_120} test fn(s) > {DEFAULT_TEST_FN_LINES}L exceeds ceiling {} \
                 [{}]",
                allow.over_120_test_fns,
                preview(&sum.over_120_test_fns)
            ));
        } else if over_120 > 0 && sum.max_test_fn_lines > allow.max_test_fn_lines {
            failures.push(format!(
                "{module}: longest test fn is {}L, exceeding ceiling {}L [{}]",
                sum.max_test_fn_lines,
                allow.max_test_fn_lines,
                preview(&sum.over_120_test_fns)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "test function length hygiene failures:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn unconditional_allows_do_not_grow() {
    let mut failures = Vec::new();
    for (module, sum) in &workspace_scan().modules {
        let allow = lookup_allowance(module);
        if sum.too_many_args_allows.len() > allow.too_many_args_allows {
            failures.push(format!(
                "{module}: {} #[allow(clippy::too_many_arguments)] exceeds ceiling {} [{}]",
                sum.too_many_args_allows.len(),
                allow.too_many_args_allows,
                preview(&sum.too_many_args_allows)
            ));
        }
        if sum.dead_code_allows.len() > allow.dead_code_allows {
            failures.push(format!(
                "{module}: {} unconditional #[allow(dead_code)] exceeds ceiling {} [{}]",
                sum.dead_code_allows.len(),
                allow.dead_code_allows,
                preview(&sum.dead_code_allows)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "attribute suppression hygiene failures:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn comment_breadcrumbs_and_slop_do_not_grow() {
    let mut failures = Vec::new();
    for (module, sum) in &workspace_scan().modules {
        let allow = lookup_allowance(module);
        if sum.doc_breadcrumbs.len() > allow.doc_breadcrumbs {
            failures.push(format!(
                "{module}: {} inline docs/*.md comment breadcrumb(s) exceeds ceiling {} [{}]",
                sum.doc_breadcrumbs.len(),
                allow.doc_breadcrumbs,
                preview(&sum.doc_breadcrumbs)
            ));
        }
        if sum.slop_comments.len() > allow.slop_comments {
            failures.push(format!(
                "{module}: {} past-state/war-story comment(s) exceeds ceiling {} [{}]",
                sum.slop_comments.len(),
                allow.slop_comments,
                preview(&sum.slop_comments)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "comment hygiene failures:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn retired_research_dump_references_do_not_grow() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut failures = workspace_scan().rust_ai_slop_hits.clone();
    failures.extend(collect_docs_retired_violations(&root));
    assert!(
        failures.is_empty(),
        "retired research-dump reference failures:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn code_hygiene_suite_obeys_strict_paper1_limits() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut own_files = vec![root.join("tests/code_hygiene.rs")];
    collect_rs_files(&root.join("tests/code_hygiene"), &mut own_files);
    own_files.sort();

    for path in own_files {
        let rel = relative_path(&root, &path);
        let src = read_utf8(&path);
        let total_lines = src.lines().count();
        assert!(
            total_lines <= SELF_MAX_FILE_LINES,
            "{rel}: {total_lines} lines exceeds self-ceiling {SELF_MAX_FILE_LINES}"
        );
        let allows = find_any_allow_lines(&src);
        assert!(allows.is_empty(), "{rel}: #[allow] at lines {allows:?}");
        let report = analyze_source("src/self_audit.rs", &src);
        assert!(report.comments.doc_breadcrumbs.is_empty(), "{rel}: crumbs");
        assert!(report.comments.slop_phrases.is_empty(), "{rel}: slop");
        assert!(report.comments.ai_slop_refs.is_empty(), "{rel}: dump ref");
        assert!(
            report.comments.issue_breadcrumbs.is_empty(),
            "{rel}: issue breadcrumbs {:?}",
            report.comments.issue_breadcrumbs
        );
        for func in report.functions {
            assert!(
                func.lines.unwrap_or(0) <= SELF_MAX_FN_LINES,
                "{rel}:{} `{}` is {:?}L > {SELF_MAX_FN_LINES}L",
                func.line,
                func.name,
                func.lines
            );
            assert!(
                func.params <= SELF_MAX_FN_PARAMS,
                "{rel}:{} `{}` has {} params > {SELF_MAX_FN_PARAMS}",
                func.line,
                func.name,
                func.params
            );
        }
    }
}
