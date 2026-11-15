//! Single-pass workspace walker and per-module aggregator for `code_hygiene`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::analyzer::{analyze_source, FileReport, FnRecord};
use crate::lexer::{lex, Delim, TokenKind};
use crate::ratchet::{DEFAULT_FN_LINES, DEFAULT_MAX_PARAMS, DEFAULT_TEST_FN_LINES};
use crate::slop::count_ai_slop_mentions;

const ROOTS: &[&str] = &["src", "crates"];

#[derive(Default)]
pub(crate) struct ModuleSummary {
    pub(crate) over_80_fns: Vec<String>,
    pub(crate) max_fn_lines: usize,
    pub(crate) over_120_test_fns: Vec<String>,
    pub(crate) max_test_fn_lines: usize,
    pub(crate) over_7_params: Vec<String>,
    pub(crate) max_params: usize,
    pub(crate) too_many_args_allows: Vec<String>,
    pub(crate) dead_code_allows: Vec<String>,
    pub(crate) doc_breadcrumbs: Vec<String>,
    pub(crate) slop_comments: Vec<String>,
}

pub(crate) struct WorkspaceScan {
    pub(crate) modules: BTreeMap<String, ModuleSummary>,
    pub(crate) rust_ai_slop_hits: Vec<String>,
}

pub(crate) fn workspace_scan() -> &'static WorkspaceScan {
    static SCAN: OnceLock<WorkspaceScan> = OnceLock::new();
    SCAN.get_or_init(run_workspace_scan)
}

fn run_workspace_scan() -> WorkspaceScan {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ROOTS {
        collect_rs_files(&root.join(dir), &mut files);
    }
    files.sort();
    assert!(!files.is_empty(), "no .rs files found under {ROOTS:?}");

    let mut modules: BTreeMap<String, ModuleSummary> = BTreeMap::new();
    let mut rust_ai_slop_hits = Vec::new();
    for path in &files {
        let rel = relative_path(&root, path);
        let src = read_utf8(path);
        let report = analyze_source(&rel, &src);
        record_ai_slop_in_rust(&rel, &src, &report, &mut rust_ai_slop_hits);
        let key = module_key(&rel);
        fold_file_into_module(&rel, report, modules.entry(key).or_default());
    }
    WorkspaceScan {
        modules,
        rust_ai_slop_hits,
    }
}

fn fold_file_into_module(rel: &str, report: FileReport, sum: &mut ModuleSummary) {
    for func in report.functions {
        fold_fn_record(rel, &func, sum);
    }
    for line in report.too_many_args_allows {
        sum.too_many_args_allows.push(format!("{rel}:{line}"));
    }
    for line in report.dead_code_allows {
        sum.dead_code_allows.push(format!("{rel}:{line}"));
    }
    for (line, crumb) in report.comments.doc_breadcrumbs {
        sum.doc_breadcrumbs.push(format!("{rel}:{line} ({crumb})"));
    }
    for (line, phrase) in report.comments.slop_phrases {
        sum.slop_comments.push(format!("{rel}:{line} ({phrase:?})"));
    }
}

fn fold_fn_record(rel: &str, func: &FnRecord, sum: &mut ModuleSummary) {
    if func.is_test {
        if let Some(lines) = func.lines {
            sum.max_test_fn_lines = sum.max_test_fn_lines.max(lines);
            if lines > DEFAULT_TEST_FN_LINES {
                sum.over_120_test_fns
                    .push(format!("{rel}:{} `{}` ({lines}L)", func.line, func.name));
            }
        }
        return;
    }
    if let Some(lines) = func.lines {
        sum.max_fn_lines = sum.max_fn_lines.max(lines);
        if lines > DEFAULT_FN_LINES {
            sum.over_80_fns
                .push(format!("{rel}:{} `{}` ({lines}L)", func.line, func.name));
        }
    }
    sum.max_params = sum.max_params.max(func.params);
    if func.params > DEFAULT_MAX_PARAMS {
        sum.over_7_params.push(format!(
            "{rel}:{} `{}` ({} params)",
            func.line, func.name, func.params
        ));
    }
}

fn record_ai_slop_in_rust(rel: &str, src: &str, report: &FileReport, out: &mut Vec<String>) {
    for (line, text) in &report.comments.ai_slop_refs {
        out.push(format!("{rel}:{line}: {text}"));
    }
    if report.comments.ai_slop_refs.is_empty() && count_ai_slop_mentions(src) > 0 {
        out.push(format!(
            "{rel}: non-comment reference to retired research dump"
        ));
    }
}

pub(crate) fn module_key(rel_path: &str) -> String {
    let parts: Vec<&str> = rel_path.split('/').collect();
    match parts.as_slice() {
        ["src", "bin", name, ..] => format!("src/bin/{}", strip_rs(name)),
        ["src", name, ..] => format!("src/{}", strip_rs(name)),
        ["crates", pkg, section, name, ..] => {
            format!("crates/{pkg}/{section}/{}", strip_rs(name))
        }
        _ => strip_rs(rel_path).to_owned(),
    }
}

fn strip_rs(segment: &str) -> &str {
    segment.strip_suffix(".rs").unwrap_or(segment)
}

pub(crate) fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
        let path = entry.path();
        let kind = entry
            .file_type()
            .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()));
        if kind.is_dir() {
            if entry.file_name() == "target" {
                continue;
            }
            collect_rs_files(&path, out);
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

pub(crate) fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or_else(|_| panic!("{} is outside {}", path.display(), root.display()))
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn read_utf8(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    String::from_utf8(bytes).unwrap_or_else(|e| panic!("{} is not UTF-8: {e}", path.display()))
}

pub(crate) fn preview(items: &[String]) -> String {
    let shown: Vec<&str> = items.iter().take(5).map(String::as_str).collect();
    if items.len() > 5 {
        format!("{} (+{} more)", shown.join(", "), items.len() - 5)
    } else {
        shown.join(", ")
    }
}

pub(crate) fn find_any_allow_lines(src: &str) -> Vec<usize> {
    let tokens: Vec<_> = lex(src)
        .into_iter()
        .filter(|t| !matches!(t.kind, TokenKind::Comment(_)))
        .collect();
    let mut lines = Vec::new();
    for i in 0..tokens.len() {
        if tokens[i].kind != TokenKind::Punct(b'#') {
            continue;
        }
        let allow_pos =
            if tokens.get(i + 1).map(|t| t.kind) == Some(TokenKind::Open(Delim::Bracket)) {
                Some(i + 2)
            } else if tokens.get(i + 1).map(|t| t.kind) == Some(TokenKind::Punct(b'!'))
                && tokens.get(i + 2).map(|t| t.kind) == Some(TokenKind::Open(Delim::Bracket))
            {
                Some(i + 3)
            } else {
                None
            };
        if allow_pos
            .is_some_and(|p| tokens.get(p).map(|t| t.kind) == Some(TokenKind::Ident("allow")))
        {
            lines.push(tokens[i].line);
        }
    }
    lines
}
