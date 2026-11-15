//! Comment and markdown detectors for inline documentation breadcrumbs,
//! diff-narration phrases, war-story markers, issue breadcrumbs, and retired
//! research directories.

use std::fs;
use std::path::{Path, PathBuf};

use crate::ratchet::{DOCS_RETIRED_LINK_RATCHET, MAX_DOCS_RETIRED_DUMP_FILES};

/// Phrases in source comments that indicate diff-narration or incident war
/// stories rather than present-tense contract documentation. Each entry is
/// matched case-insensitively on ASCII word boundaries.
const SLOP_PHRASES: &[&str] = &[
    "before this change",
    "prior to this change",
    "before this commit",
    "prior to this commit",
    "before this pr",
    "prior to this pr",
    "was previously",
    "were previously",
    "previously panicked",
    "previously an",
    "previously returned",
    "previously failed",
    "used to be",
    "used to panic",
    "used to return",
    "used to fail",
    "review finding",
    "repaired in",
    "repeated the mistake in review",
    "build order step",
    "python oracle",
    "jvm shutdown hook",
];

const RETIRED_DASH: &str = concat!("ai-", "slop");
const RETIRED_UNDERSCORE: &str = concat!("ai_", "slop");

#[derive(Clone, Debug, Default)]
pub(crate) struct CommentFindings {
    pub(crate) doc_breadcrumbs: Vec<(usize, String)>,
    pub(crate) slop_phrases: Vec<(usize, &'static str)>,
    pub(crate) ai_slop_refs: Vec<(usize, String)>,
    pub(crate) issue_breadcrumbs: Vec<(usize, String)>,
}

pub(crate) fn scan_comment(comment: &str, start_line: usize, out: &mut CommentFindings) {
    for (idx, raw_line) in comment.lines().enumerate() {
        let line = start_line + idx;
        for crumb in extract_doc_breadcrumbs(raw_line) {
            out.doc_breadcrumbs.push((line, crumb));
        }
        for issue in extract_issue_breadcrumbs(raw_line) {
            out.issue_breadcrumbs.push((line, issue));
        }
        let lower = raw_line.to_ascii_lowercase();
        for &phrase in SLOP_PHRASES {
            if contains_word_bounded(&lower, phrase) {
                out.slop_phrases.push((line, phrase));
            }
        }
        if contains_ai_slop_marker(&lower) {
            out.ai_slop_refs.push((line, raw_line.trim().to_owned()));
        }
    }
}

pub(crate) fn contains_ai_slop_marker(lower_ascii: &str) -> bool {
    lower_ascii.contains(RETIRED_DASH) || lower_ascii.contains(RETIRED_UNDERSCORE)
}

pub(crate) fn count_ai_slop_mentions(text: &str) -> usize {
    let lower = text.to_ascii_lowercase();
    lower.matches(RETIRED_DASH).count() + lower.matches(RETIRED_UNDERSCORE).count()
}

pub(crate) fn collect_docs_retired_violations(root: &Path) -> Vec<String> {
    let docs_dir = root.join("docs");
    if !docs_dir.exists() {
        return Vec::new();
    }
    let mut failures = Vec::new();
    let retired_dir = docs_dir.join(RETIRED_DASH);
    let mut dump_files = Vec::new();
    collect_all_files(&retired_dir, None, &mut dump_files);
    if dump_files.len() > MAX_DOCS_RETIRED_DUMP_FILES {
        failures.push(format!(
            "docs/{RETIRED_DASH}: {} tracked files exceeds non-increasing ceiling of {}",
            dump_files.len(),
            MAX_DOCS_RETIRED_DUMP_FILES
        ));
    }

    let mut doc_files = Vec::new();
    collect_all_files(&docs_dir, Some(&retired_dir), &mut doc_files);
    doc_files.sort();
    for path in doc_files {
        let bytes = fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let Ok(text) = String::from_utf8(bytes) else {
            continue;
        };
        let mentions = count_ai_slop_mentions(&text);
        if mentions == 0 {
            continue;
        }
        let rel = crate::scan::relative_path(root, &path);
        let limit = DOCS_RETIRED_LINK_RATCHET
            .iter()
            .find(|(p, _)| *p == rel)
            .map_or(0, |(_, c)| *c);
        if mentions > limit {
            failures.push(format!(
                "{rel}: {mentions} `{RETIRED_DASH}` reference(s) exceeds ceiling of {limit}"
            ));
        }
    }
    failures
}

fn collect_all_files(dir: &Path, skip_dir: Option<&Path>, out: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
        let path = entry.path();
        if skip_dir.is_some_and(|skip| skip == path) {
            continue;
        }
        let kind = entry
            .file_type()
            .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()));
        if kind.is_dir() {
            collect_all_files(&path, skip_dir, out);
        } else if kind.is_file() {
            out.push(path);
        }
    }
}

fn extract_doc_breadcrumbs(line: &str) -> Vec<String> {
    let bytes = line.as_bytes();
    let prefix = b"docs/";
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + prefix.len() < bytes.len() {
        if &bytes[i..i + prefix.len()] == prefix && is_breadcrumb_boundary_before(bytes, i) {
            let start = i;
            let mut end = i + prefix.len();
            while end < bytes.len() && is_doc_path_byte(bytes[end]) {
                end += 1;
            }
            while end > start + prefix.len() && matches!(bytes[end - 1], b'.' | b'-' | b'/') {
                end -= 1;
            }
            let candidate = &line[start..end];
            if is_tracked_doc_breadcrumb(candidate) {
                out.push(candidate.to_owned());
            }
            i = end;
        } else {
            i += 1;
        }
    }
    out
}

fn is_breadcrumb_boundary_before(bytes: &[u8], pos: usize) -> bool {
    if pos == 0 {
        return true;
    }
    !matches!(
        bytes[pos - 1],
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'/' | b'-' | b':'
    )
}

fn is_doc_path_byte(b: u8) -> bool {
    matches!(
        b,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'/'
    )
}

fn is_tracked_doc_breadcrumb(candidate: &str) -> bool {
    let rest = &candidate["docs/".len()..];
    candidate.ends_with(".md") || rest.starts_with("plans/") || rest.starts_with("benchmarks/")
}

fn contains_word_bounded(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || h.len() < n.len() {
        return false;
    }
    let mut pos = 0usize;
    while let Some(rel) = haystack[pos..].find(needle) {
        let start = pos + rel;
        let end = start + n.len();
        let before_ok = start == 0 || !is_word_byte(h[start - 1]);
        let after_ok = end == h.len() || !is_word_byte(h[end]);
        if before_ok && after_ok {
            return true;
        }
        pos = start + 1;
    }
    false
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn extract_issue_breadcrumbs(line: &str) -> Vec<String> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        if bytes[i] == b'#'
            && bytes[i + 1].is_ascii_digit()
            && (i == 0 || !is_issue_prefix_blocker(bytes[i - 1]))
        {
            let mut end = i + 2;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end == bytes.len() || !is_word_byte(bytes[end]) {
                out.push(line[i..end].to_owned());
            }
            i = end;
        } else {
            i += 1;
        }
    }
    out
}

fn is_issue_prefix_blocker(b: u8) -> bool {
    is_word_byte(b) || b == b'&' || b == b'#'
}
