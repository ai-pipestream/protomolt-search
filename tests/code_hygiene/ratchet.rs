//! Stabilization-baseline module allowances for `code_hygiene`.
//!
//! Allowances are keyed by canonical top-level module path (for example, both
//! `src/analyzer.rs` and `src/analyzer/**` map to `"src/analyzer"`). Every
//! ceiling is non-increasing (`actual <= allowance`): modularizing a file into
//! submodules or reducing existing counts passes without merge conflicts, while
//! any module at default (`0` allowance) or any new module is locked strictly
//! at the default ceilings.

pub(crate) const DEFAULT_FN_LINES: usize = 80;
pub(crate) const DEFAULT_TEST_FN_LINES: usize = 120;
pub(crate) const DEFAULT_MAX_PARAMS: usize = 7;

/// Non-increasing file ceiling for the retired `docs/` research dump directory
/// on the stabilization baseline; locked at `0` once removed.
pub(crate) const MAX_DOCS_RETIRED_DUMP_FILES: usize = 0;

/// Files in `docs/` on the stabilization baseline with remaining links into the
/// retired research dump; locked strictly at `0`.
pub(crate) const DOCS_RETIRED_LINK_RATCHET: &[(&str, usize)] = &[];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ModuleAllowance {
    pub(crate) module: &'static str,
    pub(crate) max_fn_lines: usize,
    pub(crate) over_80_fns: usize,
    pub(crate) max_test_fn_lines: usize,
    pub(crate) over_120_test_fns: usize,
    pub(crate) max_params: usize,
    pub(crate) over_7_params: usize,
    pub(crate) too_many_args_allows: usize,
    pub(crate) dead_code_allows: usize,
    pub(crate) doc_breadcrumbs: usize,
    pub(crate) slop_comments: usize,
}

impl ModuleAllowance {
    pub(crate) const fn base(module: &'static str) -> Self {
        Self {
            module,
            max_fn_lines: DEFAULT_FN_LINES,
            over_80_fns: 0,
            max_test_fn_lines: DEFAULT_TEST_FN_LINES,
            over_120_test_fns: 0,
            max_params: DEFAULT_MAX_PARAMS,
            over_7_params: 0,
            too_many_args_allows: 0,
            dead_code_allows: 0,
            doc_breadcrumbs: 0,
            slop_comments: 0,
        }
    }

    pub(crate) const fn fn_lines(mut self, max_lines: usize, over_count: usize) -> Self {
        self.max_fn_lines = max_lines;
        self.over_80_fns = over_count;
        self
    }

    pub(crate) const fn test_fn_lines(mut self, max_lines: usize, over_count: usize) -> Self {
        self.max_test_fn_lines = max_lines;
        self.over_120_test_fns = over_count;
        self
    }

    pub(crate) const fn params(mut self, max_params: usize, over_count: usize) -> Self {
        self.max_params = max_params;
        self.over_7_params = over_count;
        self
    }

    pub(crate) const fn allows(mut self, too_many_args: usize, dead_code: usize) -> Self {
        self.too_many_args_allows = too_many_args;
        self.dead_code_allows = dead_code;
        self
    }

    pub(crate) const fn comments(mut self, breadcrumbs: usize, slop: usize) -> Self {
        self.doc_breadcrumbs = breadcrumbs;
        self.slop_comments = slop;
        self
    }

    pub(crate) fn has_any_allowance(&self) -> bool {
        *self != Self::base(self.module)
    }
}

pub(crate) fn lookup_allowance(module: &str) -> ModuleAllowance {
    RATCHET
        .binary_search_by_key(&module, |entry| entry.module)
        .map_or_else(|_| ModuleAllowance::base(""), |idx| RATCHET[idx])
}

const fn base(module: &'static str) -> ModuleAllowance {
    ModuleAllowance::base(module)
}

/// Non-increasing per-module allowances on the stabilization baseline.
/// Modules already within every default ceiling are omitted.
pub(crate) const RATCHET: &[ModuleAllowance] = &[
    base("crates/protomolt-search-embedded/tests/dependency_gate").comments(1, 0),
    base("crates/protomolt-search-embedded/tests/document_grants").test_fn_lines(126, 1),
    base("src/bin/console").comments(1, 0),
    base("src/boolean_bits").comments(3, 0),
    base("src/calendar").comments(1, 0),
    base("src/capacity_tiers").comments(1, 1),
    base("src/chunked").params(8, 2).allows(2, 0),
    base("src/column_stats").fn_lines(109, 1),
    base("src/compaction")
        .fn_lines(211, 3)
        .test_fn_lines(188, 1),
    base("src/control_plane")
        .fn_lines(299, 4)
        .test_fn_lines(130, 1)
        .comments(8, 0),
    base("src/coordinator")
        .fn_lines(455, 37)
        .test_fn_lines(177, 4)
        .params(11, 5)
        .allows(5, 0),
    base("src/demo").comments(1, 0),
    base("src/document_catalog")
        .fn_lines(275, 6)
        .test_fn_lines(253, 3)
        .comments(3, 0),
    base("src/facet_rows").comments(1, 0),
    base("src/geo").comments(3, 0),
    base("src/hnsw")
        .fn_lines(409, 11)
        .params(14, 7)
        .allows(7, 1)
        .comments(9, 0),
    base("src/index_contract").fn_lines(98, 1),
    base("src/index_definition").fn_lines(128, 1),
    base("src/link").comments(1, 0),
    base("src/live_docs").comments(1, 0),
    base("src/main").fn_lines(602, 3).comments(10, 0),
    base("src/node")
        .fn_lines(854, 35)
        .test_fn_lines(230, 1)
        .params(14, 5)
        .allows(5, 0)
        .comments(4, 0),
    base("src/number_rows").comments(1, 0),
    base("src/pb").comments(1, 0),
    base("src/placement_plan").comments(1, 0),
    base("src/query_disclosure")
        .fn_lines(115, 1)
        .test_fn_lines(151, 1),
    base("src/raft")
        .test_fn_lines(206, 4)
        .params(10, 1)
        .allows(1, 0)
        .comments(2, 0),
    base("src/schema_report").fn_lines(118, 3),
    base("src/segmented").fn_lines(125, 2).comments(6, 0),
    base("src/segmented_vectors").comments(1, 0),
    base("src/segments")
        .fn_lines(248, 7)
        .test_fn_lines(260, 1)
        .params(8, 2)
        .allows(2, 0)
        .comments(12, 0),
    base("src/sha256").comments(1, 0),
    base("src/sortkeys").comments(1, 0),
    base("src/source_authority")
        .fn_lines(233, 15)
        .test_fn_lines(197, 6)
        .allows(1, 0)
        .comments(12, 0),
    base("src/source_owner").fn_lines(83, 1),
    base("src/stats_cache").comments(1, 0),
    base("src/stream_signal").comments(1, 0),
    base("src/synonyms").fn_lines(107, 1).comments(1, 0),
    base("src/turbo_bridge").fn_lines(184, 1),
];
