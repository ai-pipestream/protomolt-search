//! Baseline module allowances for `code_hygiene` (Wave 2 Gate #0).
//!
//! Allowances are keyed by canonical top-level module path (for example, both
//! `src/analyzer.rs` and `src/analyzer/**` map to `"src/analyzer"`). Every
//! ceiling is non-increasing (`actual <= allowance`): modularizing a monolith
//! into submodules or eliminating legacy violations passes without merge
//! conflicts, while any module at default (`0` violations) or any new module
//! is locked strictly at the Paper 1 defaults.

pub(crate) const DEFAULT_FN_LINES: usize = 80;
pub(crate) const DEFAULT_TEST_FN_LINES: usize = 120;
pub(crate) const DEFAULT_MAX_PARAMS: usize = 7;

/// Legacy file count in the retired `docs/` research dump directory on
/// `86afe129` prior to `#146` removal; non-increasing so deletion drops to `0`.
pub(crate) const MAX_DOCS_RETIRED_DUMP_FILES: usize = 22;

/// Legacy `docs/` markdown files on `86afe129` that still link into the
/// retired research dump prior to `#146`; all other files are locked at `0`.
pub(crate) const DOCS_RETIRED_LINK_RATCHET: &[(&str, usize)] = &[
    ("docs/dense-quality-profile.md", 2),
    ("docs/query-api.md", 1),
];

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

/// Non-increasing per-module legacy allowances measured on `86afe129`.
/// Modules with zero legacy violations are omitted and locked at defaults.
pub(crate) const RATCHET: &[ModuleAllowance] = &[
    base("crates/protomolt-analyzer/src/lib").comments(1, 0),
    base("crates/protomolt-embedder/src/lib")
        .fn_lines(198, 1)
        .comments(0, 1),
    base("crates/protomolt-search-embedded/src/mobile").test_fn_lines(145, 1),
    base("crates/protomolt-search-embedded/tests/dependency_gate").comments(1, 0),
    base("crates/protomolt-search-embedded/tests/document_grants").test_fn_lines(126, 1),
    base("src/analyzer")
        .fn_lines(159, 7)
        .params(8, 1)
        .allows(1, 0)
        .comments(6, 0),
    base("src/analyzer_wire").fn_lines(90, 1),
    base("src/authorization").fn_lines(88, 1),
    base("src/bin/console").comments(1, 0),
    base("src/bm25")
        .fn_lines(87, 1)
        .test_fn_lines(136, 2)
        .params(10, 3)
        .allows(3, 0)
        .comments(17, 1),
    base("src/boolean_bits").comments(3, 0),
    base("src/calendar").comments(1, 0),
    base("src/capacity_tiers")
        .fn_lines(366, 4)
        .allows(1, 0)
        .comments(1, 2),
    base("src/cel")
        .fn_lines(173, 7)
        .test_fn_lines(158, 1)
        .comments(6, 0),
    base("src/chunked")
        .fn_lines(194, 2)
        .params(8, 2)
        .allows(2, 0)
        .comments(5, 0),
    base("src/collections").allows(1, 0).comments(9, 0),
    base("src/column_stats").fn_lines(109, 1),
    base("src/compaction")
        .fn_lines(256, 11)
        .test_fn_lines(188, 1)
        .comments(9, 0),
    base("src/config").fn_lines(1108, 8).comments(40, 0),
    base("src/console").fn_lines(83, 1).comments(3, 0),
    base("src/control_plane")
        .fn_lines(299, 5)
        .test_fn_lines(144, 2)
        .allows(1, 0)
        .comments(8, 0),
    base("src/coordinator")
        .fn_lines(455, 38)
        .test_fn_lines(177, 4)
        .params(11, 5)
        .allows(5, 0)
        .comments(65, 0),
    base("src/demo").fn_lines(112, 1).comments(1, 0),
    base("src/dense_policy").fn_lines(191, 1).comments(5, 0),
    base("src/derived").fn_lines(92, 1).comments(2, 0),
    base("src/diagnostics").fn_lines(101, 1).comments(2, 0),
    base("src/document_catalog")
        .fn_lines(275, 19)
        .test_fn_lines(253, 6)
        .comments(9, 0),
    base("src/document_write_service").comments(1, 0),
    base("src/embedded").fn_lines(90, 1),
    base("src/exact_vectors").fn_lines(99, 4).comments(1, 0),
    base("src/explain").fn_lines(140, 3).comments(1, 0),
    base("src/facet_rows").comments(1, 0),
    base("src/field_permissions")
        .fn_lines(100, 2)
        .comments(1, 0),
    base("src/filter")
        .fn_lines(152, 2)
        .test_fn_lines(142, 1)
        .comments(5, 0),
    base("src/filter_bits")
        .params(8, 1)
        .allows(1, 0)
        .comments(1, 0),
    base("src/fusion").fn_lines(98, 1).comments(1, 0),
    base("src/geo").comments(3, 0),
    base("src/harness").fn_lines(191, 3).comments(1, 0),
    base("src/highlight").fn_lines(138, 1).comments(1, 0),
    base("src/hnsw")
        .fn_lines(409, 17)
        .test_fn_lines(339, 13)
        .params(14, 7)
        .allows(7, 1)
        .comments(9, 0),
    base("src/index_contract").fn_lines(98, 1),
    base("src/index_definition").fn_lines(128, 1),
    base("src/link").comments(1, 0),
    base("src/live_docs").comments(1, 0),
    base("src/ltr").fn_lines(123, 2).comments(1, 0),
    base("src/main").fn_lines(602, 3).comments(10, 0),
    base("src/mapping")
        .fn_lines(184, 8)
        .params(8, 1)
        .allows(1, 0)
        .comments(1, 0),
    base("src/metrics").fn_lines(178, 3).comments(12, 0),
    base("src/multi_res").fn_lines(91, 1),
    base("src/node")
        .fn_lines(867, 53)
        .test_fn_lines(230, 1)
        .params(14, 5)
        .allows(5, 0)
        .comments(104, 1),
    base("src/node_agent").fn_lines(452, 4).comments(3, 0),
    base("src/number_rows").comments(1, 0),
    base("src/pb").comments(1, 0),
    base("src/placement")
        .fn_lines(113, 4)
        .params(8, 1)
        .allows(1, 0)
        .comments(6, 0),
    base("src/placement_plan").comments(1, 0),
    base("src/postings").comments(60, 1),
    base("src/protobuf").fn_lines(125, 1),
    base("src/proximity").fn_lines(109, 2).comments(1, 0),
    base("src/quality").fn_lines(230, 3).comments(2, 0),
    base("src/query")
        .fn_lines(1188, 8)
        .params(12, 2)
        .allows(2, 0)
        .comments(13, 0),
    base("src/query_disclosure")
        .fn_lines(115, 1)
        .test_fn_lines(151, 1),
    base("src/raft")
        .fn_lines(175, 3)
        .test_fn_lines(206, 7)
        .params(10, 1)
        .allows(1, 0)
        .comments(17, 2),
    base("src/reconcile").fn_lines(249, 3).comments(1, 0),
    base("src/relay")
        .fn_lines(509, 18)
        .params(11, 3)
        .allows(3, 0)
        .comments(6, 0),
    base("src/replication").fn_lines(355, 3),
    base("src/reshard")
        .fn_lines(1294, 12)
        .params(16, 16)
        .allows(18, 0)
        .comments(42, 0),
    base("src/schema_report").fn_lines(118, 3),
    base("src/scorefn").comments(10, 0),
    base("src/security").comments(2, 0),
    base("src/segment_prune").comments(1, 0),
    base("src/segmented").fn_lines(125, 2).comments(6, 0),
    base("src/segmented_vectors").comments(1, 0),
    base("src/segments")
        .fn_lines(248, 8)
        .test_fn_lines(260, 1)
        .params(10, 3)
        .allows(3, 0)
        .comments(13, 0),
    base("src/sha256").comments(1, 0),
    base("src/snapshot").fn_lines(106, 3).comments(2, 0),
    base("src/snapshot_repository").comments(1, 0),
    base("src/sortkeys").comments(1, 0),
    base("src/source_archive").fn_lines(84, 1),
    base("src/source_authority")
        .fn_lines(233, 15)
        .test_fn_lines(197, 6)
        .allows(1, 0)
        .comments(12, 0),
    base("src/source_owner").fn_lines(83, 1),
    base("src/stats_cache").comments(1, 0),
    base("src/stream_signal").comments(1, 0),
    base("src/synonyms").fn_lines(107, 1).comments(1, 0),
    base("src/turbo_bridge").fn_lines(267, 4).comments(1, 0),
    base("src/values").fn_lines(498, 4).comments(5, 0),
    base("src/vector").comments(6, 0),
    base("src/vocab").comments(1, 0),
    base("src/wal").fn_lines(120, 1).comments(5, 0),
];
