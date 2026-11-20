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
    base("src/coordinator").params(11, 4).allows(4, 0),
    base("src/hnsw").params(14, 2).allows(2, 0),
    base("src/node").params(14, 4).allows(4, 0),
    base("src/raft").params(10, 1).allows(1, 0),
    base("src/segments").params(8, 1).allows(1, 0),
];
