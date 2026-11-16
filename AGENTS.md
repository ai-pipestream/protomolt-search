# Repository working rules

Read `/work/worktrees/protomolt-search/AGENTS.md` first for worktree ownership,
host coordination, and the complete gate matrix. Broader workspace policy is in
`/work/worktrees/turbovec-workspace/AGENTS.md`.

## Isolation and publishing

- Work only in your assigned, clean worktree and task branch; respect file fences.
- Never edit another agent's worktree or push to `main` without explicit approval.
- Push `forgejo` FIRST, then `origin`; verify matching remote SHAs afterward.
  Forgejo (`git.rokkon.com/ai-pipestream/protomolt-search`) is the master build;
  GitHub (`github.com/ai-pipestream/protomolt-search`) is the public copy.
  Nothing automatically synchronizes the two remotes.
- Set author/committer per command: `Kristian Rickert <kristian@apache.org>`.
  Do not change repository/global Git configuration or add attribution trailers.
- Do not merge, close PRs, or rewrite shared branches without explicit approval.

## Builds and tests

- Wrap every Cargo command, including scripts that invoke Cargo, in
  `flock /tmp/psearch-import-evidence/GATE-LOCK`; create its parent directory first.
- Build hosts run every rustc through `scripts/rustc-slot.sh` (a host-wide cap on
  concurrent compiles and links, shared with Forgejo CI through `/tmp/rustc-slots`).
  Do not override `RUSTC_WRAPPER` or `build.rustc-wrapper` to get around it.
- Use an isolated `CARGO_TARGET_DIR=/tmp/target-<agent>-<task>` and
  `CARGO_BUILD_JOBS=2`. Honor the task's exact target path when one is supplied.
- Pass `-- --test-threads=2` to tests. Use workspace/library and targeted integration
  suites rather than an unconstrained all-targets test invocation.
- Record the lock holder in `GATE-LOCK.holder` while running. Do not edit source
  while its gates compile. Delete the task's target directory when finished.
- Verify the committed tip with formatting, vendored-proto and dependency checks,
  both hygiene gates, public/private rustdoc, the four Clippy modes, and tests
  covering the change. See the hub instructions for exact commands.

## Code and documentation ceilings

- `tests/tree_hygiene.rs` has ZERO `RATCHET` entries: never add one.
  Its global ceilings are 1,500 lines for ordinary Rust files and 2,200 for
  `*tests.rs`; the working rule is stricter: new/split Rust files stay <=450 lines.
  Existing files over 500 lines must not grow.
- `tests/code_hygiene.rs` enforces function and documentation hygiene. New/modified
  functions, including tests, stay <=80 lines and <=7 parameters. Existing test
  allowances are not permission to exceed these working limits.
- Add no lint suppressions, compatibility shims, format ladders, environment aliases,
  or copied mode variants. Refactor repeated behavior into cohesive helpers.
- Keep comments concise and explain why. No issue-number or `docs/*.md` breadcrumbs,
  past-state narration, or orphaned rustdoc punctuation in source comments.
- Never weaken assertions, tolerances, or deterministic seeds to make gates pass.
- Keep public server/SDK facades distinct from hidden test internals and crate-private
  implementation modules; preserve existing contracts unless the task changes them.
