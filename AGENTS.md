# AGENTS.md

## Remotes and push policy

- **Push Forgejo first, GitHub second.** Forgejo
  (`git.rokkon.com/ai-pipestream/turbovec-search`, remote `forgejo`) is the
  master build; GitHub (remote `origin`) is the public copy. Nothing
  auto-syncs between them — push both, in that order. (This repo was
  GitHub-canonical until 2026-08; the rule is now forgejo-first like every
  other ai-pipestream repo.)
- Workspace-wide policy and the per-repo remote table live in the
  workspace-root `../AGENTS.md` — read it before pushing anywhere.
