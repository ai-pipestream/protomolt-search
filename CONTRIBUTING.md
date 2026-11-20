# Contributing

This guide covers the crate layout, the console UI workflow, and the
checks a change passes before it is pushed. The user manual is
[docs/manual/README.md](docs/manual/README.md).

## Crate layout

| Path | What lives there |
|---|---|
| `src/lib.rs` | The `pipestream-search` library: modules and feature gates. |
| `src/main.rs` | The `pipestream-search` server binary (nodes, coordinator). |
| `src/bin/console.rs` | The `console` binary: the operator UI and its BFF. |
| `src/console.rs`, `src/console/` | The console server: HTTP/1.1, the BFF, the raw explorer. |
| `src/bin/console-ui/` | The console's static files, compiled into the binary. |
| `ui/` | The TypeScript client the console pages load as `bff.js`. |
| `proto/` | Vendored public protos. Never edited by hand. |
| `crates/` | `protomolt-analyzer`, `protomolt-embedder`, `protomolt-search-embedded`. |
| `tests/` | Integration suites; `tests/common/` holds shared cluster helpers. |
| `examples/`, `benchmarks/` | Runnable demos and benchmark harnesses. |
| `scripts/` | Dependency, proto and mobile checks; certificate generators. |

Features: `net` (default) builds the gRPC services and the console; `tls`
(default) adds rustls; `raft` adds the replicated control plane;
`embedded` builds the in-process library for mobile and desktop.

## The console boundary

The console is a backend for its own frontend. The browser talks to one
service, `ai.protomolt.console.bff.v1.ConsoleBffService`, declared in
`ui/proto/ai/protomolt/console/bff/v1/console_bff.proto`. It is
read-only: search, the progressive stream, suggestions, collection and
schema views, document snippets, embedding previews and a sanitized
health summary.

| Route | Protocol |
|---|---|
| `POST /bff/v1/ai.protomolt.console.bff.v1.ConsoleBffService/<Method>` | Connect unary, `application/json` |
| same path | Connect server streaming, `application/connect+json` |
| `POST /bff/v1/sse/<Method>` | Server-sent events with `: keepalive` comments |

Every BFF call is refused with 403 when it carries `?target=`, when the
`Host` is not loopback on a loopback bind, or when `Origin` names another
authority. Upstream failures reach the browser as Connect codes without
cluster addresses or internal messages.

The raw explorer (`/api/*`, `/explorer`, `/dashboard`) transcodes every
public RPC, including admin and diagnostics calls. It is off by default.
`--enable-raw-explorer` turns it on, and the console refuses to start
when that flag is combined with `--allow-remote`.

## Working on the UI

Requirements: Bun 1.3 or later, and `buf` 1.70.0 to regenerate code.

```sh
cd ui
bun install --frozen-lockfile
bun run typecheck
bun test
bun run build        # writes ../src/bin/console-ui/bff.js
```

The bundle is committed so a plain `cargo build` needs no JavaScript
toolchain. Rebuild and commit it with every change under `ui/src/`; the
build is deterministic, so a clean checkout reproduces it byte for byte.

Changing the BFF contract:

1. Edit `ui/proto/ai/protomolt/console/bff/v1/console_bff.proto`.
2. Run `bun run gen`. It regenerates `ui/src/gen/` and the descriptor set
   the server embeds, `src/console/console_bff.binpb`.
3. Add the method to the table in `src/console/bff.rs`. A unit test fails
   when the table and the descriptor disagree.
4. `bun run gen:check` confirms both outputs match the proto.

The page modules (`src/bin/console-ui/*.js`) are plain ES modules. They
import `bff` and `progressive` from `./bff.js`; only the explorer and the
dashboard use the raw `/api` routes.

### Live reload with `--ui-dir`

```sh
cargo run --bin console -- --coordinator=http://127.0.0.1:50050 \
  --nodes=http://127.0.0.1:50051,http://127.0.0.1:50052 \
  --ui-dir src/bin/console-ui
```

With `--ui-dir`, a file present in that directory is read from disk on
each request, so an edit shows on reload. Run `bun run build` (or
`bun build ... --watch`) in `ui/` to refresh `bff.js`. Paths that resolve
outside the directory, symlinks included, are refused with 403.

## Quality gates

Run these before pushing. Cargo commands that build share a target
directory and a lock on shared hosts; see `AGENTS.md`.

```sh
cargo fmt --all -- --check
./scripts/check-vendored-protos.sh
./scripts/check-dependencies.sh
cargo test --locked --test tree_hygiene --test code_hygiene
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
cargo clippy --locked --features raft --lib -- -D warnings
cargo clippy --locked --no-default-features --features embedded --lib -- -D warnings
RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links" cargo doc --locked --no-deps --workspace
RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links" \
  cargo doc --locked --no-deps --lib --document-private-items
cargo test --locked --lib console::
cargo test --locked --test console_bff_security
node tests/console_ui.mjs
(cd ui && bun test && bun run build && bun run gen:check)
git diff --exit-code src/bin/console-ui/bff.js
```

The hygiene suites enforce the code ceilings:

- hand-written `.rs`, `.ts` and `.js` files stay at or under 450 lines;
- Rust functions stay at or under 80 lines and 7 parameters;
- no new `#[allow(...)]`;
- comments say what the code does now, not how it got there.

Tests are never weakened to make a change pass. A failing test either
finds a bug or pins behavior that changed on purpose; in the second case
the test changes with the behavior and the commit says why.

## Commits and branches

Work on a branch, never on `main`. Commits carry the author's own name
and email and no generated trailers. `AGENTS.md` lists the remotes and
the push order.
