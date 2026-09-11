# Raft specification

Specification version: 1. Fronts the Raft contract documents and links
them: [admission under Raft](raft-admission.md) (the lease argument,
the fault model and invariants I1–I5), [Raft hosting of the source
authority](raft-hosting.md) (envelopes, store, state machine,
snapshots, host, transport, membership, operator configuration,
security, hosted owner writes), [logical document
acceptance](document-writes.md) (identity, receipts, and the "Write
outcomes" transition table), and the [error
registry](raft-error-registry.md) (every named rejection with its
stable identifier, code and stability class).

## Requirement words

On the contract sentences only: MUST, MUST NOT, SHOULD and MAY are the
contract words. A MUST is a sentence a test can fail, and every MUST
names the test that fails it, in the same sentence or the one after. A
sentence that resists conversion is not forced; it goes into "Open"
below, each with one line on why it resists. That list is a
deliverable, not a leftover. Prose that is explanation stays prose.
SHOULD marks the recommended path where a named alternative exists;
MAY marks a permitted behavior no test pins. The registry's human
message text is informative and MAY change; the identifier and the code
are frozen.

## Conformance

An implementation of this specification conforms if it passes these
targets:

- `src/raft/tests.rs` (single node), under `raft`;
- `src/raft/transport_tests.rs` (three voters over loopback mTLS),
  under `raft` + `tls`;
- `tests/control_raft_hosted_writes.rs`, under `raft` + `tls` +
  `fault-injection`;
- `tests/control_raft_rotation.rs`, under `raft` + `tls`;
- `tests/control_raft_migration.rs` (the migration and
  disaster-recovery exercise), under its own gates;
- `tests/control_raft_snapshots.rs`, `tests/control_raft_regressions.rs`,
  `tests/control_raft_admission.rs`, `tests/control_raft_crash_faults.rs`
  (Kimi's targets: not changed by this task, still conformance);
- the managed-catalog unit tests (`src/document_catalog/managed/tests.rs`).

## Document versions and the code's numbers

Each behavioral document carries a `Specification version: N` line
under its title, starting at 1, with a changelog at its end. The
mapping below is pinned by the unit test
`specification_versions_match_the_code` in `src/raft/tests.rs`, which
keeps it equal to the constants in the code the way the exhaustive
receipt destructure pins the receipt's field set.

| Document | Version | Code number it maps to |
| -------- | ------- | ---------------------- |
| `docs/raft-admission.md` | 1 | transport protocol version 1 |
| `docs/raft-hosting.md` | 1 | transport protocol version 1, catalog format 11 |
| `docs/document-writes.md` | 1 | catalog format 11 |
| `docs/raft-error-registry.md` | 1 | transport protocol version 1 |
| operator protocol version | reserved | operator protocol version 1 arrives with the Phase A operator service |

## Open

1. A node-only process naming managed catalogs is refused before any
   catalog is opened: the order lives in the binary's startup path and
   no test pins it.
2. The stated clock-rate assumption (monotonic clocks run at the same
   rate within the skew budget over one interval): an assumption, not a
   sentence a test can fail.
3. The crash window between the commit's return and the UNCONFIRMED
   mark: named under "Source write boundary" in
   [admission under Raft](raft-admission.md); no target in the
   conformance list above parks a crash inside it.
4. The lease actor equals the transport principal name by deployment
   convention: a deployment obligation, not a sentence a test can fail.
