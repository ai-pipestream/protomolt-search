# Security: transport, principals, capabilities and quotas

Implemented on branch 2026-09-02 (the rest of roadmap item 12). Three rules shape it,
stated once:

- **Refuse, do not clamp.** A request over its principal's quota is a
  named `RESOURCE_EXHAUSTED`; it is not trimmed to fit.
- **A shared bearer is not membership.** Cluster-internal calls
  (coordinator to node, cluster control) are authenticated by client
  certificates from the cluster CA. The bearer token identifies a
  public client and buys it nothing inside the cluster.
- **The UDP key is not the bearer.** Floor and cancel datagrams carry a
  tag under their own key, so a leaked client token cannot forge a
  floor.

## TLS on the listeners

`--tls-cert` and `--tls-key` (PEM, operator-supplied; rustls through
tonic's `tls` feature) put every listener of the process on TLS. Once
set, plaintext gRPC is refused — there is no mixed mode and no
`--allow-plaintext` alongside TLS.

Without TLS, plaintext is served on **loopback listeners only**. A
non-loopback listener without TLS refuses to start unless
`--allow-plaintext` says so on purpose. That is what keeps every
loopback test and the single-machine demo working while a fleet cannot
drift into plaintext by omission.

Node listeners run **mTLS**: `--tls-client-ca` (the cluster CA) is
required with `--tls-cert` on a node, and the listener refuses any
connection without a client certificate chaining to it. The coordinator
listener accepts a client certificate when offered (`client_auth_optional`)
and lets cluster control demand it per call; public clients reach it
with a bearer token over TLS.

The coordinator presents its own membership to nodes: `--tls-ca` (what
it trusts), `--tls-client-cert` and `--tls-client-key` (what it
presents), and `--tls-domain` when the server certificate's name differs
from the address. The same material is installed process-wide
(`security::install_client_tls`) for the channels opened outside the
coordinator — replica catch-up, snapshot install, the calibration
tools — so every cluster-internal channel in the process is the same
identity. A TLS coordinator without `--tls-ca` refuses to start.

**Rollout.** The default listen addresses are `0.0.0.0`, so an existing
plaintext deployment (the NAS compose included) must add
`--allow-plaintext` (or TLS material) when it moves to this version; a
process that starts without either on a non-loopback listener refuses
with the message above rather than serving plaintext by omission.

The metrics exporter (`--metrics-listen`) stays plain HTTP on its own
listener; it is a scrape target, not a gRPC surface, and TLS on the gRPC
listeners does not change it.

The embedded runtime (`protomolt-search-embedded`) depends on this crate
with `default-features = false`: it speaks over an in-process duplex and
carries no TLS stack. `tests/security.rs` pins that with a `cargo tree`
gate.

## Bearer principals on the public surface

`--bearer-tokens=<toml>` declares the public clients:

```toml
[[principals]]
name = "console"
token = "…at least 16 bytes…"
max_k = 200              # 0: the coordinator's max_k
concurrency = 8          # 0: unlimited
ingest_docs_per_sec = 500  # 0: unlimited
admin = false            # diagnostics also require all served collections' admin grants

[policy]
format_version = 1
revision = 1
[[policy.resources]]
workspace = "legal"
collection = "opinions"   # "" only for the unnamed dataset
[[policy.grants]]
principal = "console"
workspace = "legal"
collection = "opinions"
actions = ["search"]
```

`DiagnosticsService` requires both `admin = true` and an explicit `admin` action
grant for every collection served by the coordinator. Its runtime controls,
metrics, shard layouts and recent-query ring cover the whole process. An
operator flag alone, a missing policy or a grant for only some collections
returns `PERMISSION_DENIED` before observation or mutation. This flag grants no
search or ingest capability. See [diagnostics](diagnostics.md) for policy
replacement, stream cancellation and deployment implications.

With principals configured every `SearchService` call — search, query,
streaming query, aggregate, plan, routed ingest, topology, broadcast,
cluster health — needs `authorization: Bearer <token>`. A missing,
malformed, or unknown token is `UNAUTHENTICATED` naming which. Tokens
are compared in constant time. Without `--bearer-tokens` the surface is
anonymous, as before. An authenticated principal without a configured policy
is denied; loading a bearer file without an explicit `[policy]` refuses startup.

The check happens in `CollectionSet`, the one place every public call
passes through, before the collection is resolved: the principal is
known before any shard is asked.

## Workspace and collection capabilities

The product-owned `authorization.proto` defines `AccessPolicy`, workspace-bound
`CollectionResource`s, exact `CollectionGrant`s and `AccessDecision`. The TOML
above is a configuration adapter for that protobuf contract. Credentials supply
identity; they grant no capability by themselves. Empty grants deny access.
`format_version` must be 1. Future restrictions require a new format version,
so an older reader rejects them instead of silently dropping restrictions.
Unknown actions, duplicate resources or grants, and grants whose workspace does
not own the collection are configuration errors. Configuration typos refuse.

Every public route in `CollectionSet` declares one capability:

| Capability | Routes |
|---|---|
| `search` | Search, Bm25Search, PhraseSearch, HybridSearch, VariantSearch, Query, QueryStream, Aggregate, Suggest, TermSuggest |
| `ingest` | RoutedIngestMapped |
| `admin` | PlanIndex, DescribeSchema, PlanPlacement, BroadcastVectorBackend, BroadcastCalibration, FreezeTopologyWrites, PublishTopology, AbortTopologyCutover, ClusterHealth |

No capability implies another. An administrator who also needs to search or
write needs those grants explicitly. Names are exact; there are no implicit
wildcards. The service resolves the collection (including its configured
default) before applying the resource binding. Workspace ownership comes from
the authority's policy, not caller metadata. Authenticated callers receive the
same denial for an unknown collection and an unauthorized one; naming errors
cannot disclose the served collection list. Unnamed `ClusterHealth` on a named
set lists only collections with an admin grant, and denies when none are allowed.

`authorization::Authorizer` is the integration seam for a workspace authority.
`PolicyAuthority` supplies validated snapshots and atomic replacement. Revision
must be nonzero and strictly increase on replacement. Decisions are pinned to
one revision; a replacement invalidates outstanding decisions even if their
capabilities would remain unchanged. Unary results are checked again before
return. Query streams check before disclosing each item and wake on policy
replacement even while their producer is pending. Routed ingest checks the bind
before schema/fan-out work and checks each subsequent stream item. Already
admitted mutations are not rolled back by revocation or a later stream error.

Authorization precedes coordinator cache lookup. Revoked callers cannot retrieve
a previous cached response through the public service. This does not yet make
cache entries safe for different document/field policies within one collection;
those mandatory selection and disclosure rules remain foundation work.

Library hosts can retain `Arc<PolicyAuthority>` and call `replace`, or supply an
`Authorizer` through `Principals::with_authorizer`. The command-line bearer file
is loaded at startup; editing it does not reload a running process. Persist the
accepted revision in the ecosystem authority across restarts. `AccessDecision`
is diagnostic context, not a credential for untrusted clients or node calls.

**Migration:** add `[policy]` to existing bearer files before upgrading. Grant
only the exact datasets/actions required. `mkcerts.sh` generates a policy for the
fleet tools' unnamed dataset when creating a new file; it does not overwrite an
existing file. This increment does not alter the separate node mTLS or cluster
control membership rules. It does not establish document/field authorization or
secure a direct node call by applying the public collection policy.
The console (`docs/console-facade.md`) is the one client that holds the
cluster credentials on a browser's behalf: it binds to loopback unless
told otherwise, and whoever reaches it acts as its principal.

## Query cursor context

`Query` and `QueryStream` retain the server-issued `AccessDecision` when entering
the coordinator, instead of losing it when unwrapping the request. The opaque
cursor envelope in `query_cursor.proto` binds that decision, the resolved
collection, normalized query and frozen routing map. A different principal,
workspace, policy revision, query or map cannot resume a token just because a
boundary score matches. The check precedes execution and stream creation.
Current authorization still runs for every page and every disclosed stream item;
possessing a cursor grants no access. Client metadata cannot supply a trusted
`AccessDecision` extension. A streamed query's nested collection must also agree
with its authorized outer resource.

HMAC-SHA256 covers the versioned protobuf envelope and a domain separator; tags
are compared with the existing constant-time comparison. Parsing is bounded and
refuses noncanonical payloads and unknown versions. Keys are shared by clones
of a coordinator, are redacted in debug output, and default to ephemeral
32-byte secrets obtained through
[getrandom 0.2.17](https://docs.rs/getrandom/0.2.17/getrandom/). Entropy failure
refuses token issuance instead of using a predictable fallback. A library host
can supply a retained key through `with_cursor_signing_key`; it must also retain
the authority's revision history. The command-line server has no key-file or
live key-rotation option. An ephemeral-key restart or a host key change requires
fresh pagination. Tokens are not encrypted and are not durable index snapshots.

This closes the cursor context gap; it does not implement document/field
selection, scope term statistics or partition result caches for those policies.
Those restrictions remain required across every disclosure route before granular
grants can be advertised. See [paging](query-api.md#paging) for the live-data
boundary and old-token migration.

## Quotas, per principal

| quota | rule | on exceed |
|---|---|---|
| `max_k` | `k` above it refuses; `k = 0` keeps its meaning (the coordinator's default) and refuses when that resolved value is above the cap — the request is never rewritten to the cap | `RESOURCE_EXHAUSTED` naming the principal, the resolved `k`, and the cap |
| `concurrency` | requests in flight at once; a streaming query holds its slot until the client is done reading | `RESOURCE_EXHAUSTED` naming the principal and the limit; nothing queues |
| `ingest_docs_per_sec` | a token bucket with one second of burst, charged one token per routed document as it streams | the stream ends with `RESOURCE_EXHAUSTED` naming the rate; the batch is not trimmed |

## Signed UDP datagrams

The streaming-search fast lane (`docs/streaming-query.md`) duplicates
floor raises and cancels on UDP. A forged floor could cut candidates,
so off loopback the lane is authenticated: `--udp-hmac-key=<file>` (raw
bytes or hex, at least 16 bytes) on the coordinator and its nodes.

A signed datagram is the 20-byte typed frame, a 4-byte sequence, and a
16-byte tag: HMAC-SHA256 (RFC 2104 over the crate's own SHA-256,
`security::hmac_sha256`, pinned to RFC 4231 vectors) over frame and
sequence, truncated. The node ignores a datagram whose tag does not
verify, whose frame is malformed, or whose sequence is at or behind the
newest it applied for that stream token — forged, damaged, and replayed
datagrams change nothing, and the gRPC twin of every signal still
governs. The packet grew by 20 bytes and gained no new parser: the frame
is the frame it was.

Without a key a node opens its UDP lane on a loopback listener only;
off loopback the lane stays closed and every signal rides the gRPC
stream. A coordinator without a key sends plain frames, which a keyed
node ignores.

## Cluster control membership

`ClusterControl` calls (register, lease, drain, report, complete,
reconcile, plan, rollback) require a client certificate when the
listener runs TLS with a client CA: `ClusterControlService::membership`
refuses a call without one as `UNAUTHENTICATED` — "a bearer token is
not membership". The collection set applies the same rule per member.
A node's own control channel (`--node-id` with `--control-addr`,
`docs/cluster-control.md`) is opened under the same process-wide client
material, so a registering node presents `--tls-client-cert`; the
listeners it opens for placed replicas carry the node's server identity
and demand a client certificate like every node listener.

## The tools

The verifier (`examples/v7_verify`), the ingest driver
(`examples/court_ingest`), the console (`src/bin/console`), and the
measurement tools (`examples/leg_latency`, `shard_timings`,
`dense_profile`, `cluster_sweep`) take the client side of the same
flags, through `security::ToolClient`: `--tls-ca`
(the cluster CA), `--tls-client-cert` and `--tls-client-key` (the
identity node listeners demand), `--tls-domain` (when the certificate's
name is not the address), and the bearer for the coordinator's public
surface as `--bearer-token-file=<path>` (or `--bearer-token=<literal>`).
A tool dials `https://` once `--tls-ca` is set and `http://` otherwise,
whatever scheme its address carried, and it leaves listener flags it
does not take alone, so a runbook can hand every process one flag
string. The bearer rides a `tonic` interceptor (`security::Bearer`) on
every call; the driver installs its material process-wide so its
in-process coordinator's channels carry the identity. The sidecar has
no TLS; its address is untouched.

`deploy/v7-rebuild/mkcerts.sh` issues a fleet's material with
`openssl`: the CA, a server identity per host (SANs: the host name, its
addresses, `127.0.0.1`), the cluster-internal client identity, the UDP
key, and one principal with its token file.

## Refusals, named

- listener off loopback without TLS and without `--allow-plaintext`
- `--tls-cert` without `--tls-key` (or the reverse); `--tls-client-ca`
  without a listener identity; a node with TLS and no client CA
- a TLS coordinator without `--tls-ca`; a client identity with only one
  of cert and key; `--allow-plaintext` together with TLS
- a PEM file without a BEGIN block; a UDP key under 16 bytes; a
  principal with an empty name, a token under 16 bytes, or a repeated
  name or token
- plaintext, wrong-CA, or certificate-less connections to a node
- missing / malformed / unknown bearer; `k` over the cap; concurrency
  at the limit; ingest over the rate
- cluster control without a client certificate

## What this does not do

- No certificate rotation or reload without a restart.
- No per-principal metrics labels yet (`docs/metrics.md` counts routes).
- No TLS to the analysis sidecar or the clustered TurboVec backend:
  those are separate services with their own transports.
- No document/field policy enforcement yet. A collection search grant currently
  includes that entire collection's public search results and projections.
- No public-policy enforcement on direct node or cluster-control operations;
  those still trust cluster membership. A public bearer is not that membership.
- No automatic access-policy file reload or persisted revision high watermark
  in the command-line process; dynamic authority providers own that lifecycle.

## Tests

`tests/security.rs`: a tool built from its flags reaches a TLS node with the
identity and a TLS coordinator with the bearer, and the flag refusals; a TLS node accepts a client certificate from the
cluster CA and rejects plaintext, a certificate-less client, and a
foreign CA's certificate; a coordinator with its identity reaches TLS
nodes and serves bearer clients over TLS; the bearer table (missing,
wrong, right); `max_k`, with `k = 0` refused under a cap below the
coordinator default and served under one above it; concurrency held by an
in-flight request (a delayed analysis sidecar keeps one open); the
ingest meter; cluster control with and without membership; the metrics
listener alongside TLS; the embedded crate's dependency gate; the
configuration refusals. `src/node.rs` unit-tests the signed floor lane
(forged, wrong-key, replayed, and stale sequences leave the floor
untouched); `src/stream_signal.rs` and `src/security.rs` unit-test the
frames, HMAC vectors, principals, and buckets.

`tests/authorization.rs` checks the public route capability table, workspace
bindings, resolved defaults, hidden collection names, scoped health listing,
malformed policies, monotonic revisions, cache-entry revocation, routed-ingest
admission, and cancellation of pending/disclosing streams on policy replacement.
