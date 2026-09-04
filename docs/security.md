# Security: TLS, mTLS, bearer principals, signed datagrams, quotas

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
```

With principals configured every `SearchService` call — search, query,
streaming query, aggregate, plan, routed ingest, topology, broadcast,
cluster health — needs `authorization: Bearer <token>`. A missing,
malformed, or unknown token is `UNAUTHENTICATED` naming which. Tokens
are compared in constant time. Without `--bearer-tokens` the surface is
anonymous, as before.

The check happens in `CollectionSet`, the one place every public call
passes through, before the collection is resolved: the principal is
known before any shard is asked.

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
- No authorization beyond quotas: a principal that authenticates may
  call every public method on every collection.

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
