# Security

Three rules run through this chapter: reject, do not clamp; a shared bearer
token is not membership; and the UDP key is not the bearer.

## TLS

`--tls-cert` and `--tls-key` (PEM) put every listener of the process on TLS.
Once they are set, plaintext gRPC is rejected. There is no mixed mode.

Without TLS, plaintext is served on loopback listeners only. A non-loopback
listener with no TLS material rejects the start unless `--allow-plaintext` permits
it on purpose. Default listen addresses are `0.0.0.0`, so an existing plaintext
deployment needs either `--allow-plaintext` or TLS material when it moves to
this version.

## mTLS between the coordinator and its nodes

Node listeners run mutual TLS. `--tls-client-ca` is required alongside
`--tls-cert` on a node, and the listener rejects any connection without a client
certificate chaining to that CA.

The coordinator listener accepts a client certificate when one is presented, and
lets cluster control demand one per call; public clients reach it with a bearer
token over TLS.

The coordinator presents its own membership to nodes with `--tls-ca` (what it
trusts), `--tls-client-cert` and `--tls-client-key` (what it presents), and
`--tls-domain` when the server certificate's name differs from the address. The
same material is installed process-wide, so replica catch-up, snapshot install,
and the tools share one identity. A coordinator with TLS and no `--tls-ca`
rejects the start.

Client-side tools take the same flags plus `--bearer-token-file` (or
`--bearer-token`). A tool dials `https://` once `--tls-ca` is set and `http://`
otherwise, whatever scheme its address had, so one flag string covers an entire
runbook. The analysis sidecar has no TLS and its address is left as it is.

## Bearer principals and quotas

`--bearer-tokens=<toml>` declares public principals:

```toml
[[principals]]
name = "console"
token = "...at least 16 bytes..."
max_k = 200                # 0: the coordinator's max_k
concurrency = 8            # 0: unlimited
ingest_docs_per_sec = 500  # 0: unlimited
admin = false            # true: cluster-wide DiagnosticsService access

[policy]
format_version = 1
revision = 1
[[policy.resources]]
workspace = "legal"
collection = ""          # this example uses the unnamed dataset
[[policy.grants]]
principal = "console"
workspace = "legal"
collection = ""
actions = ["search"]
```

With principals configured, every public call needs
`authorization: Bearer <token>`. Missing, malformed, or unknown is
UNAUTHENTICATED naming which of the three. Tokens are compared in constant time.
Without the flag the public surface is anonymous. With bearer credentials,
explicit workspace/collection grants separately permit `search`, `ingest`, and
`admin`; no action implies another. The principal-level `admin` flag permits
cluster diagnostics only. See [the capability contract](../security.md) for
revision checks, stream revocation, and the remaining document/field boundary.

- `max_k`: a `k` above it is rejected. `k = 0` keeps its meaning (the
  coordinator's `--max-k`) and is rejected when that resolved value is above the
  cap. The request is not rewritten to the cap.
- `concurrency`: requests in flight at once. A streaming query keeps its slot
  until the client is done reading. Over the limit is RESOURCE_EXHAUSTED naming
  the principal and the limit; no request queues.
- `ingest_docs_per_sec`: a token bucket with one second of burst, charged one
  token per routed document as it streams. Over the rate, the stream ends
  RESOURCE_EXHAUSTED naming the rate; the batch is not trimmed.

## Signed datagrams

The streaming search path duplicates score-cutoff raises and cancellations on
UDP to shorten the time a shard takes to notice them. The gRPC stream remains
authoritative: every datagram has a counterpart on the request stream, so loss,
duplication, and reordering can only delay pruning or cancellation. A UDP
cancellation does not certify completion.

`--udp-hmac-key=<file>` (raw bytes or hex, at least 16 bytes) signs them. A
signed datagram is the typed frame plus a sequence number and a truncated
HMAC-SHA256 tag. A node with the key ignores a datagram with a tag that does not
verify, a malformed frame, or a sequence at or behind the newest it applied for
that stream. Forged, damaged, and replayed datagrams have no effect.

Without a key, a node opens its UDP lane on loopback only; off loopback the lane
remains closed and every signal goes over the gRPC stream. A coordinator with no
key sends unsigned frames, which a keyed node ignores.

## Cluster control membership

When the listener runs TLS with a client CA, `ClusterControl` calls require a
client certificate. A call without one is UNAUTHENTICATED: a bearer token is not
membership. A node's own control channel uses the same process-wide client
material, and the listeners it opens for placed replicas include the node's server
identity and demand a client certificate like every node listener.

## What is rejected at startup

- A listener off loopback with no TLS and no `--allow-plaintext`.
- `--tls-cert` without `--tls-key`, or the reverse.
- `--tls-client-ca` with no listener identity; a node with TLS and no client CA.
- A coordinator with TLS and no `--tls-ca`.
- A client identity with only one of certificate and key.
- `--allow-plaintext` together with TLS material.
- A PEM file with no BEGIN block.
- A UDP key under 16 bytes.
- A principal with an empty name, a token under 16 bytes, or a repeated name or
  token.

## What this does not cover

- Certificate rotation without a restart.
- Per-principal metrics labels.
- TLS to the analysis sidecar or to a clustered vector backend; those are
  separate services with their own transports.
- Authorization beyond quotas. A principal that authenticates may call every
  public method on every collection.
- The metrics listener, which is plain HTTP with no authentication. Bind it to a
  trusted interface.

Reference: `docs/security.md`.
