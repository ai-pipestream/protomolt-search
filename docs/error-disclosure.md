# Search error disclosure

At the public `CollectionSet` boundary, search operations with document or field
restrictions receive a fixed message for each canonical gRPC error code. Raw
handler messages, transport metadata, rich details and source chains are
withheld together. A fresh status carries only the safe message and the
structured disclosure decision. Handler failures for unrestricted collection
operations retain their existing diagnostics. An authority failure before a
grant exists is always sanitized, including failures while acquiring a permit.

`ai.protomolt.search.v1.ErrorDisclosure` contains `details_redacted` and a
`SearchErrorReason`. Transport errors carry it in a `google.rpc.Status` envelope
under the Any type URL
`type.googleapis.com/ai.protomolt.search.v1.ErrorDisclosure`. The envelope's
code and message match the transport status. Rust clients can use
`error_disclosure::status_detail`; malformed, mismatched, oversized (over 1 KiB)
or duplicate disclosure details return no claim. Unknown reason numbers are
preserved by decoding, but have no special server behavior.

`ACCESS_POLICY_CHANGED` means the authority decision no longer matches the
current policy. A client must start a new authorized operation; the old stream
or cursor cannot retain its grant. Other failures use `UNSPECIFIED` and the
canonical gRPC code alone. Sanitization preserves known error codes; an invalid
success code on an error becomes `INTERNAL`. It does not make failures retryable
or hide timing and status-code differences.

## Streaming

`QueryStreamCompletion.error_disclosure = 7` embeds the same decision for a
terminal failure. Restricted failures clear any attached response and scoring
fingerprints, replace the free-form message, and retain final revision and the
canonical code. Unknown numeric codes become `UNKNOWN`. Policy-change reasons
are recognized only with `PERMISSION_DENIED`.

The disclosure wrapper runs after the authority's per-item revocation check,
so a policy change can wake a pending stream and end it with a safe structured
error. The wrapper stops after a transport error or completion; no later item
can escape. A successful certificate must contain its response and cannot also
contain an error code, error message or error disclosure. Malformed success
certificates fail with `INTERNAL`.

Successful response metadata has its separate [execution disclosure
contract](query-disclosure.md). The subsequent [document-query admission increment](document-query-authorization.md)
certifies provisional membership and opens private-shard document-restricted
Query. This error-disclosure increment alone was not that admission decision. Direct coordinator calls are internal APIs; external
HTTP adapters and mobile client error presentation are outside this audit.
No stored format, service route or existing protobuf field number changes.

## Vendored envelope

`proto/google/rpc/status.proto` is byte-identical to [googleapis revision
64aa30b277168edd20efee0c9ceb4ca01248931d](https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/rpc/status.proto).
The source's Apache 2.0 header is retained. SHA-256:
`f5bfd262e6705c7ae73f32e0ad8ee20ce8c0a2578df8c4f76ebf76b572f295ed`.
`scripts/check-vendored-protos.sh` checks this pin independently of the
ProtoMolt-owned descriptor and indexing contracts.

## Validation scope

Tests inject private messages, text and binary metadata, rich details and source
chains across every canonical error code. They cover malformed rich envelopes,
policy revocation while a stream is pending, invalid completion codes, results
attached to failures, malformed success certificates and post-terminal items.
Collection integration tests cover failures before admission and after handler
execution with unrestricted, field-restricted, document-restricted and combined
grants. Restricted streaming completion errors are checked at the public
collection boundary.

## Validation, 2026-09-06

Against incorporated main `7b0faa9`, the final source passed 504 library tests,
691 integration tests across 119 targets, and 12 embedded tests: 1,207 passed,
zero failed, one existing live OpenNLP conformance test ignored. All five
Android/iOS compile checks, the tests/examples build, formatting, vendored-proto
identity and whitespace checks passed. Descriptor comparison against main
`7b0faa9` and feature checkpoint `cbfee32` preserved all existing declarations.

The initial full integration run exposed three assertions expecting the old
restricted-error text. Their replacements retain the status-code and denial
checks and assert structured redaction instead of physical diagnostics. The
changed target was rerun successfully before completing the remaining groups;
production and library source did not change. The final 338-file source, build,
test and script manifest was unchanged through the rest of validation. These are
local tests and compile checks, not hosted CI, device runs or fleet measurements.
