# The console: REST facade and web UI

The `console` binary is the operator's front end for a running cluster:
a small HTTP server that speaks JSON on one side and the cluster's gRPC
on the other, plus the static web UI it serves. It is a client only. It
holds the TLS material and the bearer token (`docs/security.md`), so a
browser never carries either.

## The transcoding endpoint

`POST /api/rpc/<Service>/<Method>` takes the request message in proto3
JSON (the same JSON `grpcurl` accepts, field names in `camelCase` or
`snake_case`) and answers with the response message in proto3 JSON. Every
unary method of `SearchService` and `DiagnosticsService` is reachable this
way, so the UI and any script share one contract with the wire. A gRPC
status becomes an HTTP status (`INVALID_ARGUMENT` 400, `NOT_FOUND` 404,
`PERMISSION_DENIED` 403, `RESOURCE_EXHAUSTED` 429, `UNIMPLEMENTED` 501,
anything else 502) with the status message in a JSON `error` field,
unchanged.

Server-streaming methods are exposed as server-sent events:
`GET /api/stream/DiagnosticsService/StreamMetrics?interval_ms=1000`
emits one `data:` line per message, each a proto3 JSON object.

## Convenience routes

- `GET /api/health`: `ClusterHealth`, as before.
- `GET /` and the static files under it: the UI.

## Configuration

The same flags as every tool (`--coordinator`, `--nodes`, `--analysis`,
`--tls-ca`, `--tls-client-cert`, `--tls-client-key`, `--bearer-token-file`)
plus `--listen` (default `127.0.0.1:8600`). The facade refuses to bind a
non-loopback address without `--allow-remote`, because whoever reaches it
acts as its principal.
