# API-key authentication

Joxette gates state-changing REST calls behind a single shared API key. This
document covers the full picture: what's protected, how each client supplies
the key, and what's deliberately out of scope so far.

## What's protected

`SecurityConfig` (`joxette-service/src/main/java/com/joxette/config/SecurityConfig.java`)
installs a Spring Security filter chain that:

- permits **GET/HEAD unconditionally**, unauthenticated, always;
- requires a matching `X-API-Key` header on **POST/PUT/DELETE/PATCH** when
  `joxette.security.api-key` is set;
- permits mutating requests unauthenticated when the property is blank/unset
  (the default) — a startup `WARN` log flags this so it doesn't go unnoticed
  outside local development.

A missing/incorrect key on a mutating request gets a `401` RFC 7807
`application/problem+json` body with `errorCode: ERR_UNAUTHORIZED` (see
[`docs/error-handling.md`](error-handling.md)).

## Configuring the key

Set `joxette.security.api-key` (env `JOXETTE_SECURITY_API-KEY`, Spring relaxed
binding). Via Helm: `security.existingSecret` in
[`deploy/helm/joxette/values.yaml`](../deploy/helm/joxette/values.yaml) — a
Secret name with an `api-key` key, wired to every pod's env the same way
`objectStore.existingSecret` wires S3 credentials.

## Client support

| Client | Status |
|---|---|
| `joxette-operator` | **Supported.** `JoxetteRestClient` sends `X-API-Key` on every mutating call when `joxette.operator.api-key` (env `JOXETTE_OPERATOR_API-KEY`) is configured — see [`deploy/operator/README.md`](../deploy/operator/README.md). GET calls (e.g. the readiness probe) never send it. |
| `ui/` (TanStack Start app) | **Not supported yet.** The UI has no mechanism to attach the header to its requests. Until UI-side auth support is added, either leave `joxette.security.api-key` unset for any backend the UI talks to, or front the UI with a trusted gateway that injects the header. See [`ui/README.md`](../ui/README.md). |
| Direct `curl`/scripts/other integrations | Pass `-H "X-API-Key: <key>"` on mutating calls yourself. |

## Why the operator and not the UI, in this pass

The operator's REST calls all originate server-side from a single trusted
process that already reads its config from Kubernetes Secrets — adding a
header is a small, mechanical change with no new trust boundary. The UI would
need either a server-side proxy/BFF to hold the key (the browser must never
receive it directly) or a per-user auth flow, which is a larger design
exercise deliberately deferred rather than bolted on ad hoc.
