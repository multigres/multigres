# Multiadmin REST API

[Multiadmin OpenAPI 3.1 specification](./multiadmin.openapi.yaml) describes the
18 annotated REST operations served by Vanguard on multiadmin's HTTP port.
Use the HTTP address of your multiadmin instance as the base URL.
The web UI uses Connect; internal gRPC services and unannotated RPC paths are
outside this contract.

## Regeneration and validation

Run from the repository root:

```sh
make proto          # Go, TypeScript, and OpenAPI generation; includes OpenAPI validation
make openapi        # Only the REST specification and its validation
make openapi-lint   # Validate the checked-in specification against OpenAPI 3.1
make openapi-check  # Also run Redocly's recommended-strict rules (Node.js 22.12+, npm 10+)
```

The specification is generated. Edit comments and `openapi.v3` options in
[`multiadminservice.proto`](../../proto/multiadminservice.proto), or the imported
message definitions, then regenerate. Do not edit the YAML by hand. The existing
`validate-generated-files` CI job runs `build-all`, which includes `make proto`
and detects changes to the checked-in specification. It also runs `openapi-check`
with Redocly CLI v2.54.3 to check references, operation IDs, security, parameters,
examples, and unused components. All recommended rules run as errors.

Super-linter also runs Spectral and Checkov. Two exceptions apply only to this
specification: Spectral's `path-params` rule cannot parse dotted protobuf path
placeholders, which Redocly validates; Checkov's `CKV_OPENAPI_21` requires array
limits that Vanguard does not enforce for repeated cell filters. These exceptions
do not change the API contract or disable the other checks.

The generator is
[`protoc-gen-connect-openapi` v0.25.7](https://github.com/sudorandom/protoc-gen-connect-openapi/tree/v0.25.7),
pinned in `tools/setup_build_tools.sh`. Its `features=google.api.http;gnostic`
option includes annotated REST routes and annotation metadata without Connect
RPC paths. Only `multiadminservice.proto` is a generation input; imported message
schemas are followed automatically. The vendored Gnostic annotation definitions
have their source and version recorded in `proto/openapiv3/README.md`.

`protoc-gen-connect-openapi` produces OpenAPI 3.1 directly, including JSON Schema
composition for protobuf `oneof`. The alternative, `protoc-gen-openapiv2` from
grpc-gateway v2.27.4, produces Swagger 2.0 and requires a separate converter.
The normalization step below accounts for differences between the generator's
schemas and Vanguard's JSON encoding.

## Authentication and errors

All operations declare HTTP bearer authentication with JWT format. Multiadmin
enforces this when `--enable-auth` is enabled. Send `Authorization: Bearer <JWT>`.
Development instances with authentication disabled accept requests without it.

Vanguard v0.4.0's `protocol_http.go` maps Connect codes to HTTP statuses and
serializes `google.rpc.Status` using proto3 JSON: `code` is a numeric gRPC code,
`message` is diagnostic text, and `details` is an array of typed objects with
an `@type` property.
For example, a missing cell returns HTTP 404 with `code: 5`.
The shared error schema is `google.rpc.Status`. The response entries describe
the envelope; they are not a complete classification or retry policy. Read the
operation's **Errors and recovery** description before automating retries.
Inspect `Content-Type` before parsing errors from proxies or HTTP routing layers,
which may not return this JSON format.

The Connect adapter currently copies only the code and message from gRPC status
errors. It discards attached details, including `mtrpc.RPCError` application
codes and diagnostics. Clients must tolerate missing `details` and must not
parse message text as a stable error identifier.

## Error semantics and recovery

Use the numeric body code together with the operation-specific exceptions below.
In particular, HTTP 400 covers both an invalid request and a failed precondition.
Changing an HTTP status alone does not resolve an uncertain mutation outcome.

| Body code                   | HTTP | Meaning and client action                                                                                                                                                                 |
| --------------------------- | ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `3` InvalidArgument         | 400  | Correct the request before retrying.                                                                                                                                                      |
| `9` FailedPrecondition      | 400  | Cluster state prevents the operation, such as having no standby for switchover. Resolve the precondition first. Backup creation also uses this code for pooler selection failures.        |
| `5` NotFound                | 404  | Normally indicates confirmed absence. Backup lookups and switchover discovery have exceptions below; their 404 responses do not reliably establish absence.                               |
| `14` Unavailable            | 503  | Normally indicates an unreachable dependency or unavailable quorum. Some proxies also use it for operation rejections. Reads can retry with bounded backoff; inspect persistent failures. |
| `4` DeadlineExceeded        | 504  | The deadline expired. Completion of a mutation may be uncertain; reconcile state before retrying. Some handlers remap downstream deadlines to other codes.                                |
| `13` Internal / `2` Unknown | 500  | Unexpected or unclassified failure, including remapped dependency failures. Do not infer that a mutation made no changes.                                                                 |
| `16` Unauthenticated        | 401  | Supply a valid bearer token when authentication is enabled.                                                                                                                               |

### Current operation-specific classifications

These describe current handler behavior, including distinctions that are lost
at proxy boundaries. The generated operation descriptions carry the same guidance.

| Operations                                    | Current behavior and recovery                                                                                                                                                                                                                                                                                                                        |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GetCell`, `GetDatabase`                      | Missing names return code 3; missing topology records return code 5; other topology errors become code 13. Retry reads after transient dependency failures with bounded backoff.                                                                                                                                                                     |
| `GetCellNames`, `GetDatabaseNames`            | Topology failures become code 13. An error is not an empty inventory.                                                                                                                                                                                                                                                                                |
| `GetGateways`, `GetPoolers`, `GetOrchs`       | Cell enumeration failures become code 13; individual cell failures become code 2. The REST error response does not carry partial inventory. Retry the lookup rather than treating it as resource absence.                                                                                                                                            |
| `GetBackupJobStatus`                          | Missing local state without fallback context, failed pooler selection, any pooler RPC failure, and confirmed missing metadata all return code 5. A 404 is not enough to stop monitoring a job. See backup recovery below.                                                                                                                            |
| `GetBackups`                                  | Missing selectors return code 3. Pooler selection errors become code 5, including topology failures; all downstream listing errors become code 13, including timeouts. Restore dependency health and retry the read; neither error establishes an empty inventory.                                                                                   |
| `GetPoolerStatus`                             | Missing IDs return code 3, missing topology records code 5, and other topology errors code 13. All downstream status errors become code 14, including timeouts and operation rejections. Retry the read with bounded backoff and inspect persistent failures.                                                                                        |
| `GetGatewayQueries`, `GetGatewayConsolidator` | Missing IDs return code 3, absent records code 5, and topology failures code 13. Missing gRPC configuration returns code 9. Dial and downstream RPC errors become code 14; check gateway health and configuration before retrying.                                                                                                                   |
| `SetPostgresRestartsEnabled`                  | The topology lookup distinguishes codes 3, 5, and 13 as above, but every downstream update error becomes code 14. An error does not prove the setting stayed unchanged. Confirm the desired setting and reconcile pooler state before another update.                                                                                                |
| `ExpireBackups`, `VerifyBackups`              | Missing required selectors return code 3; pooler selection errors become code 5 and all downstream errors become code 13. Expiration may partially remove backups: inspect inventory and retention overrides before retrying. Verification is synchronous with no job ID; after a timeout, check for an active verification before starting another. |

### Backup acceptance and polling

`Backup` returns HTTP 200 and a `jobId` when a job is accepted. Completion is
asynchronous; a later failure appears as `JOB_STATUS_FAILED` and `errorMessage`
in a successful `GetBackupJobStatus` response. Pooler selection failures during
submission become FailedPrecondition (9), including topology failures.

Persist the returned job ID with the original database, table group, and shard.
Pass that context when polling so a multiadmin restart can fall back to a pooler.
A fallback 404 may mean the dependency could not be queried, so preserve the job
ID, check the context and pooler health, and retry the read with bounded backoff.
If the outcome remains unknown, investigate it rather than declaring the job lost.

Backup creation has no client-supplied idempotency key. If its response is lost,
reconcile job state and backup inventory before submitting another backup.
An empty inventory does not rule out an in-progress job.

### Rule changes and switchovers

An error, timeout, or lost response does not guarantee rollback. These operations
have no client-supplied idempotency key and must not be blindly retried.

- **`ApplyCertifiedRuleChange`:** recruitment can modify consensus state before
  the request fails. Discover the shard's poolers with `GetPoolers`, then query
  the involved cohort with `GetPoolerStatus`. Compare the `currentPosition`,
  `termRevocation`, and `replicationPrimary` fields in `consensusStatus` with
  the intended transition. Confirm the installed rule and serving leader,
  or reconcile the partial transition before preparing another request. An
  unreachable member leaves its state uncertain. Do not assume a previous
  certificate remains valid or derive a new unsafe certificate from the error
  code alone.
- **`SwitchPrimary`:** no standby returns FailedPrecondition (9), still HTTP 400.
  No primary found returns NotFound (5), but failed cell lookups can hide a primary.
  Every `ResignLeadership` error becomes Internal (13), including a timeout or
  rejected precondition. Demotion may already have occurred. Inspect the old
  leader and candidates with `GetPoolers` and `GetPoolerStatus`, and let an
  in-progress election settle before deciding whether another switchover is
  needed. Blind retry can demote the replacement primary. Even a successful
  response confirms only that the old primary was quiesced; wait until the
  replacement primary is serving before considering failover complete.

## JSON contract

- Object properties and query parameters use camelCase. Path placeholder names
  preserve their `google.api.http` annotation spelling; these are template names,
  not JSON properties.
- `int64` and `uint64` values are decimal **strings**, including in responses.
  Enums use symbolic string names. This documents canonical output; proto3 JSON
  parsers also accept some noncanonical inputs, such as numeric enum values.
- Timestamps are RFC 3339 strings. Durations are protobuf seconds such as
  `1.500s`, not ISO 8601 `PT1.5S` strings. Default-valued fields may be omitted.
- Repeated query values use repeated parameters, for example
  `?cells=zone-a&cells=zone-b`.
- A protobuf `oneof` allows at most one member, including no member. Rule-change
  additionally requires exactly one of `cert` or `unsafeDeriveCert`, as expressed
  in its proto schema annotation. Backup locations may omit both choices.
- Fields bound from the URL need not be repeated in the JSON body. Vanguard
  applies path values to the request, including nested IDs and shard keys.

`make openapi` runs `tools/openapi/normalize` to correct the generated schemas.
It narrows the generator's integer/string unions to canonical 64-bit
strings, replaces the incorrect ISO-duration format with a protobuf duration
pattern, allows unset protobuf oneofs, and uses `unevaluatedProperties` so
composed schemas accept their declared properties. Temporary proto type hints
from the generator distinguish signed and unsigned values and are removed from
the final descriptions. The normalizer removes schemas unreachable from the REST
contract and orders the document overview before paths and components.

Consensus safety and cluster-dependent preconditions require server-side checks.
The proto descriptions document these requirements.

## Operations

Each operation ID is prefixed with `multiadmin.MultiadminService.`.
Orchestrator discovery is grouped under `cells` because it is scoped by cells.

| Method | Path                                                                                           | Operation                  | Tag       |
| ------ | ---------------------------------------------------------------------------------------------- | -------------------------- | --------- |
| GET    | `/api/v1/cells/{name}`                                                                         | GetCell                    | cells     |
| GET    | `/api/v1/databases/{name}`                                                                     | GetDatabase                | databases |
| GET    | `/api/v1/cells`                                                                                | GetCellNames               | cells     |
| GET    | `/api/v1/databases`                                                                            | GetDatabaseNames           | databases |
| GET    | `/api/v1/gateways`                                                                             | GetGateways                | gateways  |
| GET    | `/api/v1/poolers`                                                                              | GetPoolers                 | poolers   |
| GET    | `/api/v1/orchs`                                                                                | GetOrchs                   | cells     |
| POST   | `/api/v1/backups`                                                                              | Backup                     | backups   |
| GET    | `/api/v1/jobs/{job_id}`                                                                        | GetBackupJobStatus         | jobs      |
| GET    | `/api/v1/backups`                                                                              | GetBackups                 | backups   |
| POST   | `/api/v1/backups/expire`                                                                       | ExpireBackups              | backups   |
| POST   | `/api/v1/backups/verify`                                                                       | VerifyBackups              | backups   |
| GET    | `/api/v1/poolers/{pooler_id.cell}/{pooler_id.name}/status`                                     | GetPoolerStatus            | poolers   |
| POST   | `/api/v1/poolers/{pooler_id.cell}/{pooler_id.name}/postgres-restarts`                          | SetPostgresRestartsEnabled | poolers   |
| GET    | `/api/v1/gateways/{gateway_id.cell}/{gateway_id.name}/queries`                                 | GetGatewayQueries          | gateways  |
| GET    | `/api/v1/gateways/{gateway_id.cell}/{gateway_id.name}/consolidator`                            | GetGatewayConsolidator     | gateways  |
| POST   | `/api/v1/shards/{shard_key.database}/{shard_key.table_group}/{shard_key.shard}/rule-change`    | ApplyCertifiedRuleChange   | shards    |
| POST   | `/api/v1/shards/{shard_key.database}/{shard_key.table_group}/{shard_key.shard}/switch-primary` | SwitchPrimary              | shards    |

## Tests

Run the contract unit tests from the repository root:

```sh
go test -short -v ./go/services/multiadmin -run TestOpenAPI
```

The live REST test requires PostgreSQL and etcd on `PATH`:

```sh
make build
scripts/portpool.sh start
MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -v ./go/test/endtoend/multiadmin -run TestMultiadminHTTPAPI
```

The unit tests compare the specification's complete method/path set with the
protobuf service's HTTP annotations, check bearer security, and test canonical
JSON plus invalid counterexamples. The existing live HTTP test validates cells,
databases, poolers, and gateways, including a missing-cell 404, against the
checked-in response schemas. `openapi-lint` uses the pinned
`libopenapi-validator` library to validate against the OpenAPI meta-schema.
