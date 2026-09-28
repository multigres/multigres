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
The shared error schema is `google.rpc.Status`; each operation documents 400,
401, 404, and a default error response. The specification's introduction lists
all other status mappings. Inspect `Content-Type` before parsing errors from
proxies or HTTP routing layers, which may not return this JSON format.

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
