# Code Generation

## Overview

Several parts of the tree are generated from other sources. All generated files
are checked in, so a normal build (`make build`) never runs a generator. You
only regenerate when you change a source listed below, and you commit the
regenerated output in the same change. CI rebuilds everything and fails if the
result differs from what is committed.

Generated Go files start with a `// Code generated ... DO NOT EDIT.` comment.
Never edit them by hand; change the source or the generator and regenerate.

## What is generated

| Output                                                                               | Source                                                 | Tool                                                    | Make target      |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------ | ------------------------------------------------------- | ---------------- |
| `go/pb/**` (messages, gRPC, Connect, grpc-gateway)                                   | `proto/*.proto`                                        | `protoc` with the Go plugins in `bin/`                  | `make proto`     |
| `web/multiadmin/lib/api/generated/*.ts`                                              | the protos listed in `PROTO_TS_SRCS` in the `Makefile` | `protoc-gen-es`, `protoc-gen-connect-es`                | `make proto`     |
| `docs/api/multiadmin.openapi.yaml`                                                   | `proto/multiadminservice.proto`                        | `protoc-gen-connect-openapi`, `tools/openapi/normalize` | `make proto`     |
| `go/common/parser/postgres.go`                                                       | `go/common/parser/postgres.y`                          | `go/common/parser/goyacc`                               | `make parser`    |
| `go/common/parser/plpgsql/plpgsql.go`                                                | `go/common/parser/plpgsql/plpgsql.y`                   | `go/common/parser/goyacc`                               | `make parser`    |
| `go/common/parser/ast/ast_clone.go`, `ast_rewrite.go`                                | node types in `go/common/parser/ast`                   | `go/tools/asthelpergen`                                 | `make parser`    |
| `go/common/parser/ast/plpgsqlast/ast_clone.go`, `ast_rewrite.go`                     | node types in `go/common/parser/ast/plpgsqlast`        | `go/tools/asthelpergen`                                 | `make parser`    |
| `go/observability/metriccatalog/catalog_generated.go`                                | OpenTelemetry instrument definitions across `./go/...` | `go/tools/metricsgen`                                   | `make metrics`   |
| `go/common/pgcatalog/{types,procs,operators,casts}_generated.go`                     | `go/common/pgcatalog/data/` (PostgreSQL catalog data)  | `go/tools/pgcataloggen`                                 | `make pgcatalog` |
| `go/common/parser/pgoid/oids_generated.go`, `go/common/parser/ast/oids_generated.go` | `go/common/pgcatalog/data/pg_type.dat`                 | `go/tools/pgcataloggen`                                 | `make pgcatalog` |

Aggregate targets:

- `make generate` runs `parser`, `metrics` and `pgcatalog`.
- `make build-all` runs `proto`, `parser`, `metrics`, `pgcatalog`, then `build`.

## The generators

### Protobuf (`make proto`)

`make proto` depends on `make tools`, which installs the pinned `protoc`
release under `dist/` and the `protoc` plugins under `bin/`
(`tools/setup_build_tools.sh`). It then runs three steps:

1. `pb` compiles every file under `proto/` into `go/pb/`.
2. `proto-ts` generates the TypeScript types and Connect descriptors used by the
   Multiadmin web app. It needs `pnpm`, because the plugins come from
   `web/multiadmin/node_modules`.
3. `openapi` generates the Multiadmin REST specification and validates it with
   `make openapi-lint`. See [`docs/api/README.md`](../api/README.md).

### Parser and AST helpers (`make parser`)

`make parser` runs `go generate ./go/common/parser/...`, which executes the
`//go:generate` directives in four `generate.go` files:

- `go/common/parser/generate.go` and `go/common/parser/plpgsql/generate.go` run
  goyacc on the grammar, then `goimports` and `gofumpt` on the output.
- `go/common/parser/ast/generate.go` and
  `go/common/parser/ast/plpgsqlast/generate.go` run `asthelpergen`, which finds
  every type implementing the package's `Node` interface and writes the clone
  and rewrite helpers.

Changing a grammar file changes only the generated parser. Adding, removing or
changing the fields of an AST node type changes the clone and rewrite helpers
too. Both are covered by `make parser`. See [SQL parser and AST](sql_parser.md).

### Metric catalog (`make metrics`)

`metricsgen` loads the Go packages with type information, finds every call to an
OpenTelemetry `metric.Meter` instrument constructor (`Int64Counter`,
`Float64Histogram`, ...), and records the instrument's name and unit. It
computes the Prometheus series names with the same naming library the
OpenTelemetry Prometheus exporter uses, and records which binaries can export
each metric from the import graph.

Things to know when you add a metric:

- The instrument name must be a constant string. The generator reports an error
  for a name it cannot resolve statically.
- Metrics registered inside a dependency are invisible to the scanner. The Go
  runtime metrics are listed by hand in `runtimeMetricSpecs` in
  `go/tools/metricsgen/metricsgen.go`.
- `go run ./go/tools/metricsgen/main -verify` checks the committed catalog
  without rewriting it.

### PostgreSQL catalog tables (`make pgcatalog`)

`pgcataloggen` reads the PostgreSQL bootstrap catalog files vendored under
`go/common/pgcatalog/data/` (`pg_type.dat`, `pg_proc.dat`, `pg_operator.dat`,
`pg_cast.dat`, `pg_collation.dat`, and `system_functions.sql`) and writes Go
tables of the builtin types, functions, operators and casts, plus the type OID
constants (`pgoid.TEXTOID`, re-exported as `ast.TEXTOID`).

The data is pinned to PostgreSQL `REL_17_6`. The `.dat` files omit columns that
have their default value, and those defaults live in PostgreSQL's catalog
headers, not in the data files. The generator transcribes them by hand, each
with a comment citing the header line, so a version upgrade has to re-verify
them.

## How CI validates generated files

The `Validate Generated Files` job in `.github/workflows/lint.yml` runs:

```bash
make validate-generated-files
```

That target runs `make clean`, `make build-all` and `go mod tidy`, then fails if
`git status` reports any modified tracked file. The fix is always the same:

```bash
make build-all && go mod tidy
```

and commit the result. The job then runs `make openapi-check` for the REST
specification.

## Which target to run

| You changed                                        | Run              |
| -------------------------------------------------- | ---------------- |
| A `.proto` file                                    | `make proto`     |
| `postgres.y` or `plpgsql.y`                        | `make parser`    |
| An AST node type (added, removed, fields changed)  | `make parser`    |
| An OpenTelemetry instrument (added, renamed, unit) | `make metrics`   |
| A file under `go/common/pgcatalog/data/`           | `make pgcatalog` |
| A generator itself                                 | its target       |
| Not sure                                           | `make build-all` |

## Pitfalls

- **Line numbers in the metric catalog.** Each catalog entry records the
  `file.go:line` of the instrument definition, so moving code in a file that
  defines metrics can change the catalog even when no metric changed.
- **Tool versions.** `make proto` uses the `protoc` version pinned in the
  `Makefile` (`PROTOC_VER`) and the plugin versions pinned in
  `tools/setup_build_tools.sh`. Output from other versions will not match CI.
- **`goimports` and `gofumpt` come from `go.mod`.** The parser directives call
  them with `go tool`, so they need no separate install.
- **Lint skips generated files.** `.golangci.yml` excludes generated code from
  linting, so lint findings there are fixed in the generator, not the output.
