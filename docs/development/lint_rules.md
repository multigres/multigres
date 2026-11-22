# Lint Rules

## Overview

Beyond formatting and the standard linters, the repository encodes several of
its own design rules as lint checks: which packages may import which, which
helpers must be used instead of standard-library calls, and which state each
service is allowed to read. This guide lists those project-specific rules, what
each one protects, and how to add one.

## Where the rules live

| File                                              | Contents                                                                    |
| ------------------------------------------------- | --------------------------------------------------------------------------- |
| `.golangci.yml`                                   | Enabled linters, import boundaries (`depguard`), `forbidigo`, `sloglint`    |
| `go/tools/ruleguard/rules.go`                     | Custom pattern rules, run through `gocritic`'s `ruleguard` check            |
| `tools/naming_linter.sh`                          | Spelling of the component names (`make naming-lint`)                        |
| `misc/hooks/`                                     | Pre-commit hooks: goimports, gofumpt, golangci-lint, license, prettier, ... |
| `.github/workflows/golangci-lint.yml`, `lint.yml` | The CI jobs                                                                 |

`make tools` links `misc/git/pre-commit` into your Git hooks directory; it runs
every executable in `misc/hooks/`.

Formatting is gofumpt plus goimports with the local prefix
`github.com/multigres`.

## Import boundaries (`depguard`)

These rules enforce the dependency layering between `go/cmd`, `go/services`,
`go/common` and `go/tools`, and keep the services independent of each other.

| Rule                  | Applies to                     | Effect                                                                                                       |
| --------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `tools-isolation`     | `go/tools/**`                  | May not import Multigres packages other than `go/tools`                                                      |
| `parser-isolation`    | `go/common/parser/**`          | May not import other `go/common` packages, so the parser stays usable as a standalone library                |
| `provisioner`         | `go/provisioner/**` (non-test) | May import only `go/common/constants`, `go/common/topoclient`, `go/tools`, `go/pb` and `go/provisioner`      |
| `<service>-isolation` | everything else                | `go/services/<service>` may be imported only by itself and `go/cmd/<service>`, for each of the five services |
| `use_modern_packages` | all code                       | `math/rand` is rejected in favor of `math/rand/v2`                                                           |

Exceptions built into the service rules: `go/cmd/minigres` may import the
multigateway and multipooler services, because Minigres runs both in one
process, and the end-to-end tests may import the multiadmin service.

## Custom rules (`ruleguard`)

Each rule is a Go function in `go/tools/ruleguard/rules.go` written with the
[`go-ruleguard`](https://github.com/quasilyte/go-ruleguard) DSL. The comment
above each function explains its reasoning in full.

### Use the project helper

| Rule                                    | Instead of                                                  | Use                                                                                               |
| --------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `requireContextBackgroundJustification` | `context.Background()` in non-test code                     | `context.TODO()`, `ctxutil.Detach(ctx)`, or `//nolint:gocritic // <reason>` at a real entry point |
| `requireGrpcCommonNewClient`            | `grpc.NewClient(...)` in non-test code                      | `grpccommon.NewClient(...)`, which adds telemetry                                                 |
| `disallowDirectExecCommandContext`      | `exec.CommandContext(...)`                                  | `executil.Command(...)`, for graceful termination, environment handling and trace propagation     |
| `disallowDirectProcessTermination`      | `Signal(SIGTERM)`, `Signal(SIGKILL)`, `Kill()` on a process | `Cmd.Stop()`, or `executil`'s `StopProcess`/`StopPID`/`TerminateProcess`/`TerminatePID`           |
| `requireTestpollNever`                  | testify's `assert.Never` / `require.Never`                  | `testpoll.Never`, which cannot leak a goroutine                                                   |
| `disallowDirectPgctldStopInTests`       | calling pgctld `Stop` from end-to-end tests                 | `ShardSetup.StopPostgres`, which stops the monitor from restarting PostgreSQL                     |

### Flags and metrics

| Rule                                   | Requirement                                                         |
| -------------------------------------- | ------------------------------------------------------------------- |
| `disallowUnderscoreInFlags`            | Flag names (pflag and `viperutil`) use dashes, not underscores      |
| `disallowOtelMeterOutsideMetricsFiles` | `otel.Meter()` is called only in `*metrics.go` or `*_test.go` files |
| `disallowMetricsConstructorArgs`       | `NewMetrics()` in a `metrics.go` file takes no arguments            |

### Determinism in `go/common/consensus` and `go/common/ha`

These packages must behave identically across runs so that tests and simulation
are reproducible.

| Rule                                    | Rejects                                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `requireSortedMapIteration`             | `for ... range` over a map; use `sortedmaps.All`/`Keys`/`Values`                               |
| `requireSortedMapsOverStdlibMaps`       | `maps.Keys`/`Values`/`All`; use the `sortedmaps` equivalents                                   |
| `disallowGoroutinesInDeterministicPkgs` | `go` statements in non-test code                                                               |
| `disallowWallClockInDeterministicPkgs`  | `time.Now`, `time.Since`, `time.Sleep`, timers, tickers and `timestamppb.Now` in non-test code |

### Who may read which cluster state

These rules keep the separation between routing state, which the gateway uses,
and consensus state, which Multiorch uses. They apply to non-test code.

| Rule                                           | Rejects                                                                                                                            |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `disallowMultipoolerTypeForRouting`            | Reading `Type` / `GetType()` of a pooler topology record in the multigateway, multiorch or multiadmin services                     |
| `disallowPoolerTypeEnumInGateway`              | Any `clustermetadata.PoolerType_*` constant in the multigateway                                                                    |
| `disallowRoutingStateInOrch`                   | Reading `RoutingState` of a pooler topology record in multiorch                                                                    |
| `disallowConsensusStateInGateway`              | Calling `consensus.SelfConsensusRole`, `IsActiveLeader` or `NamesSelfAsLeader` in the multigateway                                 |
| `disallowRawConsensusStatusReplicationPrimary` | Reading `ConsensusStatus.ReplicationPrimary` directly outside a `consensus` package; use `commonconsensus.ReplicationPrimaryOrNil` |
| `onlyPoolerRecordPublishesMultipooler`         | Calling `RegisterMultipooler` anywhere but `pooler_record.go`, `go/common/topoclient` and tests                                    |

## Other project-specific checks

- **`forbidigo`** rejects `os.Exit`. Return an error instead so cleanup hooks
  run. It is allowed in `main()` and `TestMain()` with `//nolint:forbidigo`; in
  `init()`, use `panic()`.
- **`sloglint`** enforces the logging conventions: context-aware calls when a
  context is in scope, the `error` key, lowercased messages and snake_case
  keys. See [Logging](../general/logging.md).
- **`make naming-lint`** requires the component names to be written as single
  words (Multipooler, Multiorch, Multigateway, Multigres, Multiadmin, in any
  uniform case). A capital letter inside the name is rejected in every tracked
  file except under `external/`.

## How to add a ruleguard rule

1. Add a function to `go/tools/ruleguard/rules.go`:

   ```go
   // myRule explains what is banned, why, and what to use instead.
   func myRule(m dsl.Matcher) {
       m.Import("some/package")

       m.Match(`pkg.Banned($*_)`).
           Where(!m.File().Name.Matches(`_test\.go$`)).
           Report("use pkg.Preferred() instead of pkg.Banned(): <reason>")
   }
   ```

   No registration is needed; `.golangci.yml` points `gocritic` at the whole
   file.

2. Scope the rule with `Where(...)`. Existing rules filter by file name
   (`m.File().Name.Matches`), by package path (`m.File().PkgPath.Matches`), and
   by expression type (`m["x"].Type.Is(...)`).
3. Write the `Report` message so that it names the replacement. It is the only
   guidance a contributor sees when the rule fires.
4. Fix or annotate the existing violations, then run golangci-lint.

The file has the build tag `ruleguard`, which `.golangci.yml` sets, so it is not
part of a normal build.

## Suppressing a finding

Use a `//nolint` comment that names the linter and gives the reason:

```go
//nolint:gocritic // CLI entry point, no parent context available
ctx := context.Background()
```

Ruleguard findings are reported under `gocritic`. Prefer fixing the code or
narrowing the rule over suppressing it.

## Pitfalls

- **Import the packages a rule names.** Existing rules call `m.Import(...)` for
  every package whose functions or types appear in a pattern or a `Where`
  clause; do the same in a new rule.
- **The DSL cannot tell a read from a write.** A pattern like `$x.Field` also
  matches an assignment target, which is why
  `disallowRawConsensusStatusReplicationPrimary` excludes the package that has
  to assign the field.
- **Generated code is not linted** (`exclusions: generated: lax`).
- **CI and the pre-commit hook may run different golangci-lint versions.** CI
  pins its version in `.github/workflows/golangci-lint.yml`; the hook in
  `misc/hooks/golangci-lint` only requires a minimum version.
