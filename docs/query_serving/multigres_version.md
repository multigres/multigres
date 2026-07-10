# `SHOW multigres_version`

## Overview

`SHOW multigres_version` reports the build identity of the multigateway a
client is connected to. It lets an operator check which gateway build is
serving them without shell access to run a binary with `--version`, and
makes it easy to include the gateway version in a bug report.

```console
psql> SHOW multigres_version;
              multigres_version
----------------------------------------------------
 Multigres (cb4f578bfd76, modified, 2026-07-06) built with go1.26.0
(1 row)
```

The command is answered entirely by the gateway and is **never forwarded to
PostgreSQL**. It works in both the simple and extended (prepared-statement)
query protocols.

## `multigres_version` is not a gateway-managed variable

Despite the `SHOW <name>` surface, `multigres_version` is **not** a
[gateway-managed variable](./gateway_managed_variables.md). It is a read-only
pseudo-variable:

- It is not in the `gatewayManagedVariables` registry.
- It has no `SET` / `RESET` / `set_config` / transaction behavior. A
  `SET multigres_version = ...` falls through to PostgreSQL, which rejects it as
  an unrecognized configuration parameter — matching how PostgreSQL treats its
  own read-only `server_version`.
- Its value is a process-wide constant, so it reads no per-connection state.

## Version string

The string comes from `servenv.AppVersion()`, formatted from the binary's
build identity (`servenv.readBuildSnapshot()`, backed by
`runtime/debug.BuildInfo`) — the same source already used by the `/version`
HTTP endpoint and the OpenTelemetry `service.version` attribute. It contains
the VCS revision, a `modified` marker when the working tree was dirty at build
time, the commit date, and the Go toolchain version.

Two things to know:

- **Un-stamped builds.** If the binary was built without VCS stamping (notably
  a build from a linked git worktree, where Go does not embed VCS settings),
  the revision is unavailable and the string degrades to
  `Multigres (unknown revision) built with <go>`. Normal checkouts and CI
  builds stamp correctly.
- **Release tag.** `servenv.version` is an empty seam reserved for a future
  release tag stamped via `-ldflags "-X .../servenv.version=v0.1.0"`. When set,
  it is prefixed: `Multigres v0.1.0 (<revision>, ...)`.

## Code organization

| File                             | Role                                                               |
| -------------------------------- | ------------------------------------------------------------------ |
| `common/servenv/buildinfo.go`    | `AppVersion()` / `formatAppVersion()` — the version string         |
| `common/constants/gateway.go`    | `MultigresVersionVariable` — the pseudo-variable name              |
| `planner/variable_show_stmt.go`  | Intercepts `SHOW multigres_version` before the backend fallthrough |
| `engine/gateway_show_version.go` | `GatewayShowVersion` primitive; the `Describe` helpers             |
| `executor/executor.go`           | Serves the extended-protocol `Describe` locally                    |

## Extended protocol notes

Serving a gateway-only pseudo-variable over the extended protocol needs two
things beyond the simple-query path, because PostgreSQL has no such GUC:

- **Describe** is normally forwarded straight to the backend. For
  `multigres_version` the executor intercepts it and returns a synthetic
  `StatementDescription` (no bind parameters, one text column); otherwise the
  backend would reject the unknown parameter at `Describe` time.
- **Execute** must not re-send `RowDescription`. The wire layer emits it
  whenever a result carries `Fields`, so `GatewayShowVersion` attaches column
  metadata only when a `Describe('P')` was folded into the `Execute`
  (`includeDescribe`). Attaching it otherwise would send an illegal second
  `RowDescription` mid-`Execute`.
