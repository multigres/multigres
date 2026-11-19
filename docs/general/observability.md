# Observability

How Multigres binaries export traces, metrics and logs, and where to add new
instrumentation. Logging conventions have their own document:
[Logging Conventions](logging.md).

## How it is wired

All telemetry goes through OpenTelemetry. `go/tools/telemetry` owns the SDK
setup (`Telemetry.InitTelemetry`), and it is called from two places:

- **Services** (multigateway, multipooler, multiorch, multiadmin,
  `pgctld server`, minigres): `ServEnv.Init` in
  `go/common/servenv/servenv_unix.go` calls it once at startup with the
  process's `ServiceIdentity`.
- **One-shot commands** (for example `pgctld init`): the root command calls
  `Telemetry.InitForCommand`, which also starts a span named after the command.

`InitTelemetry` sets up, in order: tracing, metrics, process and Go runtime
metrics, and log export. It then wraps the default HTTP client with `otelhttp`
and installs the W3C `tracecontext` and `baggage` propagators. A failure in any
of these fails `InitTelemetry`; `ServEnv.Init` logs that error and the service
keeps running without telemetry.

## Configuration

There are no Multigres flags for exporters. Everything is configured through
the standard OpenTelemetry environment variables, read by the SDK's
`autoexport` package:

| Variable                                              | Effect in Multigres                                                                                                         |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `OTEL_TRACES_EXPORTER`                                | Unset: spans go to a no-op exporter. Set (for example `otlp`): spans are batched to that exporter.                          |
| `OTEL_METRICS_EXPORTER`                               | Unset: treated as `none`. `otlp` pushes; `prometheus` serves a scrape endpoint (port from `OTEL_EXPORTER_PROMETHEUS_PORT`). |
| `OTEL_LOGS_EXPORTER`                                  | Unset: treated as `none`, and no `LoggerProvider` is created. Otherwise log records are batched to the exporter.            |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_*` | Standard OTLP endpoint and protocol settings.                                                                               |
| `OTEL_SERVICE_NAME`                                   | Overrides the `service.name` the binary would otherwise report.                                                             |
| `OTEL_RESOURCE_ATTRIBUTES`                            | Merged into the resource alongside the attributes below.                                                                    |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_CONFIG`   | See [Sampling](#sampling).                                                                                                  |

So a binary started with none of these set exports nothing.

`demo/local/run-with-otel.sh` is a working example of the full set, and
`demo/local/README.md` shows how to run it against a local collector.

### Resource attributes

Every span, metric and exported log record carries the same resource, built in
`ServEnv.Init` from the `ServiceIdentity`:

| Attribute                 | Value                                                             |
| ------------------------- | ----------------------------------------------------------------- |
| `service.name`            | The binary's service name (`minigres` for the combined process)   |
| `service.instance.id`     | `<cell>-<service-id>`, or just the service ID when no cell is set |
| `cloud.availability_zone` | The cell                                                          |
| `multigres.shard`         | The shard, when the service has one                               |
| `multigres.database`      | The database, when the service has one                            |
| `multigres.tablegroup`    | The table group, when the service has one                         |
| `service.version`         | The binary's VCS revision, when the build recorded one            |

Empty values are omitted. Local log output carries the same values under
snake_case keys (see [Logging Conventions](logging.md)).

## Traces

- **Creating spans.** Use `telemetry.Tracer()` (one shared tracer for the
  repository) or the `telemetry.WithSpan(ctx, name, fn)` helper.
- **gRPC.** The servenv gRPC server and clients made by
  `grpccommon.NewClient` attach the `otelgrpc` stats handler, which produces
  spans and `rpc.*` metrics for every RPC. `--grpc-otel-instrumentation`
  (default `true`, env `GRPC_OTEL_INSTRUMENTATION`) turns it off for the
  process's server and clients together.
- **HTTP.** The servenv HTTP server wraps its mux with `otelhttp`, and
  `http.DefaultClient` is wrapped for outgoing requests.
- **Subprocesses.** `executil.Command` passes the current span to the child
  as a `TRACEPARENT` environment variable, and a command that initializes
  telemetry through `InitForCommand` continues that trace
  (`Telemetry.WithEnvTraceparent`).
- **Logs.** A log call made with a context that carries a valid span gets
  `trace_id` and `span_id` attributes, whether or not log export is enabled.

### Sampling

The sampler is chosen as follows (`initTracing` in
`go/tools/telemetry/telemetry.go`):

1. `OTEL_TRACES_SAMPLER=multigres_custom`: the file-based sampler described
   below. `OTEL_TRACES_SAMPLER_CONFIG` must name its YAML file, or startup of
   telemetry fails.
2. Any other `OTEL_TRACES_SAMPLER` value: the OpenTelemetry SDK's own handling
   (`always_on`, `parentbased_traceidratio`, and so on).
3. No sampler set and `OTEL_TRACES_EXPORTER=none`: parent-based, never sampling
   locally rooted spans. A sampled parent from upstream is still honoured, so
   the trace stays intact for services that do export.
4. Otherwise: the SDK default (`parentbased_always_on`).

The custom sampler (`go/tools/telemetry/sampler.go`) assigns each root span to
a named category with its own sampling probability, and is wrapped in a
parent-based sampler so a trace is either complete or absent. The category
depends on the kind of span, and falls back to the `default` category, which
the file must define:

- **gRPC spans:** the method (`/package.Service/Method`), then the service
  (`/package.Service`).
- **HTTP spans:** `"METHOD /path"` as an exact match, then as a glob pattern.
- **Any other span:** the span name as an exact match, then as a glob pattern.

```yaml
categories:
  operations:
    probability: 1.0
  queries:
    probability: 0.0001
  default:
    probability: 1.0

grpc:
  services:
    "/multipoolerservice.MultipoolerService": queries
```

`demo/observability/sampling-config.yaml` is a complete example. A file that
references an undefined category, or a probability outside `0.0`–`1.0`, is
rejected at startup.

## Metrics

- **Defining instruments.** Get a meter with `otel.Meter(<name>)` and create
  instruments on it; the global `MeterProvider` is the one `InitTelemetry`
  installed.
- **Process metrics.** Every binary that initializes telemetry exports
  `process.cpu.time` (by `cpu.mode`), `process.memory.usage` (RSS) and
  `process.memory.virtual`, plus the Go runtime metrics from the OpenTelemetry
  runtime instrumentation (`go/tools/telemetry/processmetrics.go`).
- **Build info.** Every servenv service exports `multigres.build.info`, a
  gauge fixed at 1 whose attributes carry the version, VCS revision, commit
  time and Go version (`go/common/servenv/buildinfo_metrics.go`).
- **The metric catalog.** `go/observability/metriccatalog/catalog_generated.go`
  lists every instrument defined in the repository with its OpenTelemetry
  name, unit, defining package, the binaries that link it, and the Prometheus
  series it is scraped as. It is generated by scanning the code: run
  `make metrics` (also part of `make build-all`) after adding, renaming or
  removing an instrument, and commit the result. The `validate-generated-files`
  CI job fails when the file is out of date.

## HTTP endpoints

Every servenv service serves these on its `--http-port`
(`go/common/servenv/pages.go`, `http.go`):

| Path             | Purpose                                                                        |
| ---------------- | ------------------------------------------------------------------------------ |
| `/live`          | Liveness: answers as soon as the HTTP server is up                             |
| `/ready`         | Readiness: `503` until every check registered with `RegisterReadyCheck` passes |
| `/version`       | The binary's VCS revision, commit time and Go version as JSON                  |
| `/config`        | The resolved configuration                                                     |
| `/debug/pprof/*` | Go profiling endpoints, registered unless `--pprof-http=false`                 |

`/config` and `/debug/pprof/*` require a bearer token on services that install
an HTTP auth plugin (`SetAuthPlugin`); on the others they are unauthenticated.

## Reference

- SDK setup, sampler, process metrics: `go/tools/telemetry/`
- Service identity and startup: `go/common/servenv/servenv_unix.go`
- gRPC instrumentation switch: `go/common/servenv/grpc_server.go`,
  `go/tools/grpccommon/options.go`
- Metric catalog generator: `go/tools/metricsgen/`
- Example collector stack and dashboard: `demo/local/`, `demo/observability/`
