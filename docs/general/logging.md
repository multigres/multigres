# Logging Conventions

How Multigres emits structured logs, which conventions are **enforced** by CI,
and which are **guidance** that require human judgment.

## How logging works

All services log through Go's `log/slog`. The handler is built in one place —
`buildHandler` in `go/common/servenv/logging.go` — so every service produces the
same record shape:

```json
{
  "time": "…",
  "level": "ERROR",
  "msg": "recovery action failed",
  "entity_id": "…",
  "error": "…"
}
```

When the `context.Context` passed to a log call carries an active trace span,
`go/tools/telemetry` adds `trace_id` and `span_id` to the record. This is why
context-aware log calls matter (see below).

## Enforced conventions

These are checked by `sloglint` (see `.golangci.yml`). A violation fails CI.

| Convention                                       | Rule                               | What it means                                                                                                                                                                            |
| ------------------------------------------------ | ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Use context-aware variants when a context exists | `sloglint: context: scope`         | If a `context.Context` is in scope, use `InfoContext`/`ErrorContext`/… (so trace correlation is never dropped). Plain `Info`/`Error` are allowed **only** where no context is available. |
| Error values use the `error` key                 | `sloglint: forbidden-keys: [err]`  | Write `"error", err`, never `"err", err`.                                                                                                                                                |
| Messages start lowercased                        | `sloglint: msg-style: lowercased`  | `"failed to open connection"`, not `"Failed to open connection"`.                                                                                                                        |
| Attribute keys are snake_case                    | `sloglint: key-naming-case: snake` | `backup_name`, `proposed_term`. Not `backupName` or `backup.name`.                                                                                                                       |

Error **values** need no special handling: slog already renders anything
implementing the `error` interface via its `Error()` string, so `"error", err`
serializes to a string, not an empty object.

### Documented exceptions

- **Operation-name / proper-noun message prefixes.** Messages that intentionally
  begin with an operation name or proper noun — `"MonitorPostgres: …"`,
  `"PostgreSQL is ready"`, `"SetPrimary: …"` — keep their capitalization with an
  inline `//nolint:sloglint`. These read better with the identifier up front.
- **Query-log OpenTelemetry keys.**
  `go/services/multigateway/handler/querylog.go` is excluded from the snake_case
  check. Its keys follow the OpenTelemetry semantic-convention registry
  (`db.namespace`, `db.operation.name`, `error.source`, …) and use the same
  names as the query's trace and metric attributes, so one field name is shared
  across all three. See "Attribute keys and OpenTelemetry" below.

## Lifecycle events

State-machine and orchestration lifecycle events are emitted through
`eventlog.Emit` (`go/common/eventlog`), not raw log calls. `Emit` uses the
event's canonical type as the record message (`primary.promotion`,
`consensus.recruit`, `node.join`) and attaches `event_type` and `outcome`
attributes. Failed outcomes log at `ERROR`; everything else at `INFO`.

Emit an event when you want a durable, structured record of a lifecycle
transition; use ordinary log calls for everything else.

## Guidance (not enforced)

These require judgment and cannot be mechanically enforced, but reviewers should
watch for them.

### Log at the right level

`ERROR` means something needs attention. Normal control flow and expected
conditions are not errors:

```go
// Wrong: a normal skip logged at ERROR.
e.logger.ErrorContext(ctx, "skipping backup with mismatched shard", "backup_id", id)

// Better: it's an expected condition.
e.logger.WarnContext(ctx, "skipping backup with mismatched shard", "backup_id", id)
```

There is no lint rule for this — "an `ERROR` call must carry an `error`" would be
wrong, because plenty of legitimate errors have no Go `error` value (an invariant
check, a failed precondition, a panic value carried under its own key). Choosing
the level, and whether an `error` belongs, is a per-call-site decision.

### Prefer a real context over a synthetic one

`context: scope` only requires `ErrorContext` where a context already exists. If
a function has no `context.Context`, plain `Error` is correct — do **not** invent
a `context.Background()` just to use the context variant, since it carries no
trace correlation. When trace context would be valuable, the right fix is to
thread a real `ctx` into the function, which is an API change, not a lint toggle.

### Keep messages stable; put variables in attributes

The message should be a low-cardinality constant so logs group and aggregate
cleanly; variable data belongs in attributes.

```go
// Wrong: variable data in the message fragments aggregation.
logger.InfoContext(ctx, fmt.Sprintf("promoted %s to primary", node))

// Right.
logger.InfoContext(ctx, "promoted node to primary", "node", node)
```

### Never log secrets

Do not log credentials, tokens, or sensitive query data. This is a security
boundary, not a style preference.

## Attribute keys and OpenTelemetry

snake_case is not "anti-OTel." OpenTelemetry's own attribute-naming guidance is
**dotted namespaces + snake_case leaves** — e.g. `http.response.status_code` has
a snake leaf. Dots denote a real namespace registry (`db.*`, `error.*`), so:

- Use the **semantic-convention dotted name** where a concept has a registered
  one and you want it to line up with our trace and metric attributes (the query
  log).
- Use **snake_case leaves** for ordinary application fields (`node_name`,
  `rows_returned`). Blanket-dotting these — `rows.returned` — would invent a
  namespace hierarchy that does not exist and breaks OTel's own rule.

## Reference

- Base logger: `go/common/servenv/logging.go`
- Lifecycle events: `go/common/eventlog/`
- Trace context injection: `go/tools/telemetry/`
- Query-log OTel attributes: `go/services/multigateway/handler/querylog.go`
- Enforcement config: `.golangci.yml` (`sloglint` settings)
