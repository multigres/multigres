# Error Handling

## Overview

`go/common/mterrors` is the error package every Multigres component uses. It
carries two kinds of information through the system:

- A **canonical RPC code** (`mtrpcpb.Code`, defined in `proto/mtrpc.proto`) that
  classifies an internal error and survives gRPC.
- A **PostgreSQL diagnostic** (`*mterrors.PgDiagnostic`): the full
  ErrorResponse or NoticeResponse a PostgreSQL client sees, including SQLSTATE,
  detail, hint and position.

The package comment in `go/common/mterrors/mterrors.go` is the reference for
the API. This guide explains which constructor to use and how an error reaches
the client.

## Where the code lives

```text
go/common/mterrors/
  mterrors.go        New, Errorf, Wrap, Wrapf, Code, RootCause, connection predicates
  state.go           WithCode, ErrorWithCode, pre-execution marker
  pgdiagnostic.go    PgDiagnostic, GatewayRejection, proto conversion
  code.go            SQLSTATE constants, MTError codes, PgDiagnostic constructors
  error_helpers.go   ExtractSQLSTATE, IsGatewayRejection, ClassifyErrorSource, ...
  grpc.go            ToGRPC, FromGRPC, FromContext
  stack.go           stack trace capture and formatting
```

## Choosing a constructor

| Situation                                                         | Use                                                                                                  |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Internal error with no PostgreSQL meaning                         | `mterrors.New(code, msg)` or `mterrors.Errorf(code, format, ...)`                                    |
| Adding context to an error you received                           | `mterrors.Wrap(err, msg)` or `mterrors.Wrapf(err, format, ...)`                                      |
| Error that must look like a native PostgreSQL error to the client | `mterrors.NewPgError(severity, sqlState, message, detail)` with a `PgSS*` constant                   |
| A condition that has a named helper                               | The helper: `NewQueryCanceled`, `NewStatementTimeout`, `NewInvalidPreparedStatementError`, and so on |
| A statement the gateway refuses by policy                         | `mterrors.NewFeatureNotSupported(message)`                                                           |
| A Multigres condition with no PostgreSQL equivalent               | An `MTError` definition: `mterrors.MTD01.New(args...)`                                               |
| A non-error diagnostic (WARNING, NOTICE, ...)                     | `mterrors.NewPgNotice(severity, sqlState, message, detail)`                                          |
| A parse failure                                                   | `mterrors.NewParseError` / `NewParseErrorAt`                                                         |
| Attaching an RPC code to an existing error                        | `mterrors.WithCode(err, code)`                                                                       |

SQLSTATE values are declared once as `PgSS*` constants in `code.go` (for
example `PgSSFeatureNotSupported = "0A000"`). Add a constant there rather than
writing the five-character string at a call site.

## Key types

### Coded errors

`New` and `Errorf` return an error that records a stack trace and an
`mtrpcpb.Code`. `mterrors.Code(err)` reads the code from anywhere in the unwrap
chain; it returns `CANCELED` or `DEADLINE_EXCEEDED` for the context sentinels
and `UNKNOWN` otherwise. Decide on the code, not on the message text.

`Wrap` and `Wrapf` add a message and a stack trace and keep the cause reachable
through both `Cause()` and `Unwrap()`, so `errors.Is`, `errors.As` and
`mterrors.Code` all see through them.

### PgDiagnostic

`PgDiagnostic` implements `error` and holds the PostgreSQL diagnostic fields
(`Severity`, `Code`, `Message`, `Detail`, `Hint`, `Position`, `Where`, `Schema`,
`Table`, `Column`, `Constraint`, ...). `MessageType` is `'E'` for an error and
`'N'` for a notice. A diagnostic received from PostgreSQL is kept intact, so the
client sees exactly what the backend sent.

Read a diagnostic out of an error with `errors.As`:

```go
var diag *mterrors.PgDiagnostic
if errors.As(err, &diag) {
    // diag.Code is the SQLSTATE
}
```

or with the helpers: `mterrors.IsErrorCode(err, codes...)`,
`mterrors.ExtractSQLSTATE(err)`, `mterrors.IsFatal(err)`.

### GatewayRejection

`NewFeatureNotSupported` returns a `*GatewayRejection`, which wraps a
`PgDiagnostic` with SQLSTATE `0A000`. It marks an error the gateway raised
itself before the statement reached a backend. `mterrors.IsGatewayRejection`
lets the gateway avoid marking the session's transaction as aborted for such an
error, because the backend transaction is still open.

### MTError

An `MTError` is a template for a Multigres-specific error. Its ID is a
five-character code that is sent in the SQLSTATE field, and its description is
sent as the detail:

```go
MTB01 = &MTError{
    ID: "MTB01", Severity: "ERROR",
    Format:      "failover buffer full",
    Description: "The request was evicted because the failover buffer is at capacity. Retry the query.",
}
```

`MTB01.New(args...)` builds the `PgDiagnostic`; `NewWithDetail` replaces the
description with a caller-supplied detail. The defined codes are in `code.go`:
`MTD01`-`MTD08`, `MTE01`, `MTB01`-`MTB03`, `MTF01` and `MTF02`. Use an `MTError`
only when no PostgreSQL SQLSTATE fits; otherwise use `NewPgError`.

## How an error reaches the client

```text
PostgreSQL backend
   |  ErrorResponse parsed into *PgDiagnostic
   v
multipooler
   |  mterrors.ToGRPC(err): gRPC status + RPCError detail
   |  (message, mtrpc code, optional pg_diagnostic)
   v
multigateway
   |  mterrors.FromGRPC(err): rebuilds the *PgDiagnostic,
   |  wrapped with the RPC code when one was sent
   v
pgprotocol server (Conn.writeError)
   |  PgDiagnostic found  -> sent as is
   |  code UNAVAILABLE    -> 08006 "connection to server lost"
   |  anything else       -> XX000 with the error text
   v
client
```

Details worth knowing:

- `ToGRPC` maps the `mtrpc` code to the gRPC status code when one exists and
  attaches an `RPCError` detail. Messages are truncated to fit gRPC's header
  size limit.
- `FromGRPC` returns `io.EOF` unchanged, because stream readers compare against
  it. It turns gRPC `DeadlineExceeded` and `Canceled` into the PostgreSQL
  statement-timeout and query-canceled diagnostics.
- `FromContext` does the same translation for calls made in-process (Minigres),
  where no gRPC boundary exists. It keeps a `FATAL` diagnostic as is.
- A generic Go error with no diagnostic reaches the client as SQLSTATE `XX000`
  with its message. If the client should see a specific SQLSTATE, return a
  `PgDiagnostic`.

Notices take a separate path: they are streamed to the client as they arrive,
as described in the package comment.

## Classifying errors

Predicates in the package answer the common questions. Prefer them to matching
on message text.

| Function                         | True when                                                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `IsConnectionError(err)`         | Reconnect-and-retry is worthwhile: transport failures, SQLSTATE class `08`, and PostgreSQL shutdown SQLSTATEs |
| `IsConnectionDead(err)`          | The connection must be discarded: `IsConnectionError` or a `FATAL`/`PANIC` diagnostic                         |
| `IsFatal(err)`                   | The error carries a `FATAL` or `PANIC` diagnostic                                                             |
| `IsGatewayRejection(err)`        | The gateway raised the error before the statement reached a backend                                           |
| `IsStalePreparedStatementError`  | Re-parsing a prepared statement may recover (`0A000` cached plan, `22P02`, `42883`)                           |
| `IsPreExecutionUnavailable(err)` | The failure happened before execution, so a retry is safe (set with `MarkPreExecutionUnavailable`)            |
| `ClassifyErrorSource(err)`       | Returns `backend`, `internal`, `routing` or `client` for metric attribution                                   |

## Pitfalls

- **Never put credentials or tokens in an error message.** The text of an error
  without a diagnostic is sent to the client as the `XX000` message.
- **Do not build SQLSTATE errors ad hoc.** Use the constructors so the code,
  severity and message stay consistent with PostgreSQL.
- **`fmt.Errorf` drops the stack trace.** The package asks for `Wrapf` when
  propagating errors. `errors.As` still finds a `PgDiagnostic` through
  `fmt.Errorf("...: %w", err)`, but no stack is recorded.
- **The parser cannot use `mterrors`.** It returns its own
  `ParseSyntaxError`; convert it at the serving boundary with `NewParseErrorAt`.
  See [SQL parser and AST](sql_parser.md).
- **The evaluation engine raises errors by panic.** `pgerror.Ereport` panics
  with a `PgDiagnostic` and `pgerror.Recover` converts it back to an error at
  the evaluation boundary.
- **Stack traces are opt-in.** They are printed only when an error is formatted
  with `%v` and the `--log-err-stacks` flag is set.
