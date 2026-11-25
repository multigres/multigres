# Client Connection Startup

## Overview

This document describes what multigateway does between accepting a TCP
connection and sending the first `ReadyForQuery`: encryption negotiation, the
startup packet, authentication, and the timeouts that bound them. The code is
in `go/common/pgprotocol/server/` (`startup.go`, `direct_tls.go`,
`proxy_protocol.go`, `conn.go`); multigateway configures it in
`go/services/multigateway/init.go`. The primary listener and the optional
replica-reads listener are built with the same settings.

Related documents:

- [proxy_protocol.md](./proxy_protocol.md) for PROXY v2 headers and TLS offload.
- [scram_passthrough_design.md](./scram_passthrough_design.md) for how the keys
  captured during authentication reach PostgreSQL.
- [query_cancellation_design.md](./query_cancellation_design.md) for
  `CancelRequest` routing.

## Startup sequence

`Conn.serve` runs the startup phase, then the command loop:

1. **Authentication deadline.** A socket deadline of `--authentication-timeout`
   is set before anything is read. It covers every step below and is cleared
   once authentication completes.
2. **PROXY v2 header** (`maybeHandleProxyProtocol`), if the first byte matches
   the PROXY v2 signature.
3. **Direct TLS** (`maybeHandleDirectTLS`). A connection that opens with a TLS
   ClientHello is upgraded before any startup packet is read. Direct TLS
   requires the `postgresql` ALPN protocol unless TLS offload applies. If TLS
   is not configured the connection is closed without a reply.
4. **Startup packet dispatch** (`readAndDispatchStartup`), which loops until a
   `StartupMessage` arrives:
   - `SSLRequest`: answered `S` and upgraded to TLS when certificates are
     configured, otherwise `N`. Bytes already buffered after the `S` reply
     abort the connection (CVE-2021-23222). An `SSLRequest` inside an
     established TLS session is answered `N`.
   - `GSSENCRequest`: always answered `N`; GSSAPI encryption is not supported.
   - `CancelRequest`: handed to the cancel handler, then the connection is
     closed. It needs no authentication and is accepted without TLS even when
     TLS is required.
   - `StartupMessage`: rejected with `FATAL` `28000` when `--pg-require-ssl` is
     set and TLS is not established.
5. **Startup parameters** (`handleStartupMessage`), described below.
6. **Authentication** (`authenticate`), described below.
7. **Completion** (`finishAuth`): `AuthenticationOk`, `BackendKeyData`, the
   initial `ParameterStatus` messages, then `ReadyForQuery`. The connection is
   registered for cancel lookup once `BackendKeyData` is sent.

A connection that asked for `replication=database` is then handed to the
handler's replication stream instead of the command loop.

## Startup parameters

- `options` is parsed like `PGOPTIONS`: `-c name=value`, `-cname=value` and
  `--name=value` (hyphens in the name become underscores). The parsed settings
  are merged into the startup parameters and `options` itself is dropped. A
  `replication` setting inside `options` is ignored.
- `database` defaults to the user name.
- `replication` is parsed with PostgreSQL's boolean rules plus `database`. An
  invalid value is a `FATAL` before authentication. The key is removed so it is
  never replayed to a backend as a GUC.
- `multigres.unsafe_connection` (and the deprecated alias
  `multigres.direct_connection`) is removed from the parameters; a true value
  latches unsafe mode for the connection, and a value that is not a Boolean is a
  `FATAL`. See
  [unsafe_statement_rejection.md](./unsafe_statement_rejection.md#per-connection-unsafe-mode-multigresunsafe_connection).
- Every remaining parameter is vetted with the guards a `SET` of the same GUC
  gets (`startupParamError`): a cluster-managed GUC such as
  `synchronous_commit` is refused, and so is a `search_path` that names
  `pg_temp`. Both are `FATAL`. Without this, connect-time parameters would
  bypass the planner's `SET` checks, because they are replayed onto pooled
  backends as session settings.

The handler later removes gateway-managed variables (`statement_timeout`,
`idle_session_timeout`) from the parameters and uses them as that connection's
defaults; see [gateway_managed_variables.md](./gateway_managed_variables.md).

## Authentication

Clients authenticate with SCRAM-SHA-256. The gateway holds no credentials:
`PoolerCredentialProvider` (`go/services/multigateway/auth/`) fetches the
role's SCRAM verifier and its `rolreplication` flag with one
`GetAuthCredentials` RPC to a multipooler, before any SASL message is sent.

| Lookup result                | Client sees                                                        |
| ---------------------------- | ------------------------------------------------------------------ |
| Role cannot log in           | `FATAL` `28000`, PostgreSQL's wording                              |
| Unknown role, or no password | `FATAL` `28P01` `password authentication failed for user "<name>"` |
| Password expired             | The same `28P01` message                                           |

Over TLS the gateway also advertises `SCRAM-SHA-256-PLUS` with
`tls-server-end-point` channel binding. After a successful exchange the
connection keeps the extracted `ClientKey` and `ServerKey` for passthrough.

For a replication connection, the role must have `rolreplication`. The check
runs after SCRAM succeeds and before `AuthenticationOk`, and fails with
`FATAL` `42501` `must be superuser or replication role to start walsender`.

A trust-auth provider can be installed on the listener; it exists for tests.

## Timeouts

| Setting                    | Default | Behaviour                                                                                                                                                     |
| -------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--authentication-timeout` | `60s`   | Bounds the whole startup phase. On expiry the client gets `FATAL` `08006` `canceling authentication due to timeout`. `0` uses the default; negative disables. |
| `idle_session_timeout`     | `0`     | Gateway-managed variable. Applied as a read deadline while waiting for the first message of a new command cycle, only when no transaction is open.            |

When `idle_session_timeout` expires the gateway sends `FATAL` `57P05`
`terminating connection due to idle-session timeout` and closes the
connection. The deadline is cleared as soon as the first byte of the next
message arrives, so it never governs message bodies or query execution.

## Configuration

| Flag                       | Env var                     | Default   | Description                                                                         |
| -------------------------- | --------------------------- | --------- | ----------------------------------------------------------------------------------- |
| `--pg-port`                | `MT_PG_PORT`                | `5432`    | PostgreSQL protocol listen port                                                     |
| `--pg-bind-address`        | `MT_PG_BIND_ADDRESS`        | `0.0.0.0` | Address the listener binds to                                                       |
| `--pg-tls-cert-file`       | `MT_PG_TLS_CERT_FILE`       | empty     | TLS certificate. Must be set together with the key file                             |
| `--pg-tls-key-file`        | `MT_PG_TLS_KEY_FILE`        | empty     | TLS private key                                                                     |
| `--pg-require-ssl`         | `MT_PG_REQUIRE_SSL`         | `false`   | Reject plaintext sessions. The gateway fails to start without a certificate and key |
| `--authentication-timeout` | `MT_AUTHENTICATION_TIMEOUT` | `60s`     | See [Timeouts](#timeouts)                                                           |

The certificate and key are loaded once at startup. The TLS configuration
requires TLS 1.2 or later.

## Metrics

| Metric                                       | Description                                                     |
| -------------------------------------------- | --------------------------------------------------------------- |
| `mg.gateway.auth.attempts`                   | Authentication attempts by outcome                              |
| `mg.gateway.auth.scram.duration`             | Duration of the SCRAM exchange                                  |
| `mg.gateway.auth.credential_lookup.duration` | Duration of the `GetAuthCredentials` lookup                     |
| `mg.gateway.auth.credential_lookup.rate`     | Credential lookups                                              |
| `mg.gateway.tls.connections`                 | TLS connections, by negotiation style, version and cipher suite |
| `mg.gateway.tls.handshake.duration`          | TLS handshake duration                                          |
| `mg.gateway.tls.plaintext_rejected`          | Plaintext sessions rejected because TLS is required             |
| `mg.gateway.tls.direct_rejected`             | Direct TLS connections rejected                                 |
| `mg.gateway.tls.sslrequest_declined`         | `SSLRequest`s declined because TLS is not configured            |
