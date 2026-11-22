# SCRAM Key Passthrough: Multipooler → PostgreSQL

## Overview

Multipooler authenticates to its backing PostgreSQL as the real end user, on every backend connection, using SCRAM-SHA-256 — without ever handling a plaintext password. The cryptographic material needed to authenticate to PostgreSQL is derived during the client's SCRAM handshake to Multigateway, forwarded to Multipooler on each query RPC, and used when Multipooler dials a new backend connection.

This document describes the design as a whole: where the keys live, how they travel, how they are consumed, what the security properties are, and what the boundaries of the approach are.

## Background

### Why SCRAM keys can be passed through

PostgreSQL's SCRAM-SHA-256 verifier stores only `StoredKey = H(ClientKey)` and `ServerKey`. The client proves knowledge of `ClientKey` by sending `ClientProof = ClientKey XOR ClientSignature`, where `ClientSignature = HMAC(StoredKey, AuthMessage)`. A verifier that sees the proof can validate it _and_ recover the plaintext `ClientKey` by XORing the proof with the signature it just computed.

This property — that a successful SCRAM handshake reveals `ClientKey` to the verifier — is what makes passthrough possible.

With `ClientKey` plus `ServerKey` (which Multigateway already has, because Multigateway fetched the verifier from `pg_authid` via Multipooler's admin pool as part of the incoming handshake), Multigateway can impersonate the user against any other SCRAM-SHA-256 verifier of the same password — including the backing PostgreSQL.

Our SCRAM library exposes this via `scram.ScramAuthenticator.ExtractedKeys()`, and the wire-protocol client accepts pre-computed keys via `newScramClientWithKeys(...)` in `pgprotocol/client`.

### Why this matters

Three things are true in the final design that are not true of a trust-on-the-socket design:

1. **Identity reaches PostgreSQL.** `current_user`, `pg_stat_activity`, audit logs, row-level security predicates — all see the real user. PostgreSQL is no longer asked to authorize on the basis of "some trusted local process asserted a user name."
2. **`pg_hba.conf` is authoritative.** Access decisions live in one place. The default `pg_hba.conf` template has no `trust` lines, so there is no configuration path that lets an unauthenticated local process log into PostgreSQL.
3. **Plaintext passwords never exist in Multigateway or Multipooler memory.** Only derived `ClientKey`/`ServerKey`, session-scoped, zeroized at client session end.

## Goals and Non-Goals

### Goals

- Multipooler authenticates to PostgreSQL as the real user on every backend connection opened for that user's pool.
- Neither Multigateway nor Multipooler ever holds a plaintext password.
- Keys are scoped to the client session, never persisted, zeroized on close, never logged.
- Multipooler's admin pool (used for bootstrap operations such as reading `pg_authid`) continues to work using its configured superuser credential, independently of per-user passthrough.

### Non-Goals

- **Client-certificate authentication.** Cert-authenticated sessions do not produce SCRAM keys; how Multipooler dials for them is a separate design.
- **Channel binding on the pooler-to-PostgreSQL dial.** Multigateway advertises SCRAM-SHA-256-PLUS to clients that connect over TLS (see _Channel binding_), but the passthrough dial itself uses plain SCRAM-SHA-256. Internal gRPC between Multigateway and Multipooler is mTLS-protected, which covers the bulk of the MITM story for the wire segment where keys are exposed.
- **Per-user key caching on Multipooler.** A pool caches keys at first-creation and relies on rotation-triggered eviction (see _Password rotation_) to refresh them. A short-TTL key cache is a possible future optimization for scenarios where eviction churn becomes visible.
- **Credential storage outside PostgreSQL.** All verifiers continue to live in `pg_authid`.
- **Fallback to MD5 or cleartext auth.** Both are explicitly rejected by the wire-protocol client.

## Design

### Where keys live

Keys live on the Multigateway-side client `Conn` struct (`pgprotocol/server/conn.go`), alongside `user`, `database`, and `params`. They are set exactly once, immediately after `ScramAuthenticator.HandleClientFinal` confirms authentication, by calling `ExtractedKeys()`.
They are zeroized by `zeroizeCredentials`, which runs from `Conn.Close` and from `DetachConn` (the connection-hijack path), after handler state is released.

Keys are session-scoped. A PostgreSQL password change invalidates only future client sessions; already-open backend connections remain usable until they close through their normal lifecycle.

Keys are never written to disk, never included in logs, never surfaced in error messages. Any new log sites that touch the auth path are audited for leakage.

### Wire format

A `query.UserAuth` message holds `client_key` and `server_key` as bytes. It is embedded as an optional field on `query.ExecuteOptions`.
Because `ExecuteOptions` rides on every query-path RPC — `ExecuteQuery`, `StreamExecute`, `PortalStreamExecute`, `Describe`, `CopyBidiExecute`, `ConcludeTransaction`, `DiscardTempTables`, `ReleaseReservedConnection`, and the same requests multiplexed over `ExecuteStream` — a single field covers the full query path.
`StreamReplication` does not carry `ExecuteOptions`; its `StreamReplicationInit` message has its own `user_auth` field.

Multigateway populates `UserAuth` from the session's captured keys on every outbound query RPC. Internal gRPC is mTLS-protected, so keys in flight are encrypted on the wire.

### Multipooler dial logic

`pgprotocol/client.Config` carries optional `ScramClientKey` and `ScramServerKey`. The client-side auth dispatch in `client/startup.go:handleAuthenticationRequest` branches on their presence: when both are set, it constructs a SCRAM client via `newScramClientWithKeys(conn, user, scramClientKey, scramServerKey)`; otherwise it uses the password path.

`connpoolmanager.Manager` routes keys from the inbound RPC into the user-pool backend dial. User pools are keyed by username and carry the session's keys into their `client.Config` at the pool's first-time creation.

Because `ClientKey` is deterministic over `(user, password)`, keys cached on the pool are correct for every concurrent session of that user as long as the password is stable — see _Password rotation_ below for the self-heal.

The admin pool is unaffected: it authenticates with a configured superuser password (see _`pg_hba.conf`_), independent of any user's session, and is what makes `GetAuthCredentials` (the `pg_authid` lookup that feeds the handshake) possible in the first place.

### Password rotation

`ClientKey` is a function of `SaltedPassword`, so rotating a user's password in `pg_authid` makes every pool whose `ClientConfig` was built from the old verifier unable to dial a fresh backend connection — the new pg_authid verifier rejects the proof. Without intervention, the pool stays in place and keeps failing authentication on every refill attempt.

The manager self-heals. `withReopenRetry` detects a class-28 SQLSTATE (`28P01` or any other _Invalid Authorization Specification_ code) on the dial path, evicts the stale user pool from the snapshot, and retries once using the triggering session's keys.

Those keys are known-current: they were derived moments earlier during the session's SCRAM handshake at Multigateway against whatever verifier `pg_authid` holds right now. A concurrent session racing the same rotation may evict first, in which case the late caller's eviction is a no-op and it retries against the fresh pool.

The retry is single-shot. If the retry itself auth-fails — e.g. the retrier is a long-running session still carrying old keys — the clean class-28 error propagates to the client, who reconnects, re-handshakes against the new verifier, and their reconnect is what populates a fresh pool.

### `pg_hba.conf`

The default template (`config/postgres/pg_hba_template.conf`) requires `scram-sha-256` on every line: local socket connections, host connections, and replication connections. There are no `trust` lines.

Multipooler's admin pool (superuser queries: `GetAuthCredentials`, heartbeat, replication tracking) therefore authenticates with a password.
`connpoolmanager.Config.ResolvePgPassword` reads it from `--connpool-admin-password-file` / `CONNPOOL_ADMIN_PASSWORD_FILE` / `POSTGRES_PASSWORD_FILE`, or from the `CONNPOOL_ADMIN_PASSWORD` / `POSTGRES_PASSWORD` environment variables.
When no source is configured, or the configured one is empty, Multipooler fails to start. A user pool dialed for the admin user without session keys (internal queries that carry no SCRAM session) falls back to the same password.

Standbys authenticate replication connections via SCRAM using a passfile referenced from `primary_conninfo`.

### Channel binding

When a client connects over TLS, Multigateway advertises `SCRAM-SHA-256-PLUS` alongside `SCRAM-SHA-256`, binding to the `tls-server-end-point` hash of the server certificate it presented (`server/startup.go`, `scram/channel_binding.go`).
If the hash cannot be computed, authentication falls back to advertising `SCRAM-SHA-256` only. Plaintext sessions advertise `SCRAM-SHA-256` only. Key extraction is the same for both mechanisms.

### Failure modes

- **Stale keys on an existing pool (password rotated in PostgreSQL).** The first dial from the pool with stale keys fails with class-28 SQLSTATE. The manager evicts the pool (see _Password rotation_) and retries with the triggering session's fresh keys. If the retry also auth-fails, the error propagates and the client reconnects to re-derive keys.
- **Missing keys on an RPC.** A pool created for a non-admin user without keys dials with an empty password, so PostgreSQL rejects the connection with an authentication error. In steady state this should not happen — every authenticated session has keys.
- **Admin pool failure.** Independent of passthrough. If the admin pool cannot reach PostgreSQL, Multipooler cannot serve `GetAuthCredentials` and no handshake can complete; passthrough is a no-op because no session ever reaches the authenticated state.

### Security properties

- **No plaintext password in process memory.** Only derived `ClientKey`/`ServerKey` ever exist in Multigateway or Multipooler.
- **Session-scoped exposure.** A live-memory attacker obtains keys only for currently-active sessions, not a historical set.
- **Authoritative `pg_hba`.** Access control decisions are made by PostgreSQL, not bypassed by socket trust.
- **Encrypted transit for keys.** Internal gRPC is mTLS; the RPC carrying `UserAuth` is confidential on the wire.

## Open Items / Future Work

- **Per-user key cache on Multipooler.** If RPC payload overhead or repeated handshakes show up in benchmarks, a short-TTL cache with refresh-on-auth-failure is the nearest prior art (Supavisor uses this pattern).
- **Cert-auth integration.** Cert-authenticated sessions do not produce SCRAM keys. Options include a pooler-side service credential per role, a separate cert-delegation dial path, or rejecting cert auth for the pool path entirely. Deferred to its own design.
- **Channel binding beyond the client-to-gateway handshake.** The external handshake, where TLS cert trust is broadest, already supports SCRAM-SHA-256-PLUS (see _Channel binding_).
  The internal gateway-to-pooler segment does not run SCRAM at all — the keys are a gRPC payload there, and confidentiality is handled by mTLS. The pooler-to-PostgreSQL handshake is local and has no meaningful MITM surface to bind against.

## Glossary

- **ClientKey** — `HMAC(SaltedPassword, "Client Key")`. What the client proves knowledge of. Recovered by the verifier from the proof via XOR.
- **ServerKey** — `HMAC(SaltedPassword, "Server Key")`. What the server proves knowledge of in the final message for mutual auth. Stored server-side; never secret from the storing server.
- **StoredKey** — `H(ClientKey)`. The verifier. Cannot be used to forge a proof; only to verify one.
- **Passthrough** — using `(ClientKey, ServerKey)` obtained at one SCRAM verifier to authenticate to another SCRAM verifier of the same password.
