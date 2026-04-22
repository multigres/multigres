# SCRAM Key Passthrough: MultiPooler → PostgreSQL

## Overview

MultiPooler authenticates to its backing PostgreSQL as the real end user, on every backend connection, using SCRAM-SHA-256 — without ever handling a plaintext password. The cryptographic material needed to authenticate to PostgreSQL is derived during the client's SCRAM handshake to MultiGateway, forwarded to MultiPooler on each query RPC, and used when MultiPooler dials a new backend connection.

This document describes the design as a whole: where the keys live, how they travel, how they are consumed, what the security properties are, and what the boundaries of the approach are.

## Background

### Why SCRAM keys can be passed through

PostgreSQL's SCRAM-SHA-256 verifier stores only `StoredKey = H(ClientKey)` and `ServerKey`. The client proves knowledge of `ClientKey` by sending `ClientProof = ClientKey XOR ClientSignature`, where `ClientSignature = HMAC(StoredKey, AuthMessage)`. A verifier that sees the proof can validate it *and* recover the plaintext `ClientKey` by XORing the proof with the signature it just computed.

This property — that a successful SCRAM handshake reveals `ClientKey` to the verifier — is what makes passthrough possible. With `ClientKey` plus `ServerKey` (which MultiGateway already has, because MultiGateway fetched the verifier from `pg_authid` via MultiPooler's admin pool as part of the incoming handshake), MultiGateway can impersonate the user against any other SCRAM-SHA-256 verifier of the same password — including the backing PostgreSQL.

Our SCRAM library exposes this via `scram.ScramAuthenticator.ExtractedKeys()`, and the wire-protocol client accepts pre-computed keys via `client.newScramClientWithKeys(...)`.

### Why this matters

Three things are true in the final design that are not true of a trust-on-the-socket design:

1. **Identity reaches PostgreSQL.** `current_user`, `pg_stat_activity`, audit logs, row-level security predicates — all see the real user. PostgreSQL is no longer asked to authorize on the basis of "some trusted local process asserted a user name."
2. **`pg_hba.conf` is authoritative.** Access decisions live in one place. There is no configuration path that lets an unauthenticated local process log into PostgreSQL (except replication and the configured superuser, preserved intentionally).
3. **Plaintext passwords never exist in MultiGateway or MultiPooler memory.** Only derived `ClientKey`/`ServerKey`, session-scoped, zeroized at client session end.

## Goals and Non-Goals

### Goals

- MultiPooler authenticates to PostgreSQL as the real user on every backend connection opened for that user's pool.
- Neither MultiGateway nor MultiPooler ever holds a plaintext password.
- Keys are scoped to the client session, never persisted, zeroized on close, never logged.
- MultiPooler's admin pool (used for bootstrap operations such as reading `pg_authid`) continues to work using its configured superuser credential, independently of per-user passthrough.

### Non-Goals

- **Client-certificate authentication.** Cert-authenticated sessions do not produce SCRAM keys; how MultiPooler dials for them is a separate design.
- **SCRAM-SHA-256-PLUS (channel binding).** Internal gRPC between MultiGateway and MultiPooler is mTLS-protected, which covers the bulk of the MITM story for the wire segment where keys are exposed.
- **Per-user key caching on MultiPooler.** Possible optimization; not required for correctness.
- **Credential storage outside PostgreSQL.** All verifiers continue to live in `pg_authid`.
- **Fallback to MD5 or cleartext auth.** Both are explicitly rejected by the wire-protocol client.

## Design

### Where keys live

Keys live on the MultiGateway-side client `Conn` struct (`pgprotocol/server/conn.go`), alongside `user`, `database`, and `params`. They are set exactly once, immediately after `ScramAuthenticator.HandleClientFinal` confirms authentication, by calling `ExtractedKeys()`. They are zeroized exactly once, inside `Conn.Close`, after handler state is released.

Keys are session-scoped. A PostgreSQL password change invalidates only future client sessions; already-open backend connections remain usable until they close through their normal lifecycle.

Keys are never written to disk, never included in logs, never surfaced in error messages. Any new log sites that touch the auth path are audited for leakage.

### Wire format

A `query.UserAuth` message holds `client_key` and `server_key` as bytes. It is embedded as an optional field on `query.ExecuteOptions`. Because `ExecuteOptions` rides on every query-path RPC — `StreamExecute`, `PortalStreamExecute`, `Describe`, `CopyBidiExecute`, `ConcludeTransaction`, `DiscardTempTables`, `ReleaseReservedConnection` — a single field covers the full query path.

MultiGateway populates `UserAuth` from the session's captured keys on every outbound query RPC. Internal gRPC is mTLS-protected, so keys in flight are encrypted on the wire.

### MultiPooler dial logic

`pgprotocol/client.Config` carries optional `ClientKey` and `ServerKey`. The client-side auth dispatch in `client/startup.go:handleAuthenticationRequest` branches on their presence: when set, it constructs a SCRAM client via `newScramClientWithKeys(user, clientKey, serverKey)`; otherwise it uses the password path.

`connpoolmanager.Manager` routes keys from the inbound RPC into the user-pool backend dial. User pools are keyed by username and carry the session's keys into their `client.Config` at dial time. Because keys are a property of the user session, distinct sessions for the same user carry their own keys independently — the pool's identity-of-user semantics are unchanged.

The admin pool is unaffected: it authenticates with a configured superuser password, independent of any user's session, and is what makes `GetAuthCredentials` (the `pg_authid` lookup that feeds the handshake) possible in the first place.

### `pg_hba.conf`

`pg_hba.conf` grants `scram-sha-256` for all non-replication host access and for local socket connections from any user other than the configured admin. The blanket `local all all trust` line no longer exists.

A narrow `local all <admin-user> trust` line is intentionally preserved as the bootstrap path for MultiPooler's admin pool (superuser queries: `GetAuthCredentials`, heartbeat, replication tracking). Closing this exception requires pgctld to provision the admin password during `initdb`, so it is tracked as a follow-up rather than shipped in the same change as the query-path flip. Operators running today see a warning on startup when the admin password is empty, surfacing the remaining gap.

Replication-related trust lines remain as a narrower scope, separate from the query path.

### Failure modes

- **Stale keys (password rotated in PostgreSQL).** The SCRAM handshake from MultiPooler to PostgreSQL fails. MultiPooler treats this as a non-retryable, session-fatal error: the failed connection is discarded, the client is disconnected with a clean error, and a reconnect through MultiGateway re-derives fresh keys from the new verifier.
- **Missing keys on an RPC.** The RPC fails closed with a clear error. In steady state this should not happen — every authenticated session has keys — so this is a defensive check, not an expected path.
- **Admin pool failure.** Independent of passthrough. If the admin pool cannot reach PostgreSQL, MultiPooler cannot serve `GetAuthCredentials` and no handshake can complete; passthrough is a no-op because no session ever reaches the authenticated state.

### Security properties

- **No plaintext password in process memory.** Only derived `ClientKey`/`ServerKey` ever exist in MultiGateway or MultiPooler.
- **Session-scoped exposure.** A live-memory attacker obtains keys only for currently-active sessions, not a historical set.
- **Authoritative `pg_hba`.** Access control decisions are made by PostgreSQL, not bypassed by socket trust.
- **Encrypted transit for keys.** Internal gRPC is mTLS; the RPC carrying `UserAuth` is confidential on the wire.

## Open Items / Future Work

- **Close the admin-user trust exception.** Once pgctld provisions the admin password during `initdb`, the `local all <admin-user> trust` line in `pg_hba_template.conf` can be removed and the admin pool switches to SCRAM like everything else.
- **Per-user key cache on MultiPooler.** If RPC payload overhead or repeated handshakes show up in benchmarks, a short-TTL cache with refresh-on-auth-failure is the nearest prior art (Supavisor uses this pattern).
- **Cert-auth integration.** Cert-authenticated sessions do not produce SCRAM keys. Options include a pooler-side service credential per role, a separate cert-delegation dial path, or rejecting cert auth for the pool path entirely. Deferred to its own design.
- **Channel binding (SCRAM-SHA-256-PLUS).** Future hardening for the external client-to-gateway handshake, where TLS cert trust is broadest. The internal gateway-to-pooler segment does not run SCRAM at all — the keys are a gRPC payload there, and confidentiality is handled by mTLS. The pooler-to-PostgreSQL handshake is local and has no meaningful MITM surface to bind against.

## Glossary

- **ClientKey** — `HMAC(SaltedPassword, "Client Key")`. What the client proves knowledge of. Recovered by the verifier from the proof via XOR.
- **ServerKey** — `HMAC(SaltedPassword, "Server Key")`. What the server proves knowledge of in the final message for mutual auth. Stored server-side; never secret from the storing server.
- **StoredKey** — `H(ClientKey)`. The verifier. Cannot be used to forge a proof; only to verify one.
- **Passthrough** — using `(ClientKey, ServerKey)` obtained at one SCRAM verifier to authenticate to another SCRAM verifier of the same password.
