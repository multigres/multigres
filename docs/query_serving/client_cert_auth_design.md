# Client Certificate Authentication

## Overview

Multigateway supports PostgreSQL client certificate authentication at the wire-protocol level, matching PG's `cert` auth method exactly (`trust` + `clientcert=verify-full`). Deployments can use mTLS to authenticate connections without passwords, with behavior compatible with native PostgreSQL from the client's perspective — same wire-protocol sequence, same error codes, same identity-matching semantics. Remaining parity gaps (e.g. `pg_ident.conf` maps) are tracked as follow-ups.

Key capabilities:

- `cert` auth method — require client cert, verify chain, bind `CN` (or `DN`) to the requested DB user, skip SCRAM
- Flag-driven config for v1 (no `pg_hba.conf` yet)
- Full peer DN recorded as the authenticated identity for audit
- SQLSTATE `28000` on mismatch; no fallback to SCRAM — first-match-wins per PG

Explicit non-goals:

- **No SAN / SPIFFE URI identity extraction.** PG's server-side cert auth uses only `Subject.CN` (or full DN). Adding SAN would be a superset.
- **No user-existence check at auth time.** PG doesn't do it; the pooler rejects the connection downstream when the user is missing.
- **No IP-restriction rules / `pg_hba.conf`.** Deferred.

## PostgreSQL Background

### The `cert` auth method

From [auth-cert.html](https://www.postgresql.org/docs/17/auth-cert.html):

> The `cn` (Common Name) attribute of the certificate will be compared to the requested database user name, and if they match the login will be allowed. … cert authentication is effectively trust authentication with `clientcert=verify-full`.

The semantics:

1. Identity is pulled from the peer cert — `Subject.CN` (default) or full DN.
2. Empty identity → reject.
3. Identity is compared against the requested DB user, either directly (case-sensitive equality) or via a configured `pg_ident.conf` map.
4. The full DN is recorded as the authenticated identity for audit, regardless of which field was matched for authorization.
5. Role existence is **not** checked at auth time — it is deferred to backend startup.

### The `clientcert` HBA option

Independent of the auth method:

- `clientcert=verify-ca` — require valid client cert, validate chain + CRL, no identity binding. Auth method still runs.
- `clientcert=verify-full` — same, plus enforce `CN`/`DN` matches the requested user. Auth method still runs.

The `cert` method is equivalent to `trust` + `clientcert=verify-full`.

### Wire protocol

No `AuthenticationCertificate` message exists. After a TLS handshake with a valid client cert and successful identity match, the server proceeds straight to `AuthenticationOk`, then the usual `BackendKeyData`, `ParameterStatus`, `ReadyForQuery` sequence. No SASL/password exchange.

## Architecture

### Connection flow

```text
Client                                   Multigateway
  │                                          │
  │ SSLRequest                               │
  ├──────────────────────────────────────────▶
  │                              'S' (accept)│
  ◀──────────────────────────────────────────┤
  │                                          │
  │ TLS ClientHello                          │
  ├──────────────────────────────────────────▶
  │                     TLS ServerHello +    │
  │                     Cert + CertRequest   │
  ◀──────────────────────────────────────────┤
  │                                          │
  │ TLS Client Cert + Finished               │
  ├──────────────────────────────────────────▶  [chain verified during
  │                          TLS Finished    │   handshake]
  ◀──────────────────────────────────────────┤
  │                                          │
  │ StartupMessage (user=alice, db=app)      │
  ├──────────────────────────────────────────▶
  │                                          │ mode=verify-full + TLS
  │                                          │ + verified peer cert
  │                                          │ → cert-auth path
  │                                          │
  │                                          │ compare CN (or DN) to user
  │                                          │ log DN as authn identity
  │                                          │
  │ AuthenticationOk + BackendKeyData +      │
  │ ParameterStatus… + ReadyForQuery         │
  ◀──────────────────────────────────────────┤
```

### Component interaction

Four layers participate, each with a clear responsibility:

**TLS layer** (below the PG protocol) — terminates TLS and validates the client certificate chain against the configured CA pool. In `verify-full` mode the TLS layer is configured to require a verified peer cert at handshake time, so a missing or untrusted client cert fails the handshake before any PG-level code runs. (CRL enforcement is a deferred follow-up; v1 does not consult CRLs.)

**PG protocol startup** — after the handshake, reads the `StartupMessage` over the encrypted channel and extracts `user` and `database`. Dispatches to the authentication layer based on the configured client-auth mode:

- `none` → SCRAM (current default).
- `verify-full` → cert auth path; a connection without a verified peer cert has already failed the handshake, so this path always sees a cert.

**Cert auth** — pulls the configured identity field (`CN` or `DN`) from the verified peer cert and compares it to the requested DB user. Logs the full DN for audit. On mismatch, emits a PG-format error with SQLSTATE `28000` and closes. On match, sends `AuthenticationOk` and the post-auth message sequence — no password dance.

**Pooler** (downstream) — receives queries on the authenticated connection. If the mapped DB user doesn't exist in the credential store, query execution fails there, matching PG's behavior of deferring role-existence checks past the auth step.

No new services, no new RPCs, no credential-fetch round-trip at auth time. Cert auth is a pure in-process operation on top of TLS state that is already present after the handshake.

### Flag surface

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--pg-tls-cert-file` | path | `""` | Server cert |
| `--pg-tls-key-file` | path | `""` | Server key |
| `--pg-tls-ca-file` | path | `""` | CA pool for verifying client certs |
| `--pg-tls-client-auth-mode` | enum | `none` | `none`, `verify-full` implemented; `verify-ca`, `optional` reserved |
| `--pg-tls-client-cert-name` | enum | `cn` | `cn` or `dn` — which cert field is matched against `user` |

Reserved names (fixed now to avoid churn):

- `--pg-tls-crl-file`, `--pg-tls-crl-dir` — CRL sources.
- `--pg-hba-conf` — rule-based config; will supersede the flags above when it lands.

Validation:

- `--pg-tls-ca-file` requires `--pg-tls-cert-file` + `--pg-tls-key-file`.
- `--pg-tls-client-auth-mode=verify-full` requires `--pg-tls-ca-file`.
- `--pg-tls-ca-file` set together with `--pg-tls-client-auth-mode=none` is rejected at startup. A CA configured with no client-auth mode active is almost certainly a misconfiguration (the user intended cert auth but forgot the mode); failing loudly matches the style of the existing server-TLS validations and avoids silently unverified clients.

## Identity Mapping

v1: the selected cert field (`CN` or `DN`) must equal the requested user, case-sensitive. Matches PG's default when no `map=` is configured.

**DN-format parity.** When `--pg-tls-client-cert-name=dn`, the DN string used for comparison is emitted in strict [RFC 2253](https://www.rfc-editor.org/rfc/rfc2253) form — the same format PostgreSQL produces via `X509_NAME_print_ex(..., XN_FLAG_RFC2253)`. This is not the same as Go's default `pkix.Name.String()` (which differs in attribute ordering and escaping), so multigateway implements its own RFC 2253 formatter over `Certificate.RawSubject`. Without this, a cert whose DN matches successfully against PG would silently fail here.

Deferred: `pg_ident.conf`-style mapping — exact pairs, regex with `\1` back-references, permissive-OR semantics (match succeeds if any map entry allows the pair).

## Error Handling

- **TLS chain validation failure** — handshake fails; client sees a TLS error, never reaches the auth layer.
- **No client cert in `verify-full` mode** — the strict client-cert verification setting makes this a handshake failure (same path as above).
- **CN/DN mismatch** — `ErrorResponse` with severity `FATAL`, SQLSTATE `28000`, message `certificate authentication failed for user "X"`, and a `DETAIL` line naming the matched field and the presented value (e.g. `Client certificate CN "svc-foo" does not match requested user "svc-bar"`). Connection closed. Matches PG's message + detail convention. No SCRAM fallback: a cert that passed chain validation but failed identity match means *wrong identity*, not *wrong method*.
- **Successful auth** — one log line with peer DN, matched field, cipher suite, TLS version, requested user. Mirrors PG's audit trail.

## Operational Guidance

- Clients must negotiate TLS to reach cert auth. With libpq-compatible drivers that means `sslmode=require` at minimum (or stricter — `verify-ca`/`verify-full`) together with `sslcert` and `sslkey` (and `sslrootcert` if the client also verifies the server). `sslmode=disable`/`prefer` will either skip TLS or leave it optional and never send a client cert.
- TLS cert rotation requires multigateway restart in v1. Hot reload is deferred.
- For mixed deployments (human users on SCRAM, services on mTLS), run separate multigateway instances or restrict the CA to service identities, until per-rule config arrives.
- Successful cert auth emits an info-level log line including the full DN — sufficient for audit.

## Follow-up Work

- Pair `verify-ca` and `verify-full` with non-cert auth methods (e.g. SCRAM **and** cert required) — the defense-in-depth case.
- Extend TLS to the multipooler → PostgreSQL leg, mirroring libpq's `sslmode` surface.
- Advertise `SCRAM-SHA-256-PLUS` (channel binding) when the connection is TLS, matching PG.
- CRL checking for PG TLS and internal gRPC — replicate PG's whole-chain CRL semantics.
- `pg_ident.conf` parsing and the `map=` option.
- `pg_hba.conf` parser and rule-match engine — the umbrella that ultimately subsumes the per-flag config and lets the items above land as rule types.

## References

- [PostgreSQL — Certificate Authentication](https://www.postgresql.org/docs/17/auth-cert.html)
- [PostgreSQL — `pg_hba.conf`](https://www.postgresql.org/docs/17/auth-pg-hba-conf.html) (`clientcert` option)
- [PostgreSQL — User Name Maps](https://www.postgresql.org/docs/17/auth-username-maps.html)
- [PostgreSQL — SSL Setup](https://www.postgresql.org/docs/17/ssl-tcp.html)
