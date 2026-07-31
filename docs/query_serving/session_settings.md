# Session Settings and Physical Session Safety

## Logical state

Multigateway is authoritative for a client's logical session state:

- startup parameters provide the baseline;
- `SessionSettings` contains tracked session-level GUC overrides;
- gateway-managed variables are stored separately;
- transaction and savepoint snapshots restore both stores on rollback.

The effective map is sent to multipooler on every execution. Multipooler interns
that map and keeps settings buckets, so a clean physical connection with the
same explicitly-applied settings can be reused without replaying every GUC.

A physical backend's session state may only change in lockstep with the
gateway map. That invariant is what makes the pool's settings labels
trustworthy: at final reservation release the gateway map is stamped onto the
connection as its label with zero reconciliation SQL, and on checkout a
pointer-equal bucket match needs no SQL at all.

## `SET`, `RESET`, and `set_config`

Statement handling splits on whether the session is **pinned** to a backend —
inside an explicit transaction (including its deferred-BEGIN first statement)
or holding a reserved connection (temp tables, cursors, advisory locks):

- **Pinned `SET` / `RESET` / `RESET ALL`** route the real statement to the
  pinned backend and are tracked by the gateway after PostgreSQL accepts them.
  The backend genuinely carries the change (surviving COMMIT, reverting on
  ROLLBACK exactly as the gateway's savepoint frames revert the map).
- **Unpinned `SET`** is validated by PostgreSQL with a statement-local
  `set_config` probe that reverts instantly; PostgreSQL's canonical result is
  recorded (for example, `DateStyle = 'ISO'` is stored as `ISO, MDY`).
  Persistence lives only in the gateway map and is replayed at checkout.
- **Unpinned `RESET`** validates the name with a statement-local reset probe
  (`set_config(name, NULL, true)` errors on unknown names like a real RESET),
  then drops the map entry. **Unpinned `RESET ALL`** is a pure map edit.
- **Accepted top-level `set_config`** forms route unmodified and update
  logical state only after success. A session-persisting call
  (`is_local := false`) carries a `ReasonSetConfig` reservation: the backend
  that executes it is held out of the pool until the gateway records the new
  value into its map, then released explicitly with options carrying the
  updated map, which the multipooler stamps onto the connection's settings
  label. The same flow covers the dynamic `pg_settings` shape (applied with
  its real `is_local`) and SQL `EXECUTE` of a prepared body containing such a
  call. No reconciliation SQL is ever injected mid-transaction (which would
  latch a REPEATABLE READ/SERIALIZABLE snapshot early), and the reservation
  intent derives from the statement shape alone, so these plans stay
  cacheable. A bound (`$N`) `is_local` on a non-gateway-managed call is
  rejected, since it could resolve to `false` at execute time in a shape the
  tracker cannot capture.
- **Transaction conclusion labels the released backend by outcome**: the
  gateway sends both the in-transaction map and the pre-BEGIN rollback
  snapshot on `ConcludeTransaction`; a COMMIT that PostgreSQL concludes as a
  rollback (a failed transaction, or a commit-time failure such as a deferred
  constraint) stamps the rollback snapshot, never the abandoned
  in-transaction settings.
- **`SET LOCAL`** and transaction-only forms are backend-authoritative:
  PostgreSQL unwinds them at transaction end, so they need no tracking.
- **`SET SESSION CHARACTERISTICS AS TRANSACTION <mode>`** is translated to the
  `default_transaction_*` GUC it sets and tracked like any other session GUC.
- **`SET var FROM CURRENT`** is rejected: its resulting value is only knowable
  by mutating a backend outside the gateway's tracking.

Gateway-managed variables are described in
[`gateway_managed_variables.md`](./gateway_managed_variables.md).

## Known limitations and PostgreSQL divergence

- **Session-state mutations hidden inside routine bodies are untracked.** A
  `set_config` buried in a SQL or PL/pgSQL function body executes on whatever
  backend served the statement, invisible to gateway tracking, and can leak
  across logical sessions sharing that backend. Closing this requires
  creation-time rejection of state-mutating routine definitions (separate
  work); `go/test/endtoend/queryserving/session_state_leak_test.go` is the
  skipped acceptance test for that gate. The same applies to state-mutating
  calls reachable through views, triggers, casts, and C extensions.
- Tracked values from pinned (routed) SET statements record the client's
  literal spelling rather than PostgreSQL's canonical form. Replay accepts the
  literal identically; the only cost is an occasional duplicate settings
  bucket for spelling variants.
- No sanitation statement runs on release, so process-global state maintained
  by C extensions (which even `DISCARD ALL` could not reset) is likewise
  outside the model: C extensions must not use backend-process globals as
  per-client session state.
