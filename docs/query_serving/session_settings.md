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

- **Pinned `SET` / `RESET` / `RESET ALL`** route to the pinned backend and are
  tracked by the gateway after PostgreSQL accepts them. The backend genuinely
  carries the change (surviving COMMIT, reverting on ROLLBACK exactly as the
  gateway's savepoint frames revert the map). For `GUC_REPORT` variables the
  tracker records PostgreSQL's canonical reported value, not the client's
  literal — `SET datestyle = 'dmy'` on a backend at `'German, YMD'` tracks
  `'German, DMY'`, so a pool-rotation replay reproduces the composite state.
  Startup-packet GUCs keep real-PostgreSQL RESET semantics: pooled backends
  receive startup params via replayed SET (their reset value would be the
  server default), so a pinned `RESET` of a startup-param GUC routes a
  synthesized `SET var = '<startup value>'` instead, and a pinned `RESET ALL`
  routes the raw statement followed by silent restores of every startup param
  (skipping `session_authorization`/`role`, which PostgreSQL's own RESET ALL
  preserves). Synthesized statements run through `SilentRoute`: rows and
  command tag are swallowed (the tracker emits the client tag only after every
  restore succeeded), while `ParameterStatus` is forwarded so driver caches of
  reportable GUCs stay correct.
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
  label. The same flow covers the dynamic `pg_settings` shape and SQL
  `EXECUTE` of a prepared body containing such a call. No reconciliation SQL
  is ever injected mid-transaction (which would latch a REPEATABLE
  READ/SERIALIZABLE snapshot early), and the reservation intent derives from
  the statement shape alone, so these plans stay cacheable. The reservation
  is held only for its statement: it is dropped at statement completion when
  another reason already holds the connection, handed to the portal reason
  when a row-limited execute suspends (tracking has fired by then, so every
  later drain carries the value), and unwinds with the other statement-local
  reasons on failure.
- **Gateway-managed variables never reach a backend**, whatever the shape. A
  literal-named call is rewritten out of the routed query; the dynamic shape
  applies gateway-managed names with `is_local := true` so nothing persists
  (`set_config` returns the value either way); and a parameter-bound name
  resolving to a gateway-managed variable is rejected before the statement is
  sent, because the planner's rewrite cannot see through a bind. A literal
  gateway-managed `set_config` inside a SQL `PREPARE` body is rejected at
  PREPARE time — the body executes on the backend verbatim, so no rewrite can
  apply. A bound `is_local` on a non-gateway-managed call is likewise
  rejected, since it could resolve to `false` at execute time in a shape the
  tracker cannot capture, as are bound names and the dynamic shape inside SQL
  `PREPARE` bodies.
- **Transaction conclusion labels the released backend by outcome**: the
  gateway sends both the in-transaction map and the pre-BEGIN rollback
  snapshot on every `ConcludeTransaction`; a COMMIT that PostgreSQL concludes
  as a rollback (a failed transaction, or a commit-time failure such as a
  deferred constraint) stamps the rollback snapshot, never the abandoned
  in-transaction settings. A missing rollback snapshot on a rollback outcome
  is an invariant violation: the backend is closed rather than labelled. The
  disconnect path mirrors this: a client vanishing mid-transaction has its
  backend rolled back before release, so `ReleaseAll` sends the pre-BEGIN
  snapshot whenever transaction frames exist and the current map otherwise —
  the same conditional the pooler's own rollback follows.
- **Release disposition follows error class**: a clean PostgreSQL error
  aborts atomically, so the backend is unchanged since acquisition and its
  label still truthful — the connection is recycled (`ReleaseStatementError`),
  not closed. Indeterminate failures (cancellation, deadline, dead socket)
  taint. On clean failure only the transactional statement-local reasons
  unwind; non-transactional ones (session advisory locks, `setseed`) survive,
  because their side effects can materialize before the error and real
  PostgreSQL keeps them. A clean release without a settings cache to relabel
  through taints rather than recycling with a stale label, and the reserved
  pool's inactivity killer is the backstop reaper for anything stranded.
- **`SET LOCAL`** and transaction-only forms are backend-authoritative:
  PostgreSQL unwinds them at transaction end, so they need no tracking.
- **`SET SESSION CHARACTERISTICS AS TRANSACTION <mode>`** is translated to the
  `default_transaction_*` GUC it sets and tracked like any other session GUC.
  Multi-mode lists (comma- or whitespace-separated, e.g. `ISOLATION LEVEL
SERIALIZABLE READ ONLY`) are currently rejected — a deliberate unimplemented
  convenience, not a protection; the per-mode translation design is recorded
  in the project notes should demand appear.
- **`SET var FROM CURRENT`** is rejected: its resulting value is only knowable
  by mutating a backend outside the gateway's tracking.

Gateway-managed variables are described in
[`gateway_managed_variables.md`](./gateway_managed_variables.md).

## Rejected alternative: single RPC-driven release for the capture reservation

The `ReasonSetConfig` bit is cleared through several paths (gateway release
RPC when sole, local pooler-side clear when another reason co-holds, at
portal suspension, on failure unwind). A unification was considered and
rejected: never clear locally, and let the gateway's post-statement release
RPC drop the bit in every state. It is cleaner — the entire class of
clear-vs-implicit-release ordering bugs becomes unrepresentable — but it
adds one RPC per mixed-state statement on the latency-sensitive path, and a
lost RPC strands a mixed-reason connection with no reaper: unlike the sole
case (where the gateway abandons its shard state and the idle killer reaps
the orphan), a live transaction's reservation cannot be abandoned, and a
busy connection never idles. Escalating a lost release RPC to a client
FATAL closes that hole but converts an invisible, self-healing
infrastructure blip — including lost-response false positives, correlated
across sessions on a pooler restart — into client-visible session kills.
Local clears cannot fail, cost nothing, and keep infrastructure noise
invisible to clients; each custody-transfer path is pinned by a dedicated
test. Revisit only if the set of clearing paths keeps growing.

## Known limitations and PostgreSQL divergence

- **Session-state mutations hidden inside routine bodies are untracked.** A
  `set_config` buried in a SQL or PL/pgSQL function body executes on whatever
  backend served the statement, invisible to gateway tracking, and can leak
  across logical sessions sharing that backend. Closing this requires
  creation-time rejection of state-mutating routine definitions (separate
  work); `go/test/endtoend/queryserving/session_state_leak_test.go` is the
  skipped acceptance test for that gate. The same applies to state-mutating
  calls reachable through views, triggers, casts, and C extensions.
- Row-limited portal fetches (`Execute` with `maxRows`) on statements that
  combine a gateway-managed `set_config` bound value or a gateway-managed
  `current_setting` read lose portal suspension: those statements run through
  a rewritten simple execution, which streams every row and reports
  `CommandComplete` instead of `PortalSuspended`. Pre-existing behavior,
  inherent to the rewrite.
- Tracked values from pinned (routed) SET statements record PostgreSQL's
  canonical reported form for `GUC_REPORT` variables; non-reportable GUCs
  keep the client's literal spelling (PostgreSQL emits no report to prefer).
  Replay accepts either identically; the only cost of a literal is an
  occasional duplicate settings bucket for spelling variants.
- No sanitation statement runs on release, so process-global state maintained
  by C extensions (which even `DISCARD ALL` could not reset) is likewise
  outside the model: C extensions must not use backend-process globals as
  per-client session state.
