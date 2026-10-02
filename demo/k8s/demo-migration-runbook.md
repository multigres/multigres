# Migration demo — runbook

## Prerequisites

In order to run the demo, it is necessary to ensure that you have a multigres cluster running. This is typically done by creating a local cluster using the following steps. All commands are relative to the root of the tree.

### Build the binaries and images

Build the host binaries (`bin/multigres`, used by the dashboard and the `mg` CLI) and the demo docker images:

```bash
make build
make images
```

### Launch the infrastructure

This launches infrastructure-supporting services.

```bash
cd demo/k8s
bash launch-infra.sh
```

### Launch the Multigres cluster

This step sets up a simple Multigres cluster with a primary and two standbys using `kind`. The cluster is operator-less and is deployed as a Kubernetes `StatefulSet`, meaning that the servers will be automatically restarted if they crash.

```bash
cd demo/k8s
bash launch-multigres-cluster.sh
```

### Start the migration source

This starts the freestanding external Postgres the migration imports **from** — a plain `postgres:17` container on the kind network, seeded with the ledger. Run it after the cluster is up; the dashboard reads its in-cluster address into `SRC_DSN` (and falls back to a placeholder if it isn't running).

This step is not necessary if you use the `demo-dashboard.sh` below since that will start the migration source automatically.

```bash
cd demo/k8s
bash launch-migration-source.sh
```

## Starting the demo dashboard

The script `demo-dashboard.sh` opens a four-pane tmux dashboard.

If a bare `psql` isn't on your PATH (Homebrew, for one, installs it keg-only as `psql-18`), set `PSQL` to the client to use before launching — e.g. `PSQL=psql-18 ./demo-dashboard.sh`. The console panes install a `psql` shim that dispatches to it, so you still just type `psql`. `reset-demo.sh` and `backup-restore.sh` honor `PSQL` too.

It resets the target side on start (drops any leftover migration and the copied `public.accounts` so a re-run bootstraps cleanly — pass `--no-reset` to skip, `--reset-source` to also reseed the source), starts the write client in the **background** (balance-neutral operations — transfers plus account opens and closes — so `SUM(balance)` stays fixed while the row set and individual ids change), and self-heals the balance panes — the target pane (4) shows `waiting for target ledger…` until the migration streams, then fills in on its own, and reconnects itself through the gateway after a target failover; no manual restart needed.

The panes:

- **Pane 1 — source psql console.** `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/`PGDATABASE` are preset for the source, so `psql` connects with no options. Use it to inspect the source and (after the switch) verify the reverse stream. It also has an `mg` helper (`bin/multigres` with `--admin-server` prefilled) and `SRC_DSN` / `SRC_LOCAL_DSN` loaded, for the `mg`-CLI [appendix](#appendix--driving-with-the-mg-cli-pane-1).
- **Pane 2 — target psql console.** `psql` connects to the Multigres gateway (`localhost:15432`) with no options. **This is where you drive the whole migration**, as gateway SQL — open one session here and keep it open for the entire run (see the note below).
- **Panes 3 and 4 — source and target balances.** Live ledger summaries. The source is seeded large (1000 accounts by default, `ACCOUNTS` in `launch-migration-source.sh`), but each pane shows only a small window — the first `WATCH_LIMIT` accounts (default 12, in `run-appclient.sh`) ordered by account id — so both panes render the **same** subset and you can compare source vs. target row by row. The "accounts: N · total …" line and the `TOTAL` row still roll up the whole ledger, so the no-data-loss invariant covers every account, not just the window. The target fills in once the migration streams.

**One gateway session drives the whole demo.** Everything below runs in **pane 2** as SQL against the gateway — `CREATE CONNECTION` / `CREATE MIGRATION` / `ALTER MIGRATION … START | ACTIVATE | DEACTIVATE` / `SHOW MIGRATION` / `DROP MIGRATION` (migrations are addressed by name, no id needed). Keep **one** psql session open in pane 2 from the create step through the whole run: once you `CREATE MIGRATION` the shard stops serving (within a monitor tick) and the gateway **refuses new connections** (`FATAL: database is temporarily unavailable`), but migration DDL on the already-open session keeps working — it goes to the migrator (a per-shard gRPC service aimed at the primary), not the serving-gated query path. `CREATE CONNECTION` is also gateway-local (in-memory, per replica), so staying on one session keeps it pinned to the gateway that holds it. The `mg` CLI can drive the same steps from pane 1 at any phase — see the [appendix](#appendix--driving-with-the-mg-cli-pane-1).

**Serving gate.** As soon as a migration **exists** on the shard — even `CREATED`, before it is started — the pooler stops serving client queries (within a monitor tick, ~5s), so nothing can change the database while a migration is staged against it. Serving is held from `CREATE MIGRATION` through the import, released only at `ACTIVATE` (EXPORT), re-held on `DEACTIVATE`, and cleared once `DROP MIGRATION` removes the row. Migration DDL on the open session is exempt from the gate; a plain `SELECT` against the target is not — so keep one session open from the create step, and expect new connections and plain queries to be refused from create until activate. The blocking is the point of the demo, not a fault.

## 1. Create the connection and migration (pane 2, gateway SQL)

In **pane 2**, open one psql session and **keep it open for the rest of the demo**. `CREATE CONNECTION` takes libpq options individually; the only per-run value is the source's in-cluster host, which the dashboard loads into `$SRC_HOST`:

```bash
psql -v src_host="$SRC_HOST"
```

```sql
CREATE CONNECTION src OPTIONS (host :'src_host', port '5432', user 'postgres', password 'sourcepass', dbname 'postgres', sslmode 'disable');
CREATE MIGRATION accounts CONNECTION src FOR TABLE public.accounts;
SHOW MIGRATIONS;   -- lists the migration (phase CREATED); the shard now stops serving within a few seconds
```

## 2. (optional) See the migration row on the shard

`SHOW MIGRATIONS` already reads it through the gateway. To see the replicated sidecar table directly, read the primary pooler's postgres (pane 1) — ports `15433/15434/15435` are the three poolers; the primary is the one not in recovery:

```bash
for p in 15433 15434 15435; do [ "$(PGPASSWORD=postgres psql -h localhost -p $p -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = f ] && \
  PGPASSWORD=postgres psql -h localhost -p $p -U postgres -c "select migration_id, name, phase from multigres.migration"; done
```

## 3. Start the migration

In **pane 2** (same session):

```sql
ALTER MIGRATION accounts START;
```

The shard is already non-serving (it stopped serving when the migration was created), so `START` just begins the import. A **new** `psql` to the gateway returns `FATAL: database is temporarily unavailable; please retry`, and a plain `SELECT` on this session is refused too — expected (the serving gate). Migration DDL on this already-open session still works, which is the next step.

## 4. Wait until it is streaming

In **pane 2** (same session) — this works even though the shard is non-serving, because migration DDL bypasses the serving gate:

```sql
SHOW MIGRATION accounts;   -- repeat until phase IMPORTING, caught_up = t
```

## 5. Move the app to the gateway — it blocks

```bash
failover-app     # SIGUSR1 to the write client; it reconnects to the gateway
```

The app blocks, retrying the gateway's `57P03`; the source balance (pane 3) stops advancing. The gateway buffers because the shard is not serving yet.

## 6. Switch direction — the app picks up

In **pane 2** (same session):

```sql
ALTER MIGRATION accounts ACTIVATE;   -- IMPORT -> EXPORT; the shard starts serving
```

The gateway serves, the app's retry succeeds, and it resumes writing to Multigres (pane 4 moves). The external source becomes a standby and stays in sync via the reverse stream. (New gateway connections work again now, too.)

**No data lost.** The drain barrier makes source == target before the switch, and the reverse slot — pre-created on the target during the switch and advanced to the handoff LSN — captures every post-switch write, so nothing is dropped in the window before the reverse subscription attaches. Verify after quiescing writes — target through the gateway (pane 2, now serving), source directly (pane 1):

```sql
-- pane 2 (gateway, EXPORT, serving):
SELECT count(*), sum(balance) FROM public.accounts;
```

```bash
# pane 1 (source, now a standby):
PGPASSWORD=sourcepass psql -h localhost -p 5433 -tAc "select count(*), sum(balance) from public.accounts"
```

**Data streams to the source.** Write on the target (pane 2) and see it on the source (pane 1):

```sql
-- pane 2 (gateway). The writer opens and closes accounts, so fixed ids like 1/2
-- may not exist — pick two that do, then move a unit between them:
SELECT min(id) AS a, max(id) AS b FROM public.accounts \gset
UPDATE public.accounts SET balance=balance-1 WHERE id=:a;
UPDATE public.accounts SET balance=balance+1 WHERE id=:b;
\echo moved 1 unit from :a to :b
```

```bash
# pane 1 (source) — reflects the writes within a second or two (use the a/b ids printed above):
PGPASSWORD=sourcepass psql -h localhost -p 5433 -c "select id, balance from public.accounts where id in (<a>, <b>)"
```

## 7. Switch back to IMPORT (why the app can't switch the same way)

The forward switch let the app move to the gateway **first**, because the gateway buffers (retryable `57P03`) until it serves. The switch **back** cannot be done that way: in IMPORT the app writes to the **external source** — a plain Postgres that does not buffer — while the gateway fronts the target (a _subscriber_ in IMPORT). If the app moved to the source before the switch completed, it would write into a node still receiving the reverse stream and lose data.

So switch back **switch-first, then move the app**:

1. Quiesce the app on the gateway (stop new writes, or let them block).
2. In **pane 2** (same session): `ALTER MIGRATION accounts DEACTIVATE;` — drains the reverse stream to lag-zero (the source gets every committed target write), then flips to IMPORT (the source becomes the writable primary). The gateway stops serving again; keep this session open (new connections are refused).
3. **Only after** deactivate completes, reconnect the app to the source and resume — in the demo, restart `./run-appclient.sh write` (it targets the source).

The safety comes from the deactivate drain barrier plus not touching the source until the switch is done.

## 8. Tear down

In **pane 2** (same session):

```sql
DROP MIGRATION accounts;   -- graceful (requires a caught-up STREAMING state); add FORCE to abort from any phase
DROP CONNECTION src;
```

## Optional — survive a target-primary failover

The migration survives a target-primary failover: the coordinator resumes on the newly elected primary. Kill the current primary's pod (pane 1):

```bash
PRIMARY_ID=$(mg getpoolers | jq -r '.poolers[] | select(.routing_state.role=="ROUTING_ROLE_PRIMARY") | .id.name')
kubectl --context kind-multidemo -n default delete pod "multipooler-zone1-$PRIMARY_ID"
```

If an EXPORT (fail-back) reverse stream is active, the source's subscription keeps flowing because it targets the gateway's `replication=database` tunnel (the multipoolers advertise `--migration-target-advertise-host=multidemo-control-plane --migration-target-advertise-port=31432`, the gateway NodePort reachable from `pg-source`), so no `ALTER SUBSCRIPTION ... CONNECTION` is needed across the failover.

Starting the dashboard already resets the target side (see above), so a fresh run needs no manual step. To reset by hand between runs — drop the migration(s), stop the writer, drop the copied target table, and optionally reseed the source — **while keeping the cluster and infra running**, use the reset script:

```bash
./reset-demo.sh                # full reset (also recreates + reseeds the source)
./reset-demo.sh --keep-source  # reset migration + target table only; leave the source as-is
```

## Inspecting the cluster (pods, primary, topology)

List pods (the kind cluster uses its own kube context):

```bash
kubectl --context kind-multidemo -n default get pods -o wide
```

Find the primary pooler — the one whose postgres is not in recovery:

```bash
for p in 15433:zone1-0 15434:zone1-1 15435:zone1-2; do
  [ "$(PGPASSWORD=postgres psql -h localhost -p ${p%%:*} -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = f ] && echo "primary: multipooler-${p##*:}"
done
```

Inspect the topology in etcd. Records are proto-encoded, so decode them with the repo's `protoc`; the primary is the pooler whose `routing_state.role` is `ROUTING_ROLE_PRIMARY`:

```bash
KEX="kubectl --context kind-multidemo -n default exec etcd-0 -- etcdctl"
DECODE="dist/protoc-25.1/bin/protoc --decode=clustermetadata.Multipooler --proto_path=proto proto/clustermetadata.proto"

# whole topology tree (cells, databases, gateways, orchs, shards, poolers)
$KEX get --prefix --keys-only /multigres

# decode each pooler record; PRIMARY = routing_state { role: ROUTING_ROLE_PRIMARY }
for key in $($KEX get --prefix --keys-only /multigres | grep '/poolers/.*/Pooler$'); do
  echo "== $key =="
  $KEX get "$key" --print-value-only | $DECODE | grep -E 'hostname:|type:|role:|rule:'
done
```

## Appendix — driving with the `mg` CLI (pane 1)

The demo above is driven entirely from the gateway. As an alternative, the `mg` CLI reaches the migrator through multiadmin, so it works from **pane 1** at every phase — no kept-open gateway session needed. Migrations are addressed by generated id, so capture it once:

```bash
mg create-migration --source-dsn "$SRC_DSN" --target-database postgres --tables public.accounts  # CREATED; shard stops serving
ID=$(mg list-migrations | jq -r '.migrations[0].id'); echo "$ID"
mg start-migration      --id "$ID"     # CREATED -> IMPORTING (already non-serving; begins import)
mg get-migration        --id "$ID"     # repeat until phase IMPORTING, caught_up: true
mg activate-migration   --id "$ID"     # IMPORT -> EXPORT (shard serves)
mg deactivate-migration --id "$ID"     # EXPORT -> IMPORT (shard stops serving)
mg drop-migration       --id "$ID"     # graceful; add --force to abort from any phase
```

Verb mapping to the gateway SQL: `start` / `activate` / `deactivate` = `ALTER MIGRATION name START | ACTIVATE | DEACTIVATE`; `get` / `list` = `SHOW MIGRATION[S]`; `drop` = `DROP MIGRATION name [FORCE]`.
