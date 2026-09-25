// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestActivateBuffersClientQueriesAcrossCutover proves the readiness-gated cutover:
// while a client drives concurrent writes THROUGH THE GATEWAY, activating the
// migration (IMPORT->EXPORT) must BUFFER those writes across the brief cutover
// window rather than refuse them with 57P03, and the source and target must end
// byte-identical (row count + a content checksum — the no-data-loss invariant).
//
// Topology: the multigateway sits in the client query path (its failover buffer is
// what absorbs the cutover). The EXPORT reverse subscription dials the pooler's
// Postgres directly (--migration-target-advertise-port is the pooler PG port), so
// it is independent of the gateway — the same reverse path switch_test exercises.
func TestActivateBuffersClientQueriesAcrossCutover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator cutover-buffering e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		// The client reaches the shard through the gateway; its failover buffer
		// (buffer-enabled, buffer-window 10s, max-failover-duration 20s) is what holds
		// queries across the cutover.
		shardsetup.WithMultigatewayBuffering(),
		// EXPORT makes the Multigres target the logical publisher (wal_level=logical +
		// non-temporary reverse slot), matching targetConnInfo's guardrail.
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
		// GetTestUserDSN connects to the gateway with dbname=postgres.
		shardsetup.WithDatabase("postgres"),
	)
	defer cleanup()

	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)
	// Confirm the gateway serves before a migration takes the shard non-serving.
	setup.WaitForMultigatewayQueryServing(t)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		SourceDsn:      sourceDSN(srcPort),
		TargetDatabase: targetDB,
		Objects:        objs("public.orders"),
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()
	_, err = mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err)

	// Wait until the IMPORT is caught up (lag ~0). The readiness gate then admits the
	// cutover immediately, so it fits inside the gateway buffer window.
	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		return err == nil && len(resp.GetMigrations()) == 1 && resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "IMPORT must catch up")

	// Client connections to the gateway. The pool is opened while the shard is
	// non-serving (IMPORTING) on purpose: the first writes buffer at the gateway.
	db, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable"))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(4)

	// Concurrent write load against the migrated table, THROUGH the gateway.
	var (
		success atomic.Int64
		mu      sync.Mutex
		errs    = map[string]int{}
		wg      sync.WaitGroup
	)
	stop := make(chan struct{})
	recordErr := func(e error) {
		mu.Lock()
		errs[e.Error()]++
		mu.Unlock()
	}
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Per-query timeout longer than the buffer window, so a correctly
				// buffered-and-replayed query has time to complete; a query the buffer
				// evicts (MTB02) or the gateway refuses (57P03) returns promptly as an
				// error we record.
				qctx, cancel := context.WithTimeout(ctx, 18*time.Second)
				_, err := db.ExecContext(qctx, "INSERT INTO public.orders (v) VALUES ('load')")
				cancel()
				if err != nil {
					recordErr(err)
				} else {
					success.Add(1)
				}
				time.Sleep(25 * time.Millisecond)
			}
		})
	}

	// Give the writers a moment to actually buffer against the non-serving shard,
	// then cut over. The writes in flight must survive the cutover.
	time.Sleep(500 * time.Millisecond)
	exportResp, err := mt.ActivateMigration(ctx, &migratorpb.ActivateMigrationRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, exportResp.GetMigration().GetActiveDirection())

	// Let the workload run a little past the cutover so post-serving writes land too,
	// then stop and join.
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	// The core assertion: no query was refused with 57P03 across the cutover, and the
	// buffer absorbed the window with zero client-visible failures.
	mu.Lock()
	failed := 0
	for msg, n := range errs {
		failed += n
		assert.NotContains(t, msg, "57P03", "client query refused with 57P03 during cutover (should be buffered): %s", msg)
	}
	mu.Unlock()
	assert.Zero(t, failed, "buffering should absorb the cutover with zero failed writes; errors=%v", errs)
	require.Greater(t, int(success.Load()), 0, "some client writes must have committed across the cutover")

	// No data loss: once the reverse EXPORT stream settles, the source and target must
	// be byte-identical (row count + content checksum over all columns).
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	require.Eventually(t, func() bool {
		sc := dialSource(t, ctx, srcPort)
		defer sc.Close()
		srcN, srcSum, ok1 := tableFingerprint(ctx, sc, "public.orders")
		tgtN, tgtSum, ok2 := tableFingerprint(ctx, tc, "public.orders")
		if !ok1 || !ok2 {
			return false
		}
		t.Logf("fingerprint source=(%d,%s) target=(%d,%s)", srcN, srcSum, tgtN, tgtSum)
		return srcN == tgtN && srcN > 3 && srcSum == tgtSum
	}, 30*time.Second, 500*time.Millisecond, "source and target must converge byte-identical after EXPORT")

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id, Force: true})
	require.NoError(t, err)
}

// tableFingerprint returns the row count and an ordered content checksum (md5 over
// every column, ordered by primary key) for a table, so two databases can be
// compared byte-for-byte. ok is false if the query fails or returns no row.
func tableFingerprint(ctx context.Context, conn *client.Conn, table string) (count int, sum string, ok bool) {
	q := "SELECT count(*), coalesce(md5(string_agg((t.*)::text, '' ORDER BY t.id)), '') FROM " + table + " t"
	res, err := conn.Query(ctx, q)
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 || len(res[0].Rows[0].Values) < 2 {
		return 0, "", false
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(res[0].Rows[0].Values[0])))
	if err != nil {
		return 0, "", false
	}
	return n, string(res[0].Rows[0].Values[1]), true
}
