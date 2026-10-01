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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestActivateHardQuiesceCutsOffSourceWriter proves the hardened source quiesce at
// the ACTIVATE cutover: an application role writing continuously to the source is
// cut off at the switch (its live backend terminated and CONNECT revoked, so it
// cannot reconnect and write once the source becomes a subscriber), and the source
// and target end byte-identical — no stray subscriber-side write diverges. It is
// the real-Postgres complement to the coordinator orchestration unit tests, which
// assert the call sequencing but cannot exercise the actual PostgreSQL fence.
func TestActivateHardQuiesceCutsOffSourceWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator quiesce e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	// An application role — separate from the migrator's superuser DSN — that writes
	// to the source and is the one fenced by the quiesce. It must exist before create
	// (create validates the quiesce roles), and it gets CONNECT only via PUBLIC, so
	// the barrier's REVOKE ... FROM PUBLIC + FROM <role> fully fences it.
	const appRole, appPass = "appwriter", "apppass"
	admin := dialSource(t, ctx, srcPort)
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", appRole, appPass),
		"GRANT INSERT, SELECT ON public.orders TO " + appRole,
		"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO " + appRole,
	} {
		_, err := admin.Query(ctx, stmt)
		require.NoError(t, err, "prepare app role: %s", stmt)
	}
	admin.Close()

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		SourceDsn:      sourceDSN(srcPort),
		TargetDatabase: targetDB,
		Objects:        objs("public.orders"),
		QuiesceRoles:   []string{appRole},
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()
	_, err = mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err)

	// A continuous writer as the app role, hammering the source across the cutover.
	w := &sourceWriter{port: srcPort, user: appRole, pass: appPass}
	w.start(ctx)
	defer w.stop()

	// The writer must actually be committing before we proceed, so the cut-off later
	// is meaningful.
	require.Eventually(t, func() bool { return w.stats().ok >= 5 }, 30*time.Second, 200*time.Millisecond,
		"writer must be inserting on the source before activation")

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		return err == nil && len(resp.GetMigrations()) == 1 && resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "IMPORT must catch up")

	// Activate — the hard quiesce fences appRole (REVOKE CONNECT) and terminates its
	// live backend before capturing the barrier LSN.
	_, err = mt.ActivateMigration(ctx, &migratorpb.ActivateMigrationRequest{Id: id})
	require.NoError(t, err)

	// Once the switch has returned, the source is a subscriber and the app role is
	// fenced: no further write may succeed. Snapshot successes, let the writer keep
	// trying, then assert it made no progress and observed the cut-off.
	okAtCutover := w.stats().ok
	time.Sleep(3 * time.Second)
	after := w.stats()
	require.Equal(t, okAtCutover, after.ok, "no source write may succeed after the cutover (writer must be cut off)")
	require.NotEmpty(t, after.lastErr, "the writer must observe its writes failing after the cutover")
	require.True(t, isFenceError(after.lastErr),
		"the cut-off must be a fence (terminated / CONNECT revoked / read-only); got %q", after.lastErr)
	w.stop()

	// No divergence: source and target must be byte-identical — same row count and
	// the same order-independent content checksum (sum of per-row hashtextextended).
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	sc := dialSource(t, ctx, srcPort) // superuser DSN: unaffected by the app-role fence
	defer sc.Close()
	require.Eventually(t, func() bool {
		sSig, sOK := contentSig(ctx, sc, "public.orders")
		tSig, tOK := contentSig(ctx, tc, "public.orders")
		sCount, cOK := countRows(t, ctx, sc, "public.orders")
		t.Logf("source sig=%q target sig=%q count=%d", sSig, tSig, sCount)
		return sOK && tOK && cOK && sCount > 3 && sSig == tSig
	}, 30*time.Second, 500*time.Millisecond,
		"source and target must end byte-identical (no divergence from a stray subscriber-side write)")

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id, Force: true})
	require.NoError(t, err)
}

// sourceWriter is a background client that inserts into public.orders on the source
// as a given role, reconnecting on failure, until stopped. It records how many
// inserts committed and the most recent error, so a test can prove the writer was
// cut off at the cutover.
type sourceWriter struct {
	port       int
	user, pass string

	mu      sync.Mutex
	ok      int
	lastErr string

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

type writerStats struct {
	ok      int
	lastErr string
}

func (w *sourceWriter) stats() writerStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return writerStats{ok: w.ok, lastErr: w.lastErr}
}

func (w *sourceWriter) recordOK() {
	w.mu.Lock()
	w.ok++
	w.mu.Unlock()
}

func (w *sourceWriter) recordErr(err error) {
	w.mu.Lock()
	w.lastErr = err.Error()
	w.mu.Unlock()
}

func (w *sourceWriter) start(ctx context.Context) {
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	go func() {
		defer close(w.doneCh)
		for {
			select {
			case <-w.stopCh:
				return
			default:
			}
			conn, err := client.Connect(ctx, ctx, &client.Config{
				Host:        "127.0.0.1",
				Port:        w.port,
				User:        w.user,
				Password:    w.pass,
				Database:    "postgres",
				SSLMode:     client.SSLModeDisable,
				DialTimeout: 2 * time.Second,
			})
			if err != nil {
				w.recordErr(err)
				if !sleepOrStop(w.stopCh, 100*time.Millisecond) {
					return
				}
				continue
			}
			for {
				select {
				case <-w.stopCh:
					conn.Close()
					return
				default:
				}
				if _, err := conn.Query(ctx, "INSERT INTO public.orders (v) VALUES ('w')"); err != nil {
					w.recordErr(err)
					break // drop the connection and try to reconnect
				}
				w.recordOK()
				if !sleepOrStop(w.stopCh, 50*time.Millisecond) {
					conn.Close()
					return
				}
			}
			conn.Close()
		}
	}()
}

func (w *sourceWriter) stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.doneCh
}

// sleepOrStop waits for d or until stop is closed; it returns false if stopped.
func sleepOrStop(stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}

// isFenceError reports whether an error message indicates the source rejected or
// severed the writer at the quiesce: a terminated backend, a lost connection, a
// revoked CONNECT privilege, or a read-only transaction.
func isFenceError(msg string) bool {
	msg = strings.ToLower(msg)
	for _, needle := range []string{
		"terminat",           // terminating connection due to administrator command
		"connect privilege",  // permission denied for database ... CONNECT privilege
		"permission denied",  // permission denied for database
		"read-only",          // cannot execute INSERT in a read-only transaction
		"connection",         // connection reset/closed/refused
		"eof",                // server hung up mid-statement
		"server closed",      // server closed the connection
		"cannot_connect_now", // 57P03
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// contentSig returns a byte-content signature of a table: "<rowcount>:<checksum>",
// where checksum is the order-independent sum of hashtextextended over each row's
// text image. Two tables with identical rows produce identical signatures.
func contentSig(ctx context.Context, conn *client.Conn, table string) (string, bool) {
	res, err := conn.Query(ctx,
		"SELECT count(*)::text || ':' || COALESCE(sum(hashtextextended(t::text, 0))::text, '0') FROM "+table+" t")
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
		return "", false
	}
	sig := string(res[0].Rows[0].Values[0])
	if _, err := strconv.Atoi(strings.SplitN(sig, ":", 2)[0]); err != nil {
		return "", false
	}
	return sig, true
}
