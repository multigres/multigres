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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// copyRecoveryProxy holds only the affected table's COPY and successful slot-drop
// response. All authentication and replication traffic passes through unchanged.
// The COPY gate lets the healthy table finish before the fault is injected.
// The DROP gate exposes PostgreSQL's uncommitted SYNCDONE transaction without
// timing-dependent sleeps or writes to PostgreSQL catalogs.
type copyRecoveryProxy struct {
	listener     net.Listener
	copyStarted  chan struct{}
	allowCopy    chan struct{}
	dropped      chan string
	allowDrop    chan struct{}
	closeCopy    sync.Once
	closeDrop    sync.Once
	dropHeld     atomic.Bool
	mu           sync.Mutex
	connections  []net.Conn
	closed       bool
	createdSlots map[string]int
}

func newCopyRecoveryProxy(t *testing.T, sourcePort int) *copyRecoveryProxy {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", utils.GetFreePort(t)))
	require.NoError(t, err)
	p := &copyRecoveryProxy{listener: listener, copyStarted: make(chan struct{}, 1), allowCopy: make(chan struct{}), dropped: make(chan string, 1), allowDrop: make(chan struct{}), createdSlots: make(map[string]int)}
	go func() {
		for {
			front, err := listener.Accept()
			if err != nil {
				return
			}
			back, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", sourcePort), 5*time.Second)
			if err != nil {
				_ = front.Close()
				continue
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				_ = front.Close()
				_ = back.Close()
				return
			}
			p.connections = append(p.connections, front, back)
			p.mu.Unlock()
			go p.serve(front, back)
		}
	}()
	return p
}

func (p *copyRecoveryProxy) releaseCopy() { p.closeCopy.Do(func() { close(p.allowCopy) }) }
func (p *copyRecoveryProxy) releaseDrop() { p.closeDrop.Do(func() { close(p.allowDrop) }) }
func (p *copyRecoveryProxy) slotCreationCount(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.createdSlots[name]
}

func (p *copyRecoveryProxy) tableCopyCount(slotPrefix string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var count int
	for name, created := range p.createdSlots {
		if strings.HasPrefix(name, slotPrefix) {
			count += created
		}
	}
	return count
}

func (p *copyRecoveryProxy) close() {
	p.releaseCopy()
	p.releaseDrop()
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, conn := range p.connections {
		_ = conn.Close()
	}
}

func copyRecoveryFrame(conn net.Conn) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 || length > 64*1024*1024 {
		return 0, nil, fmt.Errorf("invalid PostgreSQL frame length %d", length)
	}
	payload := make([]byte, length-4)
	_, err := io.ReadFull(conn, payload)
	return header[0], payload, err
}

func copyRecoveryWrite(conn net.Conn, tag byte, payload []byte) error {
	frame := make([]byte, len(payload)+5)
	frame[0] = tag
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)+4))
	copy(frame[5:], payload)
	_, err := io.Copy(conn, bytes.NewReader(frame))
	return err
}

func (p *copyRecoveryProxy) serve(front, back net.Conn) {
	defer front.Close()
	defer back.Close()
	// Relay optional SSL/GSS negotiation before the ordinary Startup packet.
	for {
		var header [4]byte
		if _, err := io.ReadFull(front, header[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header[:])
		if length < 8 || length > 1024*1024 {
			return
		}
		startup := make([]byte, length)
		copy(startup, header[:])
		if _, err := io.ReadFull(front, startup[4:]); err != nil {
			return
		}
		if _, err := io.Copy(back, bytes.NewReader(startup)); err != nil {
			return
		}
		code := binary.BigEndian.Uint32(startup[4:8])
		if length == 8 && (code == 80877103 || code == 80877104) {
			var reply [1]byte
			if _, err := io.ReadFull(back, reply[:]); err != nil {
				return
			}
			if _, err := front.Write(reply[:]); err != nil {
				return
			}
			continue
		}
		break
	}
	var pendingDrop atomic.Bool
	var droppedSlot atomic.Value
	go func() {
		defer front.Close()
		defer back.Close()
		for {
			tag, payload, err := copyRecoveryFrame(back)
			if err != nil {
				return
			}
			if tag == 'C' && pendingDrop.Swap(false) && p.dropHeld.CompareAndSwap(false, true) {
				select {
				case p.dropped <- droppedSlot.Load().(string):
				default:
				}
				<-p.allowDrop
			}
			if copyRecoveryWrite(front, tag, payload) != nil {
				return
			}
		}
	}()
	affected := false
	for {
		tag, payload, err := copyRecoveryFrame(front)
		if err != nil {
			return
		}
		if tag == 'Q' {
			query := strings.TrimSuffix(string(payload), "\x00")
			if strings.HasPrefix(query, "CREATE_REPLICATION_SLOT") {
				parts := strings.Fields(query)
				if len(parts) >= 2 {
					p.mu.Lock()
					p.createdSlots[strings.Trim(parts[1], "\"")]++
					p.mu.Unlock()
				}
			}
			if strings.HasPrefix(query, "COPY ") && strings.Contains(query, "recovery_stuck") {
				affected = true
				select {
				case p.copyStarted <- struct{}{}:
				default:
				}
				<-p.allowCopy
			}
			if affected && strings.HasPrefix(query, "DROP_REPLICATION_SLOT") && strings.Contains(query, "_sync_") {
				parts := strings.Fields(query)
				if len(parts) >= 2 {
					droppedSlot.Store(strings.Trim(parts[1], "\""))
					pendingDrop.Store(true)
				}
			}
		}
		if copyRecoveryWrite(back, tag, payload) != nil {
			return
		}
	}
}

func copyRecoveryScalar(ctx context.Context, conn *client.Conn, query string) (string, error) {
	results, err := conn.Query(ctx, query)
	if err != nil {
		return "", err
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0].Values) != 1 {
		return "", fmt.Errorf("expected one scalar result: %s", query)
	}
	return string(results[0].Rows[0].Values[0]), nil
}

// TestMissingTableSyncSlotRecovery reproduces the PostgreSQL handoff failure by
// aborting the worker after its source slot disappears but before SYNCDONE
// commits. Multigres must recopy that table without restarting the healthy copy.
func TestMissingTableSyncSlotRecovery(t *testing.T) {
	testMissingTableSyncSlotRecovery(t, "")
}

// TestTableCopyRecoveryAfterFailover promotes a physical standby after the
// repair's TRUNCATE commits. The new primary must resume at reattachment and
// must preserve the healthy table while the old primary remains down.
func TestTableCopyRecoveryAfterFailover(t *testing.T) {
	t.Run("attach", func(t *testing.T) { testMissingTableSyncSlotRecovery(t, "attach") })
}

func testMissingTableSyncSlotRecovery(t *testing.T, failoverStage string) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator recovery e2e in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("no postgres binaries")
	}
	ctx := t.Context()
	poolerCount := 2
	if failoverStage != "" {
		poolerCount = 3
	}
	options := []shardsetup.SetupOption{shardsetup.WithMultipoolerCount(poolerCount), shardsetup.WithDatabase("postgres"), shardsetup.WithCellName("test-cell")}
	if failoverStage != "" {
		options = append(options, shardsetup.WithMultiorchCount(3))
	}
	setup, cleanup := shardsetup.NewIsolated(t, options...)
	defer cleanup()
	if failoverStage != "" {
		setup.StartMultiorchs(ctx, t)
	}
	primary := setup.GetPrimary(t)
	oldPrimaryName := setup.PrimaryName
	targetDB := waitForPrimaryDB(t, ctx, setup)
	srcPort := startStandaloneSource(t)
	sc := dialSource(t, ctx, srcPort)
	defer sc.Close()
	_, err := sc.Query(ctx, `CREATE TABLE public.recovery_healthy(id integer PRIMARY KEY,v text);
CREATE TABLE public.recovery_stuck(id integer PRIMARY KEY,v text);
INSERT INTO public.recovery_healthy SELECT i,'original-'||i FROM generate_series(1,200)i;
INSERT INTO public.recovery_stuck SELECT * FROM public.recovery_healthy`)
	require.NoError(t, err)
	proxy := newCopyRecoveryProxy(t, srcPort)
	defer proxy.close()
	mt, closeMT := migrationClient(t, primary)
	defer closeMT()
	proxyPort := proxy.listener.Addr().(*net.TCPAddr).Port
	created, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{Migration: &migratorpb.MigrationRecord{
		Target:         &clustermetadatapb.ShardRef{Database: targetDB},
		ConnectionName: createTestConnection(t, ctx, mt, sourceDSN(proxyPort)),
		Objects:        objs("public.recovery_healthy", "public.recovery_stuck"),
	}})
	require.NoError(t, err)
	id := created.GetMigration().GetId()
	_, err = mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err)
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	mustScalar := func(conn *client.Conn, query string) string {
		t.Helper()
		value, err := copyRecoveryScalar(ctx, conn, query)
		require.NoError(t, err)
		return value
	}
	stateSQL := "SELECT srsubstate FROM pg_subscription_rel WHERE srrelid='public.recovery_stuck'::regclass"
	healthySQL := "SELECT srsubstate FROM pg_subscription_rel WHERE srrelid='public.recovery_healthy'::regclass"
	require.Eventually(t, func() bool { value, err := copyRecoveryScalar(ctx, tc, healthySQL); return err == nil && value == "r" }, 30*time.Second, 50*time.Millisecond, "healthy table must finish while the affected COPY waits")
	select {
	case <-proxy.copyStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("affected COPY did not reach the proxy")
	}
	subscriptionOID := mustScalar(tc, fmt.Sprintf("SELECT oid FROM pg_subscription WHERE subname='mt_sub_%d'", id))
	healthyOID := mustScalar(tc, "SELECT 'public.recovery_healthy'::regclass::oid")
	stuckOID := mustScalar(tc, "SELECT 'public.recovery_stuck'::regclass::oid")
	healthyFilenode := mustScalar(tc, "SELECT pg_relation_filenode('public.recovery_healthy')")
	mainSlotSQL := fmt.Sprintf("SELECT slot_name FROM pg_replication_slots WHERE slot_name='mt_sub_%d'", id)
	mainSlot := mustScalar(sc, mainSlotSQL)
	publicationSQL := fmt.Sprintf("SELECT json_agg(t ORDER BY tablename)::text FROM (SELECT schemaname,tablename,attnames,rowfilter FROM pg_publication_tables WHERE pubname='mt_pub_%d')t", id)
	originalPublication := mustScalar(sc, publicationSQL)
	proxy.releaseCopy()
	var missingSlot string
	select {
	case missingSlot = <-proxy.dropped:
	case <-time.After(30 * time.Second):
		t.Fatal("affected table did not reach source slot deletion")
	}
	state := mustScalar(tc, stateSQL)
	if state == "s" || state == "r" {
		t.Skipf("PostgreSQL completed the handoff before deleting its slot (state %q)", state)
	}
	require.Equal(t, "f", state, "table must reach FINISHEDCOPY before the fault")
	require.Equal(t, "0", mustScalar(sc, fmt.Sprintf("SELECT count(*) FROM pg_replication_slots WHERE slot_name='%s'", missingSlot)))
	markerSQL := fmt.Sprintf("SELECT copy_recovery IS NULL FROM multigres.migration WHERE migration_id=%d", id)
	// Observe the background coordinator waiting for the live SYNCDONE
	// transaction. A timeout must leave the table and recovery marker unchanged.
	waitingLockSQL := fmt.Sprintf(`SELECT count(*) FROM pg_locks l
JOIN pg_subscription s ON s.oid=l.objid
WHERE l.locktype='object' AND l.classid='pg_subscription'::regclass
AND l.mode='AccessExclusiveLock' AND NOT l.granted AND s.subname='mt_sub_%d'`, id)
	require.Eventually(t, func() bool {
		value, err := copyRecoveryScalar(ctx, tc, waitingLockSQL)
		return err == nil && value != "0"
	}, 30*time.Second, 10*time.Millisecond, "coordinator must wait for the live handoff lock")
	require.Eventually(t, func() bool {
		value, err := copyRecoveryScalar(ctx, tc, waitingLockSQL)
		return err == nil && value == "0"
	}, 5*time.Second, 10*time.Millisecond, "coordinator guard must time out")
	status, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(id)})
	require.NoError(t, err)
	require.False(t, status.GetStatus().GetCaughtUp())
	require.Equal(t, "t", mustScalar(tc, markerSQL), "live handoff must not start recovery")
	require.Equal(t, "f", mustScalar(tc, stateSQL))
	require.Equal(t, originalPublication, mustScalar(sc, publicationSQL))
	require.Equal(t, "t", mustScalar(tc, "SELECT coalesce(bool_and(pg_terminate_backend(pid)),false) FROM pg_stat_subscription WHERE relid='public.recovery_stuck'::regclass AND pid IS NOT NULL"), "held synchronization worker must terminate")
	proxy.releaseDrop()
	checkpointSQL := fmt.Sprintf("SELECT coalesce(copy_recovery->>'stage','') FROM multigres.migration WHERE migration_id=%d", id)
	repairStarted := make(chan struct{})
	var repairStartedOnce sync.Once
	signalRepairStarted := func(stage string) {
		if stage == "attach" {
			repairStartedOnce.Do(func() { close(repairStarted) })
		}
	}
	writerCtx, cancelWriter := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelWriter()
	writer := dialSource(t, writerCtx, srcPort)
	writerDone := make(chan error, 1)
	go func() {
		defer writer.Close()
		writeBatch := func(start, end int, paced bool) error {
			for i := start; i <= end; i++ {
				_, err := writer.Query(writerCtx, fmt.Sprintf(`BEGIN;
INSERT INTO public.recovery_healthy VALUES(%d,'new-%d');
INSERT INTO public.recovery_stuck VALUES(%d,'new-%d');
UPDATE public.recovery_healthy SET v='updated-%d' WHERE id=10;
UPDATE public.recovery_stuck SET v='updated-%d' WHERE id=10;
DELETE FROM public.recovery_healthy WHERE id=%d;
DELETE FROM public.recovery_stuck WHERE id=%d;
COMMIT`, i, i, i, i, i, i, i-100, i-100))
				if err != nil {
					return err
				}
				if paced {
					select {
					case <-time.After(50 * time.Millisecond):
					case <-writerCtx.Done():
						return writerCtx.Err()
					}
				}
			}
			return nil
		}
		// The first batch fills the lost interval. The second starts only after
		// the table is detached and truncated, before reattachment at a later tick.
		if err := writeBatch(201, 260, false); err != nil {
			writerDone <- err
			return
		}
		select {
		case <-repairStarted:
		case <-writerCtx.Done():
			writerDone <- writerCtx.Err()
			return
		}
		writerDone <- writeBatch(261, 320, true)
	}()
	if failoverStage != "" {
		manager, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
		require.NoError(t, err)
		defer manager.Close()
		disableCtx, cancelDisable := context.WithTimeout(ctx, 5*time.Second)
		_, err = manager.Manager.SetPostgresRestartsEnabled(disableCtx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		cancelDisable()
		require.NoError(t, err)
		defer func() {
			resumeCtx, cancelResume := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelResume()
			_, err := manager.Manager.SetPostgresRestartsEnabled(resumeCtx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
			if err != nil {
				t.Logf("restore postgres restarts on %s: %v", oldPrimaryName, err)
				return
			}
			// Let PostgreSQL recover its crash resources before setup cleanup
			// stops the control processes. This runs after all recovery assertions.
			restartCtx, cancelRestart := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelRestart()
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				attemptCtx, cancelAttempt := context.WithTimeout(restartCtx, time.Second)
				conn, err := client.Connect(attemptCtx, restartCtx, &client.Config{
					Host: "localhost", Port: primary.Pgctld.PgPort,
					User: "postgres", Password: shardsetup.TestPostgresPassword,
					Database: targetDB, SSLMode: client.SSLModeDisable,
					DialTimeout: time.Second,
				})
				cancelAttempt()
				if err == nil {
					_ = conn.Close()
					t.Logf("old primary %s accepted an authenticated connection before cleanup", oldPrimaryName)
					return
				}
				select {
				case <-restartCtx.Done():
					t.Logf("old primary %s did not recover before cleanup: %v (last connection error: %v)", oldPrimaryName, restartCtx.Err(), err)
					return
				case <-ticker.C:
				}
			}
		}()
		require.Eventually(t, func() bool {
			stage, err := copyRecoveryScalar(ctx, tc, checkpointSQL)
			if err == nil {
				signalRepairStarted(stage)
			}
			return err == nil && stage == failoverStage
		}, 60*time.Second, 50*time.Millisecond, "repair must persist the %s checkpoint before failover", failoverStage)
		t.Logf("killing primary %s at repair checkpoint %s", oldPrimaryName, failoverStage)
		setup.KillPostgres(t, oldPrimaryName)
		newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 60*time.Second)
		require.NotEmpty(t, newPrimaryName)
		newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimary)
		shardsetup.WaitForManagerReady(t, newPrimary.Multipooler)
		tc = targetConn(t, ctx, newPrimary, targetDB)
		defer tc.Close()
		var closeNewMT func()
		mt, closeNewMT = migrationClient(t, newPrimary)
		defer closeNewMT()
	}
	require.Eventually(t, func() bool {
		if stage, err := copyRecoveryScalar(ctx, tc, checkpointSQL); err == nil {
			signalRepairStarted(stage)
		}
		response, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(id)})
		if err != nil {
			return false
		}
		status := response.GetStatus()
		t.Logf("recovery phase=%s ready=%d/%d error=%q", status.GetPhase(), status.GetReadyRelations(), status.GetTotalRelations(), status.GetLastError())
		return status.GetPhase() == migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING && status.GetCaughtUp()
	}, 90*time.Second, 250*time.Millisecond, "affected table must recover and migration must reach IMPORTING")
	select {
	case err := <-writerDone:
		require.NoError(t, err)
	case <-writerCtx.Done():
		t.Fatalf("source writer did not finish: %v", writerCtx.Err())
	case <-time.After(15 * time.Second):
		t.Fatal("source writer did not finish after target recovery")
	}
	for _, table := range []string{"recovery_healthy", "recovery_stuck"} {
		query := fmt.Sprintf("SELECT json_agg(t ORDER BY id)::text FROM public.%s t", table)
		expected := mustScalar(sc, query)
		require.Eventually(t, func() bool { value, err := copyRecoveryScalar(ctx, tc, query); return err == nil && value == expected }, 30*time.Second, 100*time.Millisecond, "%s must match source after inserts, updates, and deletes", table)
	}
	require.Equal(t, healthyFilenode, mustScalar(tc, "SELECT pg_relation_filenode('public.recovery_healthy')"), "healthy table must not be recopied")
	require.Equal(t, mainSlot, mustScalar(sc, mainSlotSQL), "main source slot must survive recovery")
	require.Equal(t, 1, proxy.slotCreationCount(mainSlot), "main source slot must not be recreated")
	require.Equal(t, 1, proxy.tableCopyCount(fmt.Sprintf("pg_%s_sync_%s_", subscriptionOID, healthyOID)), "healthy table must be copied once")
	require.Equal(t, 2, proxy.tableCopyCount(fmt.Sprintf("pg_%s_sync_%s_", subscriptionOID, stuckOID)), "only affected table must be copied again")
	require.Equal(t, originalPublication, mustScalar(sc, publicationSQL), "original source publication must remain unchanged")
	require.Equal(t, "t", mustScalar(tc, markerSQL), "repair marker must clear")
	require.Equal(t, "0", mustScalar(sc, fmt.Sprintf("SELECT count(*) FROM pg_publication WHERE pubname='mt_pub_%d_recopy'", id)), "temporary publication must be removed")
	require.Equal(t, "r", mustScalar(tc, stateSQL))
	require.Equal(t, "r", mustScalar(tc, healthySQL))
}
