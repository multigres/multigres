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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestUpdateMigrationConnection proves update-migration repoints a live
// subscription's connection: after a migration is streaming, rotate the source
// password, update the migration's source DSN (which issues ALTER SUBSCRIPTION
// CONNECTION), and confirm a subsequent source write still reaches the target —
// which can only happen if the apply worker reconnected with the new
// credentials. It also confirms a source-database change is rejected.
func TestUpdateMigrationConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator update e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		SourceDsn:      sourceDSN(srcPort),
		TargetDatabase: targetDB,
		Tables:         []string{"public.orders"},
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()
	_, err = mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		return err == nil && len(resp.GetMigrations()) == 1 && resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up")

	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()

	// Rotate the source password. The target's existing apply connection is
	// unaffected until it reconnects; ALTER SUBSCRIPTION CONNECTION forces that.
	const rotated = "rotated_pw"
	sc := dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "ALTER ROLE postgres PASSWORD '"+rotated+"'")
	require.NoError(t, err)
	_ = sc.Close()

	newDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable", srcPort, rotated)
	_, err = mt.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
		Id:         id,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"source_dsn"}},
		SourceDsn:  newDSN,
	})
	require.NoError(t, err, "update source connection")

	// A source write after the rotation must reach the target, proving the apply
	// worker reconnected with the new credentials.
	scNew := dialSourcePw(t, ctx, srcPort, rotated)
	_, err = scNew.Query(ctx, "INSERT INTO public.orders (v) VALUES ('d')")
	require.NoError(t, err)
	_ = scNew.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		t.Logf("target count after connection update=%d ok=%v", n, ok)
		return ok && n == 4
	}, 30*time.Second, 500*time.Millisecond, "streaming must resume with the rotated credentials")

	// A source-database change is rejected (the slot and origin are tied to the
	// source database).
	otherDB := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=otherdb sslmode=disable", srcPort, rotated)
	_, err = mt.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
		Id:         id,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"source_dsn"}},
		SourceDsn:  otherDB,
	})
	require.Error(t, err, "changing the source database must be rejected")

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id, Force: true})
	require.NoError(t, err)
}

// dialSourcePw opens a pgprotocol connection to the standalone source with an
// explicit password (used after a password rotation).
func dialSourcePw(t *testing.T, ctx context.Context, port int, password string) *client.Conn {
	t.Helper()
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "127.0.0.1",
		Port:        port,
		User:        "postgres",
		Password:    password,
		Database:    "postgres",
		SSLMode:     client.SSLModeDisable,
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return conn
}
