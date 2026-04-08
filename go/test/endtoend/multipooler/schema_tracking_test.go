// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSchemaTracking_EventTriggerInstalled verifies that the DDL event trigger
// is installed in the multigres sidecar schema during bootstrap. This requires
// superuser access (the postgres user created by initdb).
func TestSchemaTracking_EventTriggerInstalled(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 10*time.Second)

	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	// Verify the event trigger function exists in the multigres schema.
	result, err := primaryClient.Pooler.ExecuteQuery(ctx,
		"SELECT proname::text FROM pg_catalog.pg_proc p "+
			"JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "+
			"WHERE n.nspname = 'multigres' AND p.proname = 'notify_schema_change'", 1)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "notify_schema_change function should exist in multigres schema")
	assert.Equal(t, "notify_schema_change", string(result.Rows[0].Values[0]))

	// Verify the event trigger itself exists.
	result, err = primaryClient.Pooler.ExecuteQuery(ctx,
		"SELECT evtname::text FROM pg_catalog.pg_event_trigger "+
			"WHERE evtname = 'multigres_schema_change'", 1)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "multigres_schema_change event trigger should exist")
	assert.Equal(t, "multigres_schema_change", string(result.Rows[0].Values[0]))
}

// TestSchemaTracking_DDLIncrementsVersion verifies the full schema tracking
// pipeline: DDL on the primary increments the schema version, which is then
// visible in the multipooler health stream.
//
// The test uses CREATE TABLE to prime the NOTIFY channel (the pubsub LISTEN
// may take a moment to establish after bootstrap), then verifies that ALTER
// TABLE triggers an immediate version bump via the fast path.
func TestSchemaTracking_DDLIncrementsVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	primary := setup.GetPrimary(t)
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	// CREATE TABLE — this primes the pubsub LISTEN channel and triggers the
	// first schema version increment (detected via periodic poll or NOTIFY).
	tableName := fmt.Sprintf("schema_track_test_%d", time.Now().UnixNano())
	_, err := primaryClient.Pooler.ExecuteQuery(ctx,
		fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY)", tableName), 0)
	require.NoError(t, err)
	defer func() {
		cleanCtx := utils.WithTimeout(t, 5*time.Second)
		_, _ = primaryClient.Pooler.ExecuteQuery(cleanCtx,
			"DROP TABLE IF EXISTS "+tableName, 0)
	}()

	// Open a health stream and read the current version (post-CREATE).
	addr := fmt.Sprintf("localhost:%d", primary.Multipooler.GrpcPort)
	conn, err := grpc.NewClient(
		"passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	healthClient := multipoolerpb.NewMultiPoolerServiceClient(conn)
	stream, err := healthClient.StreamPoolerHealth(ctx, &multipoolerpb.StreamPoolerHealthRequest{})
	require.NoError(t, err)

	initialResp, err := stream.Recv()
	require.NoError(t, err)
	versionAfterCreate := initialResp.SchemaVersion
	t.Logf("version after CREATE: %d", versionAfterCreate)

	// ALTER TABLE — by now the NOTIFY channel is established, so the schema
	// tracker detects this immediately via the fast path.
	_, err = primaryClient.Pooler.ExecuteQuery(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN data TEXT", tableName), 0)
	require.NoError(t, err)

	// The schema version should increase. Each Recv() blocks until the next
	// health broadcast (up to the 30s heartbeat interval).
	for {
		resp, err := stream.Recv()
		require.NoError(t, err, "health stream should not error")
		t.Logf("schema version: %d (waiting for > %d)", resp.SchemaVersion, versionAfterCreate)
		if resp.SchemaVersion > versionAfterCreate {
			break
		}
	}
}
