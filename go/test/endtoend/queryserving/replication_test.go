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

package queryserving

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestReplicationStubs verifies that logical-replication commands are
// recognized by the multigateway (no "unrecognized configuration parameter
// \"replication\"" error) and return feature_not_supported (SQLSTATE 0A000).
// On the same connection, a regular SELECT must still work — that's the
// fix for the GUC-forwarding bug.
func TestReplicationStubs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication stubs test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	dsn := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	cfg, err := pgconn.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	require.NoError(t, err, "connect with replication=database must succeed")
	defer conn.Close(ctx)

	t.Run("IDENTIFY_SYSTEM returns 0A000", func(t *testing.T) {
		_, err := pglogrepl.IdentifySystem(ctx, conn)
		assertFeatureNotSupported(t, err, "IDENTIFY_SYSTEM")
	})

	t.Run("CREATE_REPLICATION_SLOT returns 0A000", func(t *testing.T) {
		_, err := pglogrepl.CreateReplicationSlot(ctx, conn, "s1", "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
		assertFeatureNotSupported(t, err, "CREATE_REPLICATION_SLOT")
	})

	t.Run("DROP_REPLICATION_SLOT returns 0A000", func(t *testing.T) {
		err := pglogrepl.DropReplicationSlot(ctx, conn, "s1",
			pglogrepl.DropReplicationSlotOptions{})
		assertFeatureNotSupported(t, err, "DROP_REPLICATION_SLOT")
	})

	t.Run("START_REPLICATION returns 0A000", func(t *testing.T) {
		err := pglogrepl.StartReplication(ctx, conn, "s1", 0,
			pglogrepl.StartReplicationOptions{
				Mode: pglogrepl.LogicalReplication,
				PluginArgs: []string{
					"proto_version '1'",
					"publication_names 'p'",
				},
			})
		assertFeatureNotSupported(t, err, "START_REPLICATION")
	})

	t.Run("regular SELECT 1 still works on the same connection", func(t *testing.T) {
		// This validates the consume-and-delete fix: the `replication`
		// startup param must NOT be forwarded to PG as a runtime GUC.
		result := conn.ExecParams(ctx, "SELECT 1", nil, nil, nil, nil).Read()
		require.NoError(t, result.Err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, []byte("1"), result.Rows[0][0])
	})
}

func assertFeatureNotSupported(t *testing.T, err error, opName string) {
	t.Helper()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "want *pgconn.PgError, got %T (%v)", err, err)
	assert.Equal(t, "0A000", pgErr.Code, "SQLSTATE for %s", opName)
	assert.Contains(t, strings.ToUpper(pgErr.Message), opName,
		"error message for %s should mention the op name", opName)
}
