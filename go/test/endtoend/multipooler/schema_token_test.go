// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/testpoll"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// schemaTokenInterval is short so a schema change shows up in the token within
// a fraction of a second, and so "the token did not change" can be asserted by
// watching several intervals go by.
const schemaTokenInterval = 200 * time.Millisecond

// schemaTokenSetupManager runs a dedicated cluster with a short schema token
// interval. The shared clusters keep the default interval: a 200ms recompute
// on the admin pool would change the background load every other test in this
// package runs under.
var schemaTokenSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultipoolerExtraArgs("--schema-token-interval="+schemaTokenInterval.String()),
	)
})

// readSchemaToken reads the schema token from the first message of the
// pooler's health stream, which carries the current state.
func readSchemaToken(ctx context.Context, grpcPort int) (string, error) {
	conn, err := grpc.NewClient(
		"passthrough:///localhost:"+strconv.Itoa(grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := multipoolerservicepb.NewMultipoolerServiceClient(conn).
		StreamPoolerHealth(ctx, &multipoolerservicepb.StreamPoolerHealthRequest{})
	if err != nil {
		return "", err
	}
	resp, err := stream.Recv()
	if err != nil {
		return "", err
	}
	return resp.GetSchemaToken(), nil
}

// poolerSchemaToken is readSchemaToken for the test goroutine.
func poolerSchemaToken(t *testing.T, grpcPort int) string {
	t.Helper()
	token, err := readSchemaToken(t.Context(), grpcPort)
	require.NoError(t, err)
	return token
}

// TestSchemaToken drives schema changes directly against the primary's
// postgres and checks the token the primary reports on its health stream.
func TestSchemaToken(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := newMultipoolerTestSetup(schemaTokenSetupManager.Get(t))
	primaryPort := setup.PrimaryMultipooler.GrpcPort

	exec := func(sql string) {
		t.Helper()
		ctx := utils.WithTimeout(t, 10*time.Second)
		_, err := dialNormalConn(t, setup).Query(ctx, sql)
		require.NoError(t, err, sql)
	}
	waitForTokenChange := func(from string) string {
		t.Helper()
		var token string
		require.Eventually(t, func() bool {
			got, err := readSchemaToken(t.Context(), primaryPort)
			if err != nil {
				return false
			}
			token = got
			return token != "" && token != from
		}, 10*time.Second, schemaTokenInterval/2, "schema token did not change from %q", from)
		return token
	}
	// settledToken returns the token once it has held for several intervals.
	// The token is recomputed every interval, so a settled token reflects every
	// schema change committed before the call, including a previous run's
	// cleanup DDL on this shared cluster.
	settledToken := func() string {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		token := poolerSchemaToken(t, primaryPort)
		for time.Now().Before(deadline) {
			time.Sleep(3 * schemaTokenInterval)
			next := poolerSchemaToken(t, primaryPort)
			if next != "" && next == token {
				return token
			}
			token = next
		}
		require.FailNow(t, "schema token did not settle")
		return ""
	}
	requireTokenStays := func(want, msg string) {
		t.Helper()
		testpoll.Never(t, func() bool {
			return poolerSchemaToken(t, primaryPort) != want
		}, 5*schemaTokenInterval, schemaTokenInterval/2, msg)
	}

	settledToken()

	t.Run("follower reports no token", func(t *testing.T) {
		assert.Empty(t, poolerSchemaToken(t, setup.StandbyMultipooler.GrpcPort))
	})

	t.Run("changes that are not schema leave the token alone", func(t *testing.T) {
		start := settledToken()
		exec("CREATE TABLE schema_token_stats (id int)")
		t.Cleanup(func() {
			conn, ctx, done := cleanupConn(t, setup, 10*time.Second)
			defer done()
			if conn != nil {
				_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS schema_token_stats")
			}
		})
		withTable := waitForTokenChange(start)

		exec("INSERT INTO schema_token_stats SELECT generate_series(1, 1000)")
		exec("ANALYZE schema_token_stats")
		exec("CREATE TEMP TABLE schema_token_temp (id int)")
		requireTokenStays(withTable, "data, statistics and temp tables are not schema")

		exec("DROP TABLE schema_token_stats")
		dropped := waitForTokenChange(withTable)
		assert.Equal(t, start, dropped, "the token is derived from the schema, so undoing a change restores it")
	})

	t.Run("uncommitted DDL is not seen until commit", func(t *testing.T) {
		start := settledToken()
		conn := dialNormalConn(t, setup)
		ctx := utils.WithTimeout(t, 30*time.Second)
		_, err := conn.Query(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = conn.Query(ctx, "CREATE TABLE schema_token_txn (id int)")
		require.NoError(t, err)
		requireTokenStays(start, "DDL in an open transaction must not change the token")

		_, err = conn.Query(ctx, "COMMIT")
		require.NoError(t, err)
		t.Cleanup(func() {
			c, cctx, done := cleanupConn(t, setup, 10*time.Second)
			defer done()
			if c != nil {
				_, _ = c.Query(cctx, "DROP TABLE IF EXISTS schema_token_txn")
			}
		})
		waitForTokenChange(start)
	})

	t.Run("each kind of schema object changes the token", func(t *testing.T) {
		start := settledToken()
		exec("CREATE TABLE schema_token_kinds (id int PRIMARY KEY, v text)")
		t.Cleanup(func() {
			conn, ctx, done := cleanupConn(t, setup, 10*time.Second)
			defer done()
			if conn != nil {
				_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS schema_token_kinds CASCADE")
			}
		})
		token := waitForTokenChange(start)

		for _, ddl := range []string{
			"ALTER TABLE schema_token_kinds ADD COLUMN n numeric DEFAULT 1.5",
			"ALTER TABLE schema_token_kinds ALTER COLUMN v SET NOT NULL",
			"CREATE INDEX schema_token_kinds_v ON schema_token_kinds (v)",
			"ALTER TABLE schema_token_kinds ADD CONSTRAINT schema_token_kinds_n CHECK (n > 0)",
			"CREATE VIEW schema_token_kinds_view AS SELECT id FROM schema_token_kinds",
			"ALTER TABLE schema_token_kinds ENABLE ROW LEVEL SECURITY",
			"CREATE POLICY schema_token_kinds_policy ON schema_token_kinds USING (id > 0)",
		} {
			exec(ddl)
			token = waitForTokenChange(token)
		}
	})
}
