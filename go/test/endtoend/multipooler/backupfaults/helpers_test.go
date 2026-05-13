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

package backupfaults

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
)

// connectToPostgresViaSocket opens a SQL connection to PostgreSQL over its
// Unix socket. The caller is responsible for Close().
func connectToPostgresViaSocket(t *testing.T, socketDir string, port int) *sql.DB {
	t.Helper()

	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", socketDir, port)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "open postgres via socket")
	require.NoError(t, db.Ping(), "ping postgres via socket")
	return db
}

// createBackupClient returns a multipooler manager gRPC client suitable for
// issuing Backup RPCs. Connection is closed via t.Cleanup.
func createBackupClient(t *testing.T, grpcPort int) multipoolermanagerpb.MultiPoolerManagerClient {
	t.Helper()

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "create gRPC connection")
	t.Cleanup(func() { conn.Close() })

	return multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
}

// findMissingIDs returns the subset of want that is not present in
// SELECT id FROM <table>. Uses a single ANY($1) lookup so the query cost is
// O(matched rows), not O(want).
func findMissingIDs(t *testing.T, db *sql.DB, table string, want []int64) []int64 {
	t.Helper()

	// #nosec G202 -- table is WriterValidator's generated test-only name, not user input.
	q := "SELECT id FROM " + table + " WHERE id = ANY($1)"
	queryCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(queryCtx, q, pq.Array(want))
	require.NoError(t, err)
	defer rows.Close()

	found := make(map[int64]struct{}, len(want))
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		found[id] = struct{}{}
	}
	require.NoError(t, rows.Err())

	var missing []int64
	for _, id := range want {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}
