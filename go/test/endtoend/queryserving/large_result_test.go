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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestLargeResultThroughGateway is the regression for the gRPC 4 MiB result cap.
// A query result larger than the gRPC client default (4194304 bytes) must stream
// back through the pooler->gateway gRPC channel without RESOURCE_EXHAUSTED. The
// server limit is grpccommon.MaxMessageSize() (16 MiB default); 8 MiB exceeds the
// old client default but stays under the server limit.
func TestLargeResultThroughGateway(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	const sizeBytes = 8 * 1024 * 1024 // 8 MiB: > 4 MiB client default, < 16 MiB server limit
	query := "SELECT repeat('x', 8388608)"

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			// Low-level pgprotocol client (simple protocol) — a single 8 MiB row
			// forces one gRPC message > 4 MiB on the pooler->gateway hop.
			t.Run("low-level", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				results, err := conn.Query(ctx, query)
				require.NoError(t, err, "large result must not error (gRPC recv cap)")

				var got int
				for _, r := range results {
					for _, row := range r.Rows {
						got = len(row.Values[0])
					}
				}
				assert.Equal(t, sizeBytes, got, "full 8 MiB value should come back")
			})

			// pgx (extended protocol) — mirrors the real Postgrex/Ecto failure mode.
			t.Run("pgx", func(t *testing.T) {
				connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				var s string
				err = conn.QueryRow(ctx, query).Scan(&s)
				require.NoError(t, err, "large result must not error (gRPC recv cap)")
				assert.Equal(t, sizeBytes, len(s))
			})
		})
	}
}
