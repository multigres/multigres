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

// TestLargeResultThroughGateway verifies that a single DataRow larger than the
// gRPC message limit is chunked across stream responses and reconstructed by the
// gateway without RESOURCE_EXHAUSTED. The default gRPC limit is 16 MiB, so the
// test uses one 20 MiB field rather than merely a large multi-row result set.
func TestLargeResultThroughGateway(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	const sizeBytes = 20 * 1024 * 1024 // One DataRow larger than the 16 MiB gRPC default.
	query := "SELECT repeat('x', 20971520)"

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			// Low-level pgprotocol client exercises the simple-query response loop.
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
				assert.Equal(t, sizeBytes, got, "full 20 MiB value should come back")
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
