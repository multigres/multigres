// Copyright 2026 Supabase, Inc.
// Portions derived from PgBouncer (ISC License),
// Copyright (c) 2007-2009 Marko Kreen, Skype Technologies OÜ.
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

package pgbouncertests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Message sizes chosen to exceed PgBouncer's historical 4 KB packet buffer
// (PKT_BUF_SIZE) and its 4x threshold, so they exercise the multigateway's
// large-message relay the same way test_prepared.py's
// test_{parse,bind}_larger_than_pkt_buf{,_but_smaller_than_4x} do.
const (
	largeParseBytes = 16 * 1024 // 16 KB of SQL text in the Parse message
	largeBindBytes  = 64 * 1024 // 64 KB of parameter data in the Bind message
)

// TestLargePreparedStatementRelay verifies that the multigateway relays Parse
// and Bind messages far larger than a typical 4 KB packet buffer without
// truncation or corruption. Ported from PgBouncer's
// test_prepared.py::test_parse_larger_than_pkt_buf and
// test_bind_larger_than_pkt_buf.
//
// PostgreSQL is the oracle: each case runs against both direct PostgreSQL and
// the multigateway and must return identical results.
func TestLargePreparedStatementRelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	// All-'x' / all-'y' payloads contain no quote or backslash, so they embed
	// safely in a string literal and round-trip as a bind value unchanged.
	largeLiteral := strings.Repeat("x", largeParseBytes)
	largeParam := strings.Repeat("y", largeBindBytes)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")

			t.Run("large parse message", func(t *testing.T) {
				conn := connectGateway(t, ctx, connStr)
				defer conn.Close(ctx)

				// The big literal lives in the SQL text, so the Parse message
				// is ~16 KB. Returning it also confirms it survived intact.
				sql := "SELECT $1::int AS v, '" + largeLiteral + "'::text AS big"
				var v int
				var big string
				require.NoError(t, conn.QueryRow(ctx, sql, 7).Scan(&v, &big))
				assert.Equal(t, 7, v)
				assert.Equal(t, largeLiteral, big, "large SQL literal must round-trip intact")
			})

			t.Run("large bind parameter", func(t *testing.T) {
				conn := connectGateway(t, ctx, connStr)
				defer conn.Close(ctx)

				// The big value travels as a bind parameter, so the Bind
				// message is ~64 KB.
				var got string
				var n int
				require.NoError(t, conn.QueryRow(ctx,
					"SELECT $1::text AS p, length($1::text) AS n", largeParam).Scan(&got, &n))
				assert.Equal(t, largeBindBytes, n, "server-side length must match the sent parameter")
				assert.Equal(t, largeParam, got, "large bind parameter must round-trip intact")
			})
		})
	}
}

// TestLargePreparedStatementAcrossPooledBackends combines the large-Parse path
// with the pooled-backend rotation of TestPreparedStatementAcrossPooledBackends:
// a ~16 KB prepared statement must re-prepare correctly (multipooler
// ensurePrepared) on every backend the pool serves it from and always return the
// full literal intact. Black-box (see that test for the rationale): rotation is
// driven by concurrent churn and observed via pg_backend_pid() in results.
func TestLargePreparedStatementAcrossPooledBackends(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 90*time.Second)

	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	largeLiteral := strings.Repeat("x", largeParseBytes)
	const psName = "ps_large_swap"
	_, err := conn.Prepare(ctx, psName, "SELECT pg_backend_pid(), $1::int, '"+largeLiteral+"'::text")
	require.NoError(t, err)

	stopChurn := startPoolChurn(t, ctx, gatewayDSN, churnClients)
	defer stopChurn()

	runAcrossBackends(t, ctx, func(ctx context.Context, i int) int {
		var pid, v int
		var big string
		require.NoError(t, conn.QueryRow(ctx, psName, i).Scan(&pid, &v, &big),
			"large prepared statement must re-prepare on its backend (execute %d)", i)
		require.Equal(t, i, v, "execute %d returned the wrong bound parameter", i)
		require.Equal(t, largeLiteral, big, "execute %d: large SQL literal must survive re-prepare", i)
		return pid
	})
}
