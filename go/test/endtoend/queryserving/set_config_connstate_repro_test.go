// Copyright 2026 Supabase, Inc.
//
// One-off repro: does set_config(..., false) without a follow-up query leave a
// pooled backend with session GUCs that connstate does not describe?

package queryserving

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestSetConfigWithoutFollowUpQuery_CrossClientLeak(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// Connection A: set_config then disconnect with no SHOW/SELECT after.
	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "SELECT set_config('work_mem', '256MB', false)")
	require.NoError(t, err)
	require.NoError(t, connA.Close())

	// Connection B: fresh client expecting default work_mem.
	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	require.NoError(t, connB.PingContext(ctx))

	var workMem string
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	t.Logf("connection B SHOW work_mem = %q", workMem)
	require.NotEqual(t, "256MB", workMem,
		"set_config on connection A must not leak work_mem to a fresh connection B")
}

func TestSetConfigInTxnCommitWithoutFollowUp_CrossClientLeak(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	tx, err := connA.BeginTx(ctx, nil)
	require.NoError(t, err)
	var ignored string
	require.NoError(t, tx.QueryRowContext(ctx,
		"SELECT set_config('client_min_messages', 'error', false)").Scan(&ignored))
	require.NoError(t, tx.Commit())
	require.NoError(t, connA.Close())

	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	require.NoError(t, connB.PingContext(ctx))

	var clientMin string
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW client_min_messages").Scan(&clientMin))
	t.Logf("connection B SHOW client_min_messages = %q", clientMin)
	require.NotEqual(t, "error", clientMin,
		"set_config in txn on connection A must not leak client_min_messages to connection B")
}

func TestSetConfigWithoutFollowUpQuery_SameClientNextQuery(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := context.Background()
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SELECT set_config('work_mem', '256MB', false)")
	require.NoError(t, err)

	var workMem string
	require.NoError(t, conn.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.Equal(t, "256MB", workMem, "same client next query should observe set_config via gateway tracker")
}
