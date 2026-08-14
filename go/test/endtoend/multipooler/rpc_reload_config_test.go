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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// appendPrimaryPostgresConf appends configuration lines to the END of the
// primary's postgresql.conf on disk and registers a cleanup that restores the
// original file and reloads. Appending makes the new line the last occurrence in
// postgresql.conf (last write wins), so it shadows any earlier assignment of the
// same GUC.
func appendPrimaryPostgresConf(t *testing.T, setup *MultipoolerTestSetup, client *shardsetup.MultipoolerClient, lines ...string) {
	t.Helper()
	confPath := filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "postgresql.conf")

	original, err := os.ReadFile(confPath)
	require.NoError(t, err, "read postgresql.conf")
	info, err := os.Stat(confPath)
	require.NoError(t, err, "stat postgresql.conf")

	t.Cleanup(func() {
		if err := os.WriteFile(confPath, original, info.Mode()); err != nil {
			t.Logf("cleanup: restore postgresql.conf failed: %v", err)
			return
		}
		// Reload so the running config returns to baseline for the shared fixture.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		shardsetup.ReloadConfig(ctx, t, client.Pooler, setup.PrimaryName)
	})

	var b bytes.Buffer
	b.Write(original)
	if n := len(original); n > 0 && original[n-1] != '\n' {
		b.WriteByte('\n')
	}
	for _, line := range lines {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(confPath, b.Bytes(), info.Mode()), "write postgresql.conf")
}

// rawFileSetting returns the value PostgreSQL sees for a GUC in the config file
// it would read now (pg_file_settings.setting), taking the last occurrence by
// seqno — the one that wins. pg_file_settings re-reads the config files at query
// time, so this reflects the on-disk edit even before a reload. Returns "" if the
// setting is not in any file.
func rawFileSetting(ctx context.Context, t *testing.T, pooler *shardsetup.MultipoolerTestClient, name string) string {
	t.Helper()
	val, err := shardsetup.QueryStringValue(ctx, pooler, fmt.Sprintf(
		"SELECT setting FROM pg_file_settings WHERE name = '%s' ORDER BY seqno DESC LIMIT 1", name))
	require.NoError(t, err)
	return val
}

// TestManagerReloadConfig_ExpectedSettings_MatchAndMismatch drives the reload
// verdict end-to-end against a real PostgreSQL by writing postgresql.conf on
// disk, using work_mem (a reload-safe PGC_USER GUC). It asks ReloadConfig two
// ways: with an expectation that matches the file (the reload runs) and with one
// that differs from the file (the reload is skipped and the setting is reported
// as a mismatch by name). The mismatch case is the operator-facing scenario where
// the value the RPC is told to expect diverges from what actually landed in the
// file (a stale or not-yet-synced write).
func TestManagerReloadConfig_ExpectedSettings_MatchAndMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithoutReplication())
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	client, err := shardsetup.NewMultipoolerClient(setup.PrimaryMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	ctx := utils.WithTimeout(t, 30*time.Second)

	// Operator writes the reload-safe GUC into the config file on disk.
	const wantValue = "48MB"
	appendPrimaryPostgresConf(t, setup, client, fmt.Sprintf("work_mem = '%s'", wantValue))

	// Confirm the file really carries our value (guards against an unforeseen
	// override from auto.conf or an include).
	fileValue := rawFileSetting(ctx, t, client.Pooler, "work_mem")
	require.Equal(t, wantValue, fileValue, "postgresql.conf edit should be the effective occurrence")

	t.Run("expectation matches the file", func(t *testing.T) {
		resp, err := client.Manager.ReloadConfig(utils.WithTimeout(t, 30*time.Second),
			&multipoolermanagerdatapb.ReloadConfigRequest{
				ExpectedSettings: map[string]string{"work_mem": wantValue},
			})
		require.NoError(t, err)
		require.NotNil(t, resp.GetConfigLoadTime(), "config_load_time proves the reload happened")

		assert.Empty(t, resp.GetMismatches())
		assert.False(t, resp.GetNeedsRestart())
	})

	t.Run("expectation differs from the file", func(t *testing.T) {
		// The operator believes it wrote 64MB, but the file on disk still holds
		// wantValue (the stale-file / wrong-value scenario). The reload must be
		// skipped so the operator retries rather than seeing a false success.
		const expected = "64MB"
		require.NotEqual(t, expected, wantValue, "test precondition: the file must hold a different value")

		resp, err := client.Manager.ReloadConfig(utils.WithTimeout(t, 30*time.Second),
			&multipoolermanagerdatapb.ReloadConfigRequest{
				ExpectedSettings: map[string]string{"work_mem": expected},
			})
		require.NoError(t, err)
		assert.Nil(t, resp.GetConfigLoadTime(), "the reload is skipped when the file does not match")
		assert.False(t, resp.GetNeedsRestart(), "a plain value mismatch is not a restart situation")
		require.Len(t, resp.GetMismatches(), 1)

		m := resp.GetMismatches()[0]
		assert.Equal(t, "work_mem", m.GetName(), "the unsatisfied setting is named, without echoing its file value")
		assert.False(t, m.GetRequiresRestart())
		assert.Empty(t, m.GetError(), "a stale value is valid, just not the desired one")
	})
}

// TestManagerReloadConfig_ExpectedSettings_NeedsRestart drives the verdict for a
// change that a reload cannot satisfy. max_prepared_transactions is a
// PGC_POSTMASTER GUC, so writing a new value into postgresql.conf leaves it in
// the file (pg_file_settings.setting matches) but not reload-applicable
// (applied=false, pg_settings.context='postmaster'). ReloadConfig must detect
// this before reloading, skip the reload, and surface needs_restart so the
// operator escalates to a restart rather than spinning on a reload.
func TestManagerReloadConfig_ExpectedSettings_NeedsRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithoutReplication())
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	client, err := shardsetup.NewMultipoolerClient(setup.PrimaryMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	ctx := utils.WithTimeout(t, 30*time.Second)

	// Pick a value guaranteed to differ from the running value, so the change is a
	// real (restart-only) change rather than a no-op.
	current, err := shardsetup.QueryStringValue(ctx, client.Pooler, "SHOW max_prepared_transactions")
	require.NoError(t, err)
	currentN, err := strconv.Atoi(current)
	require.NoError(t, err, "max_prepared_transactions should be an integer, got %q", current)
	want := strconv.Itoa(currentN + 13)

	// Operator writes the restart-only GUC into the config file on disk.
	appendPrimaryPostgresConf(t, setup, client, "max_prepared_transactions = "+want)

	require.Equal(t, want, rawFileSetting(ctx, t, client.Pooler, "max_prepared_transactions"),
		"postgresql.conf edit should be the effective occurrence")

	resp, err := client.Manager.ReloadConfig(ctx, &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: map[string]string{"max_prepared_transactions": want},
	})
	require.NoError(t, err)
	assert.Nil(t, resp.GetConfigLoadTime(), "the reload is skipped for a restart-only change")
	assert.True(t, resp.GetNeedsRestart(), "the operator must be told a restart is required")
	require.Len(t, resp.GetMismatches(), 1)

	m := resp.GetMismatches()[0]
	assert.Equal(t, "max_prepared_transactions", m.GetName())
	assert.True(t, m.GetRequiresRestart(), "PostgreSQL reports it as a postmaster-context change")
}
