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

package consensus

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// legacyHeartbeatTableSQL is the heartbeat table as the first release created it, before
// quorum_commit_lsn and quorum_commit_ts.
const legacyHeartbeatTableSQL = `CREATE TABLE multigres.heartbeat (
	shard_id BYTEA PRIMARY KEY,
	leader_id TEXT NOT NULL,
	ts BIGINT NOT NULL
)`

// heartbeatSchemaFixture recreates multigres.heartbeat from createSQL on a real postgres
// and returns a Writer over it plus a helper that runs DDL against it.
type heartbeatSchemaFixture struct {
	t      *testing.T
	ctx    context.Context
	qs     *connQueryService
	writer *heartbeat.Writer
}

func newHeartbeatSchemaFixture(t *testing.T, createSQL string) *heartbeatSchemaFixture {
	t.Helper()
	skipIfNoPG(t)
	ctx := t.Context()
	conn, err := pgTestFixture.newClientConn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	f := &heartbeatSchemaFixture{t: t, ctx: ctx, qs: &connQueryService{conn: conn}}
	f.exec(`DROP TABLE IF EXISTS multigres.heartbeat`)
	f.exec(createSQL)
	f.writer = heartbeat.NewWriter(f.qs, slog.New(slog.DiscardHandler), []byte("0"), "pooler-1", 1000)
	return f
}

func (f *heartbeatSchemaFixture) exec(sql string) {
	f.t.Helper()
	_, err := f.qs.conn.Query(f.ctx, sql)
	require.NoError(f.t, err, sql)
}

// write makes one heartbeat write the way a promotion does.
func (f *heartbeatSchemaFixture) write() error {
	return f.writer.WriteProven(f.ctx, pgutil.LSN(1), time.Now())
}

func (f *heartbeatSchemaFixture) check() *heartbeat.SchemaReport {
	f.t.Helper()
	report, err := heartbeat.CheckSchema(f.ctx, f.qs)
	require.NoError(f.t, err)
	return report
}

func problemKinds(r *heartbeat.SchemaReport) []heartbeat.SchemaProblemKind {
	kinds := make([]heartbeat.SchemaProblemKind, 0, len(r.Problems))
	for _, p := range r.Problems {
		kinds = append(kinds, p.Kind)
	}
	return kinds
}

// TestHeartbeatSchemaPG_Incompatible covers each way the table can stop matching the
// Writer's queries. The check must flag it up front, and the Writer must classify the
// resulting postgres error as a schema error rather than a transient one.
func TestHeartbeatSchemaPG_Incompatible(t *testing.T) {
	tests := []struct {
		name  string
		alter string
		// want lists the problems the check must report, as kind+column.
		want    []string
		upgrade bool
	}{
		{
			name:    "missing column",
			alter:   `ALTER TABLE multigres.heartbeat DROP COLUMN quorum_commit_ts`,
			want:    []string{"missing_column quorum_commit_ts"},
			upgrade: true,
		},
		{
			// The old column is missing and the new name is an unknown NOT NULL column.
			name:  "renamed column",
			alter: `ALTER TABLE multigres.heartbeat RENAME COLUMN leader_id TO leader`,
			want:  []string{"missing_column leader_id", "extra_required_column leader"},
		},
		{
			// A renamed newer column is missing as far as the queries go, and renaming it
			// back is not something the pooler should guess at, but adding it is safe.
			name:    "renamed newer column",
			alter:   `ALTER TABLE multigres.heartbeat RENAME COLUMN quorum_commit_lsn TO quorum_lsn`,
			want:    []string{"missing_column quorum_commit_lsn"},
			upgrade: true,
		},
		{
			name:  "wrong type",
			alter: `ALTER TABLE multigres.heartbeat ALTER COLUMN ts TYPE timestamptz USING to_timestamp(ts / 1e9)`,
			want:  []string{"wrong_type ts"},
		},
		{
			name:  "wrong type on a newer column",
			alter: `ALTER TABLE multigres.heartbeat DROP COLUMN quorum_commit_lsn; ALTER TABLE multigres.heartbeat ADD COLUMN quorum_commit_lsn integer`,
			want:  []string{"wrong_type quorum_commit_lsn"},
		},
		{
			name:  "extra NOT NULL column",
			alter: `ALTER TABLE multigres.heartbeat ADD COLUMN region text NOT NULL DEFAULT 'x'; ALTER TABLE multigres.heartbeat ALTER COLUMN region DROP DEFAULT`,
			want:  []string{"extra_required_column region"},
		},
		{
			name:  "no unique key on shard_id",
			alter: `ALTER TABLE multigres.heartbeat DROP CONSTRAINT heartbeat_pkey`,
			want:  []string{"missing_unique_key shard_id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newHeartbeatSchemaFixture(t, heartbeat.CreateTableSQL)
			require.True(t, f.check().Compatible(), "the current table must be compatible")
			require.NoError(t, f.write())
			require.False(t, f.writer.SchemaBroken())

			f.exec(tt.alter)

			report := f.check()
			require.False(t, report.Compatible(), report.String())
			var got []string
			for _, p := range report.Problems {
				got = append(got, string(p.Kind)+" "+p.Column)
			}
			assert.ElementsMatch(t, tt.want, got)
			assert.Equal(t, tt.upgrade, report.Upgradable(), "only newer missing columns are upgradable")

			// The same mismatch surfaces as a write failure the Writer recognises.
			require.Error(t, f.write())
			assert.EqualValues(t, 1, f.writer.WriteSchemaErrors())
			assert.True(t, f.writer.SchemaBroken())
		})
	}
}

// TestHeartbeatSchemaPG_ExtraColumnsWithDefaultsAreCompatible: an unknown column that an
// INSERT need not supply does not break the heartbeat.
func TestHeartbeatSchemaPG_ExtraColumnsWithDefaultsAreCompatible(t *testing.T) {
	f := newHeartbeatSchemaFixture(t, heartbeat.CreateTableSQL)
	f.exec(`ALTER TABLE multigres.heartbeat
		ADD COLUMN note text,
		ADD COLUMN created timestamptz NOT NULL DEFAULT now(),
		ADD COLUMN seq bigint GENERATED ALWAYS AS IDENTITY`)

	assert.True(t, f.check().Compatible(), f.check().String())
	require.NoError(t, f.write())
}

func TestHeartbeatSchemaPG_MissingTable(t *testing.T) {
	f := newHeartbeatSchemaFixture(t, heartbeat.CreateTableSQL)
	f.exec(`DROP TABLE multigres.heartbeat`)

	report := f.check()
	assert.Equal(t, []heartbeat.SchemaProblemKind{heartbeat.ProblemMissingTable}, problemKinds(report))
	assert.False(t, report.Upgradable(), "EnsureSchema does not create the table")

	require.Error(t, f.write())
	assert.True(t, f.writer.SchemaBroken())
}

// TestHeartbeatSchemaPG_UpgradeFromLegacySchema is the upgrade path: a shard bootstrapped
// by the first release has a three-column table; the current release's Writer cannot use
// it until EnsureSchema adds the newer columns, and doing so keeps the existing row.
func TestHeartbeatSchemaPG_UpgradeFromLegacySchema(t *testing.T) {
	f := newHeartbeatSchemaFixture(t, legacyHeartbeatTableSQL)
	f.exec(`INSERT INTO multigres.heartbeat (shard_id, leader_id, ts) VALUES ('0', 'old-leader', 42)`)

	before := f.check()
	assert.ElementsMatch(t, []heartbeat.SchemaProblemKind{heartbeat.ProblemMissingColumn, heartbeat.ProblemMissingColumn}, problemKinds(before))
	assert.True(t, before.Upgradable())

	// Without the upgrade, the new release's heartbeat writes fail, and are recognised
	// as a schema failure.
	require.Error(t, f.write())
	assert.True(t, f.writer.SchemaBroken())

	report, err := heartbeat.EnsureSchema(f.ctx, f.qs)
	require.NoError(t, err)
	assert.True(t, report.Compatible(), report.String())
	assert.True(t, f.check().Compatible())

	// The row written by the old release survives, with the new columns NULL.
	results, err := f.qs.conn.QueryArgs(f.ctx, `SELECT leader_id, ts::text, quorum_commit_lsn IS NULL, quorum_commit_ts IS NULL FROM multigres.heartbeat`)
	require.NoError(t, err)
	row := results[0].Rows[0].Values
	assert.Equal(t, "old-leader", string(row[0]))
	assert.Equal(t, "42", string(row[1]))
	assert.Equal(t, "t", string(row[2]))
	assert.Equal(t, "t", string(row[3]))

	// And the new release's writes now succeed, clearing the broken state.
	require.NoError(t, f.write())
	assert.False(t, f.writer.SchemaBroken())
	assert.Equal(t, "pooler-1", queryOne(t, f, `SELECT leader_id FROM multigres.heartbeat`))
	assert.NotEmpty(t, queryOne(t, f, `SELECT quorum_commit_lsn::text FROM multigres.heartbeat`))
}

// TestHeartbeatSchemaPG_EnsureIsIdempotent: running the upgrade on an already-current
// table changes nothing and does not fail.
func TestHeartbeatSchemaPG_EnsureIsIdempotent(t *testing.T) {
	f := newHeartbeatSchemaFixture(t, heartbeat.CreateTableSQL)
	require.NoError(t, f.write())
	for range 2 {
		report, err := heartbeat.EnsureSchema(f.ctx, f.qs)
		require.NoError(t, err)
		assert.True(t, report.Compatible(), report.String())
	}
	assert.Equal(t, "pooler-1", queryOne(t, f, `SELECT leader_id FROM multigres.heartbeat`))
}

// TestHeartbeatSchemaPG_EnsureLeavesOtherProblemsAlone: EnsureSchema only adds missing
// newer columns; a renamed or retyped column is reported and left for an operator, even
// when a newer column is missing too.
func TestHeartbeatSchemaPG_EnsureLeavesOtherProblemsAlone(t *testing.T) {
	f := newHeartbeatSchemaFixture(t, legacyHeartbeatTableSQL)
	f.exec(`ALTER TABLE multigres.heartbeat RENAME COLUMN ts TO ts_nanos`)

	report, err := heartbeat.EnsureSchema(f.ctx, f.qs)
	require.NoError(t, err)
	assert.False(t, report.Compatible())
	assert.False(t, report.Upgradable())
	assert.Contains(t, report.String(), "missing_column ts")

	// Nothing was added.
	assert.Equal(t, "0", queryOne(t, f, `SELECT count(*)::text FROM pg_attribute
		WHERE attrelid = 'multigres.heartbeat'::regclass AND attname LIKE 'quorum_commit%'`))
}

func queryOne(t *testing.T, f *heartbeatSchemaFixture, sql string) string {
	t.Helper()
	results, err := f.qs.conn.QueryArgs(f.ctx, sql)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	return string(results[0].Rows[0].Values[0])
}

// TestHeartbeatSchemaPG_ReaderMeasuresLagOnLegacyTable: a standby cannot alter the table,
// so on a table from before the quorum_commit columns the Reader must still measure lag.
func TestHeartbeatSchemaPG_ReaderMeasuresLagOnLegacyTable(t *testing.T) {
	f := newHeartbeatSchemaFixture(t, legacyHeartbeatTableSQL)
	f.exec(fmt.Sprintf(`INSERT INTO multigres.heartbeat (shard_id, leader_id, ts) VALUES ('0', 'old-leader', %d)`,
		time.Now().Add(-time.Second).UnixNano()))

	reader := heartbeat.NewReader(f.qs, slog.New(slog.DiscardHandler), []byte("0"))
	reader.Open()
	defer reader.Close()

	require.Eventually(t, func() bool { return reader.Reads() >= 1 }, 10*time.Second, 50*time.Millisecond)
	lag, err := reader.Status()
	require.NoError(t, err)
	assert.Greater(t, lag, 500*time.Millisecond)
	_, have := reader.QuorumCommitLSN()
	assert.False(t, have)
	assert.EqualValues(t, 1, reader.ReadSchemaErrors(), "the missing columns are reported once, not on every read")
}
