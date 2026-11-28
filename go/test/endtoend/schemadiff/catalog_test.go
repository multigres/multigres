// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemadiff

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/schemadiff"
)

// The catalog tests diff every released shape of the sharding catalog
// (multigres.*, pg_catalog_schema.go) against every other. Each snapshot is a
// golden file of catalog_schema.golden's format: the CREATE and ALTER
// statements of catalogTables, then the catalogSeed inserts, each after a
// "== table" or "== seed" line.
//
// legacy is the pre-versioning catalog that format 1 drops; the prNNNN files
// are catalog_schema.golden as the pull request that changed it left it; and
// current is catalog_schema.golden itself, so a catalog change is diffed
// against every earlier shape the moment it is made.
var catalogSnapshots = []struct {
	name string
	path string
}{
	{"legacy", "testdata/catalog/legacy.golden"},
	{"pr1597", "testdata/catalog/pr1597.golden"},
	{"pr1604", "testdata/catalog/pr1604.golden"},
	{"pr1684", "testdata/catalog/pr1684.golden"},
	{"pr1689", "testdata/catalog/pr1689.golden"},
	{"current", "../../../services/multipooler/internal/manager/testdata/catalog_schema.golden"},
}

// catalogSnapshot is one parsed snapshot.
type catalogSnapshot struct {
	name   string
	tables []string
	seed   []string
}

func readCatalogSnapshots(t *testing.T) []catalogSnapshot {
	t.Helper()
	var snaps []catalogSnapshot
	for _, s := range catalogSnapshots {
		f, err := os.Open(filepath.FromSlash(s.path))
		require.NoError(t, err)
		snap := catalogSnapshot{name: s.name}
		var cur *[]string
		var stmt strings.Builder
		flush := func() {
			if cur != nil {
				*cur = append(*cur, strings.TrimSpace(stmt.String()))
			}
			stmt.Reset()
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "== table":
				flush()
				cur = &snap.tables
			case line == "== seed":
				flush()
				cur = &snap.seed
			case cur == nil:
				// The header: comments and the format line.
			default:
				stmt.WriteString(line + "\n")
			}
		}
		flush()
		require.NoError(t, sc.Err())
		require.NoError(t, f.Close())
		require.NotEmpty(t, snap.tables, s.path)
		snaps = append(snaps, snap)
	}
	return snaps
}

// catalogServer is a postgres with one database per snapshot, holding that
// snapshot's tables, and an admin connection that creates the database a case
// changes.
type catalogServer struct {
	port  int
	admin conn
	want  map[string]*schemadiff.Schema
}

func startCatalogServer(t *testing.T, ctx context.Context, snaps []catalogSnapshot) *catalogServer {
	t.Helper()
	port := startPostgres(t)
	s := &catalogServer{port: port, admin: conn{dial(t, ctx, port, "postgres")}, want: map[string]*schemadiff.Schema{}}
	for _, snap := range snaps {
		c := s.createDB(t, ctx, "want_"+snap.name, snap.tables...)
		s.want[snap.name] = introspectCatalog(t, ctx, c)
		require.NotEmpty(t, s.want[snap.name].Tables, snap.name)
	}
	return s
}

// createDB creates database name, replacing one of that name, creates the
// multigres schema in it, and runs stmts there.
func (s *catalogServer) createDB(t *testing.T, ctx context.Context, name string, stmts ...string) conn {
	t.Helper()
	run(t, ctx, s.admin, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name))
	run(t, ctx, s.admin, "CREATE DATABASE "+name)
	c := conn{dial(t, ctx, s.port, name)}
	run(t, ctx, c, "CREATE SCHEMA multigres")
	for _, stmt := range stmts {
		run(t, ctx, c, stmt)
	}
	return c
}

func introspectCatalog(t *testing.T, ctx context.Context, c conn) *schemadiff.Schema {
	t.Helper()
	run(t, ctx, c, "BEGIN")
	defer run(t, ctx, c, "ROLLBACK")
	s, err := schemadiff.Introspect(ctx, c, "multigres")
	require.NoError(t, err)
	return s
}

// applyChanges runs changes in one transaction with the search_path set to
// multigres, and rolls it back if one fails.
func applyChanges(ctx context.Context, c conn, changes []schemadiff.Change) error {
	if _, err := c.Conn.Query(ctx, "BEGIN; SET LOCAL search_path = multigres"); err != nil {
		return err
	}
	for _, ch := range changes {
		if _, err := c.Conn.Query(ctx, ch.SQL); err != nil {
			_, _ = c.Conn.Query(ctx, "ROLLBACK")
			return fmt.Errorf("%s: %w", ch.SQL, err)
		}
	}
	_, err := c.Conn.Query(ctx, "COMMIT")
	return err
}

// requireConverges diffs the catalog c holds against want with opts, requires
// that the diff applies and that a second diff is empty, and returns the first
// diff.
func requireConverges(t *testing.T, ctx context.Context, c conn, want *schemadiff.Schema, opts schemadiff.Options) []schemadiff.Change {
	t.Helper()
	changes, err := schemadiff.Diff(introspectCatalog(t, ctx, c), want, opts)
	require.NoError(t, err)
	require.NoError(t, applyChanges(ctx, c, changes))
	again, err := schemadiff.Diff(introspectCatalog(t, ctx, c), want, opts)
	require.NoError(t, err)
	require.Empty(t, again, "the diff must converge")
	return changes
}

func changeSQL(changes []schemadiff.Change) []string {
	out := make([]string, len(changes))
	for i, ch := range changes {
		out[i] = ch.SQL
	}
	return out
}

// TestCatalogHistory_EverySnapshotToEveryOther diffs the empty catalog of every
// snapshot to every other, upgrades and downgrades alike, both dropping the
// tables the target does not have and keeping them, and requires each diff to
// converge. Diff refuses every downgrade to legacy: legacy's keys are serial
// columns, which Diff does not create; and when it keeps the extra tables, it
// refuses earlier, since legacy re-keys tablegroup, tablegroup_table and
// shard, which tables legacy does not have (shard_key and later ones)
// reference.
func TestCatalogHistory_EverySnapshotToEveryOther(t *testing.T) {
	ctx := t.Context()
	snaps := readCatalogSnapshots(t)
	s := startCatalogServer(t, ctx, snaps)
	for _, mode := range []struct {
		name string
		opts schemadiff.Options
	}{
		{"drop_extra", schemadiff.Options{}},
		{"keep_extra", schemadiff.Options{KeepExtraTables: true}},
	} {
		for _, from := range snaps {
			for _, to := range snaps {
				if from.name == to.name {
					continue
				}
				t.Run(mode.name+"/"+from.name+"_to_"+to.name, func(t *testing.T) {
					c := s.createDB(t, ctx, "have", from.tables...)
					if to.name == "legacy" {
						want := "is a serial column"
						if mode.opts.KeepExtraTables {
							want = "which the target schema does not have, references table"
						}
						_, err := schemadiff.Diff(introspectCatalog(t, ctx, c), s.want[to.name], mode.opts)
						require.ErrorContains(t, err, want)
						return
					}
					changes := requireConverges(t, ctx, c, s.want[to.name], mode.opts)
					for _, ch := range changes {
						if strings.HasPrefix(ch.SQL, "DROP TABLE") {
							require.False(t, mode.opts.KeepExtraTables, ch.SQL)
						}
					}
				})
			}
		}
	}
}

// TestCatalogHistory_SnapshotIsItsOwnFixpoint requires that a catalog diffs
// to nothing against the shape it was created at: introspection reads the
// same model from equal catalogs in different databases.
func TestCatalogHistory_SnapshotIsItsOwnFixpoint(t *testing.T) {
	ctx := t.Context()
	snaps := readCatalogSnapshots(t)
	s := startCatalogServer(t, ctx, snaps)
	for _, snap := range snaps {
		t.Run(snap.name, func(t *testing.T) {
			c := s.createDB(t, ctx, "have", append(snap.tables, snap.seed...)...)
			changes, err := schemadiff.Diff(introspectCatalog(t, ctx, c), s.want[snap.name], schemadiff.Options{})
			require.NoError(t, err)
			assert.Empty(t, changes)
		})
	}
}

// TestCatalogHistory_UpgradesAreAdditive upgrades a seeded catalog of every
// versioned snapshot to every later one. The catalog only ever appends tables
// and nullable or defaulted columns, so every upgrade must apply over the
// seed rows, keep them, and contain no breaking change.
func TestCatalogHistory_UpgradesAreAdditive(t *testing.T) {
	ctx := t.Context()
	snaps := readCatalogSnapshots(t)[1:] // without legacy
	s := startCatalogServer(t, ctx, snaps)
	for i, from := range snaps {
		for _, to := range snaps[i+1:] {
			t.Run(from.name+"_to_"+to.name, func(t *testing.T) {
				c := s.createDB(t, ctx, "have", append(from.tables, from.seed...)...)
				changes := requireConverges(t, ctx, c, s.want[to.name], schemadiff.Options{})
				require.NotEmpty(t, changes)
				for _, ch := range changes {
					assert.False(t, ch.Breaking, ch.SQL)
				}
				res, err := c.QueryArgs(ctx, "SELECT count(*) FROM multigres.shard s JOIN multigres.tablegroup tg USING (tablegroup_id) WHERE tg.name = 'default' AND s.shard_name = '0-inf'")
				require.NoError(t, err)
				assert.Equal(t, "1", string(res.Rows[0].Values[0]), "the seed rows must survive the upgrade")
			})
		}
	}
}

// TestCatalogHistory_EachReleaseDiff pins the diff of each catalog release to
// the next, seeded.
func TestCatalogHistory_EachReleaseDiff(t *testing.T) {
	want := map[string][]string{
		"pr1597_to_pr1604": {
			"CREATE TABLE connections (\n\tconnection_id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL,\n\tname text NOT NULL,\n\tdsn text NOT NULL,\n\towner_role text DEFAULT ''::text NOT NULL,\n\tcreated_at timestamp with time zone DEFAULT now() NOT NULL\n)",
			"ALTER TABLE connections ADD CONSTRAINT connections_name_key UNIQUE (name)",
			"ALTER TABLE connections ADD CONSTRAINT connections_pkey PRIMARY KEY (connection_id)",
		},
		"pr1604_to_pr1684": {
			"CREATE TABLE routing_rule (\n\trule_id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,\n\toperation_id uuid NOT NULL,\n\tscope text NOT NULL,\n\tfrom_tablegroup_id bigint NOT NULL,\n\tfrom_table_id bigint,\n\tto_tablegroup_id bigint NOT NULL,\n\tto_shard_id bigint,\n\tto_shard_key_id bigint,\n\tcreated_at timestamp with time zone DEFAULT now() NOT NULL,\n\tcreated_version bigint NOT NULL\n)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_check CHECK ((scope = 'table'::text) = (from_table_id IS NOT NULL))",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_check1 CHECK ((scope = 'tablegroup'::text) = (to_shard_id IS NOT NULL))",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_pkey PRIMARY KEY (rule_id)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_scope_check CHECK (scope = ANY (ARRAY['tablegroup'::text, 'table'::text]))",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_from_table_id_fkey FOREIGN KEY (from_table_id) REFERENCES tablegroup_table(table_id)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_from_tablegroup_id_fkey FOREIGN KEY (from_tablegroup_id) REFERENCES tablegroup(tablegroup_id)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES operation(operation_id)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_to_shard_id_fkey FOREIGN KEY (to_shard_id) REFERENCES shard(shard_id)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_to_shard_key_id_fkey FOREIGN KEY (to_shard_key_id) REFERENCES shard_key(shard_key_id)",
			"ALTER TABLE routing_rule ADD CONSTRAINT routing_rule_to_tablegroup_id_fkey FOREIGN KEY (to_tablegroup_id) REFERENCES tablegroup(tablegroup_id)",
		},
		"pr1684_to_pr1689": {
			"ALTER TABLE connections ADD COLUMN conninfo jsonb",
			"ALTER TABLE connections ADD CONSTRAINT connections_conninfo_servers CHECK (conninfo IS NULL OR\nCASE\n    WHEN COALESCE(jsonb_typeof(conninfo) = 'object'::text, false) AND COALESCE(jsonb_typeof(conninfo -> 'host'::text) = 'array'::text, false) AND COALESCE(jsonb_typeof(conninfo -> 'port'::text) = 'array'::text, false) THEN jsonb_array_length(conninfo -> 'host'::text) >= 1 AND jsonb_array_length(conninfo -> 'port'::text) >= 1 AND NOT jsonb_path_exists(conninfo, 'strict $.\"host\"[*]?(@.type() != \"string\" || @ == \"\")'::jsonpath) AND NOT jsonb_path_exists(conninfo, 'strict $.\"port\"[*]?(((@.type() != \"number\" || @ < 1) || @ > 65535) || @ != @.floor())'::jsonpath) AND (jsonb_array_length(conninfo -> 'port'::text) = 1 OR jsonb_array_length(conninfo -> 'port'::text) = jsonb_array_length(conninfo -> 'host'::text))\n    ELSE false\nEND)",
		},
		"pr1689_to_current": {
			"CREATE TABLE admission_fence (\n\tfence_id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,\n\toperation_id uuid NOT NULL,\n\ttablegroup_id bigint NOT NULL,\n\tshard_id bigint,\n\tstate text NOT NULL,\n\tversion bigint NOT NULL\n)",
			"ALTER TABLE admission_fence ADD CONSTRAINT admission_fence_pkey PRIMARY KEY (fence_id)",
			"ALTER TABLE admission_fence ADD CONSTRAINT admission_fence_state_check CHECK (state = ANY (ARRAY['FENCING'::text, 'FENCED'::text]))",
			"ALTER TABLE admission_fence ADD CONSTRAINT admission_fence_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES operation(operation_id)",
			"ALTER TABLE admission_fence ADD CONSTRAINT admission_fence_shard_id_fkey FOREIGN KEY (shard_id) REFERENCES shard(shard_id)",
			"ALTER TABLE admission_fence ADD CONSTRAINT admission_fence_tablegroup_id_fkey FOREIGN KEY (tablegroup_id) REFERENCES tablegroup(tablegroup_id)",
		},
	}
	ctx := t.Context()
	snaps := readCatalogSnapshots(t)[1:] // without legacy
	s := startCatalogServer(t, ctx, snaps)
	for i := range len(snaps) - 1 {
		from, to := snaps[i], snaps[i+1]
		name := from.name + "_to_" + to.name
		t.Run(name, func(t *testing.T) {
			c := s.createDB(t, ctx, "have", append(from.tables, from.seed...)...)
			changes := requireConverges(t, ctx, c, s.want[to.name], schemadiff.Options{})
			expected, ok := want[name]
			require.True(t, ok, "no expected diff for %s", name)
			assert.Equal(t, expected, changeSQL(changes))
		})
	}
}

// TestCatalogHistory_LegacyNeedsAMigration shows why format 1 is a numbered
// migration and not a diff: the seeded legacy catalog cannot be altered into
// format 1, since tablegroup gains a NOT NULL state column with no default.
// On the empty legacy catalog the same diff applies (see
// TestCatalogHistory_EverySnapshotToEveryOther).
func TestCatalogHistory_LegacyNeedsAMigration(t *testing.T) {
	ctx := t.Context()
	snaps := readCatalogSnapshots(t)
	legacy, first := snaps[0], snaps[1]
	s := startCatalogServer(t, ctx, snaps[:2])
	c := s.createDB(t, ctx, "have", append(legacy.tables, legacy.seed...)...)
	changes, err := schemadiff.Diff(introspectCatalog(t, ctx, c), s.want[first.name], schemadiff.Options{})
	require.NoError(t, err)

	var breaking []string
	for _, ch := range changes {
		if ch.Breaking {
			breaking = append(breaking, ch.SQL)
		}
	}
	assert.Equal(t, []string{
		"ALTER TABLE shard DROP COLUMN oid",
		"ALTER TABLE shard DROP COLUMN tablegroup_oid",
		"ALTER TABLE shard DROP COLUMN key_range_start",
		"ALTER TABLE shard DROP COLUMN key_range_end",
		"ALTER TABLE tablegroup DROP COLUMN oid",
		"ALTER TABLE tablegroup DROP COLUMN type",
		"ALTER TABLE tablegroup_table DROP COLUMN oid",
		"ALTER TABLE tablegroup_table DROP COLUMN tablegroup_oid",
		"ALTER TABLE tablegroup_table DROP COLUMN name",
	}, breaking)
	require.ErrorContains(t, applyChanges(ctx, c, changes), "contains null values")
}
