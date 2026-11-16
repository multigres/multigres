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

package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// catalogStatePresentPattern matches readCatalogFormat's existence probe.
const catalogStatePresentPattern = `to_regclass\('multigres\.catalog_state'\)`

// catalogFormatWritePattern matches ensureCatalogSchema's format_version write.
const catalogFormatWritePattern = "UPDATE multigres.catalog_state SET format_version"

// exactPattern matches stmt and nothing else.
func exactPattern(stmt string) string {
	return "^" + regexp.QuoteMeta(stmt) + "$"
}

// addCatalogEnsureTail registers catalogTables, catalogSeed, the read of the connections without a
// conninfo and the format_version write, which ensureCatalogSchema runs on every call that is
// not at a newer format.
func addCatalogEnsureTail(m *mock.QueryService) {
	for _, stmt := range catalogTables {
		m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
	}
	for _, stmt := range catalogSeed {
		m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
	}
	m.AddQueryPatternOnce(exactPattern(selectUnfilledConnectionsSQL), mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce(catalogFormatWritePattern, mock.MakeQueryResult(nil, nil))
}

// addFreshCatalogEnsure registers the queries ensureCatalogSchema runs against
// a catalog shard with no catalog_state (fresh or legacy): the lock, the
// existence probe, every migration statement, catalogTables, catalogSeed and
// the format_version write.
func addFreshCatalogEnsure(m *mock.QueryService) {
	m.AddQueryPatternOnce("pg_advisory_xact_lock", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce(catalogStatePresentPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))
	for _, mig := range catalogMigrations {
		for _, stmt := range mig.statements {
			m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
		}
	}
	addCatalogEnsureTail(m)
}

// addExistingCatalogState registers the lock, the probe and the format_version
// read for a catalog that already has catalog_state at the given format.
func addExistingCatalogState(m *mock.QueryService, format int) {
	m.AddQueryPatternOnce("pg_advisory_xact_lock", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce(catalogStatePresentPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))
	m.AddQueryPatternOnce("SELECT format_version FROM multigres.catalog_state",
		mock.MakeQueryResult([]string{"format_version"}, [][]any{{format}}))
}

// recordCatalogEnsure runs ensureCatalogSchema with every query succeeding and
// returns the queries it ran, in order. exists and format answer the format
// read.
func recordCatalogEnsure(t *testing.T, exists bool, format int) []string {
	t.Helper()
	pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)

	var queries []string
	record := func(q string) { queries = append(queries, q) }
	m.AddQueryPatternWithCallback(catalogStatePresentPattern,
		mock.MakeQueryResult([]string{"exists"}, [][]any{{exists}}), record)
	m.AddQueryPatternWithCallback("^SELECT format_version FROM multigres.catalog_state FOR UPDATE$",
		mock.MakeQueryResult([]string{"format_version"}, [][]any{{format}}), record)
	m.AddQueryPatternWithCallback("SET routing_version",
		mock.MakeQueryResult([]string{"routing_version", "plan_epoch"}, [][]any{{int64(1), int64(0)}}), record)
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	require.NoError(t, pm.ensureCatalogSchema(context.Background()))
	return queries
}

// catalogEnsureTail is the statement sequence ensureCatalogSchema ends with
// whenever it is not at a newer format.
func catalogEnsureTail() []string {
	var tail []string
	tail = append(tail, catalogTables...)
	tail = append(tail, catalogSeed...)
	tail = append(tail, selectUnfilledConnectionsSQL)
	return append(tail, "UPDATE multigres.catalog_state SET format_version = $1 WHERE format_version IS DISTINCT FROM $1", "COMMIT")
}

func TestCatalogMigrations_OrderedAndContiguous(t *testing.T) {
	require.NotEmpty(t, catalogMigrations)
	for i, m := range catalogMigrations {
		assert.Equal(t, i+1, m.version, "catalogMigrations[%d] must upgrade to format %d", i, i+1)
		assert.NotEmpty(t, m.statements, "format %d migration has no statements", m.version)
	}
	assert.Equal(t, len(catalogMigrations), latestCatalogFormat())
}

func TestEnsureCatalogSchema_FreshAppliesMigrationsThenTablesAndSeed(t *testing.T) {
	queries := recordCatalogEnsure(t, false, 0)

	want := []string{"BEGIN", "SELECT pg_advisory_xact_lock($1)", "SELECT to_regclass('multigres.catalog_state') IS NOT NULL"}
	for _, mig := range catalogMigrations {
		want = append(want, mig.statements...)
	}
	want = append(want, catalogEnsureTail()...)
	assert.Equal(t, want, queries)
}

func TestEnsureCatalogSchema_AtLatestRunsTablesAndSeed(t *testing.T) {
	queries := recordCatalogEnsure(t, true, latestCatalogFormat())

	// No migration runs, but catalogTables and catalogSeed do, so an existing
	// catalog picks up tables added since it was created.
	want := []string{
		"BEGIN", "SELECT pg_advisory_xact_lock($1)",
		"SELECT to_regclass('multigres.catalog_state') IS NOT NULL",
		"SELECT format_version FROM multigres.catalog_state FOR UPDATE",
	}
	want = append(want, catalogEnsureTail()...)
	assert.Equal(t, want, queries)
	assert.Empty(t, queriesContaining(queries, "SET routing_version"), "a routine open advances nothing")
}

// queriesContaining returns the queries that contain substr.
func queriesContaining(queries []string, substr string) []string {
	var out []string
	for _, q := range queries {
		if strings.Contains(q, substr) {
			out = append(out, q)
		}
	}
	return out
}

// withRewritingMigration appends a format that rewrites snapshot rows and
// declares change, for the length of the test.
func withRewritingMigration(t *testing.T, change catalogChange) {
	t.Helper()
	saved := catalogMigrations
	catalogMigrations = append(slices.Clone(saved), catalogMigration{
		version:    len(saved) + 1,
		statements: []string{"UPDATE multigres.shard SET state = state"},
		change:     change,
	})
	t.Cleanup(func() { catalogMigrations = saved })
}

// TestEnsureCatalogSchema_LocksCatalogStateFirst pins the lock order shared
// with writeCatalog: the catalog_state row is locked before any statement
// that locks another catalog table, such as CREATE INDEX IF NOT EXISTS on
// shard_key.
func TestEnsureCatalogSchema_LocksCatalogStateFirst(t *testing.T) {
	queries := recordCatalogEnsure(t, true, latestCatalogFormat())
	lock := slices.Index(queries, "SELECT format_version FROM multigres.catalog_state FOR UPDATE")
	require.GreaterOrEqual(t, lock, 0, "the format read locks catalog_state")
	for i, q := range queries {
		if strings.Contains(q, "CREATE") {
			assert.Less(t, lock, i, "catalog_state is locked before %q", q)
		}
	}
}

func TestEnsureCatalogSchema_MigrationAdvancesWhatItDeclares(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change catalogChange
		want   []string
	}{
		{"no snapshot change", catalogNoSnapshotChange, nil},
		{"routing change", catalogRoutingChange, []string{
			"UPDATE multigres.catalog_state SET routing_version = routing_version + 1 RETURNING routing_version, plan_epoch",
		}},
		{"invalidates plans", catalogInvalidatesPlans, []string{
			"UPDATE multigres.catalog_state SET routing_version = routing_version + 1, plan_epoch = plan_epoch + 1 RETURNING routing_version, plan_epoch",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withRewritingMigration(t, tc.change)
			queries := recordCatalogEnsure(t, true, latestCatalogFormat()-1)
			assert.Equal(t, tc.want, queriesContaining(queries, "SET routing_version"))
			if tc.want != nil {
				assert.Less(t, slices.Index(queries, tc.want[0]), slices.Index(queries, "COMMIT"), "advanced in the same transaction")
			}
		})
	}
}

// A fresh or legacy catalog has no catalog_state, so no gateway can hold a
// snapshot of it: even a migration that declares a routing change advances
// nothing, and a fresh catalog starts at version 0.
func TestEnsureCatalogSchema_FreshCatalogAdvancesNothing(t *testing.T) {
	withRewritingMigration(t, catalogRoutingChange)
	queries := recordCatalogEnsure(t, false, 0)
	assert.Empty(t, queriesContaining(queries, "SET routing_version"))
}

func TestEnsureCatalogSchema_NewerFormatLeftAlone(t *testing.T) {
	pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	addExistingCatalogState(m, latestCatalogFormat()+1)

	// Any query beyond the lock and the format read (migrations, catalogTables,
	// catalogSeed, the format write) has no pattern and would fail.
	require.NoError(t, pm.ensureCatalogSchema(context.Background()))
	assert.NoError(t, m.ExpectationsWereMet())
}

func TestCatalogFormat1_DropsLegacyTablesOnly(t *testing.T) {
	// Format 1 only drops the legacy tables; catalogTables recreates the
	// current ones under the same names after it runs.
	require.Len(t, catalogMigrations[0].statements, 1)
	stmt := catalogMigrations[0].statements[0]
	assert.True(t, strings.HasPrefix(stmt, "DROP TABLE IF EXISTS"), "format 1 statement: %s", stmt)
	for _, table := range []string{"multigres.shard", "multigres.tablegroup_table", "multigres.tablegroup"} {
		assert.Contains(t, stmt, table)
	}
}

func TestConnectionsTable_Shape(t *testing.T) {
	assert.Contains(t, catalogTables, connectionsTableSQL)
	for _, want := range []string{
		"connection_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
		"name          text NOT NULL UNIQUE",
		"dsn           text NOT NULL",
		"owner_role    text NOT NULL DEFAULT ''",
	} {
		assert.Contains(t, connectionsTableSQL, want)
	}
	assert.NotContains(t, connectionsTableSQL, "GRANT", "the table has no PUBLIC grant")
}

// The conninfo column is added after its table, nullable so that it can be added to a table that has
// rows, and the statement that creates the table is not edited. The constraint allows a NULL
// explicitly.
func TestConnectionsConnInfoColumn(t *testing.T) {
	tableAt, columnAt := slices.Index(catalogTables, connectionsTableSQL), slices.Index(catalogTables, connectionsConnInfoColumnSQL)
	require.GreaterOrEqual(t, tableAt, 0)
	assert.Greater(t, columnAt, tableAt, "the column is added after its table")
	assert.Regexp(t, `^ALTER TABLE multigres\.connections ADD COLUMN IF NOT EXISTS conninfo jsonb\s+CONSTRAINT connections_conninfo_servers CHECK \(\s+conninfo IS NULL OR CASE\b`, connectionsConnInfoColumnSQL)
	assert.NotContains(t, connectionsConnInfoColumnSQL, "NOT NULL")
	assert.NotContains(t, connectionsConnInfoColumnSQL, "DEFAULT")
	assert.NotContains(t, connectionsTableSQL, "conninfo", "the original statement is not edited")
}

func TestCatalogTables_IdempotentInDependencyOrder(t *testing.T) {
	for _, stmt := range catalogTables {
		assert.Regexp(t, `^(CREATE TABLE IF NOT EXISTS|CREATE UNIQUE INDEX IF NOT EXISTS|CREATE INDEX IF NOT EXISTS|ALTER TABLE \S+ ADD COLUMN IF NOT EXISTS) `,
			stmt, "catalogTables statements must be idempotent")
	}
	for _, stmt := range catalogSeed {
		assert.Contains(t, stmt, "ON CONFLICT", "catalogSeed statements must be idempotent")
	}

	// Each statement comes after the creation of every table it references.
	created := map[string]bool{}
	createRE := regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS (multigres\.\w+)`)
	refRE := regexp.MustCompile(`REFERENCES (multigres\.\w+)`)
	for _, stmt := range catalogTables {
		for _, ref := range refRE.FindAllStringSubmatch(stmt, -1) {
			assert.True(t, created[ref[1]], "a statement references %s before it is created:\n%s", ref[1], stmt)
		}
		if match := createRE.FindStringSubmatch(stmt); match != nil {
			created[match[1]] = true
		}
	}
}

// catalogGoldenPath is the checked-in copy of catalogTables and catalogSeed
// that TestCatalogSchema_Golden pins them to.
var catalogGoldenPath = filepath.Join("testdata", "catalog_schema.golden")

// catalogGolden is the parsed golden file: the catalog format it was written
// at, and the catalogTables and catalogSeed statements.
type catalogGolden struct {
	format int
	tables []string
	seed   []string
}

const (
	goldenFormatPrefix = "format "
	goldenTableMarker  = "== table"
	goldenSeedMarker   = "== seed"
)

func (g catalogGolden) String() string {
	var b strings.Builder
	b.WriteString(`# Golden copy of catalogTables and catalogSeed (pg_catalog_schema.go),
# checked by TestCatalogSchema_Golden. Appending entries is fine; editing or
# removing one needs a new numbered migration in catalogMigrations. Rewrite with
# CATALOG_SCHEMA_REWRITE=1.
`)
	fmt.Fprintf(&b, "%s%d\n", goldenFormatPrefix, g.format)
	for _, stmt := range g.tables {
		fmt.Fprintf(&b, "%s\n%s\n", goldenTableMarker, stmt)
	}
	for _, stmt := range g.seed {
		fmt.Fprintf(&b, "%s\n%s\n", goldenSeedMarker, stmt)
	}
	return b.String()
}

func parseCatalogGolden(t *testing.T, data string) catalogGolden {
	t.Helper()
	var g catalogGolden
	if data == "" {
		return g
	}
	var cur *[]string
	var stmt []string
	flush := func() {
		if cur != nil {
			*cur = append(*cur, strings.Join(stmt, "\n"))
		}
		stmt = nil
	}
	for line := range strings.SplitSeq(strings.TrimSuffix(data, "\n"), "\n") {
		switch {
		case cur == nil && strings.HasPrefix(line, "#"):
		case cur == nil && strings.HasPrefix(line, goldenFormatPrefix):
			format, err := strconv.Atoi(strings.TrimPrefix(line, goldenFormatPrefix))
			require.NoError(t, err, "golden format line")
			g.format = format
		case line == goldenTableMarker:
			flush()
			cur = &g.tables
		case line == goldenSeedMarker:
			flush()
			cur = &g.seed
		default:
			require.NotNil(t, cur, "golden line outside an entry: %q", line)
			stmt = append(stmt, line)
		}
	}
	flush()
	return g
}

// firstChangedEntry returns the index of the first entry of golden that
// current no longer has unchanged at the same position, or -1 if golden is a
// prefix of current (current only appends).
func firstChangedEntry(golden, current []string) int {
	for i, stmt := range golden {
		if i >= len(current) || current[i] != stmt {
			return i
		}
	}
	return -1
}

// TestCatalogSchema_Golden pins catalogTables and catalogSeed to
// testdata/catalog_schema.golden. Deployed catalogs never re-run a
// catalogTables statement whose table already exists, so silently editing one
// would leave them on the old shape: an edit or removal must come with a new
// numbered migration (a format newer than the golden file's) that converts
// existing catalogs. Appending is always fine. Either way, rewrite the golden
// file with CATALOG_SCHEMA_REWRITE=1.
func TestCatalogSchema_Golden(t *testing.T) {
	current := catalogGolden{format: latestCatalogFormat(), tables: catalogTables, seed: catalogSeed}

	data, err := os.ReadFile(catalogGoldenPath)
	if err != nil && !os.IsNotExist(err) {
		require.NoError(t, err)
	}
	golden := parseCatalogGolden(t, string(data))

	if current.format == golden.format {
		for _, list := range []struct {
			name            string
			golden, current []string
		}{
			{"catalogTables", golden.tables, current.tables},
			{"catalogSeed", golden.seed, current.seed},
		} {
			if i := firstChangedEntry(list.golden, list.current); i >= 0 {
				t.Fatalf("%s[%d] was edited or removed without a new catalog migration.\n"+
					"Existing catalogs already ran it and will never re-run an edit, so they would keep the old shape.\n"+
					"Leave the entry as it was and append the change instead, or add a numbered migration to\n"+
					"catalogMigrations that converts existing catalogs, then rewrite %s with CATALOG_SCHEMA_REWRITE=1.\n"+
					"golden entry:\n%s", list.name, i, catalogGoldenPath, list.golden[i])
			}
		}
	}

	want := current.String()
	if string(data) == want {
		return
	}
	if os.Getenv("CATALOG_SCHEMA_REWRITE") != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(catalogGoldenPath), 0o755))
		require.NoError(t, os.WriteFile(catalogGoldenPath, []byte(want), 0o644))
		return
	}
	t.Fatalf("%s is out of date with catalogTables/catalogSeed; rewrite it with CATALOG_SCHEMA_REWRITE=1", catalogGoldenPath)
}

func TestEnsureCatalogSchema_Errors(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(m *mock.QueryService)
		errorContains string
	}{
		{
			name: "lock fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("pg_advisory_xact_lock", errors.New("canceling statement"))
			},
			errorContains: "failed to lock catalog schema",
		},
		{
			name: "format read fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_advisory_xact_lock", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce(catalogStatePresentPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))
				m.AddQueryPatternOnce("SELECT format_version FROM multigres.catalog_state", mock.MakeQueryResult([]string{"format_version"}, nil))
			},
			errorContains: "failed to read catalog format version",
		},
		{
			name: "migration statement fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_advisory_xact_lock", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce(catalogStatePresentPattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))
				m.AddQueryPatternOnceWithError("^DROP TABLE IF EXISTS", errors.New("permission denied"))
			},
			errorContains: "failed to apply catalog format 1 migration",
		},
		{
			name: "catalog table statement fails",
			setupMock: func(m *mock.QueryService) {
				addExistingCatalogState(m, latestCatalogFormat())
				m.AddQueryPatternOnceWithError(exactPattern(catalogTables[0]), errors.New("permission denied"))
			},
			errorContains: "failed to ensure catalog tables",
		},
		{
			name: "seed statement fails",
			setupMock: func(m *mock.QueryService) {
				addExistingCatalogState(m, latestCatalogFormat())
				for _, stmt := range catalogTables {
					m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
				}
				m.AddQueryPatternOnceWithError(exactPattern(catalogSeed[0]), errors.New("permission denied"))
			},
			errorContains: "failed to seed catalog",
		},
		{
			name: "the connections without a conninfo cannot be listed",
			setupMock: func(m *mock.QueryService) {
				addExistingCatalogState(m, latestCatalogFormat())
				for _, stmt := range slices.Concat(catalogTables, catalogSeed) {
					m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
				}
				m.AddQueryPatternOnceWithError(exactPattern(selectUnfilledConnectionsSQL), errors.New("permission denied"))
			},
			errorContains: "failed to list connections without a conninfo",
		},
		{
			name: "a conninfo cannot be stored",
			setupMock: func(m *mock.QueryService) {
				addExistingCatalogState(m, latestCatalogFormat())
				for _, stmt := range slices.Concat(catalogTables, catalogSeed) {
					m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
				}
				m.AddQueryPatternOnce(exactPattern(selectUnfilledConnectionsSQL), mock.MakeQueryResult(
					[]string{"connection_id", "name", "dsn"}, [][]any{{int64(1), "legacy", legacyDSN}}))
				m.AddQueryPatternOnceWithError(exactPattern(fillConnInfoSQL), errors.New("permission denied"))
			},
			errorContains: "failed to store the conninfo of a connection",
		},
		{
			name: "format write fails",
			setupMock: func(m *mock.QueryService) {
				addExistingCatalogState(m, latestCatalogFormat())
				for _, stmt := range catalogTables {
					m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
				}
				for _, stmt := range catalogSeed {
					m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
				}
				m.AddQueryPatternOnce(exactPattern(selectUnfilledConnectionsSQL), mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError(catalogFormatWritePattern, errors.New("permission denied"))
			},
			errorContains: "failed to record catalog format version",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
			tt.setupMock(m)
			committed := false
			m.AddQueryPatternWithCallback("^COMMIT$", mock.MakeQueryResult(nil, nil), func(string) { committed = true })

			err := pm.ensureCatalogSchema(context.Background())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
			assert.False(t, committed, "a failed ensure must not commit")
			assert.NoError(t, m.ExpectationsWereMet())
		})
	}
}

// legacyDSN is what a connection created before the conninfo column holds: a dsn, and a password.
const legacyDSN = "host=db.example.com port=5433 dbname=app user=alice password='s3 cret' sslmode=require"

// fillConnInfoRun runs ensureCatalogSchema on a catalog that has the given connections without a
// conninfo, and returns the log it wrote and the connections it stored a conninfo for.
func fillConnInfoRun(t *testing.T, rows [][]any) (log string, stored int) {
	t.Helper()
	pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var buf bytes.Buffer
	pm.logger = slog.New(slog.NewTextHandler(&buf, nil))

	addExistingCatalogState(m, latestCatalogFormat())
	for _, stmt := range slices.Concat(catalogTables, catalogSeed) {
		m.AddQueryPatternOnce(exactPattern(stmt), mock.MakeQueryResult(nil, nil))
	}
	m.AddQueryPatternOnce(exactPattern(selectUnfilledConnectionsSQL),
		mock.MakeQueryResult([]string{"connection_id", "name", "dsn"}, rows))
	m.AddQueryPatternWithCallback(exactPattern(fillConnInfoSQL), mock.MakeQueryResult(nil, nil), func(string) { stored++ })
	m.AddQueryPatternOnce(catalogFormatWritePattern, mock.MakeQueryResult(nil, nil))

	require.NoError(t, pm.ensureCatalogSchema(context.Background()))
	require.NoError(t, m.ExpectationsWereMet())
	return buf.String(), stored
}

// A connection with a dsn and no conninfo is given one when the catalog is set up; one whose dsn
// cannot be a connection (no password, a socket host) is left as it is, named in the log, and the
// log holds nothing of any dsn.
func TestEnsureCatalogSchema_FillsTheConnInfoOfALegacyConnection(t *testing.T) {
	log, stored := fillConnInfoRun(t, [][]any{
		{int64(1), "legacy", legacyDSN},
		{int64(2), "no-password", "host=secret-host.example.com dbname=app user=alice sslmode=require"},
		{int64(3), "socket", "host=/var/run/secret-socket dbname=app user=alice password=hunter2"},
		{int64(4), "not-a-dsn", "this is not hunter3"},
	})
	assert.Equal(t, 1, stored, "only the connection that can be converted is written")
	for _, name := range []string{"no-password", "socket", "not-a-dsn"} {
		assert.Contains(t, log, "connection="+name)
	}
	assert.NotContains(t, log, "legacy", "a connection that was converted is not reported")
	for _, secret := range []string{"s3 cret", "cret", "secret-host", "db.example.com", "secret-socket", "hunter", "alice", "sslmode"} {
		assert.NotContains(t, log, secret, "the log holds nothing of a dsn")
	}
}

// Setup that finds nothing to convert writes nothing and says nothing.
func TestEnsureCatalogSchema_FillsNothingWhenEveryConnectionHasAConnInfo(t *testing.T) {
	log, stored := fillConnInfoRun(t, nil)
	assert.Zero(t, stored)
	assert.Empty(t, log)
}

// The stored conninfo and dsn are what CreateConnection stores for the same dsn, so that a
// converted connection is indistinguishable from one created now.
func TestLegacyConnInfoIsWhatCreateConnectionStores(t *testing.T) {
	conn, err := backingconn.FromDSN(legacyDSN)
	require.NoError(t, err)
	want, err := json.Marshal(conn)
	require.NoError(t, err)

	got, rendering, err := legacyConnInfo(legacyDSN)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), got)
	assert.JSONEq(t, `{"host":["db.example.com"],"port":[5433],"dbname":"app","user":"alice","password":"s3 cret","sslmode":"require"}`, got)
	assert.Equal(t, conn.DSN(), rendering)
	assert.Equal(t, `dbname='app' host='db.example.com' password='s3 cret' port='5433' sslmode='require' user='alice'`, rendering)

	_, _, err = legacyConnInfo("host=h dbname=app user=alice")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "alice")
}
