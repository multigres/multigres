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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPreparedDDLMatrix characterizes what a client observes when it reuses a
// prepared statement across a schema change, comparing direct PostgreSQL (the
// ground truth) against the multigateway. It sweeps:
//
//   - DDL kind: none, add column, drop column, rename column, change a
//     non-parameter column's type, change the $1 column's type, drop+recreate
//     with the same shape, drop+recreate changing the $1 column's type.
//   - Operation on the (possibly stale) statement: Describe statement,
//     Describe portal (binds $1), Bind+Execute (binds $1).
//   - Reuse mode: reuse the same statement name, or Close it and re-Parse the
//     same query text under a fresh name (what a driver does after a schema
//     reload).
//   - Transaction mode: autocommit, or everything inside one transaction.
//
// The single prepared query is `SELECT * FROM <t> WHERE id = $1`, so a DDL can
// change the result shape (via `*`), the $1 parameter type (via id), or both.
// Each cell records what the client sees: for a successful op a compact
// signature (parameter type OIDs and/or result column names), otherwise the
// SQLSTATE. Direct postgres is the reference; any multigateway cell that
// differs is a consolidator divergence.
//
// The full matrix is logged for characterization, but the test also ASSERTS a
// regression invariant: the multigateway may only diverge from PostgreSQL on the
// autocommit reuse-without-reparse path (where the reactive heal transparently
// re-Parses instead of surfacing PostgreSQL's stale-plan error). Everything a
// well-behaved driver does — re-Parse after a schema reload — and everything
// inside a transaction MUST match PostgreSQL. Exact per-cell outcomes/SQLSTATEs
// are not asserted (they vary with PostgreSQL version and pooling timing); only
// the structural invariant is. See mayDiverge.
//
// A unique table (and therefore a unique canonical prepared statement at the
// pooler) per scenario isolates PoolerConsolidator state across cells, and the
// pool is settled to a single backend so a single connection's reuse is
// deterministic.
func TestPreparedDDLMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(describeStalePoolCapacity, describeStaleReservedRatio, describeStaleRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 8*time.Minute)

	// Settle the regular sub-pool to a single backend.
	settleDB, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable"))
	require.NoError(t, err)
	_, _ = settleDB.ExecContext(ctx, "SELECT 1")
	time.Sleep(5 * time.Second)
	settleDB.Close()

	// cols/idLit/idBind default to an int id when empty (see runMatrixCell). Only
	// the scenarios that need a non-int starting type set them: recreating the $1
	// column across types produces different errors depending on direction —
	// int→uuid yields 42883 (operator "uuid = integer" does not exist), whereas
	// uuid→bigint yields 22P02 (the bound "1" fails to decode against the stale
	// uuid parameter — the exact PostgREST notify_reloading_catalog_cache case).
	ddls := []struct {
		name   string
		cols   string   // baseline column defs; "" → "id int, a int, b text"
		idLit  string   // seed-row id SQL literal; "" → "1"
		idBind string   // warm-bind id value (text form); "" → "1"
		sql    []string // DDL applied to the baseline table (named %s)
	}{
		{name: "none"},
		{name: "add_column", sql: []string{"ALTER TABLE %s ADD COLUMN c int"}},
		{name: "drop_column", sql: []string{"ALTER TABLE %s DROP COLUMN b"}},
		{name: "rename_column", sql: []string{"ALTER TABLE %s RENAME COLUMN a TO a2"}},
		{name: "retype_other", sql: []string{"ALTER TABLE %s ALTER COLUMN a TYPE bigint"}},
		{name: "retype_param", sql: []string{"ALTER TABLE %s ALTER COLUMN id TYPE bigint"}},
		{name: "drop_create_same", sql: []string{"DROP TABLE %s", "CREATE TABLE %s (id int, a int, b text)"}},
		{name: "drop_create_int_to_uuid", sql: []string{"DROP TABLE %s", "CREATE TABLE %s (id uuid, a int, b text)"}},
		{
			name:   "drop_create_uuid_to_bigint",
			cols:   "id uuid, a int, b text",
			idLit:  "'11111111-1111-1111-1111-111111111111'",
			idBind: "11111111-1111-1111-1111-111111111111",
			sql:    []string{"DROP TABLE %s", "CREATE TABLE %s (id bigint, a int, b text)"},
		},
	}
	ops := []string{"describe_stmt", "describe_portal", "execute"}
	reuseModes := []string{"reuse", "reprepare"}
	txnModes := []string{"autocommit", "in_txn"}

	targets := setup.GetComparisonTargets(t)
	// results[cellKey][targetName] = outcome
	results := map[string]map[string]string{}
	// mayDiverge marks the cells the multigateway is allowed to answer
	// differently from direct PostgreSQL. The only accepted divergence is the
	// autocommit "reuse" path: a client that reuses a prepared statement across a
	// schema change without re-Parsing gets PostgreSQL's stale-plan error
	// (0A000/22P02/42883), whereas the gateway transparently re-Parses and heals
	// it (the reactive cachedPlanRetry). Every other cell — anything a
	// well-behaved driver does (re-Parse after a reload), and everything inside a
	// transaction — MUST match PostgreSQL, so a divergence there is a regression.
	mayDiverge := map[string]bool{}
	var order []string

	tbl := 0
	for _, txn := range txnModes {
		for _, reuse := range reuseModes {
			for _, ddl := range ddls {
				for _, op := range ops {
					key := fmt.Sprintf("%-11s %-10s %-22s %s", txn, reuse, ddl.name, op)
					order = append(order, key)
					mayDiverge[key] = txn == "autocommit" && reuse == "reuse"
					results[key] = map[string]string{}
					for _, target := range targets {
						tbl++
						table := fmt.Sprintf("mtx_%d", tbl)
						out := runMatrixCell(t, ctx, target.Port, table, ddl.cols, ddl.idLit, ddl.idBind, ddl.sql, op, reuse, txn)
						results[key][target.Name] = out
					}
				}
			}
		}
	}

	// Render the matrix, flagging cells where multigateway != postgres.
	var b strings.Builder
	fmt.Fprintf(&b, "\n%-55s | %-28s | %-28s | %s\n", "txn / reuse / ddl / op", "postgres", "multigateway", "")
	b.WriteString(strings.Repeat("-", 130))
	b.WriteByte('\n')
	diverged := 0
	var unexpected []string
	for _, key := range order {
		pg := results[key]["postgres"]
		gw := results[key]["multigateway"]
		flag := ""
		if pg != gw {
			flag = "  <-- DIVERGES"
			diverged++
			if !mayDiverge[key] {
				unexpected = append(unexpected, fmt.Sprintf("%s: postgres=%q multigateway=%q", strings.TrimSpace(key), pg, gw))
			}
		}
		fmt.Fprintf(&b, "%-55s | %-28s | %-28s |%s\n", key, pg, gw, flag)
	}
	fmt.Fprintf(&b, "\n%d / %d cells diverge between postgres and multigateway\n", diverged, len(order))
	t.Log(b.String())

	// The matrix is logged above for characterization; the regression assertion is
	// only that no disallowed cell diverges. Exact per-cell outcomes and SQLSTATEs
	// are deliberately NOT asserted (they vary with PostgreSQL version and pooling
	// timing) — only the structural invariant that reprepare and in-transaction
	// behavior matches PostgreSQL. See mayDiverge for what is allowed to differ.
	require.Empty(t, unexpected,
		"multigateway must match PostgreSQL except on the autocommit reuse-without-reparse path; "+
			"a reprepare or in-transaction divergence is a regression:\n%s", strings.Join(unexpected, "\n"))
}

// runMatrixCell runs one scenario on a fresh connection and returns the
// observed outcome string (see TestPreparedDDLMatrix).
func runMatrixCell(t *testing.T, ctx context.Context, port int, table, cols, idLit, idBind string, ddlSQL []string, op, reuse, txn string) (outcome string) {
	t.Helper()
	if cols == "" {
		cols = "id int, a int, b text"
	}
	if idLit == "" {
		idLit = "1"
	}
	if idBind == "" {
		idBind = "1"
	}
	conn := connectLowLevelToPort(t, ctx, port)
	defer conn.Close()

	query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", table)
	// warmParams matches the baseline id type (so the warm executes cleanly);
	// reuseParams is always "1" — valid for the post-DDL id type but decoded
	// against whatever type the (possibly stale) statement carries for $1.
	warmParams := [][]byte{[]byte(idBind)}
	reuseParams := [][]byte{[]byte("1")}

	mustQuery := func(sql string) error { _, e := conn.Query(ctx, sql); return e }

	// Baseline table + row.
	if err := mustQuery("DROP TABLE IF EXISTS " + table); err != nil {
		return "SETUP:" + code(err)
	}
	if err := mustQuery(fmt.Sprintf("CREATE TABLE %s (%s)", table, cols)); err != nil {
		return "SETUP:" + code(err)
	}
	if err := mustQuery(fmt.Sprintf("INSERT INTO %s VALUES (%s, 10, 'x')", table, idLit)); err != nil {
		return "SETUP:" + code(err)
	}
	defer func() {
		c := connectLowLevelToPort(t, context.Background(), port)
		defer c.Close()
		_, _ = c.Query(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	inTxn := txn == "in_txn"
	if inTxn {
		if err := mustQuery("BEGIN"); err != nil {
			return "BEGIN:" + code(err)
		}
		defer func() { _, _ = conn.Query(ctx, "ROLLBACK") }()
	}

	// Warm the statement so a backend caches it against the pre-DDL schema.
	if err := conn.Parse(ctx, "s1", query, nil); err != nil {
		return "PARSE1:" + code(err)
	}
	if _, err := conn.BindAndExecute(ctx, "", "s1", warmParams, nil, nil, 0, discard); err != nil {
		return "WARM:" + code(err)
	}

	// Apply the DDL.
	for _, s := range ddlSQL {
		if err := mustQuery(strings.ReplaceAll(s, "%s", table)); err != nil {
			return "DDL:" + code(err)
		}
	}

	// Choose the statement name to operate on.
	name := "s1"
	if reuse == "reprepare" {
		if err := conn.CloseStatement(ctx, "s1"); err != nil {
			return "CLOSE:" + code(err)
		}
		if err := conn.Parse(ctx, "s2", query, nil); err != nil {
			return "PARSE2:" + code(err)
		}
		name = "s2"
	}

	switch op {
	case "describe_stmt":
		d, err := conn.DescribePrepared(ctx, name)
		if err != nil {
			return code(err)
		}
		return fmt.Sprintf("p%s f%s", paramSig(d), fieldSig(d))
	case "describe_portal":
		d, err := conn.BindAndDescribe(ctx, name, reuseParams, nil, nil)
		if err != nil {
			return code(err)
		}
		return "f" + fieldSig(d)
	case "execute":
		var first *sqltypes.Result
		_, err := conn.BindDescribeAndExecute(ctx, "", name, reuseParams, nil, nil, 0,
			func(_ context.Context, r *sqltypes.Result) error {
				if first == nil && r.Fields != nil {
					first = r
				}
				return nil
			})
		if err != nil {
			return code(err)
		}
		if first == nil {
			return "ok(no-desc)"
		}
		return "f" + fieldNames(first.Fields)
	}
	return "?"
}

func discard(context.Context, *sqltypes.Result) error { return nil }

// code returns the SQLSTATE of a backend error, or a short marker.
func code(err error) string {
	if err == nil {
		return "OK"
	}
	var d *mterrors.PgDiagnostic
	if errors.As(err, &d) {
		return d.Code
	}
	return "ERR"
}

func paramSig(d *query.StatementDescription) string {
	if d == nil {
		return "[]"
	}
	oids := make([]string, 0, len(d.Parameters))
	for _, p := range d.Parameters {
		oids = append(oids, strconv.FormatUint(uint64(p.DataTypeOid), 10))
	}
	return "[" + strings.Join(oids, ",") + "]"
}

func fieldSig(d *query.StatementDescription) string {
	if d == nil {
		return "[]"
	}
	return fieldNames(d.Fields)
}

func fieldNames[T interface{ GetName() string }](fields []T) string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.GetName())
	}
	return "[" + strings.Join(names, " ") + "]"
}
