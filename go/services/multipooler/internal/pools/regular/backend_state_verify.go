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

package regular

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
)

// The checkers in this file cover the backend state the pool trusts without
// verification beyond session GUCs (see session_verify.go for those):
//
//   - prepared statements: tracked per connection in ConnectionState and
//     legitimately kept across borrowers, so this is a two-sided diff
//     against pg_prepared_statements.
//   - session-level advisory locks: never legitimate on an idle pooled
//     backend. Acquiring functions route to a reserved connection whose
//     release runs pg_advisory_unlock_all; a lock still held means the
//     acquisition escaped tracking (e.g. inside a routine body).
//   - temp-schema objects: never legitimate on an idle pooled backend. Temp
//     statements pin a reserved connection that is closed at release, and
//     pg_temp-qualified CREATE is rejected at the gateway; an object still
//     present means the creation escaped tracking.
//
// All probes are read-only single statements. Like VerifySessionState they
// use plain Query, never the retry variants: a retry reconnects the socket,
// which resets the very state being inspected and would mask divergence.

// PreparedStatementChecker is the connpool.ConnChecker that verifies a
// connection's prepared statements; it delegates to VerifyPreparedStatements.
type PreparedStatementChecker struct{}

// Name implements connpool.ConnChecker.
func (PreparedStatementChecker) Name() string { return "prepared_statements" }

// Check implements connpool.ConnChecker.
func (PreparedStatementChecker) Check(ctx context.Context, conn *Conn) (connpool.Divergence, error) {
	return conn.VerifyPreparedStatements(ctx)
}

// foreignPreparedStatementName is reported in place of an untracked
// statement name that does not have the consolidator's shape. Such a name
// reached the backend outside the pooler (e.g. a PREPARE hidden in a routine
// body) and may embed client data, so it never enters Divergence or logs.
// One entry is reported per statement so counts survive redaction.
const foreignPreparedStatementName = "foreign_name"

// VerifyPreparedStatements compares the connection's tracked prepared
// statements against pg_prepared_statements. A statement the backend holds
// but tracking does not know is Untracked (the next borrower could collide
// with or execute it); a tracked statement the backend no longer has is
// Phantom (the next EXECUTE would fail). Tracked names are consolidator
// assigned and reported verbatim; an untracked name is reported verbatim
// only when it has the consolidator's shape (pointing at a tracking bug),
// otherwise it is redacted to foreignPreparedStatementName.
func (c *Conn) VerifyPreparedStatements(ctx context.Context) (connpool.Divergence, error) {
	var div connpool.Divergence
	if !c.IsIdle() {
		return div, errors.New("connection is not idle")
	}

	results, err := c.Query(ctx, constants.PreparedStatementsProbeSQL)
	if err != nil {
		return div, err
	}
	if len(results) != 1 {
		return div, errors.New("prepared statement probe returned unexpected result count")
	}

	backend := make(map[string]struct{}, results[0].RowCount())
	for _, row := range results[0].StructuredRows() {
		if len(row.Values) != 1 || row.Values[0].IsNull() {
			return div, errors.New("prepared statement probe returned malformed row")
		}
		backend[string(row.Values[0])] = struct{}{}
	}

	tracked := make(map[string]struct{})
	for _, name := range c.State().PreparedStatementNames() {
		// The unnamed statement is tracked under "" but never listed by
		// pg_prepared_statements.
		if name == "" {
			continue
		}
		tracked[name] = struct{}{}
		if _, ok := backend[name]; !ok {
			div.Phantom = append(div.Phantom, name)
		}
	}
	for name := range backend {
		if _, ok := tracked[name]; ok {
			continue
		}
		if !preparedstatement.IsCanonicalName(name) {
			name = foreignPreparedStatementName
		}
		div.Untracked = append(div.Untracked, name)
	}

	sort.Strings(div.Untracked)
	sort.Strings(div.Phantom)
	return div, nil
}

// AdvisoryLockChecker is the connpool.ConnChecker that verifies a connection
// holds no session-level advisory lock; it delegates to VerifyAdvisoryLocks.
type AdvisoryLockChecker struct{}

// Name implements connpool.ConnChecker.
func (AdvisoryLockChecker) Name() string { return "advisory_locks" }

// Check implements connpool.ConnChecker.
func (AdvisoryLockChecker) Check(ctx context.Context, conn *Conn) (connpool.Divergence, error) {
	return conn.VerifyAdvisoryLocks(ctx)
}

// advisoryLockDivergenceName is the single Untracked entry reported when an
// idle backend still holds an advisory lock. Lock keys are client data and
// are never reported.
const advisoryLockDivergenceName = "session_advisory_lock"

// VerifyAdvisoryLocks reports Untracked divergence when the idle backend
// holds any advisory lock. Outside a transaction every advisory lock in
// pg_locks is session-level, and tracking never leaves one on a pooled
// connection.
func (c *Conn) VerifyAdvisoryLocks(ctx context.Context) (connpool.Divergence, error) {
	var div connpool.Divergence
	if !c.IsIdle() {
		return div, errors.New("connection is not idle")
	}

	results, err := c.Query(ctx, constants.PgLocksAdvisoryProbeSQL)
	if err != nil {
		return div, err
	}
	if len(results) != 1 || results[0].RowCount() != 1 {
		return div, errors.New("advisory lock probe returned unexpected result shape")
	}
	row := results[0].StructuredRows()[0]
	if len(row.Values) != 1 || row.Values[0].IsNull() {
		return div, errors.New("advisory lock probe returned malformed row")
	}
	held, err := strconv.ParseBool(string(row.Values[0]))
	if err != nil {
		return div, errors.New("advisory lock probe returned a non-boolean value")
	}
	if held {
		div.Untracked = []string{advisoryLockDivergenceName}
	}
	return div, nil
}

// TempObjectChecker is the connpool.ConnChecker that verifies a connection's
// temporary schema is empty; it delegates to VerifyTempObjects.
type TempObjectChecker struct{}

// Name implements connpool.ConnChecker.
func (TempObjectChecker) Name() string { return "temp_objects" }

// Check implements connpool.ConnChecker.
func (TempObjectChecker) Check(ctx context.Context, conn *Conn) (connpool.Divergence, error) {
	return conn.VerifyTempObjects(ctx)
}

// tempObjectKinds maps the codes the temp probe can return — pg_class
// relkinds, 'type:' plus a pg_type typtype, and the fixed tags of the other
// catalogs — to bounded labels for logs and metrics. Object names are never
// reported. A code outside this map is reported as unknown_<code>.
var tempObjectKinds = map[string]string{
	"r":               "table",
	"p":               "partitioned_table",
	"i":               "index",
	"I":               "partitioned_index",
	"S":               "sequence",
	"v":               "view",
	"m":               "materialized_view",
	"c":               "composite_type",
	"t":               "toast_table",
	"f":               "foreign_table",
	"function":        "function",
	"type:d":          "domain",
	"type:e":          "enum",
	"type:r":          "range",
	"type:m":          "multirange",
	"type:b":          "base_type",
	"type:p":          "pseudo_type",
	"operator":        "operator",
	"collation":       "collation",
	"statistics":      "statistics",
	"operator_class":  "operator_class",
	"operator_family": "operator_family",
	"conversion":      "conversion",
	"ts_parser":       "ts_parser",
	"ts_dictionary":   "ts_dictionary",
	"ts_template":     "ts_template",
	"ts_config":       "ts_config",
}

// VerifyTempObjects reports one Untracked entry per kind of object found in
// the backend's temporary schema. An idle pooled backend must own none.
func (c *Conn) VerifyTempObjects(ctx context.Context) (connpool.Divergence, error) {
	var div connpool.Divergence
	if !c.IsIdle() {
		return div, errors.New("connection is not idle")
	}

	results, err := c.Query(ctx, constants.TempObjectsProbeSQL)
	if err != nil {
		return div, err
	}
	if len(results) != 1 {
		return div, errors.New("temp object probe returned unexpected result count")
	}

	kinds := make(map[string]struct{})
	for _, row := range results[0].StructuredRows() {
		if len(row.Values) != 1 || row.Values[0].IsNull() {
			return div, errors.New("temp object probe returned malformed row")
		}
		code := string(row.Values[0])
		kind, ok := tempObjectKinds[code]
		if !ok {
			kind = "unknown_" + code
		}
		kinds[kind] = struct{}{}
	}
	for kind := range kinds {
		div.Untracked = append(div.Untracked, kind)
	}
	sort.Strings(div.Untracked)
	return div, nil
}
