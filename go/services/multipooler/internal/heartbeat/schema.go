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

package heartbeat

import (
	"context"
	"errors"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// CreateTableSQL creates the table the Writer and Reader share. It is defined here, next
// to the queries that use it, so the multipooler's schema setup and the tests build
// exactly the same table.
const CreateTableSQL = `CREATE TABLE multigres.heartbeat (
		shard_id BYTEA PRIMARY KEY,
		leader_id TEXT NOT NULL,
		ts BIGINT NOT NULL,
		quorum_commit_lsn pg_lsn,
		quorum_commit_ts TIMESTAMPTZ
	)`

// AddMissingColumnsSQL upgrades a heartbeat table created by an older release to the
// current shape. It lists every column added after the original table, and each must
// stay nullable: ADD COLUMN IF NOT EXISTS leaves existing rows with NULL, and the
// Writer/Reader already treat NULL as "no value yet".
//
// IF EXISTS keeps it a no-op where the table is absent; CheckSchema reports that case.
const AddMissingColumnsSQL = `ALTER TABLE IF EXISTS multigres.heartbeat
		ADD COLUMN IF NOT EXISTS quorum_commit_lsn pg_lsn,
		ADD COLUMN IF NOT EXISTS quorum_commit_ts TIMESTAMPTZ`

// expectedColumn is a column the Writer or Reader reads or writes by name.
type expectedColumn struct {
	name string
	// typ is the format_type() rendering of the column's type.
	typ string
	// upgradable columns were added after the original table, so AddMissingColumnsSQL
	// can add them to an older table.
	upgradable bool
}

var expectedColumns = []expectedColumn{
	{name: "shard_id", typ: "bytea"},
	{name: "leader_id", typ: "text"},
	{name: "ts", typ: "bigint"},
	{name: "quorum_commit_lsn", typ: "pg_lsn", upgradable: true},
	{name: "quorum_commit_ts", typ: "timestamp with time zone", upgradable: true},
}

// SchemaProblemKind classifies one way multigres.heartbeat can be incompatible with
// the queries the Writer and Reader run.
type SchemaProblemKind string

const (
	// ProblemMissingTable: the table does not exist (42P01 at runtime).
	ProblemMissingTable SchemaProblemKind = "missing_table"
	// ProblemMissingColumn: a column the queries name is absent, e.g. dropped or renamed
	// (42703 at runtime).
	ProblemMissingColumn SchemaProblemKind = "missing_column"
	// ProblemWrongType: a column has a different type (42804/22P02 at runtime).
	ProblemWrongType SchemaProblemKind = "wrong_type"
	// ProblemExtraRequiredColumn: a column the queries never name is NOT NULL without a
	// default, so every INSERT fails (23502 at runtime).
	ProblemExtraRequiredColumn SchemaProblemKind = "extra_required_column"
	// ProblemMissingUniqueKey: shard_id has no unique index, so the Writer's
	// ON CONFLICT (shard_id) fails (42P10 at runtime).
	ProblemMissingUniqueKey SchemaProblemKind = "missing_unique_key"
)

// SchemaProblem is one incompatibility between multigres.heartbeat and the code.
type SchemaProblem struct {
	Kind   SchemaProblemKind
	Column string
	Detail string
}

func (p SchemaProblem) String() string {
	if p.Column == "" {
		return string(p.Kind) + ": " + p.Detail
	}
	return string(p.Kind) + " " + p.Column + ": " + p.Detail
}

// SchemaReport is the result of CheckSchema.
type SchemaReport struct {
	Problems []SchemaProblem
}

// Compatible reports whether the table works with the Writer and Reader as-is.
func (r *SchemaReport) Compatible() bool {
	return r != nil && len(r.Problems) == 0
}

// Upgradable reports whether the table is incompatible only because columns added by a
// newer release are missing, which AddMissingColumnsSQL fixes. Every other problem
// needs an operator: guessing at a renamed or retyped column could destroy data.
func (r *SchemaReport) Upgradable() bool {
	if r == nil || len(r.Problems) == 0 {
		return false
	}
	for _, p := range r.Problems {
		if p.Kind != ProblemMissingColumn || !isUpgradable(p.Column) {
			return false
		}
	}
	return true
}

func isUpgradable(column string) bool {
	for _, c := range expectedColumns {
		if c.name == column {
			return c.upgradable
		}
	}
	return false
}

// String renders the problems for logs and error messages.
func (r *SchemaReport) String() string {
	if r == nil {
		return "unknown"
	}
	if len(r.Problems) == 0 {
		return "compatible"
	}
	parts := make([]string, len(r.Problems))
	for i, p := range r.Problems {
		parts[i] = p.String()
	}
	return strings.Join(parts, "; ")
}

// schemaColumnsSQL lists the live columns of multigres.heartbeat. required is true for
// a NOT NULL column with no default, identity or generation, i.e. one an INSERT must
// supply. No rows means the table is absent (to_regclass returns NULL).
const schemaColumnsSQL = `SELECT a.attname::text,
		format_type(a.atttypid, a.atttypmod),
		(a.attnotnull AND NOT a.atthasdef AND a.attidentity = '' AND a.attgenerated = '')
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = to_regclass('multigres.heartbeat')
	  AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY a.attnum`

// shardIDUniqueSQL reports whether shard_id alone has a unique index, which
// ON CONFLICT (shard_id) needs.
const shardIDUniqueSQL = `SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE i.indrelid = to_regclass('multigres.heartbeat')
		  AND i.indisunique AND i.indisvalid AND i.indnkeyatts = 1
		  AND i.indpred IS NULL AND i.indexprs IS NULL
		  AND a.attname = 'shard_id')`

// CheckSchema compares multigres.heartbeat with the shape the Writer and Reader need, so
// an incompatibility is found up front instead of as an error on every heartbeat tick.
// It returns an error only when the check itself could not run.
func CheckSchema(ctx context.Context, qs executor.InternalQueryService) (*SchemaReport, error) {
	result, err := qs.QueryAdmin(ctx, schemaColumnsSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read heartbeat table columns")
	}
	report := &SchemaReport{}
	rows := result.StructuredRows()
	if len(rows) == 0 {
		report.Problems = append(report.Problems, SchemaProblem{
			Kind: ProblemMissingTable, Detail: "multigres.heartbeat does not exist",
		})
		return report, nil
	}

	type liveColumn struct {
		typ      string
		required bool
	}
	live := make(map[string]liveColumn, len(rows))
	var order []string
	for _, row := range rows {
		name, err := executor.GetString(row, 0)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse heartbeat column name")
		}
		typ, err := executor.GetString(row, 1)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse heartbeat column type")
		}
		required, err := executor.GetBool(row, 2)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse heartbeat column nullability")
		}
		live[name] = liveColumn{typ: typ, required: required}
		order = append(order, name)
	}

	known := make(map[string]struct{}, len(expectedColumns))
	for _, want := range expectedColumns {
		known[want.name] = struct{}{}
		got, ok := live[want.name]
		switch {
		case !ok:
			report.Problems = append(report.Problems, SchemaProblem{
				Kind: ProblemMissingColumn, Column: want.name, Detail: "column is missing (expected " + want.typ + ")",
			})
		case got.typ != want.typ:
			report.Problems = append(report.Problems, SchemaProblem{
				Kind: ProblemWrongType, Column: want.name, Detail: "has type " + got.typ + ", expected " + want.typ,
			})
		}
	}
	for _, name := range order {
		if _, ok := known[name]; !ok && live[name].required {
			report.Problems = append(report.Problems, SchemaProblem{
				Kind: ProblemExtraRequiredColumn, Column: name,
				Detail: "unknown column is NOT NULL without a default, so heartbeat inserts fail",
			})
		}
	}

	// The unique key is only meaningful when shard_id itself is present.
	if _, ok := live["shard_id"]; ok {
		uniq, err := qs.QueryAdmin(ctx, shardIDUniqueSQL)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to read heartbeat table keys")
		}
		if len(uniq.StructuredRows()) == 0 {
			return nil, mterrors.New(mtrpcpb.Code_INTERNAL, "heartbeat key check returned no rows")
		}
		hasKey, err := executor.GetBool(uniq.StructuredRows()[0], 0)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse heartbeat key check")
		}
		if !hasKey {
			report.Problems = append(report.Problems, SchemaProblem{
				Kind: ProblemMissingUniqueKey, Column: "shard_id", Detail: "no unique index on shard_id alone",
			})
		}
	}
	return report, nil
}

// EnsureSchema upgrades an older heartbeat table in place and returns the resulting
// report. It adds only the columns newer releases need (see AddMissingColumnsSQL), and
// only when CheckSchema finds nothing else wrong: the ALTER takes an ACCESS EXCLUSIVE
// lock even when it changes nothing, so a compatible table is never touched, and an
// otherwise-incompatible one is left for an operator. The returned report reflects the
// table after the upgrade.
func EnsureSchema(ctx context.Context, qs executor.InternalQueryService) (*SchemaReport, error) {
	report, err := CheckSchema(ctx, qs)
	if err != nil || !report.Upgradable() {
		return report, err
	}
	if err := qs.QueryAdminMultiStatement(ctx, AddMissingColumnsSQL); err != nil {
		return report, mterrors.Wrap(err, "failed to add missing heartbeat columns")
	}
	return CheckSchema(ctx, qs)
}

// IsSchemaError reports whether err is a Postgres error that a heartbeat table
// incompatible with the Writer or Reader's queries raises (see SchemaProblemKind), as
// opposed to a transient or availability error such as a timeout or a connection loss.
func IsSchemaError(err error) bool {
	var diag *mterrors.PgDiagnostic
	if !errors.As(err, &diag) {
		return false
	}
	switch diag.SQLSTATE() {
	case "42703", // undefined_column
		"42P01", // undefined_table
		"42804", // datatype_mismatch
		"22P02", // invalid_text_representation
		"23502", // not_null_violation
		"42P10": // invalid_column_reference (ON CONFLICT without a matching unique index)
		return true
	}
	return false
}
