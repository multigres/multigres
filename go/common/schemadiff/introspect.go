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

package schemadiff

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// constraintTypes maps the pg_constraint.contype values Introspect reads to
// the constraint types of the parser.
var constraintTypes = map[string]ast.ConstrType{
	"p": ast.CONSTR_PRIMARY,
	"u": ast.CONSTR_UNIQUE,
	"c": ast.CONSTR_CHECK,
	"f": ast.CONSTR_FOREIGN,
	"x": ast.CONSTR_EXCLUSION,
}

// Querier runs one parameterized query and returns its result.
type Querier interface {
	QueryArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error)
}

// The catalog queries take the schema name as $1. Every rendering function
// runs in pretty mode, the only mode in which pg_get_indexdef leaves the
// table name unqualified when it is visible.
const (
	selectTablesSQL = `SELECT c.relname, c.relkind
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')
ORDER BY c.relname`

	selectColumnsSQL = `SELECT c.relname, a.attname,
	pg_catalog.format_type(a.atttypid, a.atttypmod)
		|| CASE WHEN a.attcollation <> t.typcollation THEN ' COLLATE ' || pg_catalog.quote_ident(co.collname) ELSE '' END,
	a.attnotnull, a.attidentity::text, a.attgenerated::text,
	coalesce(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true), ''),
	a.attidentity = '' AND EXISTS (
		SELECT 1 FROM pg_catalog.pg_depend dep
		JOIN pg_catalog.pg_class seq ON seq.oid = dep.objid
		WHERE dep.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
			AND dep.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
			AND dep.refobjid = a.attrelid AND dep.refobjsubid = a.attnum
			AND dep.deptype = 'a' AND seq.relkind = 'S')
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = $1 AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum`

	selectConstraintsSQL = `SELECT c.relname, con.conname, con.contype::text, pg_catalog.pg_get_constraintdef(con.oid, true),
	CASE WHEN r.relnamespace = c.relnamespace THEN r.relname ELSE '' END
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_class r ON r.oid = con.confrelid
WHERE n.nspname = $1 AND c.relkind = 'r' AND con.contype IN ('p', 'u', 'c', 'f', 'x')
ORDER BY c.relname, con.conname`

	selectIndexesSQL = `SELECT c.relname, i.relname, pg_catalog.pg_get_indexdef(x.indexrelid, 0, true)
FROM pg_catalog.pg_index x
JOIN pg_catalog.pg_class c ON c.oid = x.indrelid
JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind = 'r'
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_constraint con
		WHERE con.conrelid = x.indrelid AND con.conindid = x.indexrelid AND con.contype IN ('p', 'u', 'x'))
ORDER BY c.relname, i.relname`
)

// Introspect reads the ordinary tables of schema. A schema that does not
// exist reads as one with no tables.
//
// q must run its queries in one open transaction, which the caller owns:
// Introspect sets the search_path to schema alone for the transaction while
// it reads, so that names in schema render unqualified, and sets it back
// before it returns. Introspect fails when the search_path it set does not
// hold for its next query, as when q runs each query in a transaction of its
// own.
func Introspect(ctx context.Context, q Querier, schema string) (_ *Schema, err error) {
	res, err := q.QueryArgs(ctx, "SELECT pg_catalog.current_setting('search_path')")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read search_path")
	}
	prev, err := singleString(res)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read search_path")
	}
	res, err = q.QueryArgs(ctx, "SELECT pg_catalog.set_config('search_path', pg_catalog.quote_ident($1), true)", schema)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to set search_path")
	}
	set, err := singleString(res)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to set search_path")
	}
	defer func() {
		if _, restoreErr := q.QueryArgs(ctx, "SELECT pg_catalog.set_config('search_path', $1, true)", prev); restoreErr != nil && err == nil {
			err = mterrors.Wrap(restoreErr, "failed to restore search_path")
		}
	}()

	res, err = q.QueryArgs(ctx, "SELECT pg_catalog.current_setting('search_path')")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read search_path")
	}
	got, err := singleString(res)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read search_path")
	}
	if got != set {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "introspection must run inside a transaction: the search_path it set did not hold for its next query")
	}

	s := &Schema{Tables: map[string]*Table{}}

	rows, err := queryRows(ctx, q, selectTablesSQL, schema, 2)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to read tables of schema %s", schema)
	}
	for _, r := range rows {
		if r[1] == "p" {
			return nil, mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED, "table %s.%s is partitioned, which schemadiff does not support", schema, r[0])
		}
		s.Tables[r[0]] = &Table{Name: r[0], Constraints: map[string]*Constraint{}, Indexes: map[string]string{}}
	}

	rows, err = queryRows(ctx, q, selectColumnsSQL, schema, 8)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to read columns of schema %s", schema)
	}
	for _, r := range rows {
		t := s.Tables[r[0]]
		var identity byte
		if r[4] != "" {
			identity = r[4][0]
		}
		t.Columns = append(t.Columns, &Column{
			Name:      r[1],
			Type:      r[2],
			NotNull:   r[3] == "t",
			Identity:  identity,
			Generated: r[5] != "",
			Default:   r[6],
			Serial:    r[7] == "t",
		})
	}

	rows, err = queryRows(ctx, q, selectConstraintsSQL, schema, 5)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to read constraints of schema %s", schema)
	}
	for _, r := range rows {
		typ, ok := constraintTypes[r[2]]
		if !ok {
			return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "constraint %s of table %s.%s has unexpected type %q", r[1], schema, r[0], r[2])
		}
		s.Tables[r[0]].Constraints[r[1]] = &Constraint{Name: r[1], Type: typ, Definition: r[3], References: r[4]}
	}

	rows, err = queryRows(ctx, q, selectIndexesSQL, schema, 3)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to read indexes of schema %s", schema)
	}
	for _, r := range rows {
		s.Tables[r[0]].Indexes[r[1]] = r[2]
	}
	return s, nil
}

// queryRows runs query with schema as $1 and returns its rows as strings, a
// NULL as "". Every row must have width columns.
func queryRows(ctx context.Context, q Querier, query, schema string, width int) ([][]string, error) {
	res, err := q.QueryArgs(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row.Values) != width {
			return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "got %d columns, want %d", len(row.Values), width)
		}
		r := make([]string, width)
		for i, v := range row.Values {
			r[i] = string(v)
		}
		rows = append(rows, r)
	}
	return rows, nil
}

// singleString returns the only value of a one-row, one-column result.
func singleString(res *sqltypes.Result) (string, error) {
	if res == nil || len(res.Rows) != 1 || len(res.Rows[0].Values) != 1 {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL, "want one row of one column")
	}
	return string(res.Rows[0].Values[0]), nil
}
