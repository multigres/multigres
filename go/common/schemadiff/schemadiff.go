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

// Package schemadiff computes the DDL that turns the tables of one PostgreSQL
// schema into the tables of another.
//
// Both sides are read from the catalog (Introspect), not parsed from DDL text,
// so PostgreSQL does all normalization: a type is what format_type prints, an
// expression what pg_get_expr prints, a constraint what pg_get_constraintdef
// prints, an unnamed constraint carries the name PostgreSQL chose for it. Two
// tables created from equivalent DDL therefore compare equal field by field.
// To diff a live schema against declared DDL, run the DDL into a scratch
// schema and introspect that.
//
// Every rendered expression, constraint and index leaves names unqualified
// when the search_path makes them visible, and Introspect reads each schema
// with the search_path set to that schema alone. The statements Diff returns
// are therefore unqualified too, and must run with the search_path set to the
// schema being changed.
//
// The model covers ordinary tables: columns (type, collation, default, NOT
// NULL, identity, stored generation), constraints (primary key, unique, check,
// foreign key, exclusion) and the indexes that back no constraint. It does not
// cover sequences, table options, ownership, privileges, comments, row
// security, triggers, or partitioning; Introspect fails on a partitioned
// table, and Diff on a serial column it would have to create.
package schemadiff

import "github.com/multigres/multigres/go/common/parser/ast"

// Schema is the set of ordinary tables in one PostgreSQL schema, by name.
type Schema struct {
	Tables map[string]*Table
}

// Table is one ordinary table.
type Table struct {
	Name string
	// Columns are in attribute-number order.
	Columns []*Column
	// Constraints are by name. NOT NULL is a column property, not a
	// constraint here.
	Constraints map[string]*Constraint
	// Indexes are the CREATE INDEX statements (pg_get_indexdef) of the
	// table's indexes that back no constraint, by index name.
	Indexes map[string]string
}

// Column is one column of a table.
type Column struct {
	Name string
	// Type is format_type of the column's type and typmod, followed by
	// " COLLATE <name>" when the collation differs from the type's default.
	Type    string
	NotNull bool
	// Default is the default expression, or the generation expression of a
	// generated column; empty for none.
	Default string
	// Identity is pg_attribute.attidentity: ast.ATTRIBUTE_IDENTITY_ALWAYS,
	// ast.ATTRIBUTE_IDENTITY_BY_DEFAULT, or 0 for a column that is not an
	// identity column.
	Identity byte
	// Generated reports a stored generated column, whose expression is
	// Default.
	Generated bool
	// Serial reports a column that is not an identity column and owns a
	// sequence, as a serial column does: its Default calls nextval on a
	// sequence the model does not hold.
	Serial bool
}

// Constraint is one table constraint.
type Constraint struct {
	Name string
	// Type is ast.CONSTR_PRIMARY, ast.CONSTR_UNIQUE, ast.CONSTR_CHECK,
	// ast.CONSTR_FOREIGN or ast.CONSTR_EXCLUSION.
	Type ast.ConstrType
	// Definition is pg_get_constraintdef of the constraint, such as
	// "UNIQUE (name)" or "CHECK (state = ...)".
	Definition string
	// References is the table a foreign key references, when that table is
	// in the same schema; empty otherwise.
	References string
}

// column returns the table's column with the given name, or nil.
func (t *Table) column(name string) *Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}
