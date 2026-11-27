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
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Change is one DDL statement of a diff.
type Change struct {
	// Table is the table the statement changes or creates.
	Table string
	// SQL is the statement, with names unqualified (see the package comment).
	SQL string
	// Breaking reports a statement that drops a table or a column, or changes
	// a column's type: one that can lose data, or break a reader of the old
	// shape.
	Breaking bool
}

// Options choose what Diff leaves alone. The zero value turns from into to
// completely.
type Options struct {
	// KeepExtraTables leaves the tables only from has in place: Diff emits no
	// DROP TABLE, and fails when it cannot leave one alone (see Diff).
	KeepExtraTables bool
}

// phase is a stage of a diff. Diff emits every statement of a phase before
// any statement of the next, so that no statement runs before what it
// depends on.
type phase int

const (
	// phaseDropForeignKeys drops the foreign keys that to removes or
	// changes, the foreign keys that reference a rekeyed table (see
	// rekeyedTables), and the foreign keys between two tables Diff drops.
	phaseDropForeignKeys phase = iota
	// phaseDropTables drops the tables only from has. It runs before any key
	// is dropped, since a dropped table's foreign keys may pin one.
	phaseDropTables
	// phaseDropConstraints drops the other constraints, and the indexes,
	// that to removes or changes.
	phaseDropConstraints
	// phaseCreateTables creates the tables to adds, with their columns only.
	phaseCreateTables
	// phaseColumns drops, alters and adds the columns of the tables both
	// have.
	phaseColumns
	// phaseAddConstraints adds the new and changed constraints other than
	// foreign keys, and creates the new and changed indexes.
	phaseAddConstraints
	// phaseAddForeignKeys adds the new, changed and rebuilt foreign keys,
	// once every table and key they reference exists.
	phaseAddForeignKeys

	numPhases
)

// Diff returns the statements that turn the tables of from into the tables of
// to, in an order in which they can run one after another: the phases above,
// in order. Within a phase, tables come in name order, and the members of a
// table in name order, except columns, which come in to's column order.
//
// A changed constraint or index is dropped and created again. A foreign key
// depends on the unique index of the table it references, so when Diff drops
// a primary key, unique or exclusion constraint, or an index, of a table, it
// also drops and adds again every foreign key that references that table.
// Column order is not compared, since PostgreSQL cannot reorder columns.
//
// Diff fails on a change it cannot express as DDL that keeps the column: a
// column that becomes or stops being an identity, generated or serial column,
// changes its identity kind, or changes its generation expression. It fails
// to create a serial column, whose sequence the model does not hold. With
// opts.KeepExtraTables it also fails when a table only from has holds a
// foreign key referencing a table whose keys change, since that foreign key
// would have to change and Diff leaves the table alone. Statements that
// PostgreSQL rejects for the data the table holds (a NOT NULL over NULLs, a
// type the old values do not cast to) fail when they run.
func Diff(from, to *Schema, opts Options) ([]Change, error) {
	var phases [numPhases][]Change
	add := func(p phase, table, sql string, breaking bool) {
		phases[p] = append(phases[p], Change{Table: table, SQL: sql, Breaking: breaking})
	}

	rekeyed := rekeyedTables(from, to)
	for _, name := range slices.Sorted(maps.Keys(from.Tables)) {
		if _, ok := to.Tables[name]; ok {
			continue
		}
		have := from.Tables[name]
		if !opts.KeepExtraTables {
			// DROP TABLE removes the table's own foreign keys, but not one
			// that references another table Diff drops before this one.
			for _, cname := range slices.Sorted(maps.Keys(have.Constraints)) {
				c := have.Constraints[cname]
				if c.Type != ast.CONSTR_FOREIGN || c.References == name {
					continue
				}
				if _, kept := to.Tables[c.References]; c.References != "" && !kept {
					add(phaseDropForeignKeys, name, dropConstraintSQL(name, cname), false)
				}
			}
			add(phaseDropTables, name, "DROP TABLE "+quote(name), true)
			continue
		}
		for _, cname := range slices.Sorted(maps.Keys(have.Constraints)) {
			if c := have.Constraints[cname]; c.Type == ast.CONSTR_FOREIGN && rekeyed[c.References] {
				return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "foreign key %s of table %s, which the target schema does not have, references table %s, whose keys change",
					cname, name, c.References)
			}
		}
	}
	// kept reports a constraint of a table both have that Diff leaves in place.
	kept := func(have, want *Table, cname string) bool {
		old, c := have.Constraints[cname], want.Constraints[cname]
		return old != nil && c != nil && *old == *c && !(c.Type == ast.CONSTR_FOREIGN && rekeyed[c.References])
	}

	for _, name := range slices.Sorted(maps.Keys(to.Tables)) {
		want := to.Tables[name]
		have, ok := from.Tables[name]
		if !ok {
			for _, col := range want.Columns {
				if col.Serial {
					return nil, serialError(name, col)
				}
			}
			add(phaseCreateTables, name, createTableSQL(want), false)
			for _, cname := range slices.Sorted(maps.Keys(want.Constraints)) {
				addConstraint(add, want, want.Constraints[cname])
			}
			for _, iname := range slices.Sorted(maps.Keys(want.Indexes)) {
				add(phaseAddConstraints, name, want.Indexes[iname], false)
			}
			continue
		}

		for _, cname := range slices.Sorted(maps.Keys(have.Constraints)) {
			if kept(have, want, cname) {
				continue
			}
			p := phaseDropConstraints
			if have.Constraints[cname].Type == ast.CONSTR_FOREIGN {
				p = phaseDropForeignKeys
			}
			add(p, name, dropConstraintSQL(name, cname), false)
		}
		for _, iname := range slices.Sorted(maps.Keys(have.Indexes)) {
			if def, ok := want.Indexes[iname]; ok && def == have.Indexes[iname] {
				continue
			}
			add(phaseDropConstraints, name, "DROP INDEX "+quote(iname), false)
		}

		for _, col := range have.Columns {
			if want.column(col.Name) == nil {
				add(phaseColumns, name, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quote(name), quote(col.Name)), true)
			}
		}
		for _, col := range want.Columns {
			old := have.column(col.Name)
			if old == nil {
				if col.Serial {
					return nil, serialError(name, col)
				}
				add(phaseColumns, name, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quote(name), columnSQL(col)), false)
				continue
			}
			changes, err := alterColumn(name, old, col)
			if err != nil {
				return nil, err
			}
			phases[phaseColumns] = append(phases[phaseColumns], changes...)
		}

		for _, cname := range slices.Sorted(maps.Keys(want.Constraints)) {
			if kept(have, want, cname) {
				continue
			}
			addConstraint(add, want, want.Constraints[cname])
		}
		for _, iname := range slices.Sorted(maps.Keys(want.Indexes)) {
			if def, ok := have.Indexes[iname]; ok && def == want.Indexes[iname] {
				continue
			}
			add(phaseAddConstraints, name, want.Indexes[iname], false)
		}
	}

	var changes []Change
	for _, p := range phases {
		changes = append(changes, p...)
	}
	return changes, nil
}

// rekeyedTables returns the tables both schemas have of which Diff drops a
// primary key, unique or exclusion constraint, or an index: the ones a foreign
// key referencing them may depend on.
func rekeyedTables(from, to *Schema) map[string]bool {
	rekeyed := map[string]bool{}
	for name, want := range to.Tables {
		have, ok := from.Tables[name]
		if !ok {
			continue
		}
		for cname, old := range have.Constraints {
			if old.Type == ast.CONSTR_CHECK || old.Type == ast.CONSTR_FOREIGN {
				continue
			}
			if c, ok := want.Constraints[cname]; !ok || *c != *old {
				rekeyed[name] = true
			}
		}
		for iname, def := range have.Indexes {
			if want.Indexes[iname] != def {
				rekeyed[name] = true
			}
		}
	}
	return rekeyed
}

// addConstraint adds c in phaseAddForeignKeys if it is a foreign key, and in
// phaseAddConstraints otherwise.
func addConstraint(add func(p phase, table, sql string, breaking bool), t *Table, c *Constraint) {
	p := phaseAddConstraints
	if c.Type == ast.CONSTR_FOREIGN {
		p = phaseAddForeignKeys
	}
	add(p, t.Name, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", quote(t.Name), quote(c.Name), c.Definition), false)
}

// serialError is the error for a serial column Diff would have to create.
func serialError(table string, col *Column) error {
	return mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED, "column %s.%s is a serial column, whose sequence schemadiff cannot create; use an identity column", table, col.Name)
}

// dropConstraintSQL returns the statement that drops constraint cname of
// table.
func dropConstraintSQL(table, cname string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", quote(table), quote(cname))
}

// alterColumn returns the statements that turn column old of table into col.
func alterColumn(table string, old, col *Column) ([]Change, error) {
	if old.Identity != col.Identity {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED, "column %s.%s changes its identity, which schemadiff does not support", table, col.Name)
	}
	if old.Generated != col.Generated || (col.Generated && old.Default != col.Default) {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED, "column %s.%s changes its generation, which schemadiff does not support", table, col.Name)
	}
	if old.Serial != col.Serial {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED, "column %s.%s becomes or stops being a serial column, which schemadiff does not support", table, col.Name)
	}

	prefix := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ", quote(table), quote(col.Name))
	var changes []Change
	if old.Type != col.Type {
		changes = append(changes, Change{Table: table, SQL: prefix + "TYPE " + col.Type, Breaking: true})
	}
	if !col.Generated && old.Default != col.Default {
		sql := prefix + "DROP DEFAULT"
		if col.Default != "" {
			sql = prefix + "SET DEFAULT " + col.Default
		}
		changes = append(changes, Change{Table: table, SQL: sql})
	}
	// An identity column is NOT NULL by definition, and both sides have the
	// same identity.
	if col.Identity == 0 && old.NotNull != col.NotNull {
		sql := prefix + "DROP NOT NULL"
		if col.NotNull {
			sql = prefix + "SET NOT NULL"
		}
		changes = append(changes, Change{Table: table, SQL: sql})
	}
	return changes, nil
}

// createTableSQL returns the CREATE TABLE of t with its columns only.
func createTableSQL(t *Table) string {
	cols := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = "\t" + columnSQL(c)
	}
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)", quote(t.Name), strings.Join(cols, ",\n"))
}

// columnSQL returns the column definition of c, as CREATE TABLE and ADD
// COLUMN take it.
func columnSQL(c *Column) string {
	sql := quote(c.Name) + " " + c.Type
	switch {
	case c.Generated:
		sql += " GENERATED ALWAYS AS (" + c.Default + ") STORED"
	case c.Default != "":
		sql += " DEFAULT " + c.Default
	}
	switch c.Identity {
	case ast.ATTRIBUTE_IDENTITY_ALWAYS:
		sql += " GENERATED ALWAYS AS IDENTITY"
	case ast.ATTRIBUTE_IDENTITY_BY_DEFAULT:
		sql += " GENERATED BY DEFAULT AS IDENTITY"
	}
	if c.NotNull {
		sql += " NOT NULL"
	}
	return sql
}

// quote quotes an identifier where PostgreSQL needs it quoted.
func quote(name string) string {
	return ast.QuoteIdentifier(name)
}
