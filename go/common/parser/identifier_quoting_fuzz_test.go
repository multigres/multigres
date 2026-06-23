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

package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// TestIdentifierQuotingFuzz is a property-based check that complements the
// curated identifier_quoting_roundtrip_test.go cases.
//
// Strategy: for every statement in the test corpus, parse to AST, then walk
// the AST via reflection. For each string field whose name looks like a SQL
// identifier (column, table, role, tablespace, constraint name, etc.):
//
//  1. Save its original value.
//  2. Replace it with `weird ` + original — a value that *requires*
//     double-quoting on emit (the space alone forces QuoteIdentifier to quote).
//  3. Call SqlString on the statement and feed the result back into ParseSQL.
//  4. If the re-parse fails, the deparser is emitting that field unquoted —
//     that's a missed QuoteIdentifier site.
//  5. Restore the original value before moving on.
//
// Heuristic + ignore list: any string field whose name contains "name"
// (case-insensitive) is treated as a candidate identifier, plus a small
// allow-list (AccessMethod, Subname, Newname). Fields that *look* like
// identifiers but aren't (file paths, locale strings, privilege keywords)
// go into nonIdentifierFields keyed by `TypeName.FieldName`. That map is
// the durable audit artifact — adding a new identifier-typed string field
// requires explicit classification.
//
// By default we fuzz the curated `*_cases.json` files. The full PostgreSQL
// regression corpus is included unless `go test -short` is set.
func TestIdentifierQuotingFuzz(t *testing.T) {
	queries := loadFuzzCorpus(t)
	if len(queries) == 0 {
		t.Fatal("no fuzz corpus loaded — check testdata paths")
	}

	var findings []fuzzFinding
	dedup := make(map[string]struct{})
	report := func(f fuzzFinding) {
		if _, seen := dedup[f.FieldPath]; seen {
			return
		}
		dedup[f.FieldPath] = struct{}{}
		findings = append(findings, f)
	}

	for _, q := range queries {
		asts, err := ParseSQL(q)
		if err != nil {
			// Skip queries that don't parse at all — they belong to a
			// separate test concern (expected-error cases).
			continue
		}
		for _, stmt := range asts {
			fuzzStmt(stmt, q, report)
		}
	}

	if len(findings) > 0 {
		sort.Slice(findings, func(i, j int) bool {
			return findings[i].FieldPath < findings[j].FieldPath
		})
		for _, f := range findings {
			t.Errorf("missed QuoteIdentifier:\n  field:    %s\n  original: %q\n  mutated:  %q\n  query:    %s\n  deparsed: %s\n  reparse:  %s\n",
				f.FieldPath, f.Original, f.Mutated, f.Query, f.Deparsed, f.ParseErr)
		}
		t.Fatalf("identifier-quoting fuzz found %d distinct missed sites", len(findings))
	}
}

// fuzzFinding describes a single missed-quoting site discovered by the fuzz.
type fuzzFinding struct {
	FieldPath string // Type.Field path discovered via reflection
	Original  string // original identifier value from the parsed AST
	Mutated   string // value we substituted in (forces quoting)
	Query     string // input SQL that produced the AST
	Deparsed  string // SqlString output after mutation
	ParseErr  string // error returned when re-parsing the deparsed SQL
}

// fuzzFieldRef holds a mutable reference to one identifier-typed string field
// inside a parsed statement, plus a human-readable path for diagnostics.
type fuzzFieldRef struct {
	Value     reflect.Value
	FieldPath string
	Original  string
}

// fuzzStmt walks stmt and, for each identifier-typed string field with a
// non-empty value, mutates it to a quoting-required value, deparses + re-parses
// the whole statement, then restores. Calls report for each failure.
func fuzzStmt(stmt ast.Stmt, query string, report func(fuzzFinding)) {
	var refs []fuzzFieldRef
	visited := make(map[uintptr]bool)
	collectIdentifierFields(reflect.ValueOf(stmt), "", visited, &refs)

	for _, ref := range refs {
		if !ref.Value.CanSet() {
			continue
		}
		mutated := "weird " + ref.Original
		ref.Value.SetString(mutated)

		// SqlString may panic on certain partially-formed nodes — treat as a finding.
		var deparsed string
		var deparseErr string
		func() {
			defer func() {
				if r := recover(); r != nil {
					deparseErr = fmt.Sprintf("SqlString panicked: %v", r)
				}
			}()
			deparsed = stmt.SqlString()
		}()

		if deparseErr != "" {
			report(fuzzFinding{ref.FieldPath, ref.Original, mutated, query, "", deparseErr})
		} else if _, err := ParseSQL(deparsed); err != nil {
			report(fuzzFinding{ref.FieldPath, ref.Original, mutated, query, deparsed, err.Error()})
		}

		ref.Value.SetString(ref.Original)
	}
}

// collectIdentifierFields walks v via reflection and appends settable string
// fields that look like identifiers to *out. visited prevents infinite recursion
// on cycles.
func collectIdentifierFields(v reflect.Value, parentPath string, visited map[uintptr]bool, out *[]fuzzFieldRef) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		addr := v.Pointer()
		if visited[addr] {
			return
		}
		visited[addr] = true
		collectIdentifierFields(v.Elem(), parentPath, visited, out)
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		collectIdentifierFields(v.Elem(), parentPath, visited, out)
	case reflect.Struct:
		typeName := v.Type().Name()
		path := typeName
		if parentPath != "" {
			path = parentPath + ">" + typeName
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			fv := v.Field(i)
			fieldPath := path + "." + field.Name

			if fv.Kind() == reflect.String {
				if isIdentifierField(typeName, field.Name) {
					orig := fv.String()
					if orig == "" || !fv.CanSet() {
						continue
					}
					*out = append(*out, fuzzFieldRef{fv, fieldPath, orig})
				}
				continue
			}
			collectIdentifierFields(fv, fieldPath, visited, out)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectIdentifierFields(v.Index(i), fmt.Sprintf("%s[%d]", parentPath, i), visited, out)
		}
	}
}

// isIdentifierField decides whether a string field should be fuzzed as an
// identifier. Default heuristic: name contains "name" (case-insensitive), plus
// a small allow-list of other identifier-typed fields. Specific fields are
// excluded via nonIdentifierFields.
func isIdentifierField(typeName, fieldName string) bool {
	if nonIdentifierFields[typeName+"."+fieldName] {
		return false
	}
	switch fieldName {
	case "AccessMethod", "Subname", "Newname":
		return true
	}
	return strings.Contains(strings.ToLower(fieldName), "name")
}

// nonIdentifierFields lists string fields whose name suggests "identifier" but
// are actually something else (file paths, locale strings, free-form labels,
// SQL privilege keywords). The fuzz test skips them.
//
// Adding a new identifier-typed field to the AST requires explicit
// classification: either fuzz catches any deparser bug for it automatically
// (no entry needed), or — if it's not really an identifier — add it here with
// a comment explaining why.
var nonIdentifierFields = map[string]bool{
	// CREATE TABLESPACE LOCATION '/path' — filesystem path, emitted as quoted string literal.
	"CreateTableSpaceStmt.LocationPath": true,
	// AccessPriv.PrivName holds a SQL privilege keyword (SELECT, INSERT, ALL,
	// USAGE, ...). In GrantRoleStmt it holds a role identifier and is quoted at
	// the call site; the AST shape doesn't distinguish the two contexts.
	"AccessPriv.PrivName": true,
	// VariableSetStmt.Name is a GUC (configuration parameter) name, not a
	// user-namespace identifier. PostgreSQL emits these unquoted in dumps; the
	// canonical form has no spaces, so we don't quote on deparse.
	"VariableSetStmt.Name": true,
	// DefElem.Defname is a definition option name (volatility, password,
	// inherit, receive, send, ...). Some consuming contexts emit it as a SQL
	// keyword; others (DefElem.SqlString) already wrap it in QuoteIdentifier.
	// The bare emissions are intentional keyword-form, not missed quoting.
	"DefElem.Defname": true,
	// ColumnDef.StorageName is a storage mode keyword (PLAIN, EXTERNAL,
	// EXTENDED, MAIN, DEFAULT), not a user identifier — emitted unquoted.
	"ColumnDef.StorageName": true,
	// CreatePolicyStmt.CmdName holds a SQL command keyword (SELECT, INSERT,
	// UPDATE, DELETE, ALL) — it MUST be emitted as a keyword, not quoted.
	"CreatePolicyStmt.CmdName": true,
}

// loadFuzzCorpus reads queries from the curated test-case JSON files. When not
// running in -short mode, also walks the PostgreSQL regression corpus.
func loadFuzzCorpus(t *testing.T) []string {
	t.Helper()
	fuzzCorpusFiles := []string{
		"testdata/ddl_cases.json",
		"testdata/dml_cases.json",
		"testdata/select_cases.json",
		"testdata/misc_cases.json",
		"testdata/set_cases.json",
	}
	var queries []string
	for _, f := range fuzzCorpusFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var cases []ParseTest
		if err := json.Unmarshal(data, &cases); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, c := range cases {
			if c.Query != "" && c.Error == "" {
				queries = append(queries, c.Query)
			}
		}
	}
	if !testing.Short() {
		entries, err := os.ReadDir("testdata/postgres")
		if err == nil {
			for _, e := range entries {
				if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
					continue
				}
				path := filepath.Join("testdata/postgres", e.Name())
				data, err := os.ReadFile(path)
				if err != nil {
					continue
				}
				var cases []ParseTest
				if json.Unmarshal(data, &cases) != nil {
					continue
				}
				for _, c := range cases {
					if c.Query != "" && c.Error == "" {
						queries = append(queries, c.Query)
					}
				}
			}
		}
	}
	return queries
}
