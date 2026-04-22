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

package pgparity

import (
	"os"
	"path/filepath"
	"testing"
)

// TestParseStatements verifies that basic statement/query parsing produces
// the expected record types, payloads, and error expectations.
func TestParseStatements(t *testing.T) {
	content := `# comment
statement ok
CREATE TABLE t (a INT)

statement error duplicate
INSERT INTO t VALUES (1)

query I
SELECT 1
----
1

query II rowsort
SELECT a, b FROM t
----
1	one
2	two
`
	path := filepath.Join(t.TempDir(), "sample.slt")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	tf, err := ParseFile(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, want := len(tf.Records), 4; got != want {
		t.Fatalf("record count: got %d, want %d", got, want)
	}

	// statement ok
	if r := tf.Records[0]; r.Kind != DirectiveStatement || r.ExpectError || r.SQL != "CREATE TABLE t (a INT)" {
		t.Errorf("record[0] = %+v", r)
	}
	// statement error duplicate
	if r := tf.Records[1]; r.Kind != DirectiveStatement || !r.ExpectError || r.ErrorPattern != "duplicate" {
		t.Errorf("record[1] = %+v", r)
	}
	// query I ... 1
	if r := tf.Records[2]; r.Kind != DirectiveQuery || r.TypeString != "I" || r.ExpectedCount != 1 || r.ExpectedRows[0] != "1" {
		t.Errorf("record[2] = %+v", r)
	}
	// query II rowsort — tab-split expected rows flatten to 4 values.
	if r := tf.Records[3]; r.Kind != DirectiveQuery || r.TypeString != "II" || r.SortMode != "rowsort" || r.ExpectedCount != 4 {
		t.Errorf("record[3] = %+v", r)
	}
}

// TestParseRejectsUnsupported checks that the parser errors on anything
// outside the supported grammar, rather than silently skipping it. Silent
// skips would let a misspelled directive disable a whole block of tests.
func TestParseRejectsUnsupported(t *testing.T) {
	cases := map[string]string{
		"unknown directive":  "nonsense\nSELECT 1\n",
		"bad statement kind": "statement maybe\nSELECT 1\n",
		"bad sort mode":      "query I valuesort\nSELECT 1\n----\n1\n",
		"extra query field":  "query I rowsort extra\nSELECT 1\n----\n1\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.slt")
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := ParseFile(path); err == nil {
				t.Fatalf("expected parse error for %q, got nil", name)
			}
		})
	}
}
