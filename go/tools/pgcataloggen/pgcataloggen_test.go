// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
package pgcataloggen

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []map[string]string
		wantErr string
	}{
		{
			name: "single line entry",
			input: `[
{ oid => '16', typname => 'bool', typlen => '1' },
]`,
			want: []map[string]string{
				{"oid": "16", "typname": "bool", "typlen": "1"},
			},
		},
		{
			name: "multi-line entry with comments",
			input: `# leading comment
[

# a comment between entries
{ oid => '177',
  proname => 'int4pl', prorettype => 'int4', proargtypes => 'int4 int4',
  prosrc => 'int4pl' },
]`,
			want: []map[string]string{
				{
					"oid": "177", "proname": "int4pl", "prorettype": "int4",
					"proargtypes": "int4 int4", "prosrc": "int4pl",
				},
			},
		},
		{
			name: "perl single-quote escapes",
			input: `[
{ oid => '16', descr => 'boolean, format \'t\'/\'f\'', other => 'a \\ b \n c' },
]`,
			want: []map[string]string{
				{"oid": "16", "descr": `boolean, format 't'/'f'`, "other": `a \ b \n c`},
			},
		},
		{
			name: "brace-balanced array values across lines",
			input: `[
{ oid => '1689', proargmodes => '{i,o,o}',
  proallargtypes => '{_aclitem,oid,
oid}' },
]`,
			want: []map[string]string{
				{"oid": "1689", "proargmodes": "{i,o,o}", "proallargtypes": "{_aclitem,oid,\noid}"},
			},
		},
		{
			name:    "unbalanced braces",
			input:   "{ oid => '1', typname => 'x'",
			wantErr: "unbalanced braces",
		},
		{
			name:    "missing arrow",
			input:   "{ oid '1' }",
			wantErr: "expected '=>'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries, err := ParseDat("test.dat", []byte(tt.input))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, entries, len(tt.want))
			for i, want := range tt.want {
				assert.Equal(t, want, entries[i].Fields)
			}
		})
	}
}

func TestLookupAmbiguity(t *testing.T) {
	keys := map[string]uint32{}
	addKey(keys, "int2", 100)
	addKey(keys, "int2(int8)", 100)
	addKey(keys, "int2", 200)
	addKey(keys, "int2(int4)", 200)

	// The bare name is claimed by two rows and must be rejected, mirroring
	// genbki.pl's MULTIPLE convention.
	_, err := lookup(keys, "int2", "pg_proc", false)
	require.ErrorContains(t, err, "ambiguous")

	oid, err := lookup(keys, "int2(int8)", "pg_proc", false)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), oid)

	// Optional lookups accept the none markers.
	oid, err = lookup(keys, "0", "pg_proc", true)
	require.NoError(t, err)
	assert.Zero(t, oid)
	oid, err = lookup(keys, "-", "pg_proc", true)
	require.NoError(t, err)
	assert.Zero(t, oid)
	_, err = lookup(keys, "0", "pg_proc", false)
	require.ErrorContains(t, err, "unresolved")
}

func TestSynthesizeArrayTypes(t *testing.T) {
	entries := []Entry{
		{Line: 1, Fields: map[string]string{
			"oid": "16", "array_type_oid": "1000", "typname": "bool",
			"typlen": "1", "typbyval": "t", "typcategory": "B",
			"typinput": "boolin", "typoutput": "boolout",
			"typreceive": "boolrecv", "typsend": "boolsend",
		}},
		{Line: 9, Fields: map[string]string{
			"oid": "2249", "typname": "record", "typlen": "-1",
			"typbyval": "f", "typcategory": "P",
			"typinput": "record_in", "typoutput": "record_out",
			"typreceive": "record_recv", "typsend": "record_send",
		}},
	}
	out, err := synthesizeArrayTypes(entries)
	require.NoError(t, err)
	require.Len(t, out, 3)

	elem, arr, rec := out[0], out[1], out[2]
	assert.Equal(t, "_bool", elem.Fields["typarray"], "element back-links its array by name")
	assert.Equal(t, "record", rec.Fields["typname"], "rows without array_type_oid pass through")

	assert.Equal(t, "1000", arr.Fields["oid"])
	assert.Equal(t, "_bool", arr.Fields["typname"])
	assert.Equal(t, "bool", arr.Fields["typelem"])
	assert.Equal(t, "-1", arr.Fields["typlen"])
	assert.Equal(t, "f", arr.Fields["typbyval"])
	assert.Equal(t, "A", arr.Fields["typcategory"])
	assert.Equal(t, "array_in", arr.Fields["typinput"])
	assert.Equal(t, "array_send", arr.Fields["typsend"])
}

// TestLoadRealData runs the full pipeline over the vendored REL_17_6 data and
// pins the exact row counts; pgcatalog's own tests cover row-level content.
func TestLoadRealData(t *testing.T) {
	cat, err := Load(filepath.Join("..", "..", "common", "pgcatalog", "data"))
	require.NoError(t, err)
	assert.Len(t, cat.Types, 193, "112 base types + 81 synthesized array types")
	assert.Len(t, cat.Procs, 3314)
	assert.Len(t, cat.Operators, 799)
	assert.Len(t, cat.Casts, 229)

	// Defaults come from system_functions.sql, not pg_proc.dat.
	withDefaults := map[string]int16{}
	for _, p := range cat.Procs {
		if p.NArgDefaults > 0 {
			withDefaults[p.Name] = p.NArgDefaults
		}
	}
	assert.Len(t, withDefaults, 33, "distinct function names with defaults")
	assert.Equal(t, int16(2), withDefaults["random_normal"])
	assert.Equal(t, int16(7), withDefaults["make_interval"])
	assert.Equal(t, int16(1), withDefaults["jsonb_set"])
	assert.Equal(t, int16(1), withDefaults["pg_terminate_backend"])
	assert.Equal(t, int16(2), withDefaults["jsonb_path_query"])
	assert.Equal(t, int16(1), withDefaults["pg_logical_emit_message"], "both overloads")
}

func TestParseSystemFunctionDefaults(t *testing.T) {
	known := func(name string) bool {
		switch name {
		case "int4", "float8", "text", "_text", "bool", "name", "pg_lsn", "bytea":
			return true
		}
		return false
	}
	src := []byte(`
-- a comment with CREATE OR REPLACE FUNCTION in it, DEFAULT too
/* a block comment,
   CREATE OR REPLACE FUNCTION nope(x int DEFAULT 1) */
CREATE OR REPLACE FUNCTION
 random_normal(mean float8 DEFAULT 0, stddev float8 DEFAULT 1)
 RETURNS float8
 LANGUAGE internal
 STRICT VOLATILE PARALLEL RESTRICTED
 AS 'drandom_normal';

CREATE OR REPLACE FUNCTION pg_logical_slot_get_changes(
    IN slot_name name, IN upto_lsn pg_lsn, IN upto_nchanges int, VARIADIC options text[] DEFAULT '{a,b}',
    OUT lsn pg_lsn, OUT xid xid, OUT data text)
RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'pg_logical_slot_get_changes';

CREATE OR REPLACE FUNCTION
  "normalize"(text, text DEFAULT 'NFC')
RETURNS text LANGUAGE internal AS 'unicode_normalize_func';

CREATE OR REPLACE FUNCTION
  make_interval(secs double precision DEFAULT 0.0)
RETURNS text LANGUAGE internal AS 'make_interval';

CREATE OR REPLACE FUNCTION no_defaults(a int, b text) RETURNS text LANGUAGE internal AS 'x';

REVOKE EXECUTE ON FUNCTION pg_backup_start(text, boolean) FROM public;
`)
	defaults, err := ParseSystemFunctionDefaults(src, known)
	require.NoError(t, err)
	assert.Equal(t, map[string]int16{
		"random_normal(float8,float8)":                        2,
		"pg_logical_slot_get_changes(name,pg_lsn,int4,_text)": 1,
		"normalize(text,text)":                                1,
		"make_interval(float8)":                               1,
	}, defaults)

	// A defaulted signature with a type the catalog does not know is an
	// error rather than a silently dropped default.
	_, err = ParseSystemFunctionDefaults([]byte(`CREATE OR REPLACE FUNCTION f(x nosuch DEFAULT 1) RETURNS int AS 'f';`), known)
	require.ErrorContains(t, err, `unknown parameter type "nosuch"`)
	_, err = ParseSystemFunctionDefaults([]byte(`CREATE OR REPLACE FUNCTION f(x int DEFAULT 1 RETURNS int AS 'f';`), known)
	require.ErrorContains(t, err, "unbalanced parentheses")
	_, err = ParseSystemFunctionDefaults([]byte(`CREATE OR REPLACE FUNCTION f`), known)
	require.ErrorContains(t, err, "without a parameter list")
	_, err = ParseSystemFunctionDefaults([]byte(`CREATE OR REPLACE FUNCTION f(x int DEFAULT 1) RETURNS int AS 'f';
CREATE OR REPLACE FUNCTION f(x int DEFAULT 2) RETURNS int AS 'f';`), known)
	require.ErrorContains(t, err, "defined twice")
}

func TestApplySystemFunctionDefaults(t *testing.T) {
	cat := &Catalog{
		Types: []TypeRow{{Oid: 23, Name: "int4"}, {Oid: 25, Name: "text"}},
		Procs: []ProcRow{
			{Oid: 1, Name: "f", ArgTypes: []uint32{23, 25}},
			{Oid: 2, Name: "f", ArgTypes: []uint32{23}},
			{Oid: 3, Name: "g", ArgTypes: []uint32{23}},
		},
	}
	require.NoError(t, cat.applySystemFunctionDefaults([]byte(
		`CREATE OR REPLACE FUNCTION f(a int, b text DEFAULT 'x') RETURNS int AS 'f';`)))
	assert.Equal(t, int16(1), cat.Procs[0].NArgDefaults, "the two-argument overload")
	assert.Zero(t, cat.Procs[1].NArgDefaults, "the one-argument overload is untouched")
	assert.Zero(t, cat.Procs[2].NArgDefaults)

	err := cat.applySystemFunctionDefaults([]byte(
		`CREATE OR REPLACE FUNCTION h(a int DEFAULT 1) RETURNS int AS 'h';`))
	require.ErrorContains(t, err, "h(int4) has no pg_proc.dat row")

	cat.Procs = append(cat.Procs, ProcRow{Oid: 4, Name: "g", ArgTypes: []uint32{23}})
	err = cat.applySystemFunctionDefaults([]byte(
		`CREATE OR REPLACE FUNCTION g(a int DEFAULT 1) RETURNS int AS 'g';`))
	require.ErrorContains(t, err, "g(int4) matches more than one pg_proc.dat row")
}
