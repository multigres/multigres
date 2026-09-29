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
}
