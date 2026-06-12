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

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeNameToOid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Oid
	}{
		// Boolean
		{"bool", "bool", BOOLOID},
		{"boolean", "boolean", BOOLOID},

		// Integer types
		{"int", "int", INT4OID},
		{"int4", "int4", INT4OID},
		{"integer", "integer", INT4OID},
		{"int2", "int2", INT2OID},
		{"smallint", "smallint", INT2OID},
		{"int8", "int8", INT8OID},
		{"bigint", "bigint", INT8OID},

		// Floating point
		{"float4", "float4", FLOAT4OID},
		{"real", "real", FLOAT4OID},
		{"float8", "float8", FLOAT8OID},
		{"float", "float", FLOAT8OID},
		{"double precision", "double precision", FLOAT8OID},

		// Numeric
		{"numeric", "numeric", NUMERICOID},
		{"decimal", "decimal", NUMERICOID},

		// Character types
		{"text", "text", TEXTOID},
		{"varchar", "varchar", VARCHAROID},
		{"char", "char", BPCHAROID},
		{"bpchar", "bpchar", BPCHAROID},
		{"name", "name", NAMEOID},

		// Date/time
		{"date", "date", DATEOID},
		{"time", "time", TIMEOID},
		{"timetz", "timetz", TIMETZOID},
		{"timestamp", "timestamp", TIMESTAMPOID},
		{"timestamptz", "timestamptz", TIMESTAMPTZOID},
		{"interval", "interval", INTERVALOID},

		// Binary
		{"bytea", "bytea", BYTEAOID},

		// JSON
		{"json", "json", JSONOID},
		{"jsonb", "jsonb", JSONBOID},

		// UUID
		{"uuid", "uuid", UUIDOID},

		// Network
		{"inet", "inet", INETOID},
		{"cidr", "cidr", CIDROID},
		{"macaddr", "macaddr", MACADDROID},
		{"macaddr8", "macaddr8", MACADDR8OID},

		// XML
		{"xml", "xml", XMLOID},

		// Bit string
		{"bit", "bit", BITOID},
		{"varbit", "varbit", VARBITOID},

		// Money
		{"money", "money", MONEYOID},

		// System
		{"oid", "oid", OIDOID},

		// Case insensitivity
		{"INT", "INT", INT4OID},
		{"Text", "Text", TEXTOID},
		{"BOOLEAN", "BOOLEAN", BOOLOID},

		// Unknown
		{"unknown type", "foobar", InvalidOid},
		{"empty", "", InvalidOid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, TypeNameToOid(tt.input))
		})
	}
}

// TestTypeNameToOidRoundTrip verifies that every OID with a known String()
// representation can be resolved back via TypeNameToOid.
func TestTypeNameToOidRoundTrip(t *testing.T) {
	// All OIDs that have a canonical String() name and should round-trip.
	// CHAROID (18, internal "char") is excluded: its String() returns "CHAR"
	// which correctly resolves to BPCHAROID (1042, SQL CHAR) since SQL PREPARE
	// always means the standard character type.
	oids := []Oid{
		BOOLOID, NAMEOID, TEXTOID, VARCHAROID, BPCHAROID,
		BYTEAOID,
		INT2OID, INT4OID, INT8OID,
		FLOAT4OID, FLOAT8OID,
		NUMERICOID,
		DATEOID, TIMEOID, TIMETZOID, TIMESTAMPOID, TIMESTAMPTZOID, INTERVALOID,
		JSONOID, JSONBOID,
		XMLOID,
		UUIDOID,
		INETOID, CIDROID, MACADDROID, MACADDR8OID,
		BITOID, VARBITOID,
		MONEYOID,
		OIDOID,
	}

	for _, oid := range oids {
		name := oid.String()
		require.NotEmpty(t, name, "OID %d should have a String() representation", oid)

		resolved := TypeNameToOid(name)
		assert.Equal(t, oid, resolved,
			"round-trip failed: OID %d → String() %q → TypeNameToOid() → %d", oid, name, resolved)
	}
}
