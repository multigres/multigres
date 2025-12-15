// Copyright 2025 Supabase, Inc.
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

package protocol

// PostgreSQL type OID to name mapping.
// These are the built-in PostgreSQL type OIDs from pg_type.
// Type names are uppercase as returned by PostgreSQL's format_type function.

// oidToTypeName maps PostgreSQL type OIDs to their canonical type names.
// This is used to populate human-readable type names in query results.
var oidToTypeName = map[uint32]string{
	// Boolean type
	16: "BOOL",

	// Binary types
	17: "BYTEA",

	// Character types
	18: "CHAR",
	19: "NAME",
	25: "TEXT",

	// Integer types
	20: "INT8",
	21: "INT2",
	22: "INT2VECTOR",
	23: "INT4",

	// System types
	24: "REGPROC",
	26: "OID",
	27: "TID",
	28: "XID",
	29: "CID",
	30: "OIDVECTOR",

	// JSON types
	114:  "JSON",
	3802: "JSONB",

	// XML type
	142: "XML",

	// Geometric types
	600: "POINT",
	601: "LSEG",
	602: "PATH",
	603: "BOX",
	604: "POLYGON",
	628: "LINE",
	650: "CIDR",
	718: "CIRCLE",

	// Floating point types
	700: "FLOAT4",
	701: "FLOAT8",

	// Money type
	790: "MONEY",

	// Network types
	774: "MACADDR8",
	829: "MACADDR",
	869: "INET",

	// Date/time types
	1082: "DATE",
	1083: "TIME",
	1114: "TIMESTAMP",
	1184: "TIMESTAMPTZ",
	1266: "TIMETZ",

	// Interval type
	1186: "INTERVAL",

	// Bit string types
	1560: "BIT",
	1562: "VARBIT",

	// Numeric types
	1700: "NUMERIC",

	// Character varying type
	1043: "VARCHAR",
	1042: "BPCHAR",

	// UUID type
	2950: "UUID",

	// Range types
	3904: "INT4RANGE",
	3906: "NUMRANGE",
	3908: "TSRANGE",
	3910: "TSTZRANGE",
	3912: "DATERANGE",
	3926: "INT8RANGE",

	// Full transaction ID
	5069: "XID8",

	// PostgreSQL LSN type
	3220: "PG_LSN",

	// PostgreSQL internal types
	32:   "PG_DDL_COMMAND",
	194:  "PG_NODE_TREE",
	3361: "PG_NDISTINCT",
	3402: "PG_DEPENDENCIES",
	5017: "PG_MCV_LIST",

	// Array types - prefixed with underscore as per PostgreSQL convention
	1000: "_BOOL",
	1001: "_BYTEA",
	1002: "_CHAR",
	1003: "_NAME",
	1005: "_INT2",
	1007: "_INT4",
	1009: "_TEXT",
	1015: "_VARCHAR",
	1016: "_INT8",
	1021: "_FLOAT4",
	1022: "_FLOAT8",
	1182: "_DATE",
	1183: "_TIME",
	1115: "_TIMESTAMP",
	1185: "_TIMESTAMPTZ",
	199:  "_JSON",
	3807: "_JSONB",
}

// TypeNameFromOID returns the canonical PostgreSQL type name for a given OID.
// If the OID is not recognized, it returns an empty string.
func TypeNameFromOID(oid uint32) string {
	return oidToTypeName[oid]
}
