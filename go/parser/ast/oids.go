// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
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
package ast

// PostgreSQL Type OIDs
// Ported from postgres/src/include/catalog/pg_type_d.h
// These constants represent the built-in PostgreSQL data types.

// Boolean type
const (
	BOOLOID Oid = 16 // boolean
)

// Character types
const (
	CHAROID    Oid = 18   // "char" (single byte)
	NAMEOID    Oid = 19   // name (63-byte type for storing system identifiers)
	TEXTOID    Oid = 25   // text
	VARCHAROID Oid = 1043 // varchar
)

// Binary types
const (
	BYTEAOID Oid = 17 // bytea
)

// Integer types
const (
	INT2OID Oid = 21 // smallint, int2
	INT4OID Oid = 23 // integer, int4
	INT8OID Oid = 20 // bigint, int8
)

// Floating point types
const (
	FLOAT4OID Oid = 700 // real, float4
	FLOAT8OID Oid = 701 // double precision, float8
)

// Numeric types
const (
	NUMERICOID Oid = 1700 // numeric, decimal
)

// Date/time types
const (
	DATEOID        Oid = 1082 // date
	TIMEOID        Oid = 1083 // time without time zone
	TIMESTAMPOID   Oid = 1114 // timestamp without time zone
	TIMESTAMPTZOID Oid = 1184 // timestamp with time zone
	TIMETZOID      Oid = 1266 // time with time zone
)

// System types
const (
	OIDOID     Oid = 26   // oid
	XIDOID     Oid = 28   // xid (transaction id)
	CIDOID     Oid = 29   // cid (command id)
	TIDOID     Oid = 27   // tid (tuple id)
	XID8OID    Oid = 5069 // xid8 (full transaction id)
	REGPROCOID Oid = 24   // regproc
)

// Array types
const (
	INT2VECTOROID Oid = 22 // int2vector (array of int2)
	OIDVECTOROID  Oid = 30 // oidvector (array of oid)
)

// JSON types
const (
	JSONOID  Oid = 114  // json
	JSONBOID Oid = 3802 // jsonb
)

// XML types
const (
	XMLOID Oid = 142 // xml
)

// Geometric types
const (
	POINTOID   Oid = 600 // point
	LSEGOID    Oid = 601 // lseg (line segment)
	PATHOID    Oid = 602 // path
	BOXOID     Oid = 603 // box
	POLYGONOID Oid = 604 // polygon
	LINEOID    Oid = 628 // line
	CIRCLEOID  Oid = 718 // circle
)

// Network types
const (
	INETOID     Oid = 869 // inet
	CIDROID     Oid = 650 // cidr
	MACADDROID  Oid = 829 // macaddr
	MACADDR8OID Oid = 774 // macaddr8
)

// Bit string types
const (
	BITOID    Oid = 1560 // bit
	VARBITOID Oid = 1562 // bit varying
)

// UUID type
const (
	UUIDOID Oid = 2950 // uuid
)

// Range types
const (
	DATERANGEOID Oid = 3912 // daterange
	TSRANGEOID   Oid = 3908 // tsrange
	TSTZRANGEOID Oid = 3910 // tstzrange
	INT4RANGEOID Oid = 3904 // int4range
	INT8RANGEOID Oid = 3926 // int8range
	NUMRANGEOID  Oid = 3906 // numrange
)

// Money type
const (
	MONEYOID Oid = 790      // money
	CASHOID  Oid = MONEYOID // alias for money
)

// PostgreSQL LSN type
const (
	PG_LSNOID Oid = 3220      // pg_lsn
	LSNOID    Oid = PG_LSNOID // alias for pg_lsn
)

// PostgreSQL internal types
const (
	PG_NODE_TREEOID    Oid = 194  // pg_node_tree
	PG_NDISTINCTOID    Oid = 3361 // pg_ndistinct
	PG_DEPENDENCIESOID Oid = 3402 // pg_dependencies
	PG_MCV_LISTOID     Oid = 5017 // pg_mcv_list
	PG_DDL_COMMANDOID  Oid = 32   // pg_ddl_command
)

// Array type OIDs (arrays of the basic types)
const (
	BOOLARRAYOID        Oid = 1000 // boolean[]
	BYTEAARRAYOID       Oid = 1001 // bytea[]
	CHARARRAYOID        Oid = 1002 // "char"[]
	NAMEARRAYOID        Oid = 1003 // name[]
	INT2ARRAYOID        Oid = 1005 // smallint[]
	INT4ARRAYOID        Oid = 1007 // integer[]
	INT8ARRAYOID        Oid = 1016 // bigint[]
	FLOAT4ARRAYOID      Oid = 1021 // real[]
	FLOAT8ARRAYOID      Oid = 1022 // double precision[]
	TEXTARRAYOID        Oid = 1009 // text[]
	VARCHARARRAYOID     Oid = 1015 // varchar[]
	DATEARRAYOID        Oid = 1182 // date[]
	TIMEARRAYOID        Oid = 1183 // time[]
	TIMESTAMPARRAYOID   Oid = 1115 // timestamp[]
	TIMESTAMPTZARRAYOID Oid = 1185 // timestamptz[]
	JSONARRAYOID        Oid = 199  // json[]
	JSONBARRAYOID       Oid = 3807 // jsonb[]
)

// InvalidOid represents an invalid object identifier.
const InvalidOid = Oid(0)
