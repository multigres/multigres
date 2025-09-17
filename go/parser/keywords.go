// Copyright 2025 Supabase, Inc.
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

/*
 * PostgreSQL Parser Lexer - Keyword Recognition
 *
 * This file implements keyword recognition and categorization for the lexer,
 * consolidating the functionality previously split between lexer and keywords packages.
 * Ported from postgres/src/common/keywords.c and postgres/src/include/parser/kwlist.h
 */

package parser

import (
	"sort"
)

// KeywordCategory represents the different categories of SQL keywords.
// Ported from postgres/src/include/parser/kwlist.h
type KeywordCategory int

const (
	// UnreservedKeyword - can be used as column name, function name, etc.
	// Ported from postgres/src/include/parser/kwlist.h (UNRESERVED_KEYWORD)
	UnreservedKeyword KeywordCategory = iota

	// ColNameKeyword - can be used as column name but not function name
	// Ported from postgres/src/include/parser/kwlist.h (COL_NAME_KEYWORD)
	ColNameKeyword

	// TypeFuncNameKeyword - can be used as function name or type name
	// Ported from postgres/src/include/parser/kwlist.h (TYPE_FUNC_NAME_KEYWORD)
	TypeFuncNameKeyword

	// ReservedKeyword - fully reserved, cannot be used as identifier
	// Ported from postgres/src/include/parser/kwlist.h (RESERVED_KEYWORD)
	ReservedKeyword
)

// KeywordInfo represents information about a SQL keyword.
// Ported from postgres/src/include/parser/kwlist.h structure
type KeywordInfo struct {
	Name         string          // Keyword name (lowercase)
	TokenType    int             // Token type from parser constants
	Category     KeywordCategory // Keyword category
	CanBareLabel bool            // Can be used as a bare label
}

// PostgreSQL keyword list ported from postgres/src/include/parser/kwlist.h
// Complete list of all 491 PostgreSQL keywords with proper token constants and categories
var Keywords = []KeywordInfo{
	{"abort", ABORT_P, UnreservedKeyword, true},
	{"absent", ABSENT, UnreservedKeyword, true},
	{"absolute", ABSOLUTE_P, UnreservedKeyword, true},
	{"access", ACCESS, UnreservedKeyword, true},
	{"action", ACTION, UnreservedKeyword, true},
	{"add", ADD_P, UnreservedKeyword, true},
	{"admin", ADMIN, UnreservedKeyword, true},
	{"after", AFTER, UnreservedKeyword, true},
	{"aggregate", AGGREGATE, UnreservedKeyword, true},
	{"all", ALL, ReservedKeyword, true},
	{"also", ALSO, UnreservedKeyword, true},
	{"alter", ALTER, UnreservedKeyword, true},
	{"always", ALWAYS, UnreservedKeyword, true},
	{"analyse", ANALYSE, ReservedKeyword, true},
	{"analyze", ANALYZE, ReservedKeyword, true},
	{"and", AND, ReservedKeyword, true},
	{"any", ANY, ReservedKeyword, true},
	{"array", ARRAY, ReservedKeyword, false},
	{"as", AS, ReservedKeyword, false},
	{"asc", ASC, ReservedKeyword, true},
	{"asensitive", ASENSITIVE, UnreservedKeyword, true},
	{"assertion", ASSERTION, UnreservedKeyword, true},
	{"assignment", ASSIGNMENT, UnreservedKeyword, true},
	{"asymmetric", ASYMMETRIC, ReservedKeyword, true},
	{"at", AT, UnreservedKeyword, true},
	{"atomic", ATOMIC, UnreservedKeyword, true},
	{"attach", ATTACH, UnreservedKeyword, true},
	{"attribute", ATTRIBUTE, UnreservedKeyword, true},
	{"authorization", AUTHORIZATION, TypeFuncNameKeyword, true},
	{"backward", BACKWARD, UnreservedKeyword, true},
	{"before", BEFORE, UnreservedKeyword, true},
	{"begin", BEGIN_P, UnreservedKeyword, true},
	{"between", BETWEEN, ColNameKeyword, true},
	{"bigint", BIGINT, ColNameKeyword, true},
	{"binary", BINARY, TypeFuncNameKeyword, true},
	{"bit", BIT, ColNameKeyword, true},
	{"boolean", BOOLEAN_P, ColNameKeyword, true},
	{"both", BOTH, ReservedKeyword, true},
	{"breadth", BREADTH, UnreservedKeyword, true},
	{"by", BY, UnreservedKeyword, true},
	{"cache", CACHE, UnreservedKeyword, true},
	{"call", CALL, UnreservedKeyword, true},
	{"called", CALLED, UnreservedKeyword, true},
	{"cascade", CASCADE, UnreservedKeyword, true},
	{"cascaded", CASCADED, UnreservedKeyword, true},
	{"case", CASE, ReservedKeyword, true},
	{"cast", CAST, ReservedKeyword, true},
	{"catalog", CATALOG_P, UnreservedKeyword, true},
	{"chain", CHAIN, UnreservedKeyword, true},
	{"char", CHAR_P, ColNameKeyword, false},
	{"character", CHARACTER, ColNameKeyword, false},
	{"characteristics", CHARACTERISTICS, UnreservedKeyword, true},
	{"check", CHECK, ReservedKeyword, true},
	{"checkpoint", CHECKPOINT, UnreservedKeyword, true},
	{"class", CLASS, UnreservedKeyword, true},
	{"close", CLOSE, UnreservedKeyword, true},
	{"cluster", CLUSTER, UnreservedKeyword, true},
	{"coalesce", COALESCE, ColNameKeyword, true},
	{"collate", COLLATE, ReservedKeyword, true},
	{"collation", COLLATION, TypeFuncNameKeyword, true},
	{"column", COLUMN, ReservedKeyword, true},
	{"columns", COLUMNS, UnreservedKeyword, true},
	{"comment", COMMENT, UnreservedKeyword, true},
	{"comments", COMMENTS, UnreservedKeyword, true},
	{"commit", COMMIT, UnreservedKeyword, true},
	{"committed", COMMITTED, UnreservedKeyword, true},
	{"compression", COMPRESSION, UnreservedKeyword, true},
	{"concurrently", CONCURRENTLY, TypeFuncNameKeyword, true},
	{"conditional", CONDITIONAL, UnreservedKeyword, true},
	{"configuration", CONFIGURATION, UnreservedKeyword, true},
	{"conflict", CONFLICT, UnreservedKeyword, true},
	{"connection", CONNECTION, UnreservedKeyword, true},
	{"constraint", CONSTRAINT, ReservedKeyword, true},
	{"constraints", CONSTRAINTS, UnreservedKeyword, true},
	{"content", CONTENT_P, UnreservedKeyword, true},
	{"continue", CONTINUE_P, UnreservedKeyword, true},
	{"conversion", CONVERSION_P, UnreservedKeyword, true},
	{"copy", COPY, UnreservedKeyword, true},
	{"cost", COST, UnreservedKeyword, true},
	{"create", CREATE, ReservedKeyword, false},
	{"cross", CROSS, TypeFuncNameKeyword, true},
	{"csv", CSV, UnreservedKeyword, true},
	{"cube", CUBE, UnreservedKeyword, true},
	{"current", CURRENT_P, UnreservedKeyword, true},
	{"current_catalog", CURRENT_CATALOG, ReservedKeyword, true},
	{"current_date", CURRENT_DATE, ReservedKeyword, true},
	{"current_role", CURRENT_ROLE, ReservedKeyword, true},
	{"current_schema", CURRENT_SCHEMA, TypeFuncNameKeyword, true},
	{"current_time", CURRENT_TIME, ReservedKeyword, true},
	{"current_timestamp", CURRENT_TIMESTAMP, ReservedKeyword, true},
	{"current_user", CURRENT_USER, ReservedKeyword, true},
	{"cursor", CURSOR, UnreservedKeyword, true},
	{"cycle", CYCLE, UnreservedKeyword, true},
	{"data", DATA_P, UnreservedKeyword, true},
	{"database", DATABASE, UnreservedKeyword, true},
	{"day", DAY_P, UnreservedKeyword, false},
	{"deallocate", DEALLOCATE, UnreservedKeyword, true},
	{"dec", DEC, ColNameKeyword, true},
	{"decimal", DECIMAL_P, ColNameKeyword, true},
	{"declare", DECLARE, UnreservedKeyword, true},
	{"default", DEFAULT, ReservedKeyword, true},
	{"defaults", DEFAULTS, UnreservedKeyword, true},
	{"deferrable", DEFERRABLE, ReservedKeyword, true},
	{"deferred", DEFERRED, UnreservedKeyword, true},
	{"definer", DEFINER, UnreservedKeyword, true},
	{"delete", DELETE_P, UnreservedKeyword, true},
	{"delimiter", DELIMITER, UnreservedKeyword, true},
	{"delimiters", DELIMITERS, UnreservedKeyword, true},
	{"depends", DEPENDS, UnreservedKeyword, true},
	{"depth", DEPTH, UnreservedKeyword, true},
	{"desc", DESC, ReservedKeyword, true},
	{"detach", DETACH, UnreservedKeyword, true},
	{"dictionary", DICTIONARY, UnreservedKeyword, true},
	{"disable", DISABLE_P, UnreservedKeyword, true},
	{"discard", DISCARD, UnreservedKeyword, true},
	{"distinct", DISTINCT, ReservedKeyword, true},
	{"do", DO, ReservedKeyword, true},
	{"document", DOCUMENT_P, UnreservedKeyword, true},
	{"domain", DOMAIN_P, UnreservedKeyword, true},
	{"double", DOUBLE_P, UnreservedKeyword, true},
	{"drop", DROP, UnreservedKeyword, true},
	{"each", EACH, UnreservedKeyword, true},
	{"else", ELSE, ReservedKeyword, true},
	{"empty", EMPTY_P, UnreservedKeyword, true},
	{"enable", ENABLE_P, UnreservedKeyword, true},
	{"encoding", ENCODING, UnreservedKeyword, true},
	{"encrypted", ENCRYPTED, UnreservedKeyword, true},
	{"end", END_P, ReservedKeyword, true},
	{"enum", ENUM_P, UnreservedKeyword, true},
	{"error", ERROR_P, UnreservedKeyword, true},
	{"escape", ESCAPE, UnreservedKeyword, true},
	{"event", EVENT, UnreservedKeyword, true},
	{"except", EXCEPT, ReservedKeyword, false},
	{"exclude", EXCLUDE, UnreservedKeyword, true},
	{"excluding", EXCLUDING, UnreservedKeyword, true},
	{"exclusive", EXCLUSIVE, UnreservedKeyword, true},
	{"execute", EXECUTE, UnreservedKeyword, true},
	{"exists", EXISTS, ColNameKeyword, true},
	{"explain", EXPLAIN, UnreservedKeyword, true},
	{"expression", EXPRESSION, UnreservedKeyword, true},
	{"extension", EXTENSION, UnreservedKeyword, true},
	{"external", EXTERNAL, UnreservedKeyword, true},
	{"extract", EXTRACT, ColNameKeyword, true},
	{"false", FALSE_P, ReservedKeyword, true},
	{"family", FAMILY, UnreservedKeyword, true},
	{"fetch", FETCH, ReservedKeyword, false},
	{"filter", FILTER, UnreservedKeyword, false},
	{"finalize", FINALIZE, UnreservedKeyword, true},
	{"first", FIRST_P, UnreservedKeyword, true},
	{"float", FLOAT_P, ColNameKeyword, true},
	{"following", FOLLOWING, UnreservedKeyword, true},
	{"for", FOR, ReservedKeyword, false},
	{"force", FORCE, UnreservedKeyword, true},
	{"foreign", FOREIGN, ReservedKeyword, true},
	{"format", FORMAT, UnreservedKeyword, true},
	{"forward", FORWARD, UnreservedKeyword, true},
	{"freeze", FREEZE, TypeFuncNameKeyword, true},
	{"from", FROM, ReservedKeyword, false},
	{"full", FULL, TypeFuncNameKeyword, true},
	{"function", FUNCTION, UnreservedKeyword, true},
	{"functions", FUNCTIONS, UnreservedKeyword, true},
	{"generated", GENERATED, UnreservedKeyword, true},
	{"global", GLOBAL, UnreservedKeyword, true},
	{"grant", GRANT, ReservedKeyword, false},
	{"granted", GRANTED, UnreservedKeyword, true},
	{"greatest", GREATEST, ColNameKeyword, true},
	{"group", GROUP_P, ReservedKeyword, false},
	{"grouping", GROUPING, ColNameKeyword, true},
	{"groups", GROUPS, UnreservedKeyword, true},
	{"handler", HANDLER, UnreservedKeyword, true},
	{"having", HAVING, ReservedKeyword, false},
	{"header", HEADER_P, UnreservedKeyword, true},
	{"hold", HOLD, UnreservedKeyword, true},
	{"hour", HOUR_P, UnreservedKeyword, false},
	{"identity", IDENTITY_P, UnreservedKeyword, true},
	{"if", IF_P, UnreservedKeyword, true},
	{"ilike", ILIKE, TypeFuncNameKeyword, true},
	{"immediate", IMMEDIATE, UnreservedKeyword, true},
	{"immutable", IMMUTABLE, UnreservedKeyword, true},
	{"implicit", IMPLICIT_P, UnreservedKeyword, true},
	{"import", IMPORT_P, UnreservedKeyword, true},
	{"in", IN_P, ReservedKeyword, true},
	{"include", INCLUDE, UnreservedKeyword, true},
	{"including", INCLUDING, UnreservedKeyword, true},
	{"increment", INCREMENT, UnreservedKeyword, true},
	{"indent", INDENT, UnreservedKeyword, true},
	{"index", INDEX, UnreservedKeyword, true},
	{"indexes", INDEXES, UnreservedKeyword, true},
	{"inherit", INHERIT, UnreservedKeyword, true},
	{"inherits", INHERITS, UnreservedKeyword, true},
	{"initially", INITIALLY, ReservedKeyword, true},
	{"inline", INLINE_P, UnreservedKeyword, true},
	{"inner", INNER_P, TypeFuncNameKeyword, true},
	{"inout", INOUT, ColNameKeyword, true},
	{"input", INPUT_P, UnreservedKeyword, true},
	{"insensitive", INSENSITIVE, UnreservedKeyword, true},
	{"insert", INSERT, UnreservedKeyword, true},
	{"instead", INSTEAD, UnreservedKeyword, true},
	{"int", INT_P, ColNameKeyword, true},
	{"integer", INTEGER, ColNameKeyword, true},
	{"intersect", INTERSECT, ReservedKeyword, false},
	{"interval", INTERVAL, ColNameKeyword, true},
	{"into", INTO, ReservedKeyword, false},
	{"invoker", INVOKER, UnreservedKeyword, true},
	{"is", IS, TypeFuncNameKeyword, true},
	{"isnull", ISNULL, TypeFuncNameKeyword, false},
	{"isolation", ISOLATION, UnreservedKeyword, true},
	{"join", JOIN, TypeFuncNameKeyword, true},
	{"json", JSON, ColNameKeyword, true},
	{"json_array", JSON_ARRAY, ColNameKeyword, true},
	{"json_arrayagg", JSON_ARRAYAGG, ColNameKeyword, true},
	{"json_exists", JSON_EXISTS, ColNameKeyword, true},
	{"json_object", JSON_OBJECT, ColNameKeyword, true},
	{"json_objectagg", JSON_OBJECTAGG, ColNameKeyword, true},
	{"json_query", JSON_QUERY, ColNameKeyword, true},
	{"json_scalar", JSON_SCALAR, ColNameKeyword, true},
	{"json_serialize", JSON_SERIALIZE, ColNameKeyword, true},
	{"json_table", JSON_TABLE, ColNameKeyword, true},
	{"json_value", JSON_VALUE, ColNameKeyword, true},
	{"keep", KEEP, UnreservedKeyword, true},
	{"key", KEY, UnreservedKeyword, true},
	{"keys", KEYS, UnreservedKeyword, true},
	{"label", LABEL, UnreservedKeyword, true},
	{"language", LANGUAGE, UnreservedKeyword, true},
	{"large", LARGE_P, UnreservedKeyword, true},
	{"last", LAST_P, UnreservedKeyword, true},
	{"lateral", LATERAL_P, ReservedKeyword, true},
	{"leading", LEADING, ReservedKeyword, true},
	{"leakproof", LEAKPROOF, UnreservedKeyword, true},
	{"least", LEAST, ColNameKeyword, true},
	{"left", LEFT, TypeFuncNameKeyword, true},
	{"level", LEVEL, UnreservedKeyword, true},
	{"like", LIKE, TypeFuncNameKeyword, true},
	{"limit", LIMIT, ReservedKeyword, false},
	{"listen", LISTEN, UnreservedKeyword, true},
	{"load", LOAD, UnreservedKeyword, true},
	{"local", LOCAL, UnreservedKeyword, true},
	{"localtime", LOCALTIME, ReservedKeyword, true},
	{"localtimestamp", LOCALTIMESTAMP, ReservedKeyword, true},
	{"location", LOCATION, UnreservedKeyword, true},
	{"lock", LOCK_P, UnreservedKeyword, true},
	{"locked", LOCKED, UnreservedKeyword, true},
	{"logged", LOGGED, UnreservedKeyword, true},
	{"mapping", MAPPING, UnreservedKeyword, true},
	{"match", MATCH, UnreservedKeyword, true},
	{"matched", MATCHED, UnreservedKeyword, true},
	{"materialized", MATERIALIZED, UnreservedKeyword, true},
	{"maxvalue", MAXVALUE, UnreservedKeyword, true},
	{"merge", MERGE, UnreservedKeyword, true},
	{"merge_action", MERGE_ACTION, ColNameKeyword, true},
	{"method", METHOD, UnreservedKeyword, true},
	{"minute", MINUTE_P, UnreservedKeyword, false},
	{"minvalue", MINVALUE, UnreservedKeyword, true},
	{"mode", MODE, UnreservedKeyword, true},
	{"month", MONTH_P, UnreservedKeyword, false},
	{"move", MOVE, UnreservedKeyword, true},
	{"name", NAME_P, UnreservedKeyword, true},
	{"names", NAMES, UnreservedKeyword, true},
	{"national", NATIONAL, ColNameKeyword, true},
	{"natural", NATURAL, TypeFuncNameKeyword, true},
	{"nchar", NCHAR, ColNameKeyword, true},
	{"nested", NESTED, UnreservedKeyword, true},
	{"new", NEW, UnreservedKeyword, true},
	{"next", NEXT, UnreservedKeyword, true},
	{"nfc", NFC, UnreservedKeyword, true},
	{"nfd", NFD, UnreservedKeyword, true},
	{"nfkc", NFKC, UnreservedKeyword, true},
	{"nfkd", NFKD, UnreservedKeyword, true},
	{"no", NO, UnreservedKeyword, true},
	{"none", NONE, ColNameKeyword, true},
	{"normalize", NORMALIZE, ColNameKeyword, true},
	{"normalized", NORMALIZED, UnreservedKeyword, true},
	{"not", NOT, ReservedKeyword, true},
	{"nothing", NOTHING, UnreservedKeyword, true},
	{"notify", NOTIFY, UnreservedKeyword, true},
	{"notnull", NOTNULL, TypeFuncNameKeyword, false},
	{"nowait", NOWAIT, UnreservedKeyword, true},
	{"null", NULL_P, ReservedKeyword, true},
	{"nullif", NULLIF, ColNameKeyword, true},
	{"nulls", NULLS_P, UnreservedKeyword, true},
	{"numeric", NUMERIC, ColNameKeyword, true},
	{"object", OBJECT_P, UnreservedKeyword, true},
	{"of", OF, UnreservedKeyword, true},
	{"off", OFF, UnreservedKeyword, true},
	{"offset", OFFSET, ReservedKeyword, false},
	{"oids", OIDS, UnreservedKeyword, true},
	{"old", OLD, UnreservedKeyword, true},
	{"omit", OMIT, UnreservedKeyword, true},
	{"on", ON, ReservedKeyword, false},
	{"only", ONLY, ReservedKeyword, true},
	{"operator", OPERATOR, UnreservedKeyword, true},
	{"option", OPTION, UnreservedKeyword, true},
	{"options", OPTIONS, UnreservedKeyword, true},
	{"or", OR, ReservedKeyword, true},
	{"order", ORDER, ReservedKeyword, false},
	{"ordinality", ORDINALITY, UnreservedKeyword, true},
	{"others", OTHERS, UnreservedKeyword, true},
	{"out", OUT_P, ColNameKeyword, true},
	{"outer", OUTER_P, TypeFuncNameKeyword, true},
	{"over", OVER, UnreservedKeyword, false},
	{"overlaps", OVERLAPS, TypeFuncNameKeyword, false},
	{"overlay", OVERLAY, ColNameKeyword, true},
	{"overriding", OVERRIDING, UnreservedKeyword, true},
	{"owned", OWNED, UnreservedKeyword, true},
	{"owner", OWNER, UnreservedKeyword, true},
	{"parallel", PARALLEL, UnreservedKeyword, true},
	{"parameter", PARAMETER, UnreservedKeyword, true},
	{"parser", PARSER, UnreservedKeyword, true},
	{"partial", PARTIAL, UnreservedKeyword, true},
	{"partition", PARTITION, UnreservedKeyword, true},
	{"passing", PASSING, UnreservedKeyword, true},
	{"password", PASSWORD, UnreservedKeyword, true},
	{"path", PATH, UnreservedKeyword, true},
	{"placing", PLACING, ReservedKeyword, true},
	{"plan", PLAN, UnreservedKeyword, true},
	{"plans", PLANS, UnreservedKeyword, true},
	{"policy", POLICY, UnreservedKeyword, true},
	{"position", POSITION, ColNameKeyword, true},
	{"preceding", PRECEDING, UnreservedKeyword, true},
	{"precision", PRECISION, ColNameKeyword, false},
	{"prepare", PREPARE, UnreservedKeyword, true},
	{"prepared", PREPARED, UnreservedKeyword, true},
	{"preserve", PRESERVE, UnreservedKeyword, true},
	{"primary", PRIMARY, ReservedKeyword, true},
	{"prior", PRIOR, UnreservedKeyword, true},
	{"privileges", PRIVILEGES, UnreservedKeyword, true},
	{"procedural", PROCEDURAL, UnreservedKeyword, true},
	{"procedure", PROCEDURE, UnreservedKeyword, true},
	{"procedures", PROCEDURES, UnreservedKeyword, true},
	{"program", PROGRAM, UnreservedKeyword, true},
	{"publication", PUBLICATION, UnreservedKeyword, true},
	{"quote", QUOTE, UnreservedKeyword, true},
	{"quotes", QUOTES, UnreservedKeyword, true},
	{"range", RANGE, UnreservedKeyword, true},
	{"read", READ, UnreservedKeyword, true},
	{"real", REAL, ColNameKeyword, true},
	{"reassign", REASSIGN, UnreservedKeyword, true},
	{"recheck", RECHECK, UnreservedKeyword, true},
	{"recursive", RECURSIVE, UnreservedKeyword, true},
	{"ref", REF_P, UnreservedKeyword, true},
	{"references", REFERENCES, ReservedKeyword, true},
	{"referencing", REFERENCING, UnreservedKeyword, true},
	{"refresh", REFRESH, UnreservedKeyword, true},
	{"reindex", REINDEX, UnreservedKeyword, true},
	{"relative", RELATIVE_P, UnreservedKeyword, true},
	{"release", RELEASE, UnreservedKeyword, true},
	{"rename", RENAME, UnreservedKeyword, true},
	{"repeatable", REPEATABLE, UnreservedKeyword, true},
	{"replace", REPLACE, UnreservedKeyword, true},
	{"replica", REPLICA, UnreservedKeyword, true},
	{"reset", RESET, UnreservedKeyword, true},
	{"restart", RESTART, UnreservedKeyword, true},
	{"restrict", RESTRICT, UnreservedKeyword, true},
	{"return", RETURN, UnreservedKeyword, true},
	{"returning", RETURNING, ReservedKeyword, false},
	{"returns", RETURNS, UnreservedKeyword, true},
	{"revoke", REVOKE, UnreservedKeyword, true},
	{"right", RIGHT, TypeFuncNameKeyword, true},
	{"role", ROLE, UnreservedKeyword, true},
	{"rollback", ROLLBACK, UnreservedKeyword, true},
	{"rollup", ROLLUP, UnreservedKeyword, true},
	{"routine", ROUTINE, UnreservedKeyword, true},
	{"routines", ROUTINES, UnreservedKeyword, true},
	{"row", ROW, ColNameKeyword, true},
	{"rows", ROWS, UnreservedKeyword, true},
	{"rule", RULE, UnreservedKeyword, true},
	{"savepoint", SAVEPOINT, UnreservedKeyword, true},
	{"scalar", SCALAR, UnreservedKeyword, true},
	{"schema", SCHEMA, UnreservedKeyword, true},
	{"schemas", SCHEMAS, UnreservedKeyword, true},
	{"scroll", SCROLL, UnreservedKeyword, true},
	{"search", SEARCH, UnreservedKeyword, true},
	{"second", SECOND_P, UnreservedKeyword, false},
	{"security", SECURITY, UnreservedKeyword, true},
	{"select", SELECT, ReservedKeyword, true},
	{"sequence", SEQUENCE, UnreservedKeyword, true},
	{"sequences", SEQUENCES, UnreservedKeyword, true},
	{"serializable", SERIALIZABLE, UnreservedKeyword, true},
	{"server", SERVER, UnreservedKeyword, true},
	{"session", SESSION, UnreservedKeyword, true},
	{"session_user", SESSION_USER, ReservedKeyword, true},
	{"set", SET, UnreservedKeyword, true},
	{"setof", SETOF, ColNameKeyword, true},
	{"sets", SETS, UnreservedKeyword, true},
	{"share", SHARE, UnreservedKeyword, true},
	{"show", SHOW, UnreservedKeyword, true},
	{"similar", SIMILAR, TypeFuncNameKeyword, true},
	{"simple", SIMPLE, UnreservedKeyword, true},
	{"skip", SKIP, UnreservedKeyword, true},
	{"smallint", SMALLINT, ColNameKeyword, true},
	{"snapshot", SNAPSHOT, UnreservedKeyword, true},
	{"some", SOME, ReservedKeyword, true},
	{"source", SOURCE, UnreservedKeyword, true},
	{"sql", SQL_P, UnreservedKeyword, true},
	{"stable", STABLE, UnreservedKeyword, true},
	{"standalone", STANDALONE_P, UnreservedKeyword, true},
	{"start", START, UnreservedKeyword, true},
	{"statement", STATEMENT, UnreservedKeyword, true},
	{"statistics", STATISTICS, UnreservedKeyword, true},
	{"stdin", STDIN, UnreservedKeyword, true},
	{"stdout", STDOUT, UnreservedKeyword, true},
	{"storage", STORAGE, UnreservedKeyword, true},
	{"stored", STORED, UnreservedKeyword, true},
	{"strict", STRICT_P, UnreservedKeyword, true},
	{"string", STRING_P, UnreservedKeyword, true},
	{"strip", STRIP_P, UnreservedKeyword, true},
	{"subscription", SUBSCRIPTION, UnreservedKeyword, true},
	{"substring", SUBSTRING, ColNameKeyword, true},
	{"support", SUPPORT, UnreservedKeyword, true},
	{"symmetric", SYMMETRIC, ReservedKeyword, true},
	{"sysid", SYSID, UnreservedKeyword, true},
	{"system", SYSTEM_P, UnreservedKeyword, true},
	{"system_user", SYSTEM_USER, ReservedKeyword, true},
	{"table", TABLE, ReservedKeyword, true},
	{"tables", TABLES, UnreservedKeyword, true},
	{"tablesample", TABLESAMPLE, TypeFuncNameKeyword, true},
	{"tablespace", TABLESPACE, UnreservedKeyword, true},
	{"target", TARGET, UnreservedKeyword, true},
	{"temp", TEMP, UnreservedKeyword, true},
	{"template", TEMPLATE, UnreservedKeyword, true},
	{"temporary", TEMPORARY, UnreservedKeyword, true},
	{"text", TEXT_P, UnreservedKeyword, true},
	{"then", THEN, ReservedKeyword, true},
	{"ties", TIES, UnreservedKeyword, true},
	{"time", TIME, ColNameKeyword, true},
	{"timestamp", TIMESTAMP, ColNameKeyword, true},
	{"to", TO, ReservedKeyword, false},
	{"trailing", TRAILING, ReservedKeyword, true},
	{"transaction", TRANSACTION, UnreservedKeyword, true},
	{"transform", TRANSFORM, UnreservedKeyword, true},
	{"treat", TREAT, ColNameKeyword, true},
	{"trigger", TRIGGER, UnreservedKeyword, true},
	{"trim", TRIM, ColNameKeyword, true},
	{"true", TRUE_P, ReservedKeyword, true},
	{"truncate", TRUNCATE, UnreservedKeyword, true},
	{"trusted", TRUSTED, UnreservedKeyword, true},
	{"type", TYPE_P, UnreservedKeyword, true},
	{"types", TYPES_P, UnreservedKeyword, true},
	{"uescape", UESCAPE, UnreservedKeyword, true},
	{"unbounded", UNBOUNDED, UnreservedKeyword, true},
	{"uncommitted", UNCOMMITTED, UnreservedKeyword, true},
	{"unconditional", UNCONDITIONAL, UnreservedKeyword, true},
	{"unencrypted", UNENCRYPTED, UnreservedKeyword, true},
	{"union", UNION, ReservedKeyword, false},
	{"unique", UNIQUE, ReservedKeyword, true},
	{"unknown", UNKNOWN, UnreservedKeyword, true},
	{"unlisten", UNLISTEN, UnreservedKeyword, true},
	{"unlogged", UNLOGGED, UnreservedKeyword, true},
	{"until", UNTIL, UnreservedKeyword, true},
	{"update", UPDATE, UnreservedKeyword, true},
	{"user", USER, ReservedKeyword, true},
	{"using", USING, ReservedKeyword, true},
	{"vacuum", VACUUM, UnreservedKeyword, true},
	{"valid", VALID, UnreservedKeyword, true},
	{"validate", VALIDATE, UnreservedKeyword, true},
	{"validator", VALIDATOR, UnreservedKeyword, true},
	{"value", VALUE_P, UnreservedKeyword, true},
	{"values", VALUES, ColNameKeyword, true},
	{"varchar", VARCHAR, ColNameKeyword, true},
	{"variadic", VARIADIC, ReservedKeyword, true},
	{"varying", VARYING, UnreservedKeyword, false},
	{"verbose", VERBOSE, TypeFuncNameKeyword, true},
	{"version", VERSION_P, UnreservedKeyword, true},
	{"view", VIEW, UnreservedKeyword, true},
	{"views", VIEWS, UnreservedKeyword, true},
	{"volatile", VOLATILE, UnreservedKeyword, true},
	{"when", WHEN, ReservedKeyword, true},
	{"where", WHERE, ReservedKeyword, false},
	{"whitespace", WHITESPACE_P, UnreservedKeyword, true},
	{"window", WINDOW, ReservedKeyword, false},
	{"with", WITH, ReservedKeyword, false},
	{"within", WITHIN, UnreservedKeyword, false},
	{"without", WITHOUT, UnreservedKeyword, false},
	{"work", WORK, UnreservedKeyword, true},
	{"wrapper", WRAPPER, UnreservedKeyword, true},
	{"write", WRITE, UnreservedKeyword, true},
	{"xml", XML_P, UnreservedKeyword, true},
	{"xmlattributes", XMLATTRIBUTES, ColNameKeyword, true},
	{"xmlconcat", XMLCONCAT, ColNameKeyword, true},
	{"xmlelement", XMLELEMENT, ColNameKeyword, true},
	{"xmlexists", XMLEXISTS, ColNameKeyword, true},
	{"xmlforest", XMLFOREST, ColNameKeyword, true},
	{"xmlnamespaces", XMLNAMESPACES, ColNameKeyword, true},
	{"xmlparse", XMLPARSE, ColNameKeyword, true},
	{"xmlpi", XMLPI, ColNameKeyword, true},
	{"xmlroot", XMLROOT, ColNameKeyword, true},
	{"xmlserialize", XMLSERIALIZE, ColNameKeyword, true},
	{"xmltable", XMLTABLE, ColNameKeyword, true},
	{"year", YEAR_P, UnreservedKeyword, false},
	{"yes", YES_P, UnreservedKeyword, true},
	{"zone", ZONE, UnreservedKeyword, true},
}

// keywordLookupMap provides fast keyword lookup by name.
// Built from the Keywords slice for O(1) lookups.
var keywordLookupMap map[string]*KeywordInfo

// init initializes the keyword lookup map.
// Ported from postgres/src/common/keywords.c lookup functionality
func init() {
	keywordLookupMap = make(map[string]*KeywordInfo, len(Keywords))
	for i := range Keywords {
		keywordLookupMap[Keywords[i].Name] = &Keywords[i]
	}
}

// LookupKeyword searches for a keyword by name (case-insensitive) with optimizations.
// Returns the keyword info if found, nil otherwise.
// Based on postgres/src/common/kwlookup.c:ScanKeywordLookup lines 37-85
func LookupKeyword(name string) *KeywordInfo {
	// Early termination for very long strings (PostgreSQL pattern)
	// Most keywords are short, so this saves work on long identifiers
	if len(name) > maxKeywordLength {
		return nil
	}

	// PostgreSQL does ASCII-only case conversion for SQL99 compliance
	// normalizeKeywordCase has a fast path for strings without uppercase letters
	lowerName := normalizeKeywordCase(name)
	return keywordLookupMap[lowerName]
}

// maxKeywordLength is the maximum length of any PostgreSQL keyword
// Calculated from the Keywords slice - avoids checking impossibly long strings
const maxKeywordLength = 17 // "current_timestamp" is longest at 17 chars

// normalizeKeywordCase performs PostgreSQL-style ASCII-only case normalization.
// Based on postgres/src/common/kwlookup.c:75-76 (ch >= 'A' && ch <= 'Z' conversion)
func normalizeKeywordCase(s string) string {
	// Fast path: check if string contains any uppercase letters
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			hasUpper = true
			break
		}
	}

	// If no uppercase, return original string (avoid allocation)
	if !hasUpper {
		return s
	}

	// Need to convert case - create new string
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A' // PostgreSQL ASCII-only conversion
		}
		result[i] = ch
	}
	return string(result)
}

// IsKeyword returns true if the given name is a SQL keyword.
// Ported from postgres keyword lookup functionality
func IsKeyword(name string) bool {
	return LookupKeyword(name) != nil
}

// IsReservedKeyword returns true if the given name is a reserved keyword.
// Ported from postgres keyword categorization logic
func IsReservedKeyword(name string) bool {
	if kw := LookupKeyword(name); kw != nil {
		return kw.Category == ReservedKeyword
	}
	return false
}

// GetKeywordNames returns a sorted slice of all keyword names.
// Useful for debugging and testing.
func GetKeywordNames() []string {
	names := make([]string, len(Keywords))
	for i, kw := range Keywords {
		names[i] = kw.Name
	}
	sort.Strings(names)
	return names
}

// GetKeywordsByCategory returns all keywords in a specific category.
func GetKeywordsByCategory(category KeywordCategory) []KeywordInfo {
	var result []KeywordInfo
	for _, kw := range Keywords {
		if kw.Category == category {
			result = append(result, kw)
		}
	}
	return result
}

// String returns the string representation of a KeywordCategory.
func (kc KeywordCategory) String() string {
	switch kc {
	case UnreservedKeyword:
		return "UNRESERVED_KEYWORD"
	case ColNameKeyword:
		return "COL_NAME_KEYWORD"
	case TypeFuncNameKeyword:
		return "TYPE_FUNC_NAME_KEYWORD"
	case ReservedKeyword:
		return "RESERVED_KEYWORD"
	default:
		return "UNKNOWN_KEYWORD_CATEGORY"
	}
}

// getKeywordTokenType returns the appropriate token type for a keyword.
// This preserves the identifier text while marking it for future keyword token assignment
func getKeywordTokenType(keyword *KeywordInfo) TokenType {
	// Return the keyword's specific token type
	return keyword.TokenType
}
