// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2025, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
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
//
/*
 * PostgreSQL Parser Lexer - Keyword Recognition
 *
 * This file maps keyword text to the goyacc token the lexer must emit. The
 * token constants are generated into this package, so this name -> token table
 * lives here. The keyword *classification* (which category each keyword is in)
 * is the ast package's single source of truth (go/common/parser/ast/keywords.go);
 * each entry's Category/CanBareLabel is copied in from there at init, and a test
 * guards the two tables against drifting. Ported from postgres/src/common/keywords.c
 * and postgres/src/include/parser/kwlist.h.
 */

package parser

import (
	"sort"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// KeywordCategory and its values are re-exported from the ast package (the
// single source of keyword classification) so existing parser-package callers
// keep working.
type KeywordCategory = ast.KeywordCategory

const (
	UnreservedKeyword   = ast.UnreservedKeyword
	ColNameKeyword      = ast.ColNameKeyword
	TypeFuncNameKeyword = ast.TypeFuncNameKeyword
	ReservedKeyword     = ast.ReservedKeyword
)

// KeywordInfo represents information about a SQL keyword. The Name/TokenType
// pair is defined in this package (the lexer needs the token); Category and
// CanBareLabel are populated from the ast classification at init.
type KeywordInfo struct {
	Name         string          // Keyword name (lowercase)
	TokenType    int             // Token type from parser constants
	Category     KeywordCategory // Keyword category (filled from ast at init)
	CanBareLabel bool            // Can be used as a bare label (filled from ast at init)
}

// Keywords maps each PostgreSQL keyword to its goyacc token. Category and
// CanBareLabel are filled in by init from the ast classification.
var Keywords = []KeywordInfo{
	{Name: "abort", TokenType: ABORT_P},
	{Name: "absent", TokenType: ABSENT},
	{Name: "absolute", TokenType: ABSOLUTE_P},
	{Name: "access", TokenType: ACCESS},
	{Name: "action", TokenType: ACTION},
	{Name: "add", TokenType: ADD_P},
	{Name: "admin", TokenType: ADMIN},
	{Name: "after", TokenType: AFTER},
	{Name: "aggregate", TokenType: AGGREGATE},
	{Name: "all", TokenType: ALL},
	{Name: "also", TokenType: ALSO},
	{Name: "alter", TokenType: ALTER},
	{Name: "always", TokenType: ALWAYS},
	{Name: "analyse", TokenType: ANALYSE},
	{Name: "analyze", TokenType: ANALYZE},
	{Name: "and", TokenType: AND},
	{Name: "any", TokenType: ANY},
	{Name: "array", TokenType: ARRAY},
	{Name: "as", TokenType: AS},
	{Name: "asc", TokenType: ASC},
	{Name: "asensitive", TokenType: ASENSITIVE},
	{Name: "assertion", TokenType: ASSERTION},
	{Name: "assignment", TokenType: ASSIGNMENT},
	{Name: "asymmetric", TokenType: ASYMMETRIC},
	{Name: "at", TokenType: AT},
	{Name: "atomic", TokenType: ATOMIC},
	{Name: "attach", TokenType: ATTACH},
	{Name: "attribute", TokenType: ATTRIBUTE},
	{Name: "authorization", TokenType: AUTHORIZATION},
	{Name: "backward", TokenType: BACKWARD},
	{Name: "before", TokenType: BEFORE},
	{Name: "begin", TokenType: BEGIN_P},
	{Name: "between", TokenType: BETWEEN},
	{Name: "bigint", TokenType: BIGINT},
	{Name: "binary", TokenType: BINARY},
	{Name: "bit", TokenType: BIT},
	{Name: "boolean", TokenType: BOOLEAN_P},
	{Name: "both", TokenType: BOTH},
	{Name: "breadth", TokenType: BREADTH},
	{Name: "by", TokenType: BY},
	{Name: "cache", TokenType: CACHE},
	{Name: "call", TokenType: CALL},
	{Name: "called", TokenType: CALLED},
	{Name: "cascade", TokenType: CASCADE},
	{Name: "cascaded", TokenType: CASCADED},
	{Name: "case", TokenType: CASE},
	{Name: "cast", TokenType: CAST},
	{Name: "catalog", TokenType: CATALOG_P},
	{Name: "chain", TokenType: CHAIN},
	{Name: "char", TokenType: CHAR_P},
	{Name: "character", TokenType: CHARACTER},
	{Name: "characteristics", TokenType: CHARACTERISTICS},
	{Name: "check", TokenType: CHECK},
	{Name: "checkpoint", TokenType: CHECKPOINT},
	{Name: "class", TokenType: CLASS},
	{Name: "close", TokenType: CLOSE},
	{Name: "cluster", TokenType: CLUSTER},
	{Name: "coalesce", TokenType: COALESCE},
	{Name: "collate", TokenType: COLLATE},
	{Name: "collation", TokenType: COLLATION},
	{Name: "column", TokenType: COLUMN},
	{Name: "columns", TokenType: COLUMNS},
	{Name: "comment", TokenType: COMMENT},
	{Name: "comments", TokenType: COMMENTS},
	{Name: "commit", TokenType: COMMIT},
	{Name: "committed", TokenType: COMMITTED},
	{Name: "compression", TokenType: COMPRESSION},
	{Name: "concurrently", TokenType: CONCURRENTLY},
	{Name: "conditional", TokenType: CONDITIONAL},
	{Name: "configuration", TokenType: CONFIGURATION},
	{Name: "conflict", TokenType: CONFLICT},
	{Name: "connection", TokenType: CONNECTION},
	{Name: "constraint", TokenType: CONSTRAINT},
	{Name: "constraints", TokenType: CONSTRAINTS},
	{Name: "content", TokenType: CONTENT_P},
	{Name: "continue", TokenType: CONTINUE_P},
	{Name: "conversion", TokenType: CONVERSION_P},
	{Name: "copy", TokenType: COPY},
	{Name: "cost", TokenType: COST},
	{Name: "create", TokenType: CREATE},
	{Name: "cross", TokenType: CROSS},
	{Name: "csv", TokenType: CSV},
	{Name: "cube", TokenType: CUBE},
	{Name: "current", TokenType: CURRENT_P},
	{Name: "current_catalog", TokenType: CURRENT_CATALOG},
	{Name: "current_date", TokenType: CURRENT_DATE},
	{Name: "current_role", TokenType: CURRENT_ROLE},
	{Name: "current_schema", TokenType: CURRENT_SCHEMA},
	{Name: "current_time", TokenType: CURRENT_TIME},
	{Name: "current_timestamp", TokenType: CURRENT_TIMESTAMP},
	{Name: "current_user", TokenType: CURRENT_USER},
	{Name: "cursor", TokenType: CURSOR},
	{Name: "cycle", TokenType: CYCLE},
	{Name: "data", TokenType: DATA_P},
	{Name: "database", TokenType: DATABASE},
	{Name: "day", TokenType: DAY_P},
	{Name: "deallocate", TokenType: DEALLOCATE},
	{Name: "dec", TokenType: DEC},
	{Name: "decimal", TokenType: DECIMAL_P},
	{Name: "declare", TokenType: DECLARE},
	{Name: "default", TokenType: DEFAULT},
	{Name: "defaults", TokenType: DEFAULTS},
	{Name: "deferrable", TokenType: DEFERRABLE},
	{Name: "deferred", TokenType: DEFERRED},
	{Name: "definer", TokenType: DEFINER},
	{Name: "delete", TokenType: DELETE_P},
	{Name: "delimiter", TokenType: DELIMITER},
	{Name: "delimiters", TokenType: DELIMITERS},
	{Name: "depends", TokenType: DEPENDS},
	{Name: "depth", TokenType: DEPTH},
	{Name: "desc", TokenType: DESC},
	{Name: "detach", TokenType: DETACH},
	{Name: "dictionary", TokenType: DICTIONARY},
	{Name: "disable", TokenType: DISABLE_P},
	{Name: "discard", TokenType: DISCARD},
	{Name: "distinct", TokenType: DISTINCT},
	{Name: "do", TokenType: DO},
	{Name: "document", TokenType: DOCUMENT_P},
	{Name: "domain", TokenType: DOMAIN_P},
	{Name: "double", TokenType: DOUBLE_P},
	{Name: "drop", TokenType: DROP},
	{Name: "each", TokenType: EACH},
	{Name: "else", TokenType: ELSE},
	{Name: "empty", TokenType: EMPTY_P},
	{Name: "enable", TokenType: ENABLE_P},
	{Name: "encoding", TokenType: ENCODING},
	{Name: "encrypted", TokenType: ENCRYPTED},
	{Name: "end", TokenType: END_P},
	{Name: "enum", TokenType: ENUM_P},
	{Name: "error", TokenType: ERROR_P},
	{Name: "escape", TokenType: ESCAPE},
	{Name: "event", TokenType: EVENT},
	{Name: "except", TokenType: EXCEPT},
	{Name: "exclude", TokenType: EXCLUDE},
	{Name: "excluding", TokenType: EXCLUDING},
	{Name: "exclusive", TokenType: EXCLUSIVE},
	{Name: "execute", TokenType: EXECUTE},
	{Name: "exists", TokenType: EXISTS},
	{Name: "explain", TokenType: EXPLAIN},
	{Name: "expression", TokenType: EXPRESSION},
	{Name: "extension", TokenType: EXTENSION},
	{Name: "external", TokenType: EXTERNAL},
	{Name: "extract", TokenType: EXTRACT},
	{Name: "false", TokenType: FALSE_P},
	{Name: "family", TokenType: FAMILY},
	{Name: "fetch", TokenType: FETCH},
	{Name: "filter", TokenType: FILTER},
	{Name: "finalize", TokenType: FINALIZE},
	{Name: "first", TokenType: FIRST_P},
	{Name: "float", TokenType: FLOAT_P},
	{Name: "following", TokenType: FOLLOWING},
	{Name: "for", TokenType: FOR},
	{Name: "force", TokenType: FORCE},
	{Name: "foreign", TokenType: FOREIGN},
	{Name: "format", TokenType: FORMAT},
	{Name: "forward", TokenType: FORWARD},
	{Name: "freeze", TokenType: FREEZE},
	{Name: "from", TokenType: FROM},
	{Name: "full", TokenType: FULL},
	{Name: "function", TokenType: FUNCTION},
	{Name: "functions", TokenType: FUNCTIONS},
	{Name: "generated", TokenType: GENERATED},
	{Name: "global", TokenType: GLOBAL},
	{Name: "grant", TokenType: GRANT},
	{Name: "granted", TokenType: GRANTED},
	{Name: "greatest", TokenType: GREATEST},
	{Name: "group", TokenType: GROUP_P},
	{Name: "grouping", TokenType: GROUPING},
	{Name: "groups", TokenType: GROUPS},
	{Name: "handler", TokenType: HANDLER},
	{Name: "having", TokenType: HAVING},
	{Name: "header", TokenType: HEADER_P},
	{Name: "hold", TokenType: HOLD},
	{Name: "hour", TokenType: HOUR_P},
	{Name: "identity", TokenType: IDENTITY_P},
	{Name: "if", TokenType: IF_P},
	{Name: "ilike", TokenType: ILIKE},
	{Name: "immediate", TokenType: IMMEDIATE},
	{Name: "immutable", TokenType: IMMUTABLE},
	{Name: "implicit", TokenType: IMPLICIT_P},
	{Name: "import", TokenType: IMPORT_P},
	{Name: "in", TokenType: IN_P},
	{Name: "include", TokenType: INCLUDE},
	{Name: "including", TokenType: INCLUDING},
	{Name: "increment", TokenType: INCREMENT},
	{Name: "indent", TokenType: INDENT},
	{Name: "index", TokenType: INDEX},
	{Name: "indexes", TokenType: INDEXES},
	{Name: "inherit", TokenType: INHERIT},
	{Name: "inherits", TokenType: INHERITS},
	{Name: "initially", TokenType: INITIALLY},
	{Name: "inline", TokenType: INLINE_P},
	{Name: "inner", TokenType: INNER_P},
	{Name: "inout", TokenType: INOUT},
	{Name: "input", TokenType: INPUT_P},
	{Name: "insensitive", TokenType: INSENSITIVE},
	{Name: "insert", TokenType: INSERT},
	{Name: "instead", TokenType: INSTEAD},
	{Name: "int", TokenType: INT_P},
	{Name: "integer", TokenType: INTEGER},
	{Name: "intersect", TokenType: INTERSECT},
	{Name: "interval", TokenType: INTERVAL},
	{Name: "into", TokenType: INTO},
	{Name: "invoker", TokenType: INVOKER},
	{Name: "is", TokenType: IS},
	{Name: "isnull", TokenType: ISNULL},
	{Name: "isolation", TokenType: ISOLATION},
	{Name: "join", TokenType: JOIN},
	{Name: "json", TokenType: JSON},
	{Name: "json_array", TokenType: JSON_ARRAY},
	{Name: "json_arrayagg", TokenType: JSON_ARRAYAGG},
	{Name: "json_exists", TokenType: JSON_EXISTS},
	{Name: "json_object", TokenType: JSON_OBJECT},
	{Name: "json_objectagg", TokenType: JSON_OBJECTAGG},
	{Name: "json_query", TokenType: JSON_QUERY},
	{Name: "json_scalar", TokenType: JSON_SCALAR},
	{Name: "json_serialize", TokenType: JSON_SERIALIZE},
	{Name: "json_table", TokenType: JSON_TABLE},
	{Name: "json_value", TokenType: JSON_VALUE},
	{Name: "keep", TokenType: KEEP},
	{Name: "key", TokenType: KEY},
	{Name: "keys", TokenType: KEYS},
	{Name: "label", TokenType: LABEL},
	{Name: "language", TokenType: LANGUAGE},
	{Name: "large", TokenType: LARGE_P},
	{Name: "last", TokenType: LAST_P},
	{Name: "lateral", TokenType: LATERAL_P},
	{Name: "leading", TokenType: LEADING},
	{Name: "leakproof", TokenType: LEAKPROOF},
	{Name: "least", TokenType: LEAST},
	{Name: "left", TokenType: LEFT},
	{Name: "level", TokenType: LEVEL},
	{Name: "like", TokenType: LIKE},
	{Name: "limit", TokenType: LIMIT},
	{Name: "listen", TokenType: LISTEN},
	{Name: "load", TokenType: LOAD},
	{Name: "local", TokenType: LOCAL},
	{Name: "localtime", TokenType: LOCALTIME},
	{Name: "localtimestamp", TokenType: LOCALTIMESTAMP},
	{Name: "location", TokenType: LOCATION},
	{Name: "lock", TokenType: LOCK_P},
	{Name: "locked", TokenType: LOCKED},
	{Name: "logged", TokenType: LOGGED},
	{Name: "mapping", TokenType: MAPPING},
	{Name: "match", TokenType: MATCH},
	{Name: "matched", TokenType: MATCHED},
	{Name: "materialized", TokenType: MATERIALIZED},
	{Name: "maxvalue", TokenType: MAXVALUE},
	{Name: "merge", TokenType: MERGE},
	{Name: "merge_action", TokenType: MERGE_ACTION},
	{Name: "method", TokenType: METHOD},
	{Name: "minute", TokenType: MINUTE_P},
	{Name: "minvalue", TokenType: MINVALUE},
	{Name: "mode", TokenType: MODE},
	{Name: "month", TokenType: MONTH_P},
	{Name: "move", TokenType: MOVE},
	{Name: "name", TokenType: NAME_P},
	{Name: "names", TokenType: NAMES},
	{Name: "national", TokenType: NATIONAL},
	{Name: "natural", TokenType: NATURAL},
	{Name: "nchar", TokenType: NCHAR},
	{Name: "nested", TokenType: NESTED},
	{Name: "new", TokenType: NEW},
	{Name: "next", TokenType: NEXT},
	{Name: "nfc", TokenType: NFC},
	{Name: "nfd", TokenType: NFD},
	{Name: "nfkc", TokenType: NFKC},
	{Name: "nfkd", TokenType: NFKD},
	{Name: "no", TokenType: NO},
	{Name: "none", TokenType: NONE},
	{Name: "normalize", TokenType: NORMALIZE},
	{Name: "normalized", TokenType: NORMALIZED},
	{Name: "not", TokenType: NOT},
	{Name: "nothing", TokenType: NOTHING},
	{Name: "notify", TokenType: NOTIFY},
	{Name: "notnull", TokenType: NOTNULL},
	{Name: "nowait", TokenType: NOWAIT},
	{Name: "null", TokenType: NULL_P},
	{Name: "nullif", TokenType: NULLIF},
	{Name: "nulls", TokenType: NULLS_P},
	{Name: "numeric", TokenType: NUMERIC},
	{Name: "object", TokenType: OBJECT_P},
	{Name: "of", TokenType: OF},
	{Name: "off", TokenType: OFF},
	{Name: "offset", TokenType: OFFSET},
	{Name: "oids", TokenType: OIDS},
	{Name: "old", TokenType: OLD},
	{Name: "omit", TokenType: OMIT},
	{Name: "on", TokenType: ON},
	{Name: "only", TokenType: ONLY},
	{Name: "operator", TokenType: OPERATOR},
	{Name: "option", TokenType: OPTION},
	{Name: "options", TokenType: OPTIONS},
	{Name: "or", TokenType: OR},
	{Name: "order", TokenType: ORDER},
	{Name: "ordinality", TokenType: ORDINALITY},
	{Name: "others", TokenType: OTHERS},
	{Name: "out", TokenType: OUT_P},
	{Name: "outer", TokenType: OUTER_P},
	{Name: "over", TokenType: OVER},
	{Name: "overlaps", TokenType: OVERLAPS},
	{Name: "overlay", TokenType: OVERLAY},
	{Name: "overriding", TokenType: OVERRIDING},
	{Name: "owned", TokenType: OWNED},
	{Name: "owner", TokenType: OWNER},
	{Name: "parallel", TokenType: PARALLEL},
	{Name: "parameter", TokenType: PARAMETER},
	{Name: "parser", TokenType: PARSER},
	{Name: "partial", TokenType: PARTIAL},
	{Name: "partition", TokenType: PARTITION},
	{Name: "passing", TokenType: PASSING},
	{Name: "password", TokenType: PASSWORD},
	{Name: "path", TokenType: PATH},
	{Name: "placing", TokenType: PLACING},
	{Name: "plan", TokenType: PLAN},
	{Name: "plans", TokenType: PLANS},
	{Name: "policy", TokenType: POLICY},
	{Name: "position", TokenType: POSITION},
	{Name: "preceding", TokenType: PRECEDING},
	{Name: "precision", TokenType: PRECISION},
	{Name: "prepare", TokenType: PREPARE},
	{Name: "prepared", TokenType: PREPARED},
	{Name: "preserve", TokenType: PRESERVE},
	{Name: "primary", TokenType: PRIMARY},
	{Name: "prior", TokenType: PRIOR},
	{Name: "privileges", TokenType: PRIVILEGES},
	{Name: "procedural", TokenType: PROCEDURAL},
	{Name: "procedure", TokenType: PROCEDURE},
	{Name: "procedures", TokenType: PROCEDURES},
	{Name: "program", TokenType: PROGRAM},
	{Name: "publication", TokenType: PUBLICATION},
	{Name: "quote", TokenType: QUOTE},
	{Name: "quotes", TokenType: QUOTES},
	{Name: "range", TokenType: RANGE},
	{Name: "read", TokenType: READ},
	{Name: "real", TokenType: REAL},
	{Name: "reassign", TokenType: REASSIGN},
	{Name: "recheck", TokenType: RECHECK},
	{Name: "recursive", TokenType: RECURSIVE},
	{Name: "ref", TokenType: REF_P},
	{Name: "references", TokenType: REFERENCES},
	{Name: "referencing", TokenType: REFERENCING},
	{Name: "refresh", TokenType: REFRESH},
	{Name: "reindex", TokenType: REINDEX},
	{Name: "relative", TokenType: RELATIVE_P},
	{Name: "release", TokenType: RELEASE},
	{Name: "rename", TokenType: RENAME},
	{Name: "repeatable", TokenType: REPEATABLE},
	{Name: "replace", TokenType: REPLACE},
	{Name: "replica", TokenType: REPLICA},
	{Name: "reset", TokenType: RESET},
	{Name: "restart", TokenType: RESTART},
	{Name: "restrict", TokenType: RESTRICT},
	{Name: "return", TokenType: RETURN},
	{Name: "returning", TokenType: RETURNING},
	{Name: "returns", TokenType: RETURNS},
	{Name: "revoke", TokenType: REVOKE},
	{Name: "right", TokenType: RIGHT},
	{Name: "role", TokenType: ROLE},
	{Name: "rollback", TokenType: ROLLBACK},
	{Name: "rollup", TokenType: ROLLUP},
	{Name: "routine", TokenType: ROUTINE},
	{Name: "routines", TokenType: ROUTINES},
	{Name: "row", TokenType: ROW},
	{Name: "rows", TokenType: ROWS},
	{Name: "rule", TokenType: RULE},
	{Name: "savepoint", TokenType: SAVEPOINT},
	{Name: "scalar", TokenType: SCALAR},
	{Name: "schema", TokenType: SCHEMA},
	{Name: "schemas", TokenType: SCHEMAS},
	{Name: "scroll", TokenType: SCROLL},
	{Name: "search", TokenType: SEARCH},
	{Name: "second", TokenType: SECOND_P},
	{Name: "security", TokenType: SECURITY},
	{Name: "select", TokenType: SELECT},
	{Name: "sequence", TokenType: SEQUENCE},
	{Name: "sequences", TokenType: SEQUENCES},
	{Name: "serializable", TokenType: SERIALIZABLE},
	{Name: "server", TokenType: SERVER},
	{Name: "session", TokenType: SESSION},
	{Name: "session_user", TokenType: SESSION_USER},
	{Name: "set", TokenType: SET},
	{Name: "setof", TokenType: SETOF},
	{Name: "sets", TokenType: SETS},
	{Name: "share", TokenType: SHARE},
	{Name: "show", TokenType: SHOW},
	{Name: "similar", TokenType: SIMILAR},
	{Name: "simple", TokenType: SIMPLE},
	{Name: "skip", TokenType: SKIP},
	{Name: "smallint", TokenType: SMALLINT},
	{Name: "snapshot", TokenType: SNAPSHOT},
	{Name: "some", TokenType: SOME},
	{Name: "source", TokenType: SOURCE},
	{Name: "sql", TokenType: SQL_P},
	{Name: "stable", TokenType: STABLE},
	{Name: "standalone", TokenType: STANDALONE_P},
	{Name: "start", TokenType: START},
	{Name: "statement", TokenType: STATEMENT},
	{Name: "statistics", TokenType: STATISTICS},
	{Name: "stdin", TokenType: STDIN},
	{Name: "stdout", TokenType: STDOUT},
	{Name: "storage", TokenType: STORAGE},
	{Name: "stored", TokenType: STORED},
	{Name: "strict", TokenType: STRICT_P},
	{Name: "string", TokenType: STRING_P},
	{Name: "strip", TokenType: STRIP_P},
	{Name: "subscription", TokenType: SUBSCRIPTION},
	{Name: "substring", TokenType: SUBSTRING},
	{Name: "support", TokenType: SUPPORT},
	{Name: "symmetric", TokenType: SYMMETRIC},
	{Name: "sysid", TokenType: SYSID},
	{Name: "system", TokenType: SYSTEM_P},
	{Name: "system_user", TokenType: SYSTEM_USER},
	{Name: "table", TokenType: TABLE},
	{Name: "tables", TokenType: TABLES},
	{Name: "tablesample", TokenType: TABLESAMPLE},
	{Name: "tablespace", TokenType: TABLESPACE},
	{Name: "target", TokenType: TARGET},
	{Name: "temp", TokenType: TEMP},
	{Name: "template", TokenType: TEMPLATE},
	{Name: "temporary", TokenType: TEMPORARY},
	{Name: "text", TokenType: TEXT_P},
	{Name: "then", TokenType: THEN},
	{Name: "ties", TokenType: TIES},
	{Name: "time", TokenType: TIME},
	{Name: "timestamp", TokenType: TIMESTAMP},
	{Name: "to", TokenType: TO},
	{Name: "trailing", TokenType: TRAILING},
	{Name: "transaction", TokenType: TRANSACTION},
	{Name: "transform", TokenType: TRANSFORM},
	{Name: "treat", TokenType: TREAT},
	{Name: "trigger", TokenType: TRIGGER},
	{Name: "trim", TokenType: TRIM},
	{Name: "true", TokenType: TRUE_P},
	{Name: "truncate", TokenType: TRUNCATE},
	{Name: "trusted", TokenType: TRUSTED},
	{Name: "type", TokenType: TYPE_P},
	{Name: "types", TokenType: TYPES_P},
	{Name: "uescape", TokenType: UESCAPE},
	{Name: "unbounded", TokenType: UNBOUNDED},
	{Name: "uncommitted", TokenType: UNCOMMITTED},
	{Name: "unconditional", TokenType: UNCONDITIONAL},
	{Name: "unencrypted", TokenType: UNENCRYPTED},
	{Name: "union", TokenType: UNION},
	{Name: "unique", TokenType: UNIQUE},
	{Name: "unknown", TokenType: UNKNOWN},
	{Name: "unlisten", TokenType: UNLISTEN},
	{Name: "unlogged", TokenType: UNLOGGED},
	{Name: "until", TokenType: UNTIL},
	{Name: "update", TokenType: UPDATE},
	{Name: "user", TokenType: USER},
	{Name: "using", TokenType: USING},
	{Name: "vacuum", TokenType: VACUUM},
	{Name: "valid", TokenType: VALID},
	{Name: "validate", TokenType: VALIDATE},
	{Name: "validator", TokenType: VALIDATOR},
	{Name: "value", TokenType: VALUE_P},
	{Name: "values", TokenType: VALUES},
	{Name: "varchar", TokenType: VARCHAR},
	{Name: "variadic", TokenType: VARIADIC},
	{Name: "varying", TokenType: VARYING},
	{Name: "verbose", TokenType: VERBOSE},
	{Name: "version", TokenType: VERSION_P},
	{Name: "view", TokenType: VIEW},
	{Name: "views", TokenType: VIEWS},
	{Name: "volatile", TokenType: VOLATILE},
	{Name: "when", TokenType: WHEN},
	{Name: "where", TokenType: WHERE},
	{Name: "whitespace", TokenType: WHITESPACE_P},
	{Name: "window", TokenType: WINDOW},
	{Name: "with", TokenType: WITH},
	{Name: "within", TokenType: WITHIN},
	{Name: "without", TokenType: WITHOUT},
	{Name: "work", TokenType: WORK},
	{Name: "wrapper", TokenType: WRAPPER},
	{Name: "write", TokenType: WRITE},
	{Name: "xml", TokenType: XML_P},
	{Name: "xmlattributes", TokenType: XMLATTRIBUTES},
	{Name: "xmlconcat", TokenType: XMLCONCAT},
	{Name: "xmlelement", TokenType: XMLELEMENT},
	{Name: "xmlexists", TokenType: XMLEXISTS},
	{Name: "xmlforest", TokenType: XMLFOREST},
	{Name: "xmlnamespaces", TokenType: XMLNAMESPACES},
	{Name: "xmlparse", TokenType: XMLPARSE},
	{Name: "xmlpi", TokenType: XMLPI},
	{Name: "xmlroot", TokenType: XMLROOT},
	{Name: "xmlserialize", TokenType: XMLSERIALIZE},
	{Name: "xmltable", TokenType: XMLTABLE},
	{Name: "year", TokenType: YEAR_P},
	{Name: "yes", TokenType: YES_P},
	{Name: "zone", TokenType: ZONE},
}

// keywordLookupMap provides fast keyword lookup by name.
var keywordLookupMap map[string]*KeywordInfo

// init builds the lookup map and copies each keyword's category / bare-label
// flag from the ast classification (the single source of truth). A keyword
// missing from ast leaves the zero category; TestKeywordTablesInSync guards
// against that drift.
func init() {
	keywordLookupMap = make(map[string]*KeywordInfo, len(Keywords))
	for i := range Keywords {
		if cls, ok := ast.LookupKeywordClass(Keywords[i].Name); ok {
			Keywords[i].Category = cls.Category
			Keywords[i].CanBareLabel = cls.CanBareLabel
		}
		keywordLookupMap[Keywords[i].Name] = &Keywords[i]
	}
}

// LookupKeyword searches for a keyword by name (case-insensitive) with optimizations.
// Returns the keyword info if found, nil otherwise.
// Based on postgres/src/common/kwlookup.c:ScanKeywordLookup lines 37-85
func LookupKeyword(name string) *KeywordInfo {
	// Early termination for very long strings (PostgreSQL pattern)
	if len(name) > maxKeywordLength {
		return nil
	}
	lowerName := normalizeKeywordCase(name)
	return keywordLookupMap[lowerName]
}

// maxKeywordLength is the maximum length of any PostgreSQL keyword.
const maxKeywordLength = 17 // "current_timestamp" is longest at 17 chars

// normalizeKeywordCase performs PostgreSQL-style ASCII-only case normalization.
// Based on postgres/src/common/kwlookup.c:75-76 (ch >= 'A' && ch <= 'Z' conversion)
func normalizeKeywordCase(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		result[i] = ch
	}
	return string(result)
}

// IsKeyword returns true if the given name is a SQL keyword.
func IsKeyword(name string) bool {
	return LookupKeyword(name) != nil
}

// IsReservedKeyword returns true if the given name is a reserved keyword.
func IsReservedKeyword(name string) bool {
	if kw := LookupKeyword(name); kw != nil {
		return kw.Category == ReservedKeyword
	}
	return false
}

// GetKeywordNames returns a sorted slice of all keyword names.
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
