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
// Now using parser-generated token constants directly
var Keywords = []KeywordInfo{
	{"access", ACCESS, UnreservedKeyword, true},
	{"add", ADD_P, UnreservedKeyword, true},
	{"after", AFTER, UnreservedKeyword, true},
	{"aggregate", AGGREGATE, UnreservedKeyword, true},
	{"all", ALL, ReservedKeyword, true},
	{"always", ALWAYS, UnreservedKeyword, true},
	{"alter", ALTER, ReservedKeyword, false},
	{"analyze", ANALYZE, UnreservedKeyword, true},
	{"and", AND, ReservedKeyword, true},
	{"as", AS, ReservedKeyword, false},
	{"asc", ASC, UnreservedKeyword, true},
	{"attach", ATTACH, UnreservedKeyword, true},
	{"authorization", AUTHORIZATION, UnreservedKeyword, true},

	{"before", BEFORE, UnreservedKeyword, true},
	{"between", IDENT, ColNameKeyword, true},
	{"bigint", IDENT, ColNameKeyword, true},
	{"binary", BINARY, UnreservedKeyword, true},
	{"bit", BIT, ColNameKeyword, true},
	{"boolean", IDENT, ColNameKeyword, true},
	{"both", IDENT, ReservedKeyword, true},
	{"breadth", BREADTH, UnreservedKeyword, true},
	{"by", BY, UnreservedKeyword, true},

	{"cascade", CASCADE, UnreservedKeyword, true},
	{"cascaded", CASCADED, UnreservedKeyword, true},
	{"case", CASE, ReservedKeyword, true},
	{"cast", CAST, ReservedKeyword, true},
	{"class", CLASS, UnreservedKeyword, true},
	{"catalog", CATALOG_P, UnreservedKeyword, true},
	{"char", IDENT, ColNameKeyword, false},
	{"character", CHARACTER, ColNameKeyword, false},
	{"check", CHECK, ReservedKeyword, true},
	{"coalesce", COALESCE, ColNameKeyword, true},
	{"collate", COLLATE, ReservedKeyword, true},
	{"collation", COLLATION, TypeFuncNameKeyword, true},
	{"column", COLUMN, ReservedKeyword, true},
	{"columns", COLUMNS, UnreservedKeyword, true},
	{"concurrently", CONCURRENTLY, UnreservedKeyword, true},
	{"conditional", CONDITIONAL, UnreservedKeyword, true},
	{"configuration", CONFIGURATION, UnreservedKeyword, true},
	{"conversion", CONVERSION_P, UnreservedKeyword, true},
	{"conflict", CONFLICT, UnreservedKeyword, true},
	{"constraint", CONSTRAINT, ReservedKeyword, true},
	{"content", CONTENT_P, UnreservedKeyword, true},
	{"copy", COPY, ReservedKeyword, true},
	{"create", CREATE, ReservedKeyword, false},
	{"cross", CROSS, TypeFuncNameKeyword, true},
	{"cube", CUBE, UnreservedKeyword, true},
	{"current", CURRENT_P, ReservedKeyword, false},
	{"current_catalog", IDENT, ReservedKeyword, true},
	{"current_date", IDENT, ReservedKeyword, true},
	{"current_role", IDENT, ReservedKeyword, true},
	{"current_schema", IDENT, TypeFuncNameKeyword, true},
	{"current_time", IDENT, ReservedKeyword, true},
	{"current_timestamp", IDENT, ReservedKeyword, true},
	{"current_user", IDENT, ReservedKeyword, true},
	{"cursor", CURSOR, ReservedKeyword, true},
	{"cycle", CYCLE, UnreservedKeyword, true},

	{"data", DATA_P, ColNameKeyword, true},
	{"dec", IDENT, ColNameKeyword, true},
	{"decimal", IDENT, ColNameKeyword, true},
	{"default", DEFAULT, ReservedKeyword, true},
	{"deferrable", IDENT, ReservedKeyword, true},
	{"delete", DELETE_P, ReservedKeyword, false},
	{"depth", DEPTH, UnreservedKeyword, true},
	{"desc", DESC, UnreservedKeyword, true},
	{"detach", DETACH, UnreservedKeyword, true},
	{"dictionary", DICTIONARY, UnreservedKeyword, true},
	{"disable", DISABLE_P, UnreservedKeyword, true},
	{"distinct", DISTINCT, ReservedKeyword, true},
	{"do", DO, ReservedKeyword, true},
	{"document", DOCUMENT_P, UnreservedKeyword, true},
	{"domain", DOMAIN_P, UnreservedKeyword, true},
	{"drop", DROP, ReservedKeyword, false},

	{"each", EACH, UnreservedKeyword, true},
	{"else", IDENT, ReservedKeyword, true},
	{"enable", ENABLE_P, UnreservedKeyword, true},
	{"empty", EMPTY, UnreservedKeyword, true},
	{"encoding", ENCODING, UnreservedKeyword, true},
	{"end", IDENT, ReservedKeyword, true},
	{"enum", ENUM_P, UnreservedKeyword, true},
	{"error", ERROR, UnreservedKeyword, true},
	{"event", EVENT, UnreservedKeyword, true},
	{"except", IDENT, ReservedKeyword, true},
	{"execute", EXECUTE, UnreservedKeyword, true},
	{"extension", EXTENSION, UnreservedKeyword, true},
	{"exists", EXISTS, ColNameKeyword, true},

	{"false", FALSE_P, ReservedKeyword, true},
	{"family", FAMILY, UnreservedKeyword, true},
	{"fetch", IDENT, ReservedKeyword, true},
	{"finalize", FINALIZE, UnreservedKeyword, true},
	{"first", FIRST_P, UnreservedKeyword, true},
	{"float", IDENT, ColNameKeyword, true},
	{"for", FOR, ReservedKeyword, true},
	{"foreign", FOREIGN, ReservedKeyword, true},
	{"format", FORMAT, UnreservedKeyword, true},
	{"freeze", FREEZE, UnreservedKeyword, true},
	{"from", FROM, ReservedKeyword, true},
	{"full", FULL, TypeFuncNameKeyword, true},
	{"function", FUNCTION, UnreservedKeyword, true},

	{"generated", GENERATED, UnreservedKeyword, true},
	{"global", GLOBAL, UnreservedKeyword, true},
	{"grant", IDENT, ReservedKeyword, true},
	{"group", GROUP_P, ReservedKeyword, true},
	{"grouping", GROUPING, UnreservedKeyword, true},

	{"handler", HANDLER, UnreservedKeyword, true},
	{"having", HAVING, ReservedKeyword, true},

	{"identity", IDENTITY_P, UnreservedKeyword, true},
	{"if", IF_P, UnreservedKeyword, true},
	{"in", IN_P, ReservedKeyword, true},
	{"include", INCLUDE, UnreservedKeyword, true},
	{"index", INDEX, UnreservedKeyword, true},
	{"inherits", INHERITS, UnreservedKeyword, true},
	{"initially", INITIALLY, ReservedKeyword, true},
	{"inline", INLINE_P, ReservedKeyword, true},
	{"inner", INNER_P, TypeFuncNameKeyword, true},
	{"inout", INOUT, UnreservedKeyword, true},
	{"insert", INSERT, ReservedKeyword, false},
	{"instead", INSTEAD, UnreservedKeyword, true},
	{"int", IDENT, ColNameKeyword, true},
	{"integer", IDENT, ColNameKeyword, true},
	{"intersect", IDENT, ReservedKeyword, true},
	{"into", INTO, ReservedKeyword, true},
	{"is", IS, TypeFuncNameKeyword, true},

	{"join", JOIN, TypeFuncNameKeyword, true},
	{"json", JSON, UnreservedKeyword, true},
	{"json_array", JSON_ARRAY, UnreservedKeyword, true},
	{"json_arrayagg", JSON_ARRAYAGG, UnreservedKeyword, true},
	{"json_exists", JSON_EXISTS, UnreservedKeyword, true},
	{"json_object", JSON_OBJECT, UnreservedKeyword, true},
	{"json_objectagg", JSON_OBJECTAGG, UnreservedKeyword, true},
	{"json_query", JSON_QUERY, UnreservedKeyword, true},
	{"json_scalar", JSON_SCALAR, UnreservedKeyword, true},
	{"json_serialize", JSON_SERIALIZE, UnreservedKeyword, true},
	{"json_table", JSON_TABLE, UnreservedKeyword, true},
	{"json_value", JSON_VALUE, UnreservedKeyword, true},

	{"keep", KEEP, UnreservedKeyword, true},
	{"key", KEY, UnreservedKeyword, true},

	{"language", LANGUAGE, UnreservedKeyword, true},
	{"last", LAST_P, UnreservedKeyword, true},
	{"lateral", LATERAL, ReservedKeyword, true},
	{"leading", IDENT, ReservedKeyword, true},
	{"left", LEFT, TypeFuncNameKeyword, true},
	{"like", LIKE, TypeFuncNameKeyword, true},
	{"limit", IDENT, ReservedKeyword, true},
	{"local", LOCAL, UnreservedKeyword, true},

	{"mapping", MAPPING, UnreservedKeyword, true},
	{"matched", MATCHED, UnreservedKeyword, true},
	{"materialized", MATERIALIZED, UnreservedKeyword, true},
	{"method", METHOD, UnreservedKeyword, true},
	{"merge", MERGE, ReservedKeyword, false},

	{"names", NAMES, UnreservedKeyword, true},
	{"natural", NATURAL, TypeFuncNameKeyword, true},
	{"nested", NESTED, UnreservedKeyword, true},
	{"new", NEW, UnreservedKeyword, true},
	{"no", NO, UnreservedKeyword, true},
	{"not", NOT, ReservedKeyword, true},
	{"nothing", NOTHING, UnreservedKeyword, true},
	{"null", NULL_P, ReservedKeyword, true},
	{"nulls", NULLS_P, UnreservedKeyword, true},
	{"numeric", NUMERIC, ColNameKeyword, true},

	{"of", OF, ReservedKeyword, true},
	{"offset", IDENT, ReservedKeyword, true},
	{"old", OLD, UnreservedKeyword, true},
	{"omit", OMIT, UnreservedKeyword, true},
	{"on", ON, ReservedKeyword, true},
	{"only", ONLY, ReservedKeyword, true},
	{"operator", OPERATOR, UnreservedKeyword, true},
	{"option", OPTION, UnreservedKeyword, true},
	{"options", OPTIONS, UnreservedKeyword, true},
	{"or", OR, ReservedKeyword, true},
	{"order", ORDER, ReservedKeyword, true},
	{"ordinality", ORDINALITY, UnreservedKeyword, true},
	{"out", OUT_P, UnreservedKeyword, true},
	{"outer", OUTER_P, TypeFuncNameKeyword, true},
	{"overriding", OVERRIDING, UnreservedKeyword, true},

	{"parser", PARSER, UnreservedKeyword, true},
	{"partition", PARTITION, UnreservedKeyword, true},
	{"passing", PASSING, UnreservedKeyword, true},
	{"path", PATH, UnreservedKeyword, true},
	{"policy", POLICY, UnreservedKeyword, true},
	{"primary", PRIMARY, UnreservedKeyword, true},
	{"procedure", PROCEDURE, UnreservedKeyword, true},
	{"program", PROGRAM, UnreservedKeyword, true},
	{"publication", PUBLICATION, UnreservedKeyword, true},

	{"quotes", QUOTES, UnreservedKeyword, true},

	{"range", RANGE, UnreservedKeyword, true},
	{"real", IDENT, ColNameKeyword, true},
	{"recursive", RECURSIVE, UnreservedKeyword, true},
	{"ref", REF_P, UnreservedKeyword, true},
	{"references", REFERENCES, UnreservedKeyword, true},
	{"referencing", REFERENCING, UnreservedKeyword, true},
	{"refresh", REFRESH, UnreservedKeyword, true},
	{"rename", RENAME, UnreservedKeyword, true},
	{"replace", REPLACE, UnreservedKeyword, true},
	{"replica", REPLICA, UnreservedKeyword, true},
	{"restrict", RESTRICT, UnreservedKeyword, true},
	{"restart", RESTART, UnreservedKeyword, true},
	{"return", RETURN, UnreservedKeyword, true},
	{"returning", RETURNING, UnreservedKeyword, true},
	{"returns", RETURNS, UnreservedKeyword, true},
	{"right", RIGHT, TypeFuncNameKeyword, true},
	{"role", ROLE, UnreservedKeyword, true},
	{"rollup", ROLLUP, UnreservedKeyword, true},
	{"row", ROW, UnreservedKeyword, true},
	{"rows", ROWS, UnreservedKeyword, true},

	{"scalar", SCALAR, UnreservedKeyword, true},
	{"schema", SCHEMA, UnreservedKeyword, true},
	{"search", SEARCH, UnreservedKeyword, true},
	{"select", SELECT, ReservedKeyword, true},
	{"sequence", SEQUENCE, UnreservedKeyword, true},
	{"server", SERVER, UnreservedKeyword, true},
	{"session", SESSION, UnreservedKeyword, true},
	{"session_user", IDENT, ReservedKeyword, true},
	{"set", SET, UnreservedKeyword, true},
	{"sets", SETS, UnreservedKeyword, true},
	{"smallint", IDENT, ColNameKeyword, true},
	{"snapshot", SNAPSHOT, UnreservedKeyword, true},
	{"some", IDENT, ReservedKeyword, true},
	{"source", SOURCE, UnreservedKeyword, true},
	{"sql", SQL_P, UnreservedKeyword, true},
	{"statement", STATEMENT, UnreservedKeyword, true},
	{"statistics", STATISTICS, UnreservedKeyword, true},
	{"stdin", STDIN, UnreservedKeyword, true},
	{"stdout", STDOUT, UnreservedKeyword, true},
	{"string", STRING_P, UnreservedKeyword, true},
	{"subscription", SUBSCRIPTION, UnreservedKeyword, true},
	{"symmetric", IDENT, ReservedKeyword, true},
	{"system", SYSTEM_P, UnreservedKeyword, true},
	{"system_user", IDENT, ReservedKeyword, true},

	{"table", TABLE, ReservedKeyword, true},
	{"target", TARGET, UnreservedKeyword, true},
	{"temp", TEMP, UnreservedKeyword, true},
	{"template", TEMPLATE, UnreservedKeyword, true},
	{"temporary", TEMPORARY, UnreservedKeyword, true},
	{"text", TEXT_P, UnreservedKeyword, true},
	{"then", THEN, ReservedKeyword, true},
	{"time", TIME, ColNameKeyword, true},
	{"timestamp", IDENT, ColNameKeyword, true},
	{"to", TO, ReservedKeyword, true},
	{"trailing", IDENT, ReservedKeyword, true},
	{"transaction", TRANSACTION, UnreservedKeyword, true},
	{"transform", TRANSFORM, UnreservedKeyword, true},
	{"trigger", TRIGGER, UnreservedKeyword, true},
	{"true", TRUE_P, ReservedKeyword, true},
	{"truncate", TRUNCATE, ReservedKeyword, true},
	{"trusted", TRUSTED, UnreservedKeyword, true},
	{"type", TYPE_P, UnreservedKeyword, true},

	{"unconditional", UNCONDITIONAL, UnreservedKeyword, true},
	{"union", IDENT, ReservedKeyword, true},
	{"unique", UNIQUE, ReservedKeyword, true},
	{"unlogged", UNLOGGED, UnreservedKeyword, true},
	{"update", UPDATE, ReservedKeyword, false},
	{"user", USER, UnreservedKeyword, true},
	{"using", USING, ReservedKeyword, true},
	{"utf8", UTF8, UnreservedKeyword, true},

	{"validate", VALIDATE, UnreservedKeyword, true},
	{"validator", VALIDATOR, UnreservedKeyword, true},
	{"value", VALUE_P, UnreservedKeyword, true},
	{"values", VALUES, UnreservedKeyword, true},
	{"varchar", VARCHAR, ColNameKeyword, true},
	{"variadic", VARIADIC, UnreservedKeyword, true},
	{"varying", IDENT, UnreservedKeyword, true},
	{"verbose", VERBOSE, UnreservedKeyword, true},
	{"version", VERSION_P, UnreservedKeyword, true},
	{"view", VIEW, UnreservedKeyword, true},

	{"when", WHEN, ReservedKeyword, true},
	{"where", WHERE, ReservedKeyword, true},
	{"with", WITH, ReservedKeyword, true},
	{"without", WITHOUT, UnreservedKeyword, true},
	{"wrapper", WRAPPER, UnreservedKeyword, true},

	{"xml", XML_P, UnreservedKeyword, true},
	{"xmlnamespaces", XMLNAMESPACES, ColNameKeyword, true},
	{"xmltable", XMLTABLE, UnreservedKeyword, true},

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
