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
// Note: This is a core subset for Phase 3A - full list will be expanded in later phases
var Keywords = []KeywordInfo{
	// Phase 3A keywords - using generated parser constants
	{"add", ADD_P, UnreservedKeyword, true}, // Phase 3F: ADD for ALTER TABLE ADD COLUMN
	{"all", ALL, ReservedKeyword, true},
	{"always", ALWAYS, UnreservedKeyword, true}, // Phase 3F: ALWAYS for identity columns
	{"alter", ALTER, ReservedKeyword, false},
	{"and", AND, ReservedKeyword, true},
	{"as", AS, ReservedKeyword, false},
	{"attach", ATTACH, UnreservedKeyword, true}, // Phase 3F: ATTACH for ALTER TABLE ATTACH PARTITION

	{"between", IDENT, ColNameKeyword, true}, // Will be replaced with BETWEEN token in Phase 3
	{"bigint", IDENT, ColNameKeyword, true},  // Will be replaced with BIGINT token in Phase 3
	{"bit", BIT, ColNameKeyword, true},
	{"boolean", IDENT, ColNameKeyword, true},      // Will be replaced with BOOLEAN_P token in Phase 3
	{"both", IDENT, ReservedKeyword, true},        // Will be replaced with BOTH token in Phase 3
	{"breadth", BREADTH, UnreservedKeyword, true}, // Phase 3D: BREADTH FIRST for CTE SEARCH
	{"by", BY, UnreservedKeyword, true},           // Phase 3D: BY for CTE SEARCH

	{"cascade", CASCADE, UnreservedKeyword, true},           // Phase 3F: CASCADE for DROP TABLE CASCADE
	{"case", IDENT, ReservedKeyword, true},                  // Will be replaced with CASE token in Phase 3
	{"cast", IDENT, ReservedKeyword, true},                  // Will be replaced with CAST token in Phase 3
	{"char", IDENT, ColNameKeyword, false},                  // Will be replaced with CHAR_P token in Phase 3
	{"character", IDENT, ColNameKeyword, false},             // Will be replaced with CHARACTER token in Phase 3
	{"check", CHECK, ReservedKeyword, true},                 // Phase 3F: CHECK constraint for DDL
	{"coalesce", IDENT, ColNameKeyword, true},               // Will be replaced with COALESCE token in Phase 3
	{"cross", CROSS, TypeFuncNameKeyword, true},             // Phase 3D: CROSS JOIN keyword
	{"collate", IDENT, ReservedKeyword, true},               // Will be replaced with COLLATE token in Phase 3
	{"concurrently", CONCURRENTLY, UnreservedKeyword, true}, // Phase 3F: CONCURRENTLY for CREATE INDEX
	{"cycle", CYCLE, UnreservedKeyword, true},               // Phase 3D: CYCLE for recursive CTEs
	{"collation", IDENT, TypeFuncNameKeyword, true},         // Will be replaced with COLLATION token in Phase 3
	{"column", COLUMN, ReservedKeyword, true},               // Phase 3F: COLUMN for DDL
	{"create", CREATE, ReservedKeyword, false},
	{"current", CURRENT_P, ReservedKeyword, false}, // Phase 3E: CURRENT keyword for cursor references
	{"cursor", CURSOR, ReservedKeyword, true},      // Phase 3E: CURSOR for cursor operations
	{"delete", DELETE_P, ReservedKeyword, false},   // Phase 3E: DELETE statement
	{"drop", DROP, ReservedKeyword, false},
	{"current_catalog", IDENT, ReservedKeyword, true},    // Will be replaced with CURRENT_CATALOG token in Phase 3
	{"current_date", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_DATE token in Phase 3
	{"current_role", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_ROLE token in Phase 3
	{"current_schema", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with CURRENT_SCHEMA token in Phase 3
	{"current_time", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_TIME token in Phase 3
	{"current_timestamp", IDENT, ReservedKeyword, true},  // Will be replaced with CURRENT_TIMESTAMP token in Phase 3
	{"current_user", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_USER token in Phase 3

	{"dec", IDENT, ColNameKeyword, true},          // Will be replaced with DEC token in Phase 3
	{"decimal", IDENT, ColNameKeyword, true},      // Will be replaced with DECIMAL_P token in Phase 3
	{"default", DEFAULT, ReservedKeyword, true},   // Phase 3D: DEFAULT keyword for CYCLE clause
	{"deferrable", IDENT, ReservedKeyword, true},  // Will be replaced with DEFERRABLE token in Phase 3
	{"detach", DETACH, UnreservedKeyword, true},   // Phase 3F: DETACH for ALTER TABLE DETACH PARTITION
	{"depth", DEPTH, UnreservedKeyword, true},     // Phase 3D: DEPTH FIRST for CTE SEARCH
	{"desc", IDENT, ReservedKeyword, true},        // Will be replaced with DESC token in Phase 3
	{"distinct", DISTINCT, ReservedKeyword, true}, // Phase 3C: DISTINCT clause
	{"do", DO, ReservedKeyword, true},             // Phase 3E: DO NOTHING, DO UPDATE

	{"else", IDENT, ReservedKeyword, true},   // Will be replaced with ELSE token in Phase 3
	{"end", IDENT, ReservedKeyword, true},    // Will be replaced with END_P token in Phase 3
	{"except", IDENT, ReservedKeyword, true}, // Will be replaced with EXCEPT token in Phase 3
	{"exists", EXISTS, ColNameKeyword, true},

	{"false", FALSE_P, ReservedKeyword, true},
	{"fetch", IDENT, ReservedKeyword, true},         // Will be replaced with FETCH token in Phase 3
	{"finalize", FINALIZE, UnreservedKeyword, true}, // Phase 3F: FINALIZE for DETACH PARTITION
	{"first", FIRST_P, UnreservedKeyword, true},     // Phase 3D: FIRST for CTE SEARCH
	{"float", IDENT, ColNameKeyword, true},          // Will be replaced with FLOAT_P token in Phase 3
	{"for", IDENT, ReservedKeyword, true},           // Will be replaced with FOR token in Phase 3
	{"foreign", FOREIGN, ReservedKeyword, true},     // Phase 3F: FOREIGN KEY for DDL
	{"from", FROM, ReservedKeyword, true},           // Phase 3C: FROM clause
	{"full", FULL, TypeFuncNameKeyword, true},       // Phase 3D: FULL OUTER JOIN keyword

	{"generated", GENERATED, UnreservedKeyword, true}, // Phase 3F: GENERATED for identity columns
	{"global", GLOBAL, UnreservedKeyword, true},       // Phase 3F: GLOBAL for temporary tables
	{"grant", IDENT, ReservedKeyword, true},           // Will be replaced with GRANT token in Phase 3
	{"group", IDENT, ReservedKeyword, true},           // Will be replaced with GROUP_P token in Phase 3

	{"having", IDENT, ReservedKeyword, true}, // Will be replaced with HAVING token in Phase 3

	{"identity", IDENTITY_P, UnreservedKeyword, true}, // Phase 3F: IDENTITY for identity columns
	{"if", IF_P, UnreservedKeyword, true},             // Phase 3F: IF for IF EXISTS/IF NOT EXISTS
	{"in", IN_P, ReservedKeyword, true},               // Phase 3F: IN for partition bounds
	{"include", INCLUDE, UnreservedKeyword, true},     // Phase 3F: INCLUDE for CREATE INDEX
	{"index", INDEX, UnreservedKeyword, true},         // Phase 3F: INDEX for CREATE INDEX
	{"initially", IDENT, ReservedKeyword, true},       // Will be replaced with INITIALLY token in Phase 3
	{"inner", INNER_P, TypeFuncNameKeyword, true},     // Phase 3D: INNER JOIN keyword
	{"insert", INSERT, ReservedKeyword, false},        // Phase 3E: INSERT statement
	{"int", IDENT, ColNameKeyword, true},              // Will be replaced with INT_P token in Phase 3
	{"integer", IDENT, ColNameKeyword, true},          // Will be replaced with INTEGER token in Phase 3
	{"intersect", IDENT, ReservedKeyword, true},       // Will be replaced with INTERSECT token in Phase 3
	{"into", INTO, ReservedKeyword, true},             // Phase 3C: INTO clause
	{"is", IDENT, TypeFuncNameKeyword, true},          // Will be replaced with IS token in Phase 3

	{"join", JOIN, TypeFuncNameKeyword, true}, // Phase 3D: JOIN keyword

	{"leading", IDENT, ReservedKeyword, true},   // Will be replaced with LEADING token in Phase 3
	{"left", LEFT, TypeFuncNameKeyword, true},   // Phase 3D: LEFT JOIN keyword
	{"like", IDENT, TypeFuncNameKeyword, true},  // Will be replaced with LIKE token in Phase 3
	{"lateral", LATERAL, ReservedKeyword, true}, // Phase 3D: LATERAL subqueries
	{"limit", IDENT, ReservedKeyword, true},     // Will be replaced with LIMIT token in Phase 3
	{"local", LOCAL, UnreservedKeyword, true},   // Phase 3F: LOCAL for temporary tables

	{"materialized", MATERIALIZED, UnreservedKeyword, true}, // Phase 3D: MATERIALIZED CTEs
	{"merge", MERGE, ReservedKeyword, false},                // Phase 3E: MERGE statement
	{"natural", NATURAL, TypeFuncNameKeyword, true},         // Phase 3D: NATURAL JOIN keyword
	{"no", NO, UnreservedKeyword, true},                     // Phase 3F: NO keyword for sequence options
	{"not", NOT, ReservedKeyword, true},
	{"null", NULL_P, ReservedKeyword, true},
	{"numeric", NUMERIC, ColNameKeyword, true},

	{"of", OF, ReservedKeyword, true},        // Phase 3E: OF keyword for CURRENT OF
	{"offset", IDENT, ReservedKeyword, true}, // Will be replaced with OFFSET token in Phase 3
	{"on", ON, ReservedKeyword, true},        // Phase 3C: ON keyword for DISTINCT ON
	{"only", ONLY, ReservedKeyword, true},    // Phase 3C: ONLY keyword
	{"or", OR, ReservedKeyword, true},
	{"order", IDENT, ReservedKeyword, true},             // Will be replaced with ORDER token in Phase 3
	{"partition", PARTITION, UnreservedKeyword, true},   // Phase 3F: PARTITION for ALTER TABLE ATTACH/DETACH PARTITION
	{"outer", OUTER_P, TypeFuncNameKeyword, true},       // Phase 3D: OUTER JOIN keyword
	{"overriding", OVERRIDING, UnreservedKeyword, true}, // Phase 3E: OVERRIDING clause for INSERT

	{"primary", PRIMARY, UnreservedKeyword, true}, // Phase 3F: PRIMARY KEY for DDL

	{"real", IDENT, ColNameKeyword, true},               // Will be replaced with REAL token in Phase 3
	{"recursive", RECURSIVE, UnreservedKeyword, true},   // Phase 3D: RECURSIVE CTEs
	{"references", REFERENCES, UnreservedKeyword, true}, // Phase 3F: REFERENCES for foreign keys
	{"rename", RENAME, UnreservedKeyword, true},         // Phase 3F: RENAME for ALTER statements
	{"restrict", RESTRICT, UnreservedKeyword, true},     // Phase 3F: RESTRICT for DROP TABLE RESTRICT
	{"returning", RETURNING, UnreservedKeyword, true},   // Phase 3E: RETURNING clause
	{"right", RIGHT, TypeFuncNameKeyword, true},         // Phase 3D: RIGHT JOIN keyword

	{"search", SEARCH, UnreservedKeyword, true},     // Phase 3D: SEARCH for recursive CTEs
	{"select", SELECT, ReservedKeyword, true},       // Phase 3C: SELECT statement
	{"sequence", SEQUENCE, UnreservedKeyword, true}, // Phase 3F: SEQUENCE for ALTER SEQUENCE
	{"set", SET, UnreservedKeyword, true},           // Phase 3D: SET for CTE SEARCH/CYCLE
	{"session_user", IDENT, ReservedKeyword, true},  // Will be replaced with SESSION_USER token in Phase 3
	{"smallint", IDENT, ColNameKeyword, true},       // Will be replaced with SMALLINT token in Phase 3
	{"some", IDENT, ReservedKeyword, true},          // Will be replaced with SOME token in Phase 3
	{"symmetric", IDENT, ReservedKeyword, true},     // Will be replaced with SYMMETRIC token in Phase 3
	{"system", SYSTEM_P, UnreservedKeyword, true},   // Phase 3E: SYSTEM_P keyword for OVERRIDING SYSTEM VALUE
	{"system_user", IDENT, ReservedKeyword, true},   // Will be replaced with SYSTEM_USER token in Phase 3

	{"table", TABLE, ReservedKeyword, true},           // Phase 3C: TABLE keyword
	{"temp", TEMP, UnreservedKeyword, true},           // Phase 3F: TEMP for temporary tables
	{"temporary", TEMPORARY, UnreservedKeyword, true}, // Phase 3F: TEMPORARY for temporary tables
	{"then", THEN, ReservedKeyword, true},             // Phase 3E: MERGE WHEN ... THEN
	{"matched", MATCHED, UnreservedKeyword, true},     // Phase 3E: MERGE WHEN MATCHED
	{"nothing", NOTHING, UnreservedKeyword, true},     // Phase 3E: DO NOTHING
	{"source", SOURCE, UnreservedKeyword, true},       // Phase 3E: MERGE NOT MATCHED BY SOURCE
	{"target", TARGET, UnreservedKeyword, true},       // Phase 3E: MERGE NOT MATCHED BY TARGET

	{"copy", COPY, ReservedKeyword, true},             // Phase 3E: COPY statement
	{"program", PROGRAM, UnreservedKeyword, true},     // Phase 3E: COPY PROGRAM
	{"stdin", STDIN, UnreservedKeyword, true},         // Phase 3E: COPY STDIN
	{"stdout", STDOUT, UnreservedKeyword, true},       // Phase 3E: COPY STDOUT
	{"binary", BINARY, UnreservedKeyword, true},       // Phase 3E: COPY BINARY format
	{"freeze", FREEZE, UnreservedKeyword, true},       // Phase 3E: COPY FREEZE option
	{"conflict", CONFLICT, UnreservedKeyword, true},   // Phase 3E: ON CONFLICT clause
	{"constraint", CONSTRAINT, ReservedKeyword, true}, // Phase 3E: CONSTRAINT for ON CONFLICT
	{"verbose", VERBOSE, UnreservedKeyword, true},     // Phase 3E: VERBOSE option
	{"analyze", ANALYZE, UnreservedKeyword, true},     // Phase 3E: ANALYZE option
	{"time", IDENT, ColNameKeyword, true},             // Will be replaced with TIME token in Phase 3
	{"timestamp", IDENT, ColNameKeyword, true},        // Will be replaced with TIMESTAMP token in Phase 3
	{"to", TO, ReservedKeyword, true},                 // Phase 3D: TO keyword for CYCLE clause
	{"trailing", IDENT, ReservedKeyword, true},        // Will be replaced with TRAILING token in Phase 3
	{"true", TRUE_P, ReservedKeyword, true},

	{"union", IDENT, ReservedKeyword, true},         // Will be replaced with UNION token in Phase 3
	{"unique", UNIQUE, ReservedKeyword, true},       // Phase 3F: UNIQUE constraint for DDL
	{"unlogged", UNLOGGED, UnreservedKeyword, true}, // Phase 3F: UNLOGGED for persistent tables
	{"update", UPDATE, ReservedKeyword, false},      // Phase 3E: UPDATE statement
	{"user", USER, UnreservedKeyword, true},         // Phase 3E: USER keyword for OVERRIDING USER VALUE
	{"using", USING, ReservedKeyword, true},         // Phase 3D: USING clause for JOINs

	{"values", VALUES, UnreservedKeyword, true}, // Phase 3D: VALUES clause
	{"varchar", VARCHAR, ColNameKeyword, true},  // Will be replaced with VARCHAR token in Phase 3
	{"varying", IDENT, UnreservedKeyword, true}, // Will be replaced with VARYING token in Phase 3
	{"view", VIEW, UnreservedKeyword, true},     // Phase 3F: VIEW for ALTER VIEW statements

	{"when", WHEN, ReservedKeyword, true},   // Phase 3E: MERGE WHEN clause
	{"where", WHERE, ReservedKeyword, true}, // Phase 3C: WHERE clause
	{"with", WITH, ReservedKeyword, true},

	// Phase 3D: Table function keywords
	{"columns", COLUMNS, UnreservedKeyword, true},       // COLUMNS clause for table functions
	{"ordinality", ORDINALITY, UnreservedKeyword, true}, // WITH ORDINALITY for table functions
	{"xmltable", XMLTABLE, UnreservedKeyword, true},     // XMLTABLE function
	{"json_table", JSON_TABLE, UnreservedKeyword, true}, // JSON_TABLE function
	{"rows", ROWS, UnreservedKeyword, true},             // ROWS FROM() syntax
	{"path", PATH, UnreservedKeyword, true},             // PATH clause for JSON_TABLE
	{"passing", PASSING, UnreservedKeyword, true},       // PASSING clause
	{"for", FOR, ReservedKeyword, true},                 // FOR keyword
	{"nested", NESTED, UnreservedKeyword, true},         // NESTED clause for JSON_TABLE

	// Phase 3D: JSON keywords
	{"error", ERROR, UnreservedKeyword, true},                   // ERROR clause for JSON functions
	{"empty", EMPTY, UnreservedKeyword, true},                   // EMPTY clause for JSON functions
	{"wrapper", WRAPPER, UnreservedKeyword, true},               // WRAPPER clause for JSON functions
	{"conditional", CONDITIONAL, UnreservedKeyword, true},       // CONDITIONAL for JSON functions
	{"unconditional", UNCONDITIONAL, UnreservedKeyword, true},   // UNCONDITIONAL for JSON functions
	{"quotes", QUOTES, UnreservedKeyword, true},                 // QUOTES clause for JSON functions
	{"omit", OMIT, UnreservedKeyword, true},                     // OMIT clause for JSON functions
	{"keep", KEEP, UnreservedKeyword, true},                     // KEEP clause for JSON functions
	{"key", KEY, UnreservedKeyword, true},                       // Phase 3F: KEY for PRIMARY KEY, FOREIGN KEY
	{"scalar", SCALAR, UnreservedKeyword, true},                 // SCALAR clause for JSON functions
	{"string", STRING_P, UnreservedKeyword, true},               // STRING clause for JSON functions
	{"encoding", ENCODING, UnreservedKeyword, true},             // ENCODING clause for JSON functions
	{"value", VALUE_P, UnreservedKeyword, true},                 // VALUE clause for JSON functions
	{"json_query", JSON_QUERY, UnreservedKeyword, true},         // JSON_QUERY function
	{"json_value", JSON_VALUE, UnreservedKeyword, true},         // JSON_VALUE function
	{"json_serialize", JSON_SERIALIZE, UnreservedKeyword, true}, // JSON_SERIALIZE function
	{"json_object", JSON_OBJECT, UnreservedKeyword, true},       // JSON_OBJECT function
	{"json_array", JSON_ARRAY, UnreservedKeyword, true},         // JSON_ARRAY function
	{"json_objectagg", JSON_OBJECTAGG, UnreservedKeyword, true}, // JSON_OBJECTAGG function
	{"json_arrayagg", JSON_ARRAYAGG, UnreservedKeyword, true},   // JSON_ARRAYAGG function
	{"json_exists", JSON_EXISTS, UnreservedKeyword, true},       // JSON_EXISTS function
	{"json_scalar", JSON_SCALAR, UnreservedKeyword, true},       // JSON_SCALAR function
	{"format", FORMAT, UnreservedKeyword, true},                 // FORMAT clause
	{"json", JSON, UnreservedKeyword, true},                     // JSON keyword
	{"utf8", UTF8, UnreservedKeyword, true},                     // UTF8 encoding
	{"without", WITHOUT, UnreservedKeyword, true},               // WITHOUT keyword
	{"ref", REF_P, UnreservedKeyword, true},                     // REF keyword for XML PASSING
	{"xmlnamespaces", XMLNAMESPACES, ColNameKeyword, true},      // XMLNAMESPACES for XMLTABLE
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
// In Phase 2, all keywords return IDENT (will be replaced in Phase 3 with proper tokens)
// This preserves the identifier text while marking it for future keyword token assignment
func getKeywordTokenType(keyword *KeywordInfo) TokenType {
	// Return the keyword's specific token type
	// In Phase 3, this will be replaced with goyacc-generated tokens
	return keyword.TokenType
}
