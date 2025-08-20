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
	{"all", ALL, ReservedKeyword, true},
	{"alter", ALTER, ReservedKeyword, false},
	{"and", AND, ReservedKeyword, true},
	{"as", AS, ReservedKeyword, false},

	{"between", IDENT, ColNameKeyword, true}, // Will be replaced with BETWEEN token in Phase 3
	{"bigint", IDENT, ColNameKeyword, true},  // Will be replaced with BIGINT token in Phase 3
	{"bit", BIT, ColNameKeyword, true},
	{"boolean", IDENT, ColNameKeyword, true}, // Will be replaced with BOOLEAN_P token in Phase 3
	{"both", IDENT, ReservedKeyword, true},   // Will be replaced with BOTH token in Phase 3
	{"by", IDENT, UnreservedKeyword, true},   // Will be replaced with BY token in Phase 3

	{"case", IDENT, ReservedKeyword, true},               // Will be replaced with CASE token in Phase 3
	{"cast", IDENT, ReservedKeyword, true},               // Will be replaced with CAST token in Phase 3
	{"char", IDENT, ColNameKeyword, false},               // Will be replaced with CHAR_P token in Phase 3
	{"character", IDENT, ColNameKeyword, false},          // Will be replaced with CHARACTER token in Phase 3
	{"check", IDENT, ReservedKeyword, true},              // Will be replaced with CHECK token in Phase 3
	{"coalesce", IDENT, ColNameKeyword, true},            // Will be replaced with COALESCE token in Phase 3
	{"collate", IDENT, ReservedKeyword, true},            // Will be replaced with COLLATE token in Phase 3
	{"collation", IDENT, TypeFuncNameKeyword, true},      // Will be replaced with COLLATION token in Phase 3
	{"column", IDENT, ReservedKeyword, true},             // Will be replaced with COLUMN token in Phase 3
	{"constraint", IDENT, ReservedKeyword, true},         // Will be replaced with CONSTRAINT token in Phase 3
	{"create", CREATE, ReservedKeyword, false},
	{"drop", DROP, ReservedKeyword, false},
	{"current_catalog", IDENT, ReservedKeyword, true},    // Will be replaced with CURRENT_CATALOG token in Phase 3
	{"current_date", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_DATE token in Phase 3
	{"current_role", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_ROLE token in Phase 3
	{"current_schema", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with CURRENT_SCHEMA token in Phase 3
	{"current_time", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_TIME token in Phase 3
	{"current_timestamp", IDENT, ReservedKeyword, true},  // Will be replaced with CURRENT_TIMESTAMP token in Phase 3
	{"current_user", IDENT, ReservedKeyword, true},       // Will be replaced with CURRENT_USER token in Phase 3

	{"dec", IDENT, ColNameKeyword, true},         // Will be replaced with DEC token in Phase 3
	{"decimal", IDENT, ColNameKeyword, true},     // Will be replaced with DECIMAL_P token in Phase 3
	{"default", IDENT, ReservedKeyword, true},    // Will be replaced with DEFAULT token in Phase 3
	{"deferrable", IDENT, ReservedKeyword, true}, // Will be replaced with DEFERRABLE token in Phase 3
	{"desc", IDENT, ReservedKeyword, true},       // Will be replaced with DESC token in Phase 3
	{"distinct", DISTINCT, ReservedKeyword, true}, // Phase 3C: DISTINCT clause
	{"do", IDENT, ReservedKeyword, true},         // Will be replaced with DO token in Phase 3

	{"else", IDENT, ReservedKeyword, true},   // Will be replaced with ELSE token in Phase 3
	{"end", IDENT, ReservedKeyword, true},    // Will be replaced with END_P token in Phase 3
	{"except", IDENT, ReservedKeyword, true}, // Will be replaced with EXCEPT token in Phase 3
	{"exists", EXISTS, ColNameKeyword, true},

	{"false", FALSE_P, ReservedKeyword, true},
	{"fetch", IDENT, ReservedKeyword, true},    // Will be replaced with FETCH token in Phase 3
	{"float", IDENT, ColNameKeyword, true},     // Will be replaced with FLOAT_P token in Phase 3
	{"for", IDENT, ReservedKeyword, true},      // Will be replaced with FOR token in Phase 3
	{"foreign", IDENT, ReservedKeyword, true},  // Will be replaced with FOREIGN token in Phase 3
	{"from", FROM, ReservedKeyword, true},       // Phase 3C: FROM clause
	{"full", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with FULL token in Phase 3

	{"grant", IDENT, ReservedKeyword, true}, // Will be replaced with GRANT token in Phase 3
	{"group", IDENT, ReservedKeyword, true}, // Will be replaced with GROUP_P token in Phase 3

	{"having", IDENT, ReservedKeyword, true}, // Will be replaced with HAVING token in Phase 3

	{"in", IDENT, ReservedKeyword, true},        // Will be replaced with IN_P token in Phase 3
	{"initially", IDENT, ReservedKeyword, true}, // Will be replaced with INITIALLY token in Phase 3
	{"inner", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with INNER_P token in Phase 3
	{"insert", IDENT, UnreservedKeyword, true},  // Will be replaced with INSERT token in Phase 3
	{"int", IDENT, ColNameKeyword, true},        // Will be replaced with INT_P token in Phase 3
	{"integer", IDENT, ColNameKeyword, true},    // Will be replaced with INTEGER token in Phase 3
	{"intersect", IDENT, ReservedKeyword, true}, // Will be replaced with INTERSECT token in Phase 3
	{"into", INTO, ReservedKeyword, true},       // Phase 3C: INTO clause
	{"is", IDENT, TypeFuncNameKeyword, true},    // Will be replaced with IS token in Phase 3

	{"join", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with JOIN token in Phase 3

	{"leading", IDENT, ReservedKeyword, true},  // Will be replaced with LEADING token in Phase 3
	{"left", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with LEFT token in Phase 3
	{"like", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with LIKE token in Phase 3
	{"limit", IDENT, ReservedKeyword, true},    // Will be replaced with LIMIT token in Phase 3

	{"natural", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with NATURAL token in Phase 3
	{"not", NOT, ReservedKeyword, true},
	{"null", NULL_P, ReservedKeyword, true},
	{"numeric", NUMERIC, ColNameKeyword, true},

	{"offset", IDENT, ReservedKeyword, true},    // Will be replaced with OFFSET token in Phase 3
	{"on", ON, ReservedKeyword, true},           // Phase 3C: ON keyword for DISTINCT ON
	{"only", ONLY, ReservedKeyword, true},       // Phase 3C: ONLY keyword
	{"or", OR, ReservedKeyword, true},
	{"order", IDENT, ReservedKeyword, true},     // Will be replaced with ORDER token in Phase 3
	{"outer", IDENT, TypeFuncNameKeyword, true}, // Will be replaced with OUTER_P token in Phase 3

	{"primary", IDENT, ReservedKeyword, true}, // Will be replaced with PRIMARY token in Phase 3

	{"real", IDENT, ColNameKeyword, true},        // Will be replaced with REAL token in Phase 3
	{"references", IDENT, ReservedKeyword, true}, // Will be replaced with REFERENCES token in Phase 3
	{"right", IDENT, TypeFuncNameKeyword, true},  // Will be replaced with RIGHT token in Phase 3

	{"select", SELECT, ReservedKeyword, true},     // Phase 3C: SELECT statement
	{"session_user", IDENT, ReservedKeyword, true}, // Will be replaced with SESSION_USER token in Phase 3
	{"smallint", IDENT, ColNameKeyword, true},      // Will be replaced with SMALLINT token in Phase 3
	{"some", IDENT, ReservedKeyword, true},         // Will be replaced with SOME token in Phase 3
	{"symmetric", IDENT, ReservedKeyword, true},    // Will be replaced with SYMMETRIC token in Phase 3
	{"system_user", IDENT, ReservedKeyword, true},  // Will be replaced with SYSTEM_USER token in Phase 3

	{"table", TABLE, ReservedKeyword, true},    // Phase 3C: TABLE keyword
	{"then", IDENT, ReservedKeyword, true},     // Will be replaced with THEN token in Phase 3
	{"time", IDENT, ColNameKeyword, true},      // Will be replaced with TIME token in Phase 3
	{"timestamp", IDENT, ColNameKeyword, true}, // Will be replaced with TIMESTAMP token in Phase 3
	{"to", IDENT, ReservedKeyword, true},       // Will be replaced with TO token in Phase 3
	{"trailing", IDENT, ReservedKeyword, true}, // Will be replaced with TRAILING token in Phase 3
	{"true", TRUE_P, ReservedKeyword, true},

	{"union", IDENT, ReservedKeyword, true},    // Will be replaced with UNION token in Phase 3
	{"unique", IDENT, ReservedKeyword, true},   // Will be replaced with UNIQUE token in Phase 3
	{"update", IDENT, UnreservedKeyword, true}, // Will be replaced with UPDATE token in Phase 3
	{"user", IDENT, ReservedKeyword, true},     // Will be replaced with USER token in Phase 3
	{"using", IDENT, ReservedKeyword, true},    // Will be replaced with USING token in Phase 3

	{"varchar", IDENT, ColNameKeyword, true},    // Will be replaced with VARCHAR token in Phase 3
	{"varying", IDENT, UnreservedKeyword, true}, // Will be replaced with VARYING token in Phase 3

	{"when", IDENT, ReservedKeyword, true},  // Will be replaced with WHEN token in Phase 3
	{"where", WHERE, ReservedKeyword, true}, // Phase 3C: WHERE clause
	{"with", WITH, ReservedKeyword, true},
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
