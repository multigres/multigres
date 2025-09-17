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
 * SQL Utility Functions for Deparsing
 *
 * This file contains utility functions for converting AST nodes back to SQL,
 * including identifier quoting, formatting helpers, and PostgreSQL-specific
 * SQL generation rules.
 */

package ast

import (
	"regexp"
	"strings"
)

// ==============================================================================
// IDENTIFIER QUOTING AND FORMATTING
// ==============================================================================

// identifierNeedsQuoting checks if an identifier needs to be quoted in SQL
var (
	// SQL identifier regex: must start with letter or underscore, followed by letters, digits, underscores, or dollar signs
	sqlIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]*$`)

	// PostgreSQL reserved keywords that always need quoting when used as identifiers
	// This is a subset of the most common keywords - can be expanded as needed
	reservedKeywords = map[string]bool{
		"all": true, "and": true, "any": true, "as": true, "asc": true, "between": true, "by": true,
		"case": true, "create": true, "desc": true, "distinct": true, "drop": true,
		"else": true, "end": true, "exists": true, "false": true, "from": true,
		"group": true, "having": true, "in": true, "insert": true, "into": true,
		"is": true, "join": true, "like": true, "not": true, "null": true, "or": true,
		"order": true, "select": true, "table": true, "then": true, "true": true,
		"union": true, "update": true, "values": true, "when": true, "where": true,
		"with": true, "limit": true, "offset": true, "inner": true, "outer": true,
		"left": true, "right": true, "full": true, "cross": true, "natural": true,
		"on": true, "using": true, "primary": true, "key": true, "foreign": true,
		"references": true, "unique": true, "check": true, "constraint": true,
		"default": true, "index": true, "alter": true, "add": true, "column": true,
	}
)

// QuoteIdentifier quotes an SQL identifier if necessary
// Follows PostgreSQL rules: quote if contains special chars, is a keyword, or is case-sensitive
func QuoteIdentifier(name string) string {
	if name == "" {
		return ""
	}

	// Check if the identifier needs quoting
	// Must quote if doesn't match identifier pattern
	needsQuoting := !sqlIdentifierRegex.MatchString(name)

	// Must quote if it's a reserved keyword (case-insensitive check)
	if reservedKeywords[strings.ToLower(name)] {
		needsQuoting = true
	}

	// Must quote if it contains uppercase letters (PostgreSQL folds unquoted identifiers to lowercase)
	if strings.ToLower(name) != name {
		needsQuoting = true
	}

	if needsQuoting {
		// Escape any internal double quotes by doubling them
		escaped := strings.ReplaceAll(name, `"`, `""`)
		return `"` + escaped + `"`
	}

	return name
}

// QuoteStringLiteral quotes a string literal for SQL
// Handles escaping of single quotes and other special characters
func QuoteStringLiteral(value string) string {
	// Escape single quotes by doubling them
	escaped := strings.ReplaceAll(value, `'`, `''`)
	return `'` + escaped + `'`
}

// FormatList formats a list of SQL elements with separators
func FormatList(elements []string, separator string) string {
	return strings.Join(elements, separator)
}

// FormatQualifiedName formats a qualified name (e.g., schema.table, database.schema.table)
func FormatQualifiedName(parts ...string) string {
	var quotedParts []string
	for _, part := range parts {
		if part != "" {
			quotedParts = append(quotedParts, QuoteIdentifier(part))
		}
	}
	return strings.Join(quotedParts, ".")
}

// ==============================================================================
// SQL FORMATTING HELPERS
// ==============================================================================

// FormatColumnList formats a list of column names for SQL
func FormatColumnList(columns []string) string {
	if len(columns) == 0 {
		return ""
	}

	var quoted []string
	for _, col := range columns {
		quoted = append(quoted, QuoteIdentifier(col))
	}
	return strings.Join(quoted, ", ")
}

// FormatParentheses wraps content in parentheses if not empty
func FormatParentheses(content string) string {
	if content == "" {
		return ""
	}
	return "(" + content + ")"
}

// FormatOptionalClause formats an optional SQL clause with keyword
func FormatOptionalClause(keyword, content string) string {
	if content == "" {
		return ""
	}
	return keyword + " " + content
}

// FormatCommaList formats a list with commas and proper spacing
func FormatCommaList(items []string) string {
	var nonEmpty []string
	for _, item := range items {
		if item != "" {
			nonEmpty = append(nonEmpty, item)
		}
	}
	return strings.Join(nonEmpty, ", ")
}

// ==============================================================================
// POSTGRESQL-SPECIFIC FORMATTING
// ==============================================================================

// FormatAlias formats an alias clause (AS alias_name)
func FormatAlias(aliasName string) string {
	if aliasName == "" {
		return ""
	}
	return "AS " + QuoteIdentifier(aliasName)
}

// DollarQuoteString wraps a string in dollar quotes, choosing a tag that doesn't conflict with the content
func DollarQuoteString(s string) string {
	// Start with the simplest delimiter
	delimiter := "$$"

	// If the string contains $$, find an alternative delimiter
	if strings.Contains(s, "$$") {
		// Try common alternatives
		alternatives := []string{"$_$", "$body$", "$func$", "$proc$", "$string$"}
		for _, alt := range alternatives {
			if !strings.Contains(s, alt) {
				delimiter = alt
				break
			}
		}

		// If all common alternatives are in use, generate a unique one
		if strings.Contains(s, delimiter) {
			for i := 1; ; i++ {
				delimiter = "$tag" + strings.Repeat("x", i) + "$"
				if !strings.Contains(s, delimiter) {
					break
				}
			}
		}
	}

	return delimiter + s + delimiter
}

// FormatSchemaQualifiedName formats schema.name or just name
func FormatSchemaQualifiedName(schema, name string) string {
	if schema != "" {
		return QuoteIdentifier(schema) + "." + QuoteIdentifier(name)
	}
	return QuoteIdentifier(name)
}

// FormatFullyQualifiedName formats database.schema.name or shorter versions
func FormatFullyQualifiedName(database, schema, name string) string {
	return FormatQualifiedName(database, schema, name)
}

// QuoteQualifiedIdentifier handles dotted identifiers where parts may already be quoted
// This function preserves existing quotes and only quotes parts that need it
func QuoteQualifiedIdentifier(name string) string {
	if name == "" {
		return ""
	}

	// For dotted identifiers, we need to handle each part separately
	// but preserve any existing quotes in the original string
	if strings.Contains(name, ".") {
		// Parse the qualified name, respecting existing quotes
		parts := parseQualifiedIdentifier(name)
		var quotedParts []string
		for _, part := range parts {
			// If the part is already quoted, preserve it as-is
			if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
				quotedParts = append(quotedParts, part)
			} else {
				// Otherwise apply normal quoting rules
				quotedParts = append(quotedParts, QuoteIdentifier(part))
			}
		}
		return strings.Join(quotedParts, ".")
	}

	// Single identifier - use normal quoting
	return QuoteIdentifier(name)
}

// parseQualifiedIdentifier splits a qualified identifier respecting quoted parts
func parseQualifiedIdentifier(name string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false

	for _, r := range name {
		if r == '"' {
			inQuotes = !inQuotes
			current.WriteRune(r)
		} else if r == '.' && !inQuotes {
			// Found a separator outside of quotes
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	// Add the last part
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// printAExprConst formats an expression using the appropriate syntax for constants.
// For TypeCast expressions, it uses the 'type value' syntax instead of 'CAST(value AS type)'.
// For all other expressions, it uses the standard SqlString() method.
func PrintAExprConst(expr Expression) string {
	if expr == nil {
		return ""
	}

	// Check if this is a TypeCast expression
	if typeCast, ok := expr.(*TypeCast); ok {
		// Use the shorter 'type value' syntax instead of CAST(value AS type)
		argStr := ""
		if typeCast.Arg != nil {
			argStr = typeCast.Arg.SqlString()
		}

		typeStr := ""
		if typeCast.TypeName != nil {
			typeStr = typeCast.TypeName.SqlString()
		}

		if typeStr != "" && argStr != "" {
			return typeStr + " " + argStr
		}
	}

	// For all other expressions, use the standard SqlString method
	return expr.SqlString()
}
