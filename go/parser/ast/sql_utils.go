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
		"all": true, "and": true, "as": true, "asc": true, "between": true, "by": true,
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
	needsQuoting := false
	
	// Must quote if doesn't match identifier pattern
	if !sqlIdentifierRegex.MatchString(name) {
		needsQuoting = true
	}
	
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