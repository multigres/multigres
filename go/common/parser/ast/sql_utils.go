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

// SQL identifier regex: must start with letter or underscore, followed by
// letters, digits, underscores, or dollar signs.
var sqlIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]*$`)

// identifierKeywordNeedsQuoting reports whether an identifier that is a keyword
// must be quoted to be used as a column/table identifier: reserved and
// type_func_name keywords must, but col_name keywords must NOT (they are usable
// as column names, and this predicate also governs type-name deparsing, where
// PostgreSQL keeps them bare via format_type). Mirrors quote_identifier's keyword
// test, minus the col_name category which QuoteFunctionName handles separately.
func identifierKeywordNeedsQuoting(name string) bool {
	cat, ok := LookupKeywordCategory(strings.ToLower(name))
	return ok && cat != UnreservedKeyword && cat != ColNameKeyword
}

// QuoteIdentifier quotes an SQL identifier if necessary
// Follows PostgreSQL rules: quote if contains special chars, is a keyword, or is case-sensitive
func QuoteIdentifier(name string) string {
	if name == "" {
		return ""
	}

	// Check if the identifier needs quoting
	// Must quote if doesn't match identifier pattern
	needsQuoting := !sqlIdentifierRegex.MatchString(name)

	// Must quote if it's a keyword that can't be used as a column name (case-insensitive check)
	if identifierKeywordNeedsQuoting(name) {
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

// funcNameKeywordCollisions are the col_name keywords whose special grammar
// production rejects an ordinary function call, so a bare `kw(args)` does not
// parse as a call to a function named kw at all — it is a syntax error. When such
// a name nonetheless reaches the deparser as a plain FuncCall (only from an
// explicitly quoted call, e.g. "normalize"(a, b)), it must be re-quoted or the
// backend re-parses it as the keyword's special syntax.
//
// This is a deliberately small subset of the col_name category. The rest
// (json_object, coalesce, greatest, …) either accept a bare generic call as a
// function or parse to a dedicated node, so — like PostgreSQL's ruleutils — we
// emit them bare. TestFuncNameKeywordQuoting guards that every keyword here
// genuinely needs the quoting (and that none has quietly become quotable-bare).
var funcNameKeywordCollisions = map[string]bool{
	"normalize":  true,
	"treat":      true,
	"values":     true,
	"xmlelement": true,
	"xmlexists":  true,
}

// KeywordNeedsFunctionQuoting reports whether name (lower case) must be quoted
// when emitted as a bare function name. Exported for the parser-package guard
// test, which verifies each such keyword really would be mis-parsed bare.
func KeywordNeedsFunctionQuoting(name string) bool {
	return funcNameKeywordCollisions[name]
}

// QuoteFunctionName quotes name for use as a function name in deparsed SQL. It
// applies the same rules as QuoteIdentifier, and additionally quotes the handful
// of col_name keywords whose bare function-call form the grammar would reject
// (funcNameKeywordCollisions): left unquoted, the backend re-parses the call as
// the keyword's special syntax rather than a function call.
func QuoteFunctionName(name string) string {
	if funcNameKeywordCollisions[strings.ToLower(name)] {
		escaped := strings.ReplaceAll(name, `"`, `""`)
		return `"` + escaped + `"`
	}
	return QuoteIdentifier(name)
}

// operatorNameChars are the characters a PostgreSQL operator name is built from
// (see the "self"/"op_chars" sets in the scanner). An identifier never contains
// any of these.
const operatorNameChars = "+-*/<>=~!@#%^&|`?"

// isOperatorName reports whether name looks like an operator (e.g. "+", "~~",
// "->>") rather than an identifier. Operator names are emitted verbatim;
// QuoteIdentifier would wrongly wrap them in double quotes.
func isOperatorName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !strings.ContainsRune(operatorNameChars, r) {
			return false
		}
	}
	return true
}

// QuoteIdentifierOrOperator quotes name as an identifier, unless it is an
// operator name, which is returned unchanged. Use it for qualified object names
// (e.g. ObjectWithArgs) whose parts may be either a function/type identifier or
// an operator symbol.
func QuoteIdentifierOrOperator(name string) string {
	if isOperatorName(name) {
		return name
	}
	return QuoteIdentifier(name)
}

// QuoteStringLiteral quotes a string literal for SQL.
//
// A value containing a backslash is emitted as an escape string E'...' with the
// backslash doubled, rather than an ordinary '...' literal. This keeps the
// literal scs-independent: an ordinary '...' is interpreted differently under
// standard_conforming_strings=off (backslashes become escapes), so re-emitting a
// reconstructed query as '...' would double-decode the value at the backend
// (e.g. E'a\\b', value a\b, would come back as a<backspace>b). E'...' decodes
// identically regardless of the backend's standard_conforming_strings setting.
// Single quotes are doubled in both forms.
func QuoteStringLiteral(value string) string {
	if strings.Contains(value, `\`) {
		escaped := strings.ReplaceAll(value, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `'`, `''`)
		return `E'` + escaped + `'`
	}
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
