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
 * PostgreSQL Parser Lexer - Keyword Recognition Tests
 *
 * Tests for keyword recognition, optimization, and context-sensitive handling.
 * Based on PostgreSQL's keyword lookup and context handling patterns.
 */

package parser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// TestFuncNameKeywordQuoting guards ast.funcNameKeywordCollisions: every keyword
// flagged as needing quoting when emitted as a function name must genuinely be
// mis-parsed bare — a bare `kw(1, 2)` must NOT parse to a plain function call
// named kw. If it does (like json_object), the quoting is unnecessary and the
// keyword should not be in the set.
func TestFuncNameKeywordQuoting(t *testing.T) {
	for _, name := range ast.KeywordNames() {
		if !ast.KeywordNeedsFunctionQuoting(name) {
			continue
		}
		stmts, err := ParseSQL("SELECT " + name + "(1, 2)")
		if err != nil {
			// Bare call is a syntax error, so quoting is required. Good.
			continue
		}
		for _, s := range stmts {
			ast.Rewrite(s, func(c *ast.Cursor) bool {
				fc, ok := c.Node().(*ast.FuncCall)
				if !ok || fc.Funcname == nil || len(fc.Funcname.Items) == 0 {
					return true
				}
				last, ok := fc.Funcname.Items[len(fc.Funcname.Items)-1].(*ast.String)
				if ok {
					assert.NotEqualf(t, name, last.SVal,
						"%q is in funcNameKeywordCollisions but bare %q(1, 2) parses as a plain function call — quoting is unnecessary",
						name, name)
				}
				return true
			}, nil)
		}
	}
}

// TestKeywordTablesInSync guards the split keyword tables: this package's
// name -> token table (Keywords) and the ast package's name -> classification
// table (the single source of category / bare-label data) must list exactly the
// same keywords, and each parser entry's Category/CanBareLabel must match what
// init copied from ast. A keyword added to one table but not the other fails
// here.
func TestKeywordTablesInSync(t *testing.T) {
	parserNames := make(map[string]bool, len(Keywords))
	for _, kw := range Keywords {
		parserNames[kw.Name] = true
	}
	astNames := make(map[string]bool)
	for _, n := range ast.KeywordNames() {
		astNames[n] = true
	}

	require.Equal(t, len(astNames), len(parserNames), "parser vs ast keyword count mismatch")
	for name := range parserNames {
		_, ok := ast.LookupKeywordClass(name)
		assert.Truef(t, ok, "parser keyword %q missing from ast classification", name)
	}
	for name := range astNames {
		assert.Truef(t, parserNames[name], "ast keyword %q missing from parser token table", name)
	}

	// Category / CanBareLabel must be correctly populated from ast at init.
	for _, kw := range Keywords {
		cls, ok := ast.LookupKeywordClass(kw.Name)
		if !ok {
			continue
		}
		assert.Equalf(t, cls.Category, kw.Category, "category mismatch for %q", kw.Name)
		assert.Equalf(t, cls.CanBareLabel, kw.CanBareLabel, "bare-label mismatch for %q", kw.Name)
	}
}

// TestKeywordLookup tests basic keyword lookup functionality
func TestKeywordLookup(t *testing.T) {
	testCases := []struct {
		name             string
		input            string
		expectedFound    bool
		expectedToken    TokenType
		expectedCategory KeywordCategory
	}{
		// Basic reserved keywords
		{"select keyword", "select", true, SELECT, ReservedKeyword},
		{"SELECT uppercase", "SELECT", true, SELECT, ReservedKeyword},
		{"from keyword", "from", true, FROM, ReservedKeyword},
		{"where keyword", "where", true, WHERE, ReservedKeyword},

		// Column name keywords
		{"integer type", "integer", true, INTEGER, ColNameKeyword},
		{"boolean type", "boolean", true, BOOLEAN_P, ColNameKeyword},

		// Unreserved keywords
		{"breadth keyword", "breadth", true, BREADTH, UnreservedKeyword},
		{"cycle keyword", "cycle", true, CYCLE, UnreservedKeyword},

		// Non-keywords
		{"regular identifier", "mycolumn", false, IDENT, UnreservedKeyword},
		{"long identifier", "very_long_identifier_name", false, IDENT, UnreservedKeyword},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			keyword := LookupKeyword(tc.input)
			if tc.expectedFound {
				require.NotNil(t, keyword, "Expected to find keyword: %s", tc.input)
				assert.Equal(t, tc.expectedToken, keyword.TokenType, "Token type mismatch for %s", tc.input)
				assert.Equal(t, tc.expectedCategory, keyword.Category, "Category mismatch for %s", tc.input)
			} else {
				assert.Nil(t, keyword, "Expected not to find keyword: %s", tc.input)
			}
		})
	}
}

// TestKeywordLookupOptimizations tests the keyword lookup optimizations
func TestKeywordLookupOptimizations(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expectedFound bool
	}{
		// Normal keywords
		{"select lowercase", "select", true},
		{"SELECT uppercase", "SELECT", true},
		{"MiXeD case", "sElEcT", true},

		// Very long strings should be rejected early
		{"very long non-keyword", strings.Repeat("a", 50), false},
		{"long keyword-like", "very_long_identifier_that_looks_like_keyword", false},

		// Edge cases
		{"empty string", "", false},
		{"single char", "a", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			keyword := LookupKeyword(tc.input)
			if tc.expectedFound {
				assert.NotNil(t, keyword, "Expected to find keyword: %s", tc.input)
			} else {
				assert.Nil(t, keyword, "Expected not to find keyword: %s", tc.input)
			}
		})
	}
}

// TestNormalizeKeywordCase tests the case normalization function
func TestNormalizeKeywordCase(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"lowercase unchanged", "select", "select"},
		{"uppercase converted", "SELECT", "select"},
		{"mixed case", "SeLeCt", "select"},
		{"numbers unchanged", "table123", "table123"},
		{"underscores unchanged", "current_user", "current_user"},
		{"mixed with underscore", "CURRENT_USER", "current_user"},
		{"empty string", "", ""},
		{"single char lower", "a", "a"},
		{"single char upper", "A", "a"},
		{"non-ASCII unchanged", "café", "café"}, // PostgreSQL ASCII-only conversion
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := normalizeKeywordCase(tc.input)
			assert.Equal(t, tc.expected, result, "Case normalization failed for: %s", tc.input)
		})
	}
}

// TestKeywordCategories tests keyword categorization
func TestKeywordCategories(t *testing.T) {
	categories := map[KeywordCategory][]string{
		ReservedKeyword:     {"select", "from", "where", "and", "or"},
		ColNameKeyword:      {"integer", "boolean", "char", "time"},
		TypeFuncNameKeyword: {"left", "right", "join", "full"},
		UnreservedKeyword:   {"breadth", "cycle", "by"},
	}

	for category, keywords := range categories {
		for _, kw := range keywords {
			t.Run(fmt.Sprintf("%s_%s", category, kw), func(t *testing.T) {
				keyword := LookupKeyword(kw)
				require.NotNil(t, keyword, "Expected to find keyword: %s", kw)
				assert.Equal(t, category, keyword.Category,
					"Category mismatch for %s: expected %s, got %s",
					kw, category, keyword.Category)
			})
		}
	}
}

// TestKeywordCategoryString tests string representation of categories
func TestKeywordCategoryString(t *testing.T) {
	testCases := []struct {
		category KeywordCategory
		expected string
	}{
		{UnreservedKeyword, "UNRESERVED_KEYWORD"},
		{ColNameKeyword, "COL_NAME_KEYWORD"},
		{TypeFuncNameKeyword, "TYPE_FUNC_NAME_KEYWORD"},
		{ReservedKeyword, "RESERVED_KEYWORD"},
		{KeywordCategory(999), "UNKNOWN_KEYWORD_CATEGORY"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := tc.category.String()
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestGetKeywordsByCategory tests category filtering
func TestGetKeywordsByCategory(t *testing.T) {
	reserved := GetKeywordsByCategory(ReservedKeyword)
	colName := GetKeywordsByCategory(ColNameKeyword)
	typeFuncName := GetKeywordsByCategory(TypeFuncNameKeyword)
	unreserved := GetKeywordsByCategory(UnreservedKeyword)

	// Basic validation
	assert.Greater(t, len(reserved), 0, "Should have reserved keywords")
	assert.Greater(t, len(colName), 0, "Should have column name keywords")
	assert.Greater(t, len(typeFuncName), 0, "Should have type/function keywords")
	assert.Greater(t, len(unreserved), 0, "Should have unreserved keywords")

	// Check that all returned keywords actually have the right category
	for _, kw := range reserved {
		assert.Equal(t, ReservedKeyword, kw.Category, "Reserved keyword %s has wrong category", kw.Name)
	}

	for _, kw := range colName {
		assert.Equal(t, ColNameKeyword, kw.Category, "Column name keyword %s has wrong category", kw.Name)
	}
}

// TestIsKeyword tests the convenience function
func TestIsKeyword(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"select", true},
		{"SELECT", true},
		{"integer", true},
		{"notakeyword", false},
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := IsKeyword(tc.input)
			assert.Equal(t, tc.expected, result, "IsKeyword result for: %s", tc.input)
		})
	}
}

// TestIsReservedKeyword tests reserved keyword detection
func TestIsReservedKeyword(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"select", true},
		{"from", true},
		{"integer", false}, // column name keyword
		{"cycle", false},   // unreserved keyword
		{"notakeyword", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := IsReservedKeyword(tc.input)
			assert.Equal(t, tc.expected, result, "IsReservedKeyword result for: %s", tc.input)
		})
	}
}

// Benchmark tests for performance optimization validation

// BenchmarkKeywordLookup benchmarks the keyword lookup performance
func BenchmarkKeywordLookup(b *testing.B) {
	testKeywords := []string{"select", "from", "where", "integer", "boolean", "notakeyword"}

	for b.Loop() {
		for _, kw := range testKeywords {
			LookupKeyword(kw)
		}
	}
}

// BenchmarkKeywordLookupLongStrings benchmarks lookup with very long strings
func BenchmarkKeywordLookupLongStrings(b *testing.B) {
	longStrings := []string{
		strings.Repeat("a", 100),
		strings.Repeat("select", 20),
		strings.Repeat("identifier", 10),
	}

	for b.Loop() {
		for _, s := range longStrings {
			LookupKeyword(s)
		}
	}
}

// BenchmarkNormalizeKeywordCase benchmarks case normalization
func BenchmarkNormalizeKeywordCase(b *testing.B) {
	testCases := []string{
		"select",            // no conversion needed
		"SELECT",            // full conversion
		"SeLeCt",            // mixed case
		"current_user",      // longer keyword
		"CURRENT_TIMESTAMP", // longest keyword with conversion
	}

	for b.Loop() {
		for _, tc := range testCases {
			normalizeKeywordCase(tc)
		}
	}
}
