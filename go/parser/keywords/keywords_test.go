package keywords

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKeywordLookup tests basic keyword lookup functionality.
// Validates against PostgreSQL keyword behavior from postgres/src/include/parser/kwlist.h
func TestKeywordLookup(t *testing.T) {
	tests := []struct {
		name         string
		keyword      string
		shouldExist  bool
		expectedToken Token
		expectedCategory KeywordCategory
		expectedCanBareLabel bool
	}{
		// Test reserved keywords - from postgres/src/include/parser/kwlist.h:37-43
		{"reserved_all", "all", true, ALL, ReservedKeyword, true},
		{"reserved_and", "and", true, AND, ReservedKeyword, true},
		{"reserved_any", "any", true, ANY, ReservedKeyword, true},
		{"reserved_array", "array", true, ARRAY, ReservedKeyword, false},
		{"reserved_as", "as", true, AS, ReservedKeyword, false},
		
		// Test unreserved keywords - from postgres/src/include/parser/kwlist.h:28-36  
		{"unreserved_abort", "abort", true, ABORT_P, UnreservedKeyword, true},
		{"unreserved_absolute", "absolute", true, ABSOLUTE_P, UnreservedKeyword, true},
		{"unreserved_action", "action", true, ACTION, UnreservedKeyword, true},
		
		// Test column name keywords - from postgres/src/include/parser/kwlist.h:60-65
		{"colname_between", "between", true, BETWEEN, ColNameKeyword, true},
		{"colname_bigint", "bigint", true, BIGINT, ColNameKeyword, true},
		{"colname_bit", "bit", true, BIT, ColNameKeyword, true},
		
		// Test type/function name keywords - from postgres/src/include/parser/kwlist.h:56,62
		{"typefunc_authorization", "authorization", true, AUTHORIZATION, TypeFuncNameKeyword, true},
		{"typefunc_binary", "binary", true, BINARY, TypeFuncNameKeyword, true},
		
		// Test case insensitivity
		{"case_insensitive_upper", "SELECT", false, 0, UnreservedKeyword, false}, // Not in our partial list
		{"case_insensitive_mixed", "Select", false, 0, UnreservedKeyword, false}, // Not in our partial list
		{"case_insensitive_all_upper", "ALL", true, ALL, ReservedKeyword, true},
		{"case_insensitive_all_mixed", "All", true, ALL, ReservedKeyword, true},
		
		// Test non-keywords
		{"non_keyword_random", "randomtext", false, 0, UnreservedKeyword, false},
		{"non_keyword_empty", "", false, 0, UnreservedKeyword, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw := LookupKeyword(tt.keyword)
			
			if tt.shouldExist {
				require.NotNil(t, kw, "Expected keyword '%s' to exist", tt.keyword)
				assert.Equal(t, tt.expectedToken, kw.Token, "Token mismatch for keyword '%s'", tt.keyword)
				assert.Equal(t, tt.expectedCategory, kw.Category, "Category mismatch for keyword '%s'", tt.keyword)
				assert.Equal(t, tt.expectedCanBareLabel, kw.CanBareLabel, "CanBareLabel mismatch for keyword '%s'", tt.keyword)
			} else {
				assert.Nil(t, kw, "Expected keyword '%s' to not exist", tt.keyword)
			}
		})
	}
}

// TestIsKeyword tests the convenience function for checking keyword existence.
func TestIsKeyword(t *testing.T) {
	tests := []struct {
		name     string
		keyword  string
		expected bool
	}{
		{"existing_keyword", "all", true},
		{"existing_keyword_mixed_case", "All", true},
		{"non_existing_keyword", "notakeyword", false},
		{"empty_string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsKeyword(tt.keyword)
			assert.Equal(t, tt.expected, result, "IsKeyword result mismatch for '%s'", tt.keyword)
		})
	}
}

// TestIsReservedKeyword tests reserved keyword detection.
// Based on PostgreSQL reserved keyword categories from postgres/src/include/parser/kwlist.h
func TestIsReservedKeyword(t *testing.T) {
	tests := []struct {
		name     string
		keyword  string
		expected bool
	}{
		// Reserved keywords
		{"reserved_all", "all", true},
		{"reserved_and", "and", true},
		{"reserved_case", "case", true},
		{"reserved_mixed_case", "Case", true},
		
		// Non-reserved keywords  
		{"unreserved_abort", "abort", false},
		{"unreserved_action", "action", false},
		{"colname_between", "between", false},
		{"typefunc_authorization", "authorization", false},
		
		// Non-keywords
		{"non_keyword", "notakeyword", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsReservedKeyword(tt.keyword)
			assert.Equal(t, tt.expected, result, "IsReservedKeyword result mismatch for '%s'", tt.keyword)
		})
	}
}

// TestGetKeywordNames tests the keyword name retrieval function.
func TestGetKeywordNames(t *testing.T) {
	names := GetKeywordNames()
	
	// Should have all keywords from our current list
	assert.Len(t, names, len(Keywords), "Should return all keyword names")
	
	// Should be sorted
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "Names should be sorted alphabetically")
	}
	
	// Should contain some expected keywords
	expectedKeywords := []string{"abort", "all", "and", "array", "as"}
	for _, expected := range expectedKeywords {
		assert.Contains(t, names, expected, "Should contain keyword '%s'", expected)
	}
}

// TestGetKeywordsByCategory tests keyword filtering by category.
func TestGetKeywordsByCategory(t *testing.T) {
	tests := []struct {
		name                string
		category           KeywordCategory
		expectedMinCount   int
		expectedKeywords   []string
	}{
		{
			name:               "reserved_keywords",
			category:           ReservedKeyword,
			expectedMinCount:   5, // We have at least all, and, any, array, as, etc.
			expectedKeywords:   []string{"all", "and", "case", "cast"},
		},
		{
			name:               "unreserved_keywords", 
			category:           UnreservedKeyword,
			expectedMinCount:   10, // We have many unreserved keywords
			expectedKeywords:   []string{"abort", "action", "admin", "after"},
		},
		{
			name:               "colname_keywords",
			category:           ColNameKeyword,
			expectedMinCount:   3, // between, bigint, bit, etc.
			expectedKeywords:   []string{"between", "bigint", "bit"},
		},
		{
			name:               "typefunc_keywords",
			category:           TypeFuncNameKeyword,
			expectedMinCount:   2, // authorization, binary, etc.
			expectedKeywords:   []string{"authorization", "binary"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keywords := GetKeywordsByCategory(tt.category)
			
			assert.GreaterOrEqual(t, len(keywords), tt.expectedMinCount, 
				"Should have at least %d keywords in category %s", tt.expectedMinCount, tt.category)
			
			// Verify all returned keywords have the correct category
			for _, kw := range keywords {
				assert.Equal(t, tt.category, kw.Category, 
					"Keyword '%s' should have category %s", kw.Name, tt.category)
			}
			
			// Check for expected keywords
			keywordNames := make(map[string]bool)
			for _, kw := range keywords {
				keywordNames[kw.Name] = true
			}
			
			for _, expected := range tt.expectedKeywords {
				assert.True(t, keywordNames[expected], 
					"Should contain keyword '%s' in category %s", expected, tt.category)
			}
		})
	}
}

// TestKeywordCategoryString tests the String method of KeywordCategory.
func TestKeywordCategoryString(t *testing.T) {
	tests := []struct {
		category KeywordCategory
		expected string
	}{
		{UnreservedKeyword, "UNRESERVED_KEYWORD"},
		{ColNameKeyword, "COL_NAME_KEYWORD"},
		{TypeFuncNameKeyword, "TYPE_FUNC_NAME_KEYWORD"},
		{ReservedKeyword, "RESERVED_KEYWORD"},
		{KeywordCategory(999), "UNKNOWN_KEYWORD_CATEGORY"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.category.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestTokenString tests the String method of Token (basic functionality).
func TestTokenString(t *testing.T) {
	tests := []struct {
		name     string
		token    Token
		expected string
	}{
		{"all_token", ALL, "ALL"},
		{"abort_token", ABORT_P, "ABORT"},
		{"unknown_token", Token(99999), "UNKNOWN_TOKEN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.token.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// BenchmarkKeywordLookup benchmarks the keyword lookup performance.
// Should be fast enough for parser usage.
func BenchmarkKeywordLookup(b *testing.B) {
	keywords := []string{"select", "from", "where", "and", "or", "all", "distinct", "notakeyword"}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, kw := range keywords {
			LookupKeyword(kw)
		}
	}
}

// TestKeywordInitialization verifies that the keyword system initializes correctly.
func TestKeywordInitialization(t *testing.T) {
	// Test that keywordLookupMap is initialized
	assert.NotNil(t, keywordLookupMap, "keywordLookupMap should be initialized")
	assert.Equal(t, len(Keywords), len(keywordLookupMap), "Map should contain all keywords")
	
	// Test that all keywords are properly indexed
	for _, kw := range Keywords {
		mapEntry, exists := keywordLookupMap[kw.Name]
		assert.True(t, exists, "Keyword '%s' should exist in lookup map", kw.Name)
		assert.Equal(t, &kw, mapEntry, "Map entry should point to correct keyword struct")
	}
}