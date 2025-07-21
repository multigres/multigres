package testutils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewPostgreSQLTestSuite tests the creation of a new test suite.
func TestNewPostgreSQLTestSuite(t *testing.T) {
	suite := NewPostgreSQLTestSuite("/test/postgres/path")
	
	assert.NotNil(t, suite, "Suite should not be nil")
	assert.Equal(t, "/test/postgres/path", suite.baseDir, "Base directory should be set correctly")
	assert.Empty(t, suite.testCases, "Test cases should be empty initially")
}

// TestCreateTempSQLFile tests temporary SQL file creation.
func TestCreateTempSQLFile(t *testing.T) {
	content := "SELECT * FROM users; -- Test query"
	
	filename := CreateTempSQLFile(t, content)
	
	// File should exist
	_, err := os.Stat(filename)
	assert.NoError(t, err, "Temp file should exist")
	
	// File should have correct extension
	assert.True(t, strings.HasSuffix(filename, ".sql"), "File should have .sql extension")
	
	// Read and verify content
	data, err := os.ReadFile(filename)
	require.NoError(t, err, "Should be able to read temp file")
	assert.Equal(t, content, string(data), "Content should match")
}

// TestValidateSQLSyntax tests basic SQL syntax validation.
func TestValidateSQLSyntax(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		shouldError bool
	}{
		{"valid_select", "SELECT * FROM users;", false},
		{"valid_insert", "INSERT INTO users (name) VALUES ('test');", false},
		{"empty_sql", "", true},
		{"whitespace_only", "   ", true},
		{"missing_semicolon", "SELECT * FROM users", true},
		{"valid_with_whitespace", "  SELECT * FROM users;  ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSQLSyntax(tt.sql)
			
			if tt.shouldError {
				assert.Error(t, err, "Expected error for SQL: %s", tt.sql)
			} else {
				assert.NoError(t, err, "Expected no error for SQL: %s", tt.sql)
			}
		})
	}
}

// TestExtractSQLFromComment tests SQL extraction from comments.
func TestExtractSQLFromComment(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected []string
	}{
		{
			name:     "single_sql_statement",
			comment:  "-- SELECT * FROM users;",
			expected: []string{"SELECT * FROM users;"},
		},
		{
			name: "multiple_statements",
			comment: `-- First statement
-- SELECT * FROM users;
-- Second statement  
-- INSERT INTO logs VALUES (1);`,
			expected: []string{"SELECT * FROM users;", "INSERT INTO logs VALUES (1);"},
		},
		{
			name:     "no_sql_statements",
			comment:  "-- This is just a comment\n-- No SQL here",
			expected: []string{},
		},
		{
			name: "mixed_content",
			comment: `-- Some description
-- SELECT count(*) FROM table1;
-- More description
-- Another comment without SQL`,
			expected: []string{"SELECT count(*) FROM table1;"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractSQLFromComment(tt.comment)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParsePostgreSQLVersion tests PostgreSQL version parsing.
func TestParsePostgreSQLVersion(t *testing.T) {
	tests := []struct {
		name        string
		versionStr  string
		expectedVer *PostgreSQLVersionInfo
		shouldError bool
	}{
		{
			name:       "valid_version_16_1",
			versionStr: "PostgreSQL 16.1",
			expectedVer: &PostgreSQLVersionInfo{
				Major:  16,
				Minor:  1,
				String: "PostgreSQL 16.1",
			},
			shouldError: false,
		},
		{
			name:       "valid_version_15_2",
			versionStr: "PostgreSQL 15.2",
			expectedVer: &PostgreSQLVersionInfo{
				Major:  15,
				Minor:  2,
				String: "PostgreSQL 15.2",
			},
			shouldError: false,
		},
		{
			name:        "invalid_format",
			versionStr:  "MySQL 8.0",
			expectedVer: nil,
			shouldError: true,
		},
		{
			name:        "empty_string",
			versionStr:  "",
			expectedVer: nil,
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePostgreSQLVersion(tt.versionStr)
			
			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expectedVer.Major, result.Major)
				assert.Equal(t, tt.expectedVer.Minor, result.Minor)
				assert.Equal(t, tt.expectedVer.String, result.String)
			}
		})
	}
}

// TestIsErrorCase tests the error case detection logic.
func TestIsErrorCase(t *testing.T) {
	suite := NewPostgreSQLTestSuite("")
	
	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{"normal_select", "SELECT * FROM users;", false},
		{"syntax_error_case", "SELECT * FROM users SYNTAX ERROR;", true},
		{"invalid_statement", "INVALID QUERY HERE;", true},
		{"error_keyword", "SELECT ERROR FROM table;", true},
		{"normal_insert", "INSERT INTO users VALUES (1, 'test');", false},
		{"case_insensitive", "select error from table;", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := suite.isErrorCase(tt.sql)
			assert.Equal(t, tt.expected, result, "Error case detection mismatch for: %s", tt.sql)
		})
	}
}

// TestPostgreSQLTestCase tests the test case structure.
func TestPostgreSQLTestCase(t *testing.T) {
	testCase := PostgreSQLTestCase{
		Name:        "test_select",
		SQL:         "SELECT * FROM users;",
		Expected:    "success",
		ShouldError: false,
		ErrorType:   "",
		SourceFile:  "select.sql",
		LineNumber:  10,
	}
	
	assert.Equal(t, "test_select", testCase.Name)
	assert.Equal(t, "SELECT * FROM users;", testCase.SQL)
	assert.False(t, testCase.ShouldError)
	assert.Equal(t, "select.sql", testCase.SourceFile)
	assert.Equal(t, 10, testCase.LineNumber)
}

// TestRunTestCase tests individual test case execution.
func TestRunTestCase(t *testing.T) {
	suite := NewPostgreSQLTestSuite("")
	
	// Test case that should succeed
	successCase := PostgreSQLTestCase{
		Name:        "success_case",
		SQL:         "SELECT * FROM users;",
		ShouldError: false,
		SourceFile:  "test.sql",
		LineNumber:  1,
	}
	
	// Test case that should fail
	errorCase := PostgreSQLTestCase{
		Name:        "error_case",
		SQL:         "INVALID SYNTAX;",
		ShouldError: true,
		SourceFile:  "test.sql",
		LineNumber:  2,
	}
	
	// Mock parser function that returns error for "INVALID"
	mockParser := func(sql string) error {
		if strings.Contains(sql, "INVALID") {
			return assert.AnError
		}
		return nil
	}
	
	// Test successful case
	t.Run("success_case", func(t *testing.T) {
		// This should not cause test failure
		suite.RunTestCase(t, successCase, mockParser)
	})
	
	// Test error case  
	t.Run("error_case", func(t *testing.T) {
		// This should not cause test failure (error is expected)
		suite.RunTestCase(t, errorCase, mockParser)
	})
}

// TestPostgreSQLKeywordTestData tests the keyword test data structure.
func TestPostgreSQLKeywordTestData(t *testing.T) {
	data := &PostgreSQLKeywordTestData{
		ValidKeywords:    []string{"select", "from", "where"},
		InvalidKeywords:  []string{"notakeyword", "fake123"},
		ReservedKeywords: []string{"select", "from"},
	}
	
	assert.Len(t, data.ValidKeywords, 3)
	assert.Len(t, data.InvalidKeywords, 2)
	assert.Len(t, data.ReservedKeywords, 2)
	
	assert.Contains(t, data.ValidKeywords, "select")
	assert.Contains(t, data.InvalidKeywords, "notakeyword")
	assert.Contains(t, data.ReservedKeywords, "from")
}

// TestAssertKeywordBehavior tests the keyword behavior validation.
func TestAssertKeywordBehavior(t *testing.T) {
	// Mock data
	testData := &PostgreSQLKeywordTestData{
		ValidKeywords:    []string{"select", "from"},
		InvalidKeywords:  []string{"notakeyword"},
		ReservedKeywords: []string{"select"},
	}
	
	// Mock functions
	mockKeywordFunc := func(word string) bool {
		word = strings.ToLower(word)
		return word == "select" || word == "from"
	}
	
	mockReservedFunc := func(word string) bool {
		word = strings.ToLower(word)
		return word == "select"
	}
	
	// This should not fail the test if our mock functions are correct
	AssertKeywordBehavior(t, mockKeywordFunc, mockReservedFunc, testData)
}

// TestLoadSQLTestFileWithMockData tests SQL test file loading with mock data.
func TestLoadSQLTestFileWithMockData(t *testing.T) {
	// Create a temporary directory structure
	tmpDir := t.TempDir()
	testDir := filepath.Join(tmpDir, "src", "test", "regress", "sql")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)
	
	// Create a mock SQL test file
	testContent := `-- Test file for parser
-- This is a comment

SELECT * FROM users;

-- Another comment
INSERT INTO users (name) VALUES ('test');

-- Multi-line statement
UPDATE users 
SET name = 'updated'
WHERE id = 1;`
	
	testFile := filepath.Join(testDir, "mock_test.sql")
	err = os.WriteFile(testFile, []byte(testContent), 0644)
	require.NoError(t, err)
	
	// Test loading
	suite := NewPostgreSQLTestSuite(tmpDir)
	err = suite.LoadSQLTestFile("mock_test.sql")
	
	assert.NoError(t, err, "Should load mock test file without error")
	
	testCases := suite.GetTestCases()
	assert.NotEmpty(t, testCases, "Should have loaded test cases")
	
	// Verify test cases
	for _, tc := range testCases {
		assert.NotEmpty(t, tc.SQL, "Test case should have SQL")
		assert.NotEmpty(t, tc.Name, "Test case should have name")
		assert.Equal(t, "mock_test.sql", tc.SourceFile, "Source file should be set")
		assert.Greater(t, tc.LineNumber, 0, "Line number should be set")
	}
}