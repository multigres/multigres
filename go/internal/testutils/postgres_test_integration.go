// Package testutils provides utilities for integrating PostgreSQL regression tests
// with the Go parser implementation.
// Ported from postgres/src/test/ test integration patterns
package testutils

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PostgreSQLTestCase represents a test case derived from PostgreSQL regression tests.
// Based on structure from postgres/src/test/regress/ test files
type PostgreSQLTestCase struct {
	Name        string // Test case name
	SQL         string // SQL statement to parse
	Expected    string // Expected result or behavior
	ShouldError bool   // Whether this test should produce an error
	ErrorType   string // Type of error expected
	SourceFile  string // Original PostgreSQL test file
	LineNumber  int    // Line number in source file
}

// PostgreSQLTestSuite manages a collection of PostgreSQL test cases.
// Provides integration with testify test suites for organized testing.
type PostgreSQLTestSuite struct {
	testCases []PostgreSQLTestCase
	baseDir   string // Base directory for PostgreSQL source
}

// NewPostgreSQLTestSuite creates a new test suite for PostgreSQL integration tests.
// baseDir should point to the PostgreSQL source directory (e.g., ../postgres)
func NewPostgreSQLTestSuite(baseDir string) *PostgreSQLTestSuite {
	return &PostgreSQLTestSuite{
		testCases: make([]PostgreSQLTestCase, 0),
		baseDir:   baseDir,
	}
}

// LoadSQLTestFile reads a PostgreSQL SQL test file and extracts test cases.
// Ported from postgres/src/test/regress/ test file parsing logic
func (suite *PostgreSQLTestSuite) LoadSQLTestFile(filename string) error {
	fullPath := filepath.Join(suite.baseDir, "src", "test", "regress", "sql", filename)
	
	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open PostgreSQL test file %s: %w", fullPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	var currentSQL strings.Builder

	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		
		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		
		// Handle multi-line SQL statements
		currentSQL.WriteString(line)
		currentSQL.WriteString(" ")
		
		// Check if statement is complete (ends with semicolon)
		if strings.HasSuffix(line, ";") {
			sql := strings.TrimSpace(currentSQL.String())
			if sql != "" {
				testCase := PostgreSQLTestCase{
					Name:       fmt.Sprintf("%s_line_%d", strings.TrimSuffix(filename, ".sql"), lineNumber),
					SQL:        sql,
					SourceFile: filename,
					LineNumber: lineNumber,
				}
				
				// Determine if this should be an error case
				testCase.ShouldError = suite.isErrorCase(sql)
				suite.testCases = append(suite.testCases, testCase)
			}
			currentSQL.Reset()
		}
	}

	return scanner.Err()
}

// LoadExpectedResultFile reads the expected results for a test file.
// Ported from postgres/src/test/regress/expected/ file parsing
func (suite *PostgreSQLTestSuite) LoadExpectedResultFile(filename string) error {
	expectedFile := strings.TrimSuffix(filename, ".sql") + ".out"
	fullPath := filepath.Join(suite.baseDir, "src", "test", "regress", "expected", expectedFile)
	
	file, err := os.Open(fullPath)
	if err != nil {
		// Expected files might not exist for all test files
		return nil
	}
	defer file.Close()

	// TODO: Parse expected results and match with test cases
	// This is a placeholder for Phase 2 implementation
	return nil
}

// isErrorCase determines if a SQL statement should produce a parse error.
// Based on patterns commonly found in PostgreSQL negative test cases
func (suite *PostgreSQLTestSuite) isErrorCase(sql string) bool {
	sql = strings.ToUpper(sql)
	
	// Common patterns that indicate error cases
	errorPatterns := []string{
		"SYNTAX ERROR",
		"INVALID",
		"ERROR",
		"FAIL",
	}
	
	for _, pattern := range errorPatterns {
		if strings.Contains(sql, pattern) {
			return true
		}
	}
	
	return false
}

// GetTestCases returns all loaded test cases.
func (suite *PostgreSQLTestSuite) GetTestCases() []PostgreSQLTestCase {
	return suite.testCases
}

// RunTestCase executes a single PostgreSQL test case using a parser function.
// The parseFunc should be provided by the specific parser component being tested.
func (suite *PostgreSQLTestSuite) RunTestCase(t *testing.T, testCase PostgreSQLTestCase, parseFunc func(string) error) {
	t.Run(testCase.Name, func(t *testing.T) {
		err := parseFunc(testCase.SQL)
		
		if testCase.ShouldError {
			assert.Error(t, err, "Expected error for SQL: %s (from %s:%d)", 
				testCase.SQL, testCase.SourceFile, testCase.LineNumber)
		} else {
			assert.NoError(t, err, "Unexpected error for SQL: %s (from %s:%d): %v", 
				testCase.SQL, testCase.SourceFile, testCase.LineNumber, err)
		}
	})
}

// RunAllTestCases executes all loaded test cases using the provided parser function.
func (suite *PostgreSQLTestSuite) RunAllTestCases(t *testing.T, parseFunc func(string) error) {
	require.NotEmpty(t, suite.testCases, "No test cases loaded")
	
	for _, testCase := range suite.testCases {
		suite.RunTestCase(t, testCase, parseFunc)
	}
}

// PostgreSQLKeywordTestData provides test data specifically for keyword testing.
// Ported from postgres/src/backend/parser/check_keywords.pl test patterns
type PostgreSQLKeywordTestData struct {
	ValidKeywords   []string // Keywords that should be recognized
	InvalidKeywords []string // Strings that should not be keywords
	ReservedKeywords []string // Keywords that should be reserved
}

// LoadKeywordTestData loads keyword test data from PostgreSQL source.
// Based on patterns from postgres/src/include/parser/kwlist.h
func LoadKeywordTestData(postgresBaseDir string) (*PostgreSQLKeywordTestData, error) {
	// Read the kwlist.h file to extract actual PostgreSQL keywords
	kwlistPath := filepath.Join(postgresBaseDir, "src", "include", "parser", "kwlist.h")
	
	file, err := os.Open(kwlistPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open kwlist.h: %w", err)
	}
	defer file.Close()

	data := &PostgreSQLKeywordTestData{
		ValidKeywords:   make([]string, 0),
		InvalidKeywords: []string{"notakeyword", "randomtext", "invalid_kw", "fake123"},
		ReservedKeywords: make([]string, 0),
	}

	scanner := bufio.NewScanner(file)
	// Regex to match PG_KEYWORD entries: PG_KEYWORD("word", TOKEN, CATEGORY, LABEL)
	keywordRegex := regexp.MustCompile(`PG_KEYWORD\("([^"]+)",\s*[^,]+,\s*([^,]+),`)

	for scanner.Scan() {
		line := scanner.Text()
		matches := keywordRegex.FindStringSubmatch(line)
		if len(matches) >= 3 {
			keyword := matches[1]
			category := strings.TrimSpace(matches[2])
			
			data.ValidKeywords = append(data.ValidKeywords, keyword)
			
			if category == "RESERVED_KEYWORD" {
				data.ReservedKeywords = append(data.ReservedKeywords, keyword)
			}
		}
	}

	return data, scanner.Err()
}

// AssertKeywordBehavior validates that keyword recognition matches PostgreSQL behavior.
// Uses testify assertions for clear test output
func AssertKeywordBehavior(t *testing.T, keywordFunc func(string) bool, isReservedFunc func(string) bool, testData *PostgreSQLKeywordTestData) {
	t.Run("ValidKeywords", func(t *testing.T) {
		for _, keyword := range testData.ValidKeywords {
			assert.True(t, keywordFunc(keyword), "Keyword '%s' should be recognized", keyword)
			assert.True(t, keywordFunc(strings.ToUpper(keyword)), "Keyword '%s' should be case-insensitive", keyword)
		}
	})

	t.Run("InvalidKeywords", func(t *testing.T) {
		for _, nonKeyword := range testData.InvalidKeywords {
			assert.False(t, keywordFunc(nonKeyword), "Non-keyword '%s' should not be recognized", nonKeyword)
		}
	})

	t.Run("ReservedKeywords", func(t *testing.T) {
		for _, reserved := range testData.ReservedKeywords {
			assert.True(t, isReservedFunc(reserved), "Reserved keyword '%s' should be recognized as reserved", reserved)
			assert.True(t, isReservedFunc(strings.ToUpper(reserved)), "Reserved keyword '%s' should be case-insensitive", reserved)
		}
	})
}

// CreateTempSQLFile creates a temporary SQL file for testing purposes.
// Useful for creating test cases that need actual SQL files.
func CreateTempSQLFile(t *testing.T, content string) string {
	tmpFile, err := os.CreateTemp("", "postgres_test_*.sql")
	require.NoError(t, err, "Failed to create temp file")
	
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err, "Failed to write to temp file")
	
	err = tmpFile.Close()
	require.NoError(t, err, "Failed to close temp file")
	
	// Clean up after test
	t.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})
	
	return tmpFile.Name()
}

// ValidateSQLSyntax is a placeholder function for SQL syntax validation.
// This will be implemented in later phases when the actual parser is ready.
func ValidateSQLSyntax(sql string) error {
	// Placeholder implementation - will be replaced with actual parser
	if strings.TrimSpace(sql) == "" {
		return fmt.Errorf("empty SQL statement")
	}
	
	// Basic syntax checks for now
	if !strings.HasSuffix(strings.TrimSpace(sql), ";") {
		return fmt.Errorf("SQL statement must end with semicolon")
	}
	
	return nil
}

// Example helper functions for specific PostgreSQL test integration

// ExtractSQLFromComment extracts SQL from PostgreSQL test comments.
// Many PostgreSQL tests embed SQL in comments for documentation.
func ExtractSQLFromComment(comment string) []string {
	sqlStatements := make([]string, 0) // Initialize as empty slice, not nil
	
	// Look for SQL patterns in comments
	lines := strings.Split(comment, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "--") {
			line = strings.TrimPrefix(line, "--")
			line = strings.TrimSpace(line)
			if strings.HasSuffix(line, ";") {
				sqlStatements = append(sqlStatements, line)
			}
		}
	}
	
	return sqlStatements
}

// PostgreSQLVersionInfo holds information about PostgreSQL version compatibility.
// Important for tracking which tests apply to which PostgreSQL versions.
type PostgreSQLVersionInfo struct {
	Major    int
	Minor    int
	Revision int
	String   string
}

// ParsePostgreSQLVersion parses a PostgreSQL version string.
// Format: "PostgreSQL 16.1" or similar
func ParsePostgreSQLVersion(versionStr string) (*PostgreSQLVersionInfo, error) {
	// Simplified version parsing - can be enhanced later
	re := regexp.MustCompile(`PostgreSQL (\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(versionStr)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid version string: %s", versionStr)
	}
	
	major := 0
	minor := 0
	fmt.Sscanf(matches[1], "%d", &major)
	fmt.Sscanf(matches[2], "%d", &minor)
	
	return &PostgreSQLVersionInfo{
		Major:  major,
		Minor:  minor,
		String: versionStr,
	}, nil
}