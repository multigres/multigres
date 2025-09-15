package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// ParseTest represents a single test case for the parser
type ParseTest struct {
	Comment  string `json:"comment,omitempty"`
	Query    string `json:"query,omitempty"`
	Expected string `json:"expected,omitempty"` // If empty, defaults to Query
	Error    string `json:"error,omitempty"`
}

// parseTestSuite is the test suite for parser tests
type parseTestSuite struct {
	suite.Suite
	outputDir string
}

// SetupSuite prepares the test output directory
func (s *parseTestSuite) SetupSuite() {
	dir := getTestExpectationDir()
	err := os.RemoveAll(dir)
	require.NoError(s.T(), err)
	err = os.Mkdir(dir, 0755)
	require.NoError(s.T(), err)
	s.outputDir = dir
}

// TestParseTestSuite runs the parser test suite
func TestParseTestSuite(t *testing.T) {
	suite.Run(t, new(parseTestSuite))
}

var expectedDir = "testdata/expected"

func getTestExpectationDir() string {
	return filepath.Clean(expectedDir)
}

// testFile runs tests from a JSON test file
func (s *parseTestSuite) testFile(filename string) {
	s.T().Run(filename, func(t *testing.T) {
		failed := false
		var expected []ParseTest

		for _, tcase := range readJSONTests(filename) {
			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}
			if tcase.Query == "" {
				continue
			}

			// Use Expected if provided, otherwise default to Query
			expectedQuery := tcase.Expected
			if expectedQuery == "" {
				expectedQuery = tcase.Query
			}

			current := ParseTest{
				Comment:  tcase.Comment,
				Query:    tcase.Query,
				Expected: expectedQuery,
			}

			// Parse both the query and and parse the output again.
			parsedOutput, err := getParserOutput(tcase.Query)
			var secondErr error
			var secondParsedOutput string
			if parsedOutput != "" {
				secondParsedOutput, secondErr = getParserOutput(parsedOutput)
			}

			t.Run(testName, func(t *testing.T) {
				defer func() {
					// Use the actual output to store the files.
					if current.Query != parsedOutput {
						current.Expected = parsedOutput
					} else {
						current.Expected = ""
					}
					if err != nil {
						current.Error = err.Error()
					}
					expected = append(expected, current)
					if t.Failed() {
						failed = true
					}
				}()

				// Check if we expect an error
				if tcase.Error != "" {
					require.ErrorContains(t, err, tcase.Error)
				} else {
					// We expect a successful parse
					require.NoError(t, err)
					require.EqualValues(t, expectedQuery, parsedOutput)
					require.NoError(t, secondErr, tcase.Query)
					require.EqualValues(t, expectedQuery, secondParsedOutput, tcase.Query)
				}
			})
		}

		// Write updated test file if there were failures
		if s.outputDir != "" && failed {
			name := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
			name = filepath.Join(s.outputDir, name+".json")
			file, err := os.Create(name)
			require.NoError(t, err)
			defer file.Close()

			enc := json.NewEncoder(file)
			enc.SetIndent("", "  ")
			err = enc.Encode(expected)
			require.NoError(t, err)

			t.Logf("Updated test expectations written to: %s", name)
		}
	})
}

// readJSONTests reads test cases from a JSON file
func readJSONTests(filename string) []ParseTest {
	var output []ParseTest
	filepath := locateFile(filename)

	// Check if file exists
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		// Return empty slice if file doesn't exist
		return output
	}

	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

// locateFile returns the full path to a test data file
func locateFile(name string) string {
	return filepath.Join("testdata", name)
}

// getParserOutput parses a query and returns the JSON representation of the AST
func getParserOutput(query string) (string, error) {
	// Parse the query using the actual parser
	asts, err := ParseSQL(query)

	// Check for parse errors
	if err != nil {
		return "", err
	}
	if len(asts) == 0 {
		return "", fmt.Errorf("no ASTs generated")
	}

	var sqls string
	for _, ast := range asts {
		if sqls != "" {
			sqls = sqls + "; "
		}
		sqls = sqls + ast.SqlString()
	}

	// Deparse query.
	return sqls, nil
}

// TestParsing is used test the parsing support.
func (s *parseTestSuite) TestParsing() {
	s.testFile("select_cases.json")
	s.testFile("misc_cases.json")
	s.testFile("ddl_cases.json")
	s.testFile("dml_cases.json")
	s.testFile("set_cases.json")
}

// TestOne is for testing a single case during development
func (s *parseTestSuite) TestOne() {
	// This can be used to test a single case file during development
	s.testFile("onecase.json")
}

// TestPostgresTestsParsing runs tests from all PostgreSQL test files
// The tests have been ported over from ./src/test/regress/sql in the postgres code.
func (s *parseTestSuite) TestPostgresTestsParsing() {
	// Read all JSON files from the postgres directory
	postgresDir := "testdata/postgres"
	files, err := os.ReadDir(postgresDir)
	if err != nil {
		s.T().Fatalf("Failed to read postgres test directory: %v", err)
	}

	files = files[0:50]
	// Test each JSON file
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			s.testFile(filepath.Join("postgres", file.Name()))
		}
	}
}

// TestDeparsing tests round-trip parsing and deparsing for all supported constructs
func TestDeparsingsecond(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{}

	var expected []ParseTest
	for _, tt := range tests {
		expected = append(expected, ParseTest{
			Comment:  tt.name,
			Query:    tt.input,
			Expected: tt.expected,
		})
	}

	name := "/Users/manangupta/multigres/go/parser/testdata/deparse_cases.json"
	file, err := os.Create(name)
	require.NoError(t, err)
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	err = enc.Encode(expected)
	require.NoError(t, err)
}
