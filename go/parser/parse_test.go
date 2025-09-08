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

			// Parse both the original query and expected query
			parsedOutput, err := getParserOutput(tcase.Query)

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
					require.EqualValues(t, expectedQuery, parsedOutput)
				}
			})
		}

		// Write updated test file if there were failures
		if s.outputDir != "" && failed {
			name := strings.TrimSuffix(filename, filepath.Ext(filename))
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
	if len(asts) != 1 {
		return "", fmt.Errorf("incorrect count of ASTs generated")
	}

	// Deparse query.
	return asts[0].SqlString(), nil
}

// TestParsing is used test the parsing support.
func (s *parseTestSuite) TestParsing() {
	s.testFile("select_cases.json")
}

// TestOne is for testing a single case during development
func (s *parseTestSuite) TestOne() {
	// This can be used to test a single case file during development
	s.testFile("onecase.json")
}
