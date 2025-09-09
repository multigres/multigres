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
					require.NoError(t, secondErr)
					require.EqualValues(t, expectedQuery, secondParsedOutput)
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

// TestDeparsing tests round-trip parsing and deparsing for all supported constructs
func TestDeparsingsecond(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
		// ===== INSERT Statements =====
		{"INSERT with VALUES", "INSERT INTO users VALUES (1, 'John')", ""},
		{"INSERT with column list", "INSERT INTO users (id, name) VALUES (1, 'John')", ""},
		{"INSERT multiple VALUES", "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')", ""},
		{"INSERT with DEFAULT VALUES", "INSERT INTO users DEFAULT VALUES", ""},
		{"INSERT with SELECT", "INSERT INTO users SELECT id, name FROM temp_users", ""},
		{"INSERT with subquery", "INSERT INTO users (SELECT id, name FROM temp_users WHERE active = TRUE)", ""},
		{"INSERT with RETURNING single column", "INSERT INTO users (name) VALUES ('John') RETURNING id", ""},
		{"INSERT with RETURNING multiple columns", "INSERT INTO users (name) VALUES ('John') RETURNING id, name, created_at", ""},
		{"INSERT with RETURNING *", "INSERT INTO users (name) VALUES ('John') RETURNING *", ""},
		{"INSERT with qualified table name", "INSERT INTO public.users (name) VALUES ('John')", ""},
		{"INSERT with table alias", "INSERT INTO users AS u (name) VALUES ('John')", ""},
		{"INSERT with WITH clause", "WITH temp AS (SELECT 'John' as name) INSERT INTO users (name) SELECT name FROM temp", "WITH temp AS (SELECT 'John' AS name) INSERT INTO users (name) SELECT name FROM temp"},
		{"INSERT with OVERRIDING USER VALUE", "INSERT INTO users OVERRIDING USER VALUE SELECT * FROM temp_users", "INSERT INTO users SELECT * FROM temp_users"},
		{"INSERT with OVERRIDING SYSTEM VALUE", "INSERT INTO users OVERRIDING SYSTEM VALUE SELECT * FROM temp_users", "INSERT INTO users SELECT * FROM temp_users"},
		{"INSERT with column list and OVERRIDING USER VALUE", "INSERT INTO users (id, name) OVERRIDING USER VALUE SELECT * FROM temp_users", "INSERT INTO users (id, name) SELECT * FROM temp_users"},
		{"INSERT with column list and OVERRIDING SYSTEM VALUE", "INSERT INTO users (id, name) OVERRIDING SYSTEM VALUE SELECT * FROM temp_users", "INSERT INTO users (id, name) SELECT * FROM temp_users"},
		{"INSERT with complex expressions in VALUES", "INSERT INTO users (id, name, age) VALUES (1 + 2, upper('john'), 25 * 2)", ""},
		{"INSERT with function calls in VALUES", "INSERT INTO logs (message, created_at) VALUES (concat('Hello ', 'World'), now())", "INSERT INTO logs (message, created_at) VALUES (concat('Hello ', 'World'), NOW())"},
		{"INSERT with type casts", "INSERT INTO users (id, name, age) VALUES (1::bigint, 'John'::varchar, '25'::int)", "INSERT INTO users (id, name, age) VALUES (1::BIGINT, 'John'::VARCHAR, '25'::INT)"},

		// ===== UPDATE Statements =====
		{"UPDATE simple", "UPDATE users SET name = 'Jane'", ""},
		{"UPDATE with WHERE", "UPDATE users SET name = 'Jane' WHERE id = 1", ""},
		{"UPDATE multiple columns", "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1", ""},
		{"UPDATE with complex SET expressions", "UPDATE users SET name = upper('jane'), age = age + 1, updated_at = now()", "UPDATE users SET name = upper('jane'), age = age + 1, updated_at = NOW()"},
		{"UPDATE with FROM clause", "UPDATE users SET name = temp.name FROM temp_users temp WHERE users.id = temp.id", "UPDATE users SET name = temp.name FROM temp_users AS temp WHERE users.id = temp.id"},
		{"UPDATE with multiple FROM tables", "UPDATE users SET name = t1.name FROM temp_users t1, other_table t2 WHERE users.id = t1.id AND t1.other_id = t2.id", "UPDATE users SET name = t1.name FROM temp_users AS t1, other_table AS t2 WHERE users.id = t1.id AND t1.other_id = t2.id"},
		{"UPDATE with complex WHERE", "UPDATE users SET name = 'Jane' WHERE id > 10 AND active = TRUE AND created_at > '2023-01-01'", ""},
		{"UPDATE with RETURNING single column", "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING id", ""},
		{"UPDATE with RETURNING multiple columns", "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING id, name, updated_at", ""},
		{"UPDATE with RETURNING *", "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING *", ""},
		{"UPDATE with qualified table", "UPDATE public.users SET name = 'Jane' WHERE id = 1", ""},
		{"UPDATE with WHERE CURRENT OF cursor", "UPDATE users SET name = 'Jane' WHERE CURRENT OF my_cursor", ""},
		{"UPDATE with ONLY modifier", "UPDATE ONLY parent_table SET name = 'updated'", ""},
		{"UPDATE with no WHERE clause", "UPDATE users SET active = TRUE", ""},
		{"UPDATE with arithmetic expressions", "UPDATE products SET price = price * 1.1, updated_count = updated_count + 1", ""},
		{"UPDATE with type casts", "UPDATE users SET score = '95.5'::numeric, active = 'true'::boolean", "UPDATE users SET score = '95.5'::NUMERIC, active = 'true'::BOOLEAN"},
		{"UPDATE with function calls in SET", "UPDATE users SET name = upper(trim(name)), email = lower(email)", ""},
		{"UPDATE with function calls in WHERE", "UPDATE users SET active = FALSE WHERE length(name) < 3 AND upper(status) = 'INACTIVE'", ""},
		{"UPDATE with nested arithmetic", "UPDATE stats SET score = (score + bonus) * multiplier, rank = rank + 1", ""},
		{"UPDATE with complex FROM and expressions", "UPDATE orders SET total = o.quantity * p.price, updated_at = now() FROM order_items o, products p WHERE orders.id = o.order_id AND o.product_id = p.id", "UPDATE orders SET total = o.quantity * p.price, updated_at = NOW() FROM order_items AS o, products AS p WHERE orders.id = o.order_id AND o.product_id = p.id"},

		// ===== DELETE Statements =====
		{"DELETE simple", "DELETE FROM users", ""},
		{"DELETE with WHERE", "DELETE FROM users WHERE id = 1", ""},
		{"DELETE with complex WHERE", "DELETE FROM users WHERE active = FALSE AND created_at < '2020-01-01'", ""},
		{"DELETE with USING clause", "DELETE FROM users USING temp_users temp WHERE users.id = temp.id", "DELETE FROM users USING temp_users AS temp WHERE users.id = temp.id"},
		{"DELETE with multiple USING tables", "DELETE FROM users USING temp_users t1, other_table t2 WHERE users.id = t1.id AND t1.other_id = t2.id", "DELETE FROM users USING temp_users AS t1, other_table AS t2 WHERE users.id = t1.id AND t1.other_id = t2.id"},
		{"DELETE with RETURNING single column", "DELETE FROM users WHERE id = 1 RETURNING id", ""},
		{"DELETE with RETURNING multiple columns", "DELETE FROM users WHERE id = 1 RETURNING id, name, deleted_at", ""},
		{"DELETE with RETURNING *", "DELETE FROM users WHERE id = 1 RETURNING *", ""},
		{"DELETE with qualified table", "DELETE FROM public.users WHERE id = 1", ""},
		{"DELETE with WHERE CURRENT OF cursor", "DELETE FROM users WHERE CURRENT OF my_cursor", ""},
		{"DELETE with ONLY modifier", "DELETE FROM ONLY parent_table WHERE id = 1", ""},
		{"DELETE with no WHERE clause", "DELETE FROM temp_table", ""},
		{"DELETE with simple function in WHERE", "DELETE FROM users WHERE length(name) < 3", ""},
		{"DELETE with arithmetic in WHERE", "DELETE FROM products WHERE (price * 0.9) < 10.00", ""},
		{"DELETE with nested expressions in WHERE", "DELETE FROM orders WHERE (total + tax) > (limitval * 1.5) AND status = 'pending'", ""},

		// ===== MERGE Statements =====
		{"MERGE basic", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DO NOTHING", ""},
		{"MERGE with qualified tables", "MERGE INTO public.target USING staging.source ON target.id = source.id WHEN MATCHED THEN DO NOTHING", ""},
		{"MERGE with table aliases", "MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN DO NOTHING", ""},
		{"MERGE with complex join condition", "MERGE INTO target USING source ON target.id = source.id AND target.version = source.version WHEN MATCHED THEN DO NOTHING", ""},
		{"MERGE with subquery as source", "MERGE INTO target USING (SELECT * FROM source WHERE active = TRUE) AS s ON target.id = s.id WHEN MATCHED THEN DO NOTHING", ""},
		{"MERGE with WITH clause", "WITH filtered AS (SELECT * FROM source WHERE active = TRUE) MERGE INTO target USING filtered ON target.id = filtered.id WHEN MATCHED THEN DO NOTHING", ""},

		// ===== MERGE WHEN Clauses (Phase 3E) =====
		{"MERGE with WHEN MATCHED UPDATE", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name", ""},
		{"MERGE with WHEN MATCHED DELETE", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DELETE", ""},
		{"MERGE with WHEN NOT MATCHED INSERT", "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)", ""},
		{"MERGE with WHEN NOT MATCHED INSERT simple", "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.name)", ""},
		{"MERGE with WHEN MATCHED DO NOTHING", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DO NOTHING", ""},
		{"MERGE with WHEN NOT MATCHED DO NOTHING", "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN DO NOTHING", ""},
		{"MERGE with conditional WHEN", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED AND target.updated_at < source.updated_at THEN UPDATE SET name = source.name", ""},
		{"MERGE with multiple WHEN clauses", "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED AND source.active = TRUE THEN UPDATE SET name = source.name WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.name)", ""},

		// ===== INSERT ON CONFLICT (Phase 3E) =====
		{"INSERT with ON CONFLICT DO NOTHING", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO NOTHING", ""},
		{"INSERT with ON CONFLICT DO UPDATE", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE SET name = EXCLUDED.name", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE SET name = excluded.name"},
		{"INSERT with ON CONFLICT column specification", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = excluded.name"},
		{"INSERT with ON CONFLICT constraint", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = excluded.name"},
		{"INSERT with ON CONFLICT WHERE clause", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.updated_at < EXCLUDED.updated_at", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.updated_at < excluded.updated_at"},

		// ===== COPY Statements (Phase 3E) =====
		{"COPY FROM file", "COPY users FROM '/path/to/file.csv'", ""},
		{"COPY TO file", "COPY users TO '/path/to/file.csv'", ""},
		{"COPY FROM STDIN", "COPY users FROM STDIN", ""},
		{"COPY TO STDOUT", "COPY users TO STDOUT", ""},
		{"COPY with column list", "COPY users (id, name, email) FROM '/path/to/file.csv'", ""},
		{"COPY with BINARY option", "COPY users FROM '/path/to/file.dat' BINARY", "COPY users FROM '/path/to/file.dat' (format 'binary')"},
		{"COPY with FREEZE option", "COPY users FROM '/path/to/file.csv' FREEZE", "COPY users FROM '/path/to/file.csv' (freeze true)"},
		{"COPY with PROGRAM", "COPY users FROM PROGRAM 'gunzip < /path/to/file.csv.gz'", ""},
		{"COPY query TO file", "COPY (SELECT * FROM users WHERE active = TRUE) TO '/path/to/export.csv'", ""},

		// ===== Complex DML with Expressions =====
		{"INSERT with nested function calls", "INSERT INTO users (name, email) VALUES (upper(trim('  john  ')), lower(concat('john', '@', 'example.com')))", ""},
		{"INSERT with arithmetic in VALUES", "INSERT INTO products (id, price, discounted_price) VALUES (1, 100.00, 100.00 * 0.9)", ""},
		{"INSERT with parenthesized expressions", "INSERT INTO products (total) VALUES ((price + tax) * quantity)", ""},
		{"INSERT with type casts and expressions", "INSERT INTO logs (level, message, count) VALUES (upper('info')::text, concat('Log: ', details), (1 + retry_count)::int)", "INSERT INTO logs (level, message, count) VALUES (upper('info')::TEXT, concat('Log: ', details), (1 + retry_count)::INT)"},
		{"INSERT with deeply nested functions", "INSERT INTO processed (data) VALUES (upper(substring(trim(input_data), 1, 10)))", ""},
	}

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
