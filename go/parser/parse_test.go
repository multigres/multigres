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
		// Arithmetic expressions
		{"Nested arithmetic", "SELECT ((1 + 2) * (3 - 4))", ""},
		{"Division and modulo", "SELECT (10 / 3) % 2", ""},
		{"Power operator", "SELECT 2 ^ 3", ""},

		// Comparison expressions
		{"Not equal", "SELECT * FROM users WHERE id <> 1", ""},
		{"Less than or equal", "SELECT * FROM users WHERE age <= 65", ""},
		{"Greater than or equal", "SELECT * FROM users WHERE age >= 18", ""},

		// Logical expressions
		{"Complex AND/OR", "SELECT * FROM users WHERE ((active = TRUE OR admin = TRUE) AND NOT deleted)", ""},
		{"Nested NOT", "SELECT * FROM users WHERE NOT NOT active", ""},

		// Mixed expressions
		{"Arithmetic in WHERE", "SELECT * FROM users WHERE (age * 2) > 50", ""},
		{"Function in expression", "SELECT * FROM users WHERE length(name) > 5", ""},

		// Column expressions
		{"Column arithmetic", "SELECT age + 1 FROM users", ""},
		{"Column with function", "SELECT upper(name) FROM users", ""},

		// Complex nested expressions
		{"Deeply nested", "SELECT (((1 + 2) * 3) - 4) / 5", ""},
		{"Mixed operators", "SELECT * FROM users WHERE (age + 5) * 2 > 30", ""},

		// Basic queries
		{"SELECT * FROM users", "SELECT * FROM users", ""},
		{"SELECT id, name FROM users WHERE active = TRUE", "SELECT id, name FROM users WHERE active = TRUE", ""},
		{"SELECT DISTINCT department FROM employees", "SELECT DISTINCT department FROM employees", ""},

		// Expressions
		{"SELECT 1 + 2 * 3", "SELECT 1 + 2 * 3", ""},
		{"SELECT age::TEXT FROM users", "SELECT age::TEXT FROM users", ""},
		{"SELECT length(name) FROM users", "SELECT length(name) FROM users", ""},

		// Complex queries
		{"SELECT * FROM users WHERE (age > 18 AND active = TRUE)", "SELECT * FROM users WHERE (age > 18 AND active = TRUE)", ""},
		{"SELECT id AS user_id, name AS user_name FROM users AS u", "SELECT id AS user_id, name AS user_name FROM users AS u", ""},
		{"SELECT * FROM users, orders WHERE users.id = orders.user_id", "SELECT * FROM users, orders WHERE users.id = orders.user_id", ""},

		// Special cases
		{"TABLE users", "TABLE users", "SELECT * FROM users"}, // TABLE is converted to SELECT *
		{"SELECT * FROM ONLY users", "SELECT * FROM ONLY users", ""},
		{"SELECT * INTO backup FROM users", "SELECT * INTO backup FROM users", ""},

		// COPY statements - basic round trip parsing
		{"COPY users FROM '/path/to/file.csv'", "COPY users FROM '/path/to/file.csv'", ""},
		{"COPY users FROM '/path/to/file.csv' (format 'csv')", "COPY users FROM '/path/to/file.csv' (format 'csv')", ""},                       // Options now preserved!
		{"COPY users (id, name) FROM '/path/to/file.csv' (format 'csv')", "COPY users (id, name) FROM '/path/to/file.csv' (format 'csv')", ""}, // Options now preserved!
		{"COPY users TO '/path/to/file.csv' (format 'csv', header true)", "COPY users TO '/path/to/file.csv' (format 'csv', header true)", ""}, // Options now preserved!
		{
			name:  "Simple function table",
			input: "SELECT * FROM generate_series(1, 5)",
		},
		{
			name:  "Function table with ORDINALITY",
			input: "SELECT * FROM generate_series(1, 5) WITH ORDINALITY",
		},
		{
			name:  "LATERAL function table",
			input: "SELECT * FROM LATERAL generate_series(1, t.max_val)",
		},
		{
			name:  "Function table with alias",
			input: "SELECT * FROM generate_series(1, 5) AS t",
		},
		{
			name:  "Basic XMLTABLE",
			input: "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS id INT, name TEXT)",
		},
		{
			name:  "XMLTABLE with FOR ORDINALITY",
			input: "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS pos FOR ORDINALITY, id INT)",
		},
		{
			name:  "LATERAL XMLTABLE",
			input: "SELECT * FROM LATERAL XMLTABLE('/root/item' PASSING t.xml_data COLUMNS id INT)",
		},
		{"Column alias with AS", "SELECT id AS user_id", ""},
		{"Column alias without AS", "SELECT id user_id", "SELECT id AS user_id"},
		{"Multiple column aliases", "SELECT id AS user_id, name AS user_name", ""},
		{"Table alias with AS", "SELECT * FROM users AS u", ""},
		{"Table alias without AS", "SELECT * FROM users u", "SELECT * FROM users AS u"},
		{"Complex aliases", "SELECT u.id AS user_id FROM users AS u", ""},
		{"Function with alias", "SELECT length(name) AS name_length FROM users", ""},
		{"Expression with alias", "SELECT age + 1 AS next_age FROM users", ""},
		{"simple limit", "SELECT * FROM users LIMIT 10", ""},
		{"limit with offset", "SELECT * FROM users LIMIT 10 OFFSET 5", ""},
		{"fetch first only", "SELECT * FROM users FETCH FIRST 10 ROWS ONLY", ""},
		{"fetch first with ties", "SELECT * FROM users ORDER BY id FETCH FIRST 5 ROWS WITH TIES", ""},
		{"offset with rows", "SELECT * FROM users OFFSET 10 ROWS", ""},
		{"complex query", "SELECT name, COUNT(*) FROM users WHERE active = true GROUP BY name ORDER BY COUNT(*) DESC LIMIT 20 OFFSET 10", ""},
		// Basic LIMIT tests
		{
			name:     "simple LIMIT",
			input:    "SELECT * FROM users LIMIT 10",
			expected: "SELECT * FROM users LIMIT 10",
		},
		{
			name:     "LIMIT with expression",
			input:    "SELECT * FROM users LIMIT 5 + 5",
			expected: "SELECT * FROM users LIMIT 5 + 5",
		},
		{
			name:     "LIMIT ALL",
			input:    "SELECT * FROM users LIMIT ALL",
			expected: "SELECT * FROM users LIMIT ALL",
		},

		// Basic OFFSET tests
		{
			name:     "simple OFFSET",
			input:    "SELECT * FROM users OFFSET 20",
			expected: "SELECT * FROM users OFFSET 20",
		},
		{
			name:     "OFFSET with expression",
			input:    "SELECT * FROM users OFFSET 10 * 2",
			expected: "SELECT * FROM users OFFSET 10 * 2",
		},

		// Combined LIMIT and OFFSET
		{
			name:     "LIMIT and OFFSET",
			input:    "SELECT * FROM users LIMIT 10 OFFSET 20",
			expected: "SELECT * FROM users LIMIT 10 OFFSET 20",
		},
		{
			name:     "OFFSET before LIMIT",
			input:    "SELECT * FROM users OFFSET 20 LIMIT 10",
			expected: "SELECT * FROM users LIMIT 10 OFFSET 20", // Should normalize order
		},

		// input:2008 FETCH FIRST syntax - should deparse as LIMIT (matching PostgreSQL behavior)
		{
			name:     "FETCH FIRST ROW ONLY",
			input:    "SELECT * FROM users FETCH FIRST ROW ONLY",
			expected: "SELECT * FROM users LIMIT 1",
		},
		{
			name:     "FETCH FIRST ROWS ONLY (implicit 1)",
			input:    "SELECT * FROM users FETCH FIRST ROWS ONLY",
			expected: "SELECT * FROM users LIMIT 1",
		},
		{
			name:     "FETCH NEXT ROW ONLY",
			input:    "SELECT * FROM users FETCH NEXT ROW ONLY",
			expected: "SELECT * FROM users LIMIT 1",
		},
		{
			name:     "FETCH FIRST 5 ROWS ONLY",
			input:    "SELECT * FROM users FETCH FIRST 5 ROWS ONLY",
			expected: "SELECT * FROM users LIMIT 5",
		},
		{
			name:     "FETCH NEXT 10 ROWS ONLY",
			input:    "SELECT * FROM users FETCH NEXT 10 ROWS ONLY",
			expected: "SELECT * FROM users LIMIT 10",
		},

		// FETCH with WITH TIES - must preserve FETCH FIRST syntax
		{
			name:     "FETCH FIRST ROW WITH TIES",
			input:    "SELECT * FROM users ORDER BY score FETCH FIRST ROW WITH TIES",
			expected: "SELECT * FROM users ORDER BY score FETCH FIRST ROW WITH TIES",
		},
		{
			name:     "FETCH FIRST 5 ROWS WITH TIES",
			input:    "SELECT * FROM users ORDER BY score FETCH FIRST 5 ROWS WITH TIES",
			expected: "SELECT * FROM users ORDER BY score FETCH FIRST 5 ROWS WITH TIES",
		},

		// input:2008 OFFSET syntax with ROW/ROWS - should deparse as simple OFFSET
		{
			name:     "OFFSET with ROWS keyword",
			input:    "SELECT * FROM users OFFSET 10 ROWS",
			expected: "SELECT * FROM users OFFSET 10",
		},
		{
			name:     "OFFSET with ROW keyword",
			input:    "SELECT * FROM users OFFSET 1 ROW",
			expected: "SELECT * FROM users OFFSET 1",
		},

		// Combined FETCH and OFFSET
		{
			name:     "OFFSET and FETCH FIRST",
			input:    "SELECT * FROM users OFFSET 20 FETCH FIRST 10 ROWS ONLY",
			expected: "SELECT * FROM users LIMIT 10 OFFSET 20",
		},

		// Complex scenarios
		{
			name:     "ORDER BY with LIMIT",
			input:    "SELECT * FROM users ORDER BY name LIMIT 10",
			expected: "SELECT * FROM users ORDER BY name LIMIT 10",
		},
		{
			name:     "ORDER BY with FETCH FIRST WITH TIES",
			input:    "SELECT * FROM users ORDER BY score DESC FETCH FIRST 5 ROWS WITH TIES",
			expected: "SELECT * FROM users ORDER BY score DESC FETCH FIRST 5 ROWS WITH TIES",
		},
		{
			name:     "WHERE with LIMIT and OFFSET",
			input:    "SELECT * FROM users WHERE active = true OFFSET 50 LIMIT 25",
			expected: "SELECT * FROM users WHERE active = TRUE LIMIT 25 OFFSET 50",
		},

		// Round-trip testing - these may have slight variations in formatting
		{
			name:     "Complex query with CTE and FETCH",
			input:    "WITH top_users AS (SELECT * FROM users ORDER BY score DESC FETCH FIRST 20 ROWS ONLY) SELECT * FROM top_users",
			expected: "WITH top_users AS (SELECT * FROM users ORDER BY score DESC LIMIT 20) SELECT * FROM top_users",
		},
		{
			name:     "Subquery with LIMIT",
			input:    "SELECT * FROM (SELECT * FROM users LIMIT 10) AS u WHERE u.age > 25",
			expected: "SELECT * FROM (SELECT * FROM users LIMIT 10) AS u WHERE u.age > 25",
		},

		{"TABLE without IF EXISTS", "ALTER TABLE users RENAME TO customers", ""},
		{"TABLE with IF EXISTS", "ALTER TABLE IF EXISTS users RENAME TO customers", ""},
		{"INDEX without IF EXISTS", "ALTER INDEX users_idx RENAME TO customers_idx", ""},
		{"INDEX with IF EXISTS", "ALTER INDEX IF EXISTS users_idx RENAME TO customers_idx", ""},
		{"VIEW without IF EXISTS", "ALTER VIEW user_view RENAME TO customer_view", ""},
		{"VIEW with IF EXISTS", "ALTER VIEW IF EXISTS user_view RENAME TO customer_view", ""},
		{"SEQUENCE without IF EXISTS", "ALTER SEQUENCE user_id_seq RENAME TO customer_id_seq", ""},
		{"SEQUENCE with IF EXISTS", "ALTER SEQUENCE IF EXISTS user_id_seq RENAME TO customer_id_seq", ""},
		{"MATERIALIZED VIEW without IF EXISTS", "ALTER MATERIALIZED VIEW mat_view RENAME TO new_mat_view", ""},
		{"MATERIALIZED VIEW with IF EXISTS", "ALTER MATERIALIZED VIEW IF EXISTS mat_view RENAME TO new_mat_view", ""},
		{"FOREIGN TABLE without IF EXISTS", "ALTER FOREIGN TABLE foreign_users RENAME TO foreign_customers", ""},
		{"FOREIGN TABLE with IF EXISTS", "ALTER FOREIGN TABLE IF EXISTS foreign_users RENAME TO foreign_customers", ""},
		{"COLUMN without IF EXISTS", "ALTER TABLE users RENAME COLUMN name TO full_name", ""},
		{"COLUMN with IF EXISTS", "ALTER TABLE IF EXISTS users RENAME COLUMN name TO full_name", ""},
		{"CONSTRAINT without IF EXISTS", "ALTER TABLE users RENAME CONSTRAINT old_c TO new_c", ""},
		{"CONSTRAINT with IF EXISTS", "ALTER TABLE IF EXISTS users RENAME CONSTRAINT old_c TO new_c", ""},
		{"DATABASE", "ALTER DATABASE old_db RENAME TO new_db", ""},
		{"SCHEMA", "ALTER SCHEMA old_schema RENAME TO new_schema", ""},
		{"ROLE", "ALTER ROLE old_role RENAME TO new_role", ""},
		{"USER", "ALTER USER old_user RENAME TO new_user", ""},
		{"GROUP", "ALTER GROUP old_group RENAME TO new_group", ""},
		{"TABLESPACE", "ALTER TABLESPACE old_ts RENAME TO new_ts", ""},
		{"DOMAIN", "ALTER DOMAIN old_domain RENAME TO new_domain", ""},
		{"DOMAIN CONSTRAINT", "ALTER DOMAIN my_domain RENAME CONSTRAINT old_check TO new_check", ""},
		{"TYPE", "ALTER TYPE old_type RENAME TO new_type", ""},
		{"TYPE ATTRIBUTE", "ALTER TYPE my_type RENAME ATTRIBUTE old_attr TO new_attr", ""},
		{"FUNCTION", "ALTER FUNCTION old_func() RENAME TO new_func", ""},
		{"PROCEDURE", "ALTER PROCEDURE old_proc() RENAME TO new_proc", ""},
		{"ROUTINE", "ALTER ROUTINE old_routine() RENAME TO new_routine", ""},
		{"AGGREGATE", "ALTER AGGREGATE old_agg() RENAME TO new_agg", ""},
		{"COLLATION", "ALTER COLLATION old_collation RENAME TO new_collation", ""},
		{"CONVERSION", "ALTER CONVERSION old_conv RENAME TO new_conv", ""},
		{"LANGUAGE", "ALTER LANGUAGE old_lang RENAME TO new_lang", ""},
		{"PROCEDURAL LANGUAGE", "ALTER PROCEDURAL LANGUAGE old_lang RENAME TO new_lang", ""},
		{"OPERATOR CLASS", "ALTER OPERATOR CLASS old_class USING btree RENAME TO new_class", ""},
		{"OPERATOR FAMILY", "ALTER OPERATOR FAMILY old_family USING btree RENAME TO new_family", ""},
		{"POLICY", "ALTER POLICY old_policy ON users RENAME TO new_policy", ""},
		{"RULE", "ALTER RULE old_rule ON users RENAME TO new_rule", ""},
		{"TRIGGER", "ALTER TRIGGER old_trigger ON users RENAME TO new_trigger", ""},
		{"EVENT TRIGGER", "ALTER EVENT TRIGGER old_event_trigger RENAME TO new_event_trigger", ""},
		{"PUBLICATION", "ALTER PUBLICATION old_pub RENAME TO new_pub", ""},
		{"SUBSCRIPTION", "ALTER SUBSCRIPTION old_sub RENAME TO new_sub", ""},
		{"FOREIGN DATA WRAPPER", "ALTER FOREIGN DATA WRAPPER old_fdw RENAME TO new_fdw", ""},
		{"SERVER", "ALTER SERVER old_server RENAME TO new_server", ""},
		{"STATISTICS", "ALTER STATISTICS old_stats RENAME TO new_stats", ""},
		{"TEXT SEARCH PARSER", "ALTER TEXT SEARCH PARSER old_parser RENAME TO new_parser", ""},
		{"TEXT SEARCH DICTIONARY", "ALTER TEXT SEARCH DICTIONARY old_dict RENAME TO new_dict", ""},
		{"TEXT SEARCH TEMPLATE", "ALTER TEXT SEARCH TEMPLATE old_template RENAME TO new_template", ""},
		{"TEXT SEARCH CONFIGURATION", "ALTER TEXT SEARCH CONFIGURATION old_config RENAME TO new_config", ""},

		// DATABASE renaming
		{"ALTER DATABASE RENAME", "ALTER DATABASE old_db RENAME TO new_db", ""},

		// SCHEMA renaming
		{"ALTER SCHEMA RENAME", "ALTER SCHEMA old_schema RENAME TO new_schema", ""},

		// ROLE renaming
		{"ALTER ROLE RENAME", "ALTER ROLE old_role RENAME TO new_role", ""},
		{"ALTER USER RENAME", "ALTER USER old_user RENAME TO new_user", ""},
		{"ALTER GROUP RENAME", "ALTER GROUP old_group RENAME TO new_group", ""},

		// TABLESPACE renaming
		{"ALTER TABLESPACE RENAME", "ALTER TABLESPACE old_ts RENAME TO new_ts", ""},

		// DOMAIN renaming
		{"ALTER DOMAIN RENAME", "ALTER DOMAIN old_domain RENAME TO new_domain", ""},
		{"ALTER DOMAIN RENAME CONSTRAINT", "ALTER DOMAIN my_domain RENAME CONSTRAINT old_check TO new_check", ""},

		// TYPE renaming
		{"ALTER TYPE RENAME", "ALTER TYPE old_type RENAME TO new_type", ""},
		{"ALTER TYPE RENAME ATTRIBUTE", "ALTER TYPE my_type RENAME ATTRIBUTE old_attr TO new_attr", ""},

		// FUNCTION renaming (simplified - no arguments for now)
		{"ALTER FUNCTION RENAME", "ALTER FUNCTION old_func() RENAME TO new_func", ""},
		{"ALTER PROCEDURE RENAME", "ALTER PROCEDURE old_proc() RENAME TO new_proc", ""},
		{"ALTER ROUTINE RENAME", "ALTER ROUTINE old_routine() RENAME TO new_routine", ""},

		// AGGREGATE renaming (simplified - no arguments for now)
		{"ALTER AGGREGATE RENAME", "ALTER AGGREGATE old_agg() RENAME TO new_agg", ""},

		// COLLATION renaming
		{"ALTER COLLATION RENAME", "ALTER COLLATION old_collation RENAME TO new_collation", ""},

		// CONVERSION renaming
		{"ALTER CONVERSION RENAME", "ALTER CONVERSION old_conv RENAME TO new_conv", ""},

		// LANGUAGE renaming
		{"ALTER LANGUAGE RENAME", "ALTER LANGUAGE old_lang RENAME TO new_lang", ""},
		{"ALTER PROCEDURAL LANGUAGE RENAME", "ALTER PROCEDURAL LANGUAGE old_lang RENAME TO new_lang", ""},

		// OPERATOR CLASS renaming
		{"ALTER OPERATOR CLASS RENAME", "ALTER OPERATOR CLASS old_class USING btree RENAME TO new_class", ""},

		// OPERATOR FAMILY renaming
		{"ALTER OPERATOR FAMILY RENAME", "ALTER OPERATOR FAMILY old_family USING btree RENAME TO new_family", ""},

		// POLICY renaming
		{"ALTER POLICY RENAME", "ALTER POLICY old_policy ON users RENAME TO new_policy", ""},
		{"ALTER POLICY RENAME IF EXISTS", "ALTER POLICY IF EXISTS old_policy ON users RENAME TO new_policy", ""},

		// RULE renaming
		{"ALTER RULE RENAME", "ALTER RULE old_rule ON users RENAME TO new_rule", ""},

		// TRIGGER renaming
		{"ALTER TRIGGER RENAME", "ALTER TRIGGER old_trigger ON users RENAME TO new_trigger", ""},

		// EVENT TRIGGER renaming
		{"ALTER EVENT TRIGGER RENAME", "ALTER EVENT TRIGGER old_event_trigger RENAME TO new_event_trigger", ""},

		// PUBLICATION renaming
		{"ALTER PUBLICATION RENAME", "ALTER PUBLICATION old_pub RENAME TO new_pub", ""},

		// SUBSCRIPTION renaming
		{"ALTER SUBSCRIPTION RENAME", "ALTER SUBSCRIPTION old_sub RENAME TO new_sub", ""},

		// FOREIGN DATA WRAPPER renaming
		{"ALTER FOREIGN DATA WRAPPER RENAME", "ALTER FOREIGN DATA WRAPPER old_fdw RENAME TO new_fdw", ""},

		// SERVER renaming
		{"ALTER SERVER RENAME", "ALTER SERVER old_server RENAME TO new_server", ""},

		// STATISTICS renaming
		{"ALTER STATISTICS RENAME", "ALTER STATISTICS old_stats RENAME TO new_stats", ""},

		// TEXT SEARCH objects renaming
		{"ALTER TEXT SEARCH PARSER RENAME", "ALTER TEXT SEARCH PARSER old_parser RENAME TO new_parser", ""},
		{"ALTER TEXT SEARCH DICTIONARY RENAME", "ALTER TEXT SEARCH DICTIONARY old_dict RENAME TO new_dict", ""},
		{"ALTER TEXT SEARCH TEMPLATE RENAME", "ALTER TEXT SEARCH TEMPLATE old_template RENAME TO new_template", ""},
		{"ALTER TEXT SEARCH CONFIGURATION RENAME", "ALTER TEXT SEARCH CONFIGURATION old_config RENAME TO new_config", ""},

		// Qualified names (schema-qualified objects)
		{"ALTER TABLE with schema", "ALTER TABLE public.users RENAME TO public.customers", ""},
		{"ALTER INDEX with schema", "ALTER INDEX public.users_idx RENAME TO public.customers_idx", ""},
		{"ALTER SEQUENCE with schema", "ALTER SEQUENCE public.user_id_seq RENAME TO public.customer_id_seq", ""},
		{"ALTER VIEW with schema", "ALTER VIEW public.user_view RENAME TO public.customer_view", ""},

		// TABLE renaming
		{"ALTER TABLE RENAME", "ALTER TABLE users RENAME TO customers", ""},
		{"ALTER TABLE RENAME IF EXISTS", "ALTER TABLE IF EXISTS users RENAME TO customers", ""},

		// COLUMN renaming
		{"ALTER TABLE RENAME COLUMN", "ALTER TABLE users RENAME COLUMN name TO full_name", ""},
		{"ALTER TABLE RENAME COLUMN IF EXISTS", "ALTER TABLE IF EXISTS users RENAME COLUMN name TO full_name", ""},
		{"ALTER VIEW RENAME COLUMN", "ALTER VIEW user_view RENAME COLUMN name TO full_name", ""},
		{"ALTER VIEW RENAME COLUMN IF EXISTS", "ALTER VIEW IF EXISTS user_view RENAME COLUMN name TO full_name", ""},
		{"ALTER MATERIALIZED VIEW RENAME COLUMN", "ALTER MATERIALIZED VIEW mat_view RENAME COLUMN name TO full_name", ""},
		{"ALTER MATERIALIZED VIEW RENAME COLUMN IF EXISTS", "ALTER MATERIALIZED VIEW IF EXISTS mat_view RENAME COLUMN name TO full_name", ""},
		{"ALTER FOREIGN TABLE RENAME COLUMN", "ALTER FOREIGN TABLE foreign_users RENAME COLUMN name TO full_name", ""},
		{"ALTER FOREIGN TABLE RENAME COLUMN IF EXISTS", "ALTER FOREIGN TABLE IF EXISTS foreign_users RENAME COLUMN name TO full_name", ""},

		// CONSTRAINT renaming
		{"ALTER TABLE RENAME CONSTRAINT", "ALTER TABLE users RENAME CONSTRAINT old_constraint TO new_constraint", ""},
		{"ALTER TABLE RENAME CONSTRAINT IF EXISTS", "ALTER TABLE IF EXISTS users RENAME CONSTRAINT old_constraint TO new_constraint", ""},

		// INDEX renaming
		{"ALTER INDEX RENAME", "ALTER INDEX users_idx RENAME TO customers_idx", ""},
		{"ALTER INDEX RENAME IF EXISTS", "ALTER INDEX IF EXISTS users_idx RENAME TO customers_idx", ""},

		// SEQUENCE renaming
		{"ALTER SEQUENCE RENAME", "ALTER SEQUENCE user_id_seq RENAME TO customer_id_seq", ""},
		{"ALTER SEQUENCE RENAME IF EXISTS", "ALTER SEQUENCE IF EXISTS user_id_seq RENAME TO customer_id_seq", ""},

		// VIEW renaming
		{"ALTER VIEW RENAME", "ALTER VIEW user_view RENAME TO customer_view", ""},
		{"ALTER VIEW RENAME IF EXISTS", "ALTER VIEW IF EXISTS user_view RENAME TO customer_view", ""},

		// MATERIALIZED VIEW renaming
		{"ALTER MATERIALIZED VIEW RENAME", "ALTER MATERIALIZED VIEW mat_view RENAME TO new_mat_view", ""},
		{"ALTER MATERIALIZED VIEW RENAME IF EXISTS", "ALTER MATERIALIZED VIEW IF EXISTS mat_view RENAME TO new_mat_view", ""},

		// FOREIGN TABLE renaming
		{"ALTER FOREIGN TABLE RENAME", "ALTER FOREIGN TABLE foreign_users RENAME TO foreign_customers", ""},
		{"ALTER FOREIGN TABLE RENAME IF EXISTS", "ALTER FOREIGN TABLE IF EXISTS foreign_users RENAME TO foreign_customers", ""},

		// Complex expressions in INSERT
		{
			name:  "INSERT with nested function calls",
			input: "INSERT INTO users (name, email) VALUES (upper(trim('  john  ')), lower(concat('john', '@', 'example.com')))",
		},
		{
			name:  "INSERT with arithmetic in VALUES",
			input: "INSERT INTO products (id, price, discounted_price) VALUES (1, 100.00, 100.00 * 0.9)",
		},
		{
			name:  "INSERT with parenthesized expressions",
			input: "INSERT INTO products (total) VALUES ((price + tax) * quantity)",
		},

		// Complex expressions in UPDATE SET clauses
		{
			name:  "UPDATE with complex SET expressions",
			input: "UPDATE products SET price = price * (1 + tax_rate), updated_at = now()",
		},
		{
			name:  "UPDATE with nested arithmetic",
			input: "UPDATE stats SET score = (score + bonus) * multiplier, rank = rank + 1",
		},
		{
			name:  "UPDATE with function calls in SET",
			input: "UPDATE users SET name = upper(trim(name)), email = lower(email)",
		},

		// Complex expressions in WHERE clauses
		{
			name:  "DELETE with arithmetic in WHERE",
			input: "DELETE FROM products WHERE (price * 0.9) < 10.00",
		},
		{
			name:  "UPDATE with function calls in WHERE",
			input: "UPDATE users SET active = FALSE WHERE length(name) < 3 AND upper(status) = 'INACTIVE'",
		},
		{
			name:  "DELETE with nested expressions in WHERE",
			input: "DELETE FROM orders WHERE (total + tax) > (limitval * 1.5) AND status = 'pending'",
		},

		// Expression combinations with type casts
		{
			name:  "INSERT with type casts and expressions",
			input: "INSERT INTO logs (level, message, count) VALUES (upper('info')::text, concat('Log: ', details), (1 + retry_count)::integer)",
		},
		{
			name:  "UPDATE with complex FROM and expressions",
			input: "UPDATE orders SET total = o.quantity * p.price, updated_at = now() FROM order_items o, products p WHERE orders.id = o.order_id AND o.product_id = p.id",
		},

		// Advanced function combinations
		{
			name:  "INSERT with deeply nested functions",
			input: "INSERT INTO processed (data) VALUES (upper(substring(trim(input_data), 1, 10)))",
		},
		{
			name:  "UPDATE with multiple function calls",
			input: "UPDATE users SET full_name = concat(upper(first_name), ' ', upper(last_name)), slug = lower(replace(name, ' ', '-'))",
		},
		// ===== INSERT Statement Tests =====
		{
			name:  "INSERT basic VALUES",
			input: "INSERT INTO users VALUES (1, 'John')",
		},
		{
			name:  "INSERT with column list",
			input: "INSERT INTO users (id, name) VALUES (1, 'John')",
		},
		{
			name:  "INSERT multiple VALUES",
			input: "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')",
		},
		{
			name:  "INSERT with DEFAULT VALUES",
			input: "INSERT INTO users DEFAULT VALUES",
		},
		{
			name:  "INSERT with SELECT",
			input: "INSERT INTO users SELECT id, name FROM temp_users",
		},
		{
			name:  "INSERT with subquery",
			input: "INSERT INTO users (SELECT id, name FROM temp_users WHERE active = TRUE)",
		},
		{
			name:  "INSERT with RETURNING single column",
			input: "INSERT INTO users (name) VALUES ('John') RETURNING id",
		},
		{
			name:  "INSERT with RETURNING multiple columns",
			input: "INSERT INTO users (name) VALUES ('John') RETURNING id, name, created_at",
		},
		{
			name:  "INSERT with RETURNING *",
			input: "INSERT INTO users (name) VALUES ('John') RETURNING *",
		},
		{
			name:  "INSERT with qualified table name",
			input: "INSERT INTO public.users (name) VALUES ('John')",
		},
		{
			name:  "INSERT with table alias",
			input: "INSERT INTO users AS u (name) VALUES ('John')",
		},
		{
			name:  "INSERT with WITH clause",
			input: "WITH temp AS (SELECT 'John' as name) INSERT INTO users (name) SELECT name FROM temp",
		},
		{
			name:  "INSERT with complex expressions in VALUES",
			input: "INSERT INTO users (id, name, age) VALUES (1 + 2, upper('john'), 25 * 2)",
		},

		// ===== UPDATE Statement Tests =====
		{
			name:  "UPDATE simple",
			input: "UPDATE users SET name = 'Jane'",
		},
		{
			name:  "UPDATE with WHERE",
			input: "UPDATE users SET name = 'Jane' WHERE id = 1",
		},
		{
			name:  "UPDATE multiple columns",
			input: "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1",
		},
		{
			name:  "UPDATE with complex SET expressions",
			input: "UPDATE users SET name = upper('jane'), age = age + 1, updated_at = now()",
		},
		{
			name:  "UPDATE with FROM clause",
			input: "UPDATE users SET name = temp.name FROM temp_users temp WHERE users.id = temp.id",
		},
		{
			name:  "UPDATE with multiple FROM tables",
			input: "UPDATE users SET name = t1.name FROM temp_users t1, other_table t2 WHERE users.id = t1.id AND t1.other_id = t2.id",
		},
		{
			name:  "UPDATE with complex WHERE",
			input: "UPDATE users SET name = 'Jane' WHERE id > 10 AND active = TRUE AND created_at > '2023-01-01'",
		},
		{
			name:  "UPDATE with RETURNING single column",
			input: "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING id",
		},
		{
			name:  "UPDATE with RETURNING multiple columns",
			input: "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING id, name, updated_at",
		},
		{
			name:  "UPDATE with RETURNING *",
			input: "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING *",
		},
		{
			name:  "UPDATE with qualified table",
			input: "UPDATE public.users SET name = 'Jane' WHERE id = 1",
		},
		// Note: WITH clause for UPDATE/DELETE not yet fully implemented in parser
		{
			name:  "UPDATE with WITH clause",
			input: "WITH temp AS (SELECT id FROM active_users) UPDATE users SET active = FALSE WHERE id IN (SELECT id FROM temp)",
		},
		{
			name:  "UPDATE with subquery in SET",
			input: "UPDATE users SET name = (SELECT name FROM profiles WHERE profiles.user_id = users.id)",
		},
		{
			name:  "UPDATE with subquery in WHERE",
			input: "UPDATE users SET active = FALSE WHERE id IN (SELECT user_id FROM banned_users)",
		},

		// ===== DELETE Statement Tests =====
		{
			name:  "DELETE simple",
			input: "DELETE FROM users",
		},
		{
			name:  "DELETE with WHERE",
			input: "DELETE FROM users WHERE id = 1",
		},
		{
			name:  "DELETE with complex WHERE",
			input: "DELETE FROM users WHERE active = FALSE AND created_at < '2020-01-01'",
		},
		{
			name:  "DELETE with USING clause",
			input: "DELETE FROM users USING temp_users temp WHERE users.id = temp.id",
		},
		{
			name:  "DELETE with multiple USING tables",
			input: "DELETE FROM users USING temp_users t1, other_table t2 WHERE users.id = t1.id AND t1.other_id = t2.id",
		},
		{
			name:  "DELETE with RETURNING single column",
			input: "DELETE FROM users WHERE id = 1 RETURNING id",
		},
		{
			name:  "DELETE with RETURNING multiple columns",
			input: "DELETE FROM users WHERE id = 1 RETURNING id, name, deleted_at",
		},
		{
			name:  "DELETE with RETURNING *",
			input: "DELETE FROM users WHERE id = 1 RETURNING *",
		},
		{
			name:  "DELETE with qualified table",
			input: "DELETE FROM public.users WHERE id = 1",
		},
		// Note: WITH clause and subqueries in WHERE not yet fully implemented for DELETE
		{
			name:  "DELETE with WITH clause",
			input: "WITH temp AS (SELECT id FROM inactive_users) DELETE FROM users WHERE id IN (SELECT id FROM temp)",
		},
		{
			name:  "DELETE with subquery in WHERE",
			input: "DELETE FROM users WHERE id IN (SELECT user_id FROM temp_table)",
		},

		// ===== MERGE Statement Tests =====
		{
			name:  "MERGE basic",
			input: "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name:  "MERGE with qualified tables",
			input: "MERGE INTO public.target USING staging.source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name:  "MERGE with table aliases",
			input: "MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name:  "MERGE with complex join condition",
			input: "MERGE INTO target USING source ON target.id = source.id AND target.version = source.version WHEN MATCHED THEN DO NOTHING",
		},
		{
			name:  "MERGE with subquery as source",
			input: "MERGE INTO target USING (SELECT * FROM source WHERE active = TRUE) AS s ON target.id = s.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name:  "MERGE with WITH clause",
			input: "WITH filtered AS (SELECT * FROM source WHERE active = TRUE) MERGE INTO target USING filtered ON target.id = filtered.id WHEN MATCHED THEN DO NOTHING",
		},

		// ===== Complex DML with Expressions =====
		{
			name:  "INSERT with function calls in VALUES",
			input: "INSERT INTO logs (message, created_at) VALUES (concat('Hello ', 'World'), now())",
		},
		{
			name:  "UPDATE with arithmetic expressions",
			input: "UPDATE products SET price = price * 1.1, updated_count = updated_count + 1",
		},
		// Note: extract() function not yet implemented in parser
		{
			name:  "DELETE with function in WHERE",
			input: "DELETE FROM users WHERE length(name) < 3 OR extract(year FROM created_at) < 2020",
		},
		{
			name:  "DELETE with simple function in WHERE",
			input: "DELETE FROM users WHERE length(name) < 3",
		},

		// ===== DML with Type Casts =====
		{
			name:  "INSERT with type casts",
			input: "INSERT INTO users (id, name, age) VALUES (1::bigint, 'John'::varchar, '25'::integer)",
		},
		{
			name:  "UPDATE with type casts",
			input: "UPDATE users SET score = '95.5'::decimal, active = 'true'::boolean",
		},

		// ===== DML with Advanced Table References =====
		// Note: ONLY modifier not yet fully implemented in DML parser
		{
			name:  "INSERT with ONLY modifier",
			input: "INSERT INTO ONLY parent_table (id, name) VALUES (1, 'test')",
		},
		{
			name:  "UPDATE with ONLY modifier",
			input: "UPDATE ONLY parent_table SET name = 'updated'",
		},
		{
			name:  "DELETE with ONLY modifier",
			input: "DELETE FROM ONLY parent_table WHERE id = 1",
		},

		// ===== Edge Cases =====
		// Note: Empty column lists may not be fully supported
		{
			name:  "INSERT with empty column list and VALUES",
			input: "INSERT INTO users () VALUES ()",
		},
		{
			name:  "UPDATE with no WHERE clause",
			input: "UPDATE users SET active = TRUE",
		},
		{
			name:  "DELETE with no WHERE clause",
			input: "DELETE FROM temp_table",
		},

		// INSERT statements
		{
			name:  "INSERT with VALUES",
			input: "INSERT INTO users VALUES (1, 'John')",
		},
		{
			name:  "INSERT with column list and VALUES",
			input: "INSERT INTO users (id, name) VALUES (1, 'John')",
		},
		{
			name:  "INSERT with SELECT",
			input: "INSERT INTO users SELECT * FROM temp_users",
		},
		{
			name:  "INSERT with DEFAULT VALUES",
			input: "INSERT INTO users DEFAULT VALUES",
		},
		{
			name:  "INSERT with RETURNING",
			input: "INSERT INTO users (name) VALUES ('John') RETURNING id",
		},
		{
			name:  "INSERT with schema qualified table",
			input: "INSERT INTO public.users (name) VALUES ('John')",
		},

		// UPDATE statements
		{
			name:  "simple UPDATE",
			input: "UPDATE users SET name = 'Jane'",
		},
		{
			name:  "UPDATE with WHERE",
			input: "UPDATE users SET name = 'Jane' WHERE id = 1",
		},
		{
			name:  "UPDATE with RETURNING",
			input: "UPDATE users SET name = 'Jane' RETURNING id, name",
		},
		{
			name:  "UPDATE with multiple columns",
			input: "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1",
		},
		{
			name:  "UPDATE with schema qualified table",
			input: "UPDATE public.users SET name = 'Jane'",
		},

		// DELETE statements
		{
			name:  "simple DELETE",
			input: "DELETE FROM users",
		},
		{
			name:  "DELETE with WHERE",
			input: "DELETE FROM users WHERE id = 1",
		},
		{
			name:  "DELETE with RETURNING",
			input: "DELETE FROM users WHERE id = 1 RETURNING name",
		},
		{
			name:  "DELETE with schema qualified table",
			input: "DELETE FROM public.users WHERE id = 1",
		},

		// MERGE statements (basic)
		{
			name:  "basic MERGE",
			input: "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name:  "MERGE with schema qualified tables",
			input: "MERGE INTO public.target USING staging.source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},

		// Function table tests (equivalent to TestFuncTable)
		{
			name:  "Simple function table",
			input: "SELECT * FROM generate_series(1, 5)",
		},
		{
			name:  "Function table with ORDINALITY",
			input: "SELECT * FROM generate_series(1, 5) WITH ORDINALITY",
		},
		{
			name:  "LATERAL function table",
			input: "SELECT * FROM LATERAL generate_series(1, t.max_val)",
		},
		{
			name:  "ROWS FROM syntax",
			input: "SELECT * FROM ROWS FROM (generate_series(1, 5))",
		},
		{
			name:  "ROWS FROM with ORDINALITY",
			input: "SELECT * FROM ROWS FROM (generate_series(1, 5)) WITH ORDINALITY",
		},

		// XMLTABLE tests (equivalent to TestXMLTable)
		{
			name:  "Basic XMLTABLE",
			input: "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS id INT, name TEXT)",
		},
		{
			name:  "XMLTABLE with FOR ORDINALITY",
			input: "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS pos FOR ORDINALITY, id INT)",
		},
		{
			name:  "LATERAL XMLTABLE",
			input: "SELECT * FROM LATERAL XMLTABLE('/root/item' PASSING t.xml_data COLUMNS id INT)",
		},

		// JSON_TABLE tests (equivalent to TestJSONTable)
		{
			name:  "Basic JSON_TABLE",
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (id INT))`,
		},
		{
			name:  "JSON_TABLE with FOR ORDINALITY",
			input: `SELECT * FROM JSON_TABLE('{"items": [1, 2, 3]}', '$.items[*]' COLUMNS (pos FOR ORDINALITY, val INT PATH '$'))`,
		},
		{
			name:  "JSON_TABLE with EXISTS column",
			input: `SELECT * FROM JSON_TABLE('{"items": [{"id": 1}]}', '$.items[*]' COLUMNS (has_id BOOLEAN EXISTS PATH '$.id'))`,
		},
		{
			name:  "JSON_TABLE with NESTED columns",
			input: `SELECT * FROM JSON_TABLE('{"items": [{"props": {"a": 1}}]}', '$.items[*]' COLUMNS (NESTED PATH '$.props' COLUMNS (a INT PATH '$.a')))`,
		},
		{
			name:  "LATERAL JSON_TABLE",
			input: `SELECT * FROM LATERAL JSON_TABLE(t.json_data, '$.items[*]' COLUMNS (id INT PATH '$.id'))`,
		},

		// Table function aliases tests (equivalent to TestTableFunctionAliases)
		{
			name:  "Function table with alias",
			input: "SELECT * FROM generate_series(1, 5) AS t",
		},
		{
			name:  "XMLTABLE with alias",
			input: "SELECT * FROM XMLTABLE('/root/item' PASSING '<root></root>' COLUMNS id INT) AS xt",
		},
		{
			name:  "JSON_TABLE with alias",
			input: `SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (id INT)) AS jt`,
		},
		{
			name:  "Function table with column aliases",
			input: "SELECT * FROM generate_series(1, 5) AS t(num)",
		},
		// Advanced CTE feature combinations - MATERIALIZED
		{"MATERIALIZED CTE deparsing", "WITH stats AS MATERIALIZED (SELECT COUNT(a) FROM users) SELECT * FROM stats", ""},
		{"NOT MATERIALIZED CTE deparsing", "WITH stats AS NOT MATERIALIZED (SELECT COUNT(a) FROM users) SELECT * FROM stats", ""},
		{"RECURSIVE MATERIALIZED CTE", "WITH RECURSIVE t AS MATERIALIZED (SELECT 1) SELECT * FROM t", ""},

		// Advanced CTE features - SEARCH clauses
		{"SEARCH DEPTH FIRST single column", "WITH RECURSIVE t AS (SELECT id FROM tree) SEARCH DEPTH FIRST BY id SET seq SELECT * FROM t", ""},
		{"SEARCH BREADTH FIRST single column", "WITH RECURSIVE t AS (SELECT id FROM tree) SEARCH BREADTH FIRST BY id SET seq SELECT * FROM t", ""},

		// Advanced CTE features - CYCLE clauses
		{"CYCLE simple form single column", "WITH RECURSIVE t AS (SELECT id FROM tree) CYCLE id SET mark USING path SELECT * FROM t", ""},
		{"CYCLE full form single column", "WITH RECURSIVE t AS (SELECT id FROM tree) CYCLE id SET mark TO TRUE DEFAULT FALSE USING path SELECT * FROM t", ""},

		// Combined advanced CTE features
		{"MATERIALIZED with SEARCH", "WITH RECURSIVE tree AS MATERIALIZED (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"SEARCH with CYCLE", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle USING path SELECT * FROM tree", ""},

		// JOIN with aliases and complex expressions
		{"JOIN with table aliases", "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id", "SELECT u.name, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"},

		// Edge cases for JOIN conditions
		{"JOIN with complex ON condition", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND u.active = TRUE", "SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id AND u.active = TRUE"},

		{"NATURAL JOIN with explicit type", "SELECT * FROM users NATURAL INNER JOIN orders", "SELECT * FROM users NATURAL JOIN orders"},
		{"NATURAL LEFT JOIN", "SELECT * FROM users NATURAL LEFT JOIN orders", ""},
		{"NATURAL RIGHT JOIN", "SELECT * FROM users NATURAL RIGHT JOIN orders", ""},
		{"NATURAL FULL JOIN", "SELECT * FROM users NATURAL FULL JOIN orders", ""},

		// Complex subquery scenarios
		{"LATERAL with simple subquery", "SELECT * FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id) AS recent_orders", "SELECT * FROM users AS u, LATERAL (SELECT * FROM orders AS o WHERE o.user_id = u.id) AS recent_orders"},
		{"Nested subqueries in FROM", "SELECT * FROM (SELECT id FROM (SELECT user_id AS id FROM orders) AS inner_sub) AS outer_sub", ""},
		// Basic precedence tests
		{"Addition and multiplication", "SELECT 1 + 2 * 3", ""},
		{"Parentheses override precedence", "SELECT (1 + 2) * 3", ""},
		{"Multiple operators", "SELECT 1 + 2 * 3 - 4 / 2", ""},
		{"Unary and binary", "SELECT -1 + 2", ""},
		{"Power has highest precedence", "SELECT 2 + 3 ^ 2", ""},
		{"Modulo precedence", "SELECT 10 + 5 % 3", ""},

		// Comparison and logical precedence
		{"Comparison and logical", "SELECT * FROM users WHERE age > 18 AND active", ""},
		{"Mixed precedence", "SELECT * FROM users WHERE age + 1 > 18 AND NOT deleted", ""},
		{"OR has lower precedence than AND", "SELECT * FROM users WHERE active AND verified OR admin", ""},
		{"Parentheses with logical", "SELECT * FROM users WHERE (active OR admin) AND verified", ""},

		// Complex nested expressions
		{"Deeply nested arithmetic", "SELECT ((1 + 2) * 3) - (4 / 2)", ""},
		{"Mixed arithmetic and comparison", "SELECT * FROM users WHERE (age * 2) + 5 > 30", ""},
		{"Function call precedence", "SELECT length(name) + 10 FROM users", ""},
		{"Type cast precedence", "SELECT age::text FROM users", "SELECT age::TEXT FROM users"},

		// WHERE clause complex precedence
		{"Complex WHERE precedence", "SELECT * FROM users WHERE id = 1 OR id = 2 AND active", ""},
		{"WHERE with parentheses", "SELECT * FROM users WHERE (id = 1 OR id = 2) AND active", ""},
		{"NOT precedence", "SELECT * FROM users WHERE NOT active AND verified", ""},
		{"NOT with parentheses", "SELECT * FROM users WHERE NOT (active AND verified)", ""},
		// Basic SELECT structure tests
		{"Simple SELECT star", "SELECT *", ""},
		{"SELECT single column", "SELECT id", ""},
		{"SELECT with column alias AS", "SELECT id AS user_id", ""},
		{"SELECT with column alias implicit", "SELECT id user_id", "SELECT id AS user_id"},
		{"SELECT multiple columns", "SELECT id, name", ""},
		{"SELECT all columns from table", "SELECT * FROM users", ""},

		// FROM clause variations
		{"FROM with table alias AS", "SELECT * FROM users AS u", ""},
		{"FROM with table alias implicit", "SELECT * FROM users u", "SELECT * FROM users AS u"},
		{"FROM with qualified table", "SELECT * FROM public.users", ""},
		{"FROM with ONLY modifier", "SELECT * FROM ONLY users", ""},
		{"FROM multiple tables", "SELECT * FROM users, orders", ""},
		{"SELECT without FROM", "SELECT 1", ""},

		// DISTINCT variations
		{"SELECT DISTINCT single", "SELECT DISTINCT id", ""},
		{"SELECT DISTINCT multiple", "SELECT DISTINCT id, name", ""},
		{"SELECT DISTINCT ON single", "SELECT DISTINCT ON (id) name", ""},
		{"SELECT DISTINCT ON multiple", "SELECT DISTINCT ON (department, level) name", ""},

		// WHERE clause variations
		{"WHERE simple boolean", "SELECT * FROM users WHERE active", ""},
		{"WHERE equality", "SELECT * FROM users WHERE id = 1", ""},
		{"WHERE not equal", "SELECT * FROM users WHERE id <> 1", ""},
		{"WHERE less than", "SELECT * FROM users WHERE age < 30", ""},
		{"WHERE greater than", "SELECT * FROM users WHERE age > 18", ""},
		{"WHERE less or equal", "SELECT * FROM users WHERE age <= 65", ""},
		{"WHERE greater or equal", "SELECT * FROM users WHERE age >= 18", ""},
		{"WHERE complex AND", "SELECT * FROM users WHERE id > 10 AND active = TRUE", ""},
		{"WHERE complex OR", "SELECT * FROM users WHERE admin = TRUE OR moderator = TRUE", ""},
		{"WHERE complex NOT", "SELECT * FROM users WHERE NOT deleted", ""},
		{"WHERE mixed logical", "SELECT * FROM users WHERE (active = TRUE OR admin = TRUE) AND NOT deleted", ""},

		// Arithmetic expressions in SELECT
		{"Arithmetic addition", "SELECT 1 + 2", ""},
		{"Arithmetic subtraction", "SELECT 5 - 3", ""},
		{"Arithmetic multiplication", "SELECT 4 * 6", ""},
		{"Arithmetic division", "SELECT 10 / 2", ""},
		{"Arithmetic modulo", "SELECT 10 % 3", ""},
		{"Arithmetic power", "SELECT 2 ^ 3", ""},
		{"Unary plus", "SELECT +42", ""},
		{"Unary minus", "SELECT -42", ""},
		{"Complex arithmetic", "SELECT (1 + 2) * 3 - 4 / 2", ""},
		{"Column arithmetic", "SELECT age + 1 FROM users", ""},
		{"Mixed arithmetic", "SELECT id + age * 2 FROM users", ""},

		// Type casting
		{"Type cast to text", "SELECT id::text FROM users", "SELECT id::TEXT FROM users"},
		{"Type cast with spaces", "SELECT id :: integer FROM users", "SELECT id::INT FROM users"},
		{"Complex expression cast", "SELECT (age + 1)::varchar FROM users", "SELECT (age + 1)::VARCHAR FROM users"},
		{"Multiple casts", "SELECT id::text, age::varchar FROM users", "SELECT id::TEXT, age::VARCHAR FROM users"},

		// Column references
		{"Simple column reference", "SELECT id FROM users", ""},
		{"Qualified column reference", "SELECT users.id FROM users", ""},
		{"Schema qualified column", "SELECT public.users.id FROM public.users", ""},
		{"Multiple qualified columns", "SELECT u.id, u.name FROM users AS u", ""},

		// Function calls
		{"Function no args", "SELECT now()", "SELECT NOW()"},
		{"Function single arg", "SELECT length(name) FROM users", ""},
		{"Function multiple args", "SELECT substring(name, 1, 5) FROM users", ""},
		{"Qualified function", "SELECT my_catalog.length(name) FROM users", ""},
		{"Nested functions", "SELECT upper(trim(name)) FROM users", ""},
		{"Function in WHERE", "SELECT * FROM users WHERE length(name) > 5", ""},

		// Constants and literals
		{"Integer constant", "SELECT 42", ""},
		{"Negative integer", "SELECT -123", ""},
		{"Float constant", "SELECT 3.14", ""},
		{"String literal", "SELECT 'hello world'", ""},
		{"Boolean TRUE", "SELECT TRUE", ""},
		{"Boolean FALSE", "SELECT FALSE", ""},
		{"NULL constant", "SELECT NULL", ""},

		// SELECT INTO
		{"SELECT INTO basic", "SELECT * INTO backup_users FROM users", ""},
		{"SELECT INTO with WHERE", "SELECT id, name INTO temp_users FROM users WHERE active = TRUE", ""},

		// TABLE statement (equivalent to SELECT *)
		{"TABLE statement", "TABLE users", "SELECT * FROM users"},
		{"TABLE with qualified name", "TABLE public.users", "SELECT * FROM public.users"},

		// Complex mixed queries
		{"Complex SELECT with all features", "SELECT u.id AS user_id, upper(u.name) AS user_name FROM users AS u WHERE u.active = TRUE AND u.age > 18", ""},
		{"Multiple expressions with aliases", "SELECT id AS user_id, name AS user_name, age + 1 AS next_age FROM users", ""},
		{"Arithmetic in WHERE with functions", "SELECT * FROM users WHERE length(name) + age > 25", ""},
		{
			name:     "column list in function",
			input:    "SELECT func(a, b, c)",
			expected: "",
		},
		{
			name:     "multiple column references",
			input:    "SELECT a, b, c FROM table1",
			expected: "",
		},
		{
			name:     "qualified column list",
			input:    "SELECT t1.a, t2.b, t3.c FROM t1, t2, t3",
			expected: "",
		},
		{
			name:  "array subscript",
			input: "SELECT column[1]",
		},
		{
			name:  "array slice",
			input: "SELECT column[1:5]",
		},
		{
			name:  "field access",
			input: "SELECT record.field",
		},
		{
			name:  "nested field access",
			input: "SELECT record.subrecord.field",
		},
		{
			name:  "mixed indirection",
			input: "SELECT array_col[1].field",
		},
		{
			name:  "qualified operator",
			input: "SELECT a OPERATOR(pg_catalog.+) b",
		},
		{
			name:  "schema qualified operator",
			input: "SELECT a OPERATOR(myschema.=) b",
		},
		{
			name:  "custom operator",
			input: "SELECT a OPERATOR(public.@@) b",
		},
		{
			name:  "bit type casting",
			input: "SELECT value::bit",
		},
		{
			name:  "bit with length",
			input: "SELECT value::bit(8)",
		},
		{
			name:     "timestamp type",
			input:    "SELECT value::TIMESTAMP",
			expected: "",
		},
		{
			name:     "timestamp with precision",
			input:    "SELECT value::timestamp(6)",
			expected: "SELECT value::TIMESTAMP(6)",
		},
		{
			name:     "timestamptz type",
			input:    "SELECT value::timestamptz",
			expected: "SELECT value::TIMESTAMP WITH TIME ZONE",
		},
		{
			name:     "date type",
			input:    "SELECT value::DATE",
			expected: "",
		},
		{
			name:     "time type",
			input:    "SELECT value::TIME",
			expected: "",
		},
		{
			name:  "interval type",
			input: "SELECT value::INTERVAL",
		},
		{
			name:  "select all",
			input: "SELECT ALL * FROM users",
		},
		{
			name:  "select all with columns",
			input: "SELECT ALL id, name FROM users",
		},
		{
			name:  "select all with where",
			input: "SELECT ALL * FROM users WHERE active = true",
		},
		{
			name:     "two simple statements",
			input:    "SELECT 1; SELECT 2",
			expected: "SELECT 1; SELECT 2",
		},
		{
			name:     "multiple statements with different types",
			input:    "SELECT * FROM users; SELECT name FROM orders",
			expected: "SELECT * FROM users; SELECT name FROM orders",
		},
		{
			name:     "statements with whitespace",
			input:    "SELECT 1;   SELECT 2;   SELECT 3",
			expected: "SELECT 1; SELECT 2; SELECT 3",
		},
		{
			name:     "trailing semicolon",
			input:    "SELECT 1; SELECT 2;",
			expected: "SELECT 1; SELECT 2",
		},
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
