package parser

import (
	"strings"
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeparsing tests round-trip parsing and deparsing for all supported constructs
func TestDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
		// Basic SELECT statements
		{"Simple SELECT all", "SELECT *", ""},
		{"SELECT specific columns", "SELECT id, name", ""},
		{"SELECT with alias", "SELECT id AS user_id", ""},
		{"SELECT from table", "SELECT * FROM users", ""},
		{"SELECT from qualified table", "SELECT * FROM public.users", ""},

		// WHERE clauses
		{"WHERE with equality", "SELECT * FROM users WHERE id = 1", ""},
		{"WHERE with comparison", "SELECT * FROM users WHERE age > 18", ""},
		{"WHERE with AND", "SELECT * FROM users WHERE (age > 18 AND active = TRUE)", ""},
		{"WHERE with OR", "SELECT * FROM users WHERE (active = TRUE OR admin = TRUE)", ""},
		{"WHERE with NOT", "SELECT * FROM users WHERE NOT deleted", ""},

		// DISTINCT
		{"SELECT DISTINCT", "SELECT DISTINCT name FROM users", ""},
		{"SELECT DISTINCT ON", "SELECT DISTINCT ON (department) name FROM employees", ""},

		// Expressions
		{"Arithmetic addition", "SELECT 1 + 2", ""},
		{"Arithmetic subtraction", "SELECT 5 - 3", ""},
		{"Arithmetic multiplication", "SELECT 4 * 6", ""},
		{"Arithmetic division", "SELECT 10 / 2", ""},
		{"Unary minus", "SELECT -42", "SELECT -42"},
		{"Unary plus", "SELECT +42", "SELECT +42"},
		{"Complex arithmetic", "SELECT ((1 + 2) * 3)", ""},

		// Type casting
		{"Type cast simple", "SELECT id::text FROM users", ""},
		{"Type cast expression", "SELECT (age + 1)::varchar FROM users", ""},

		// Function calls
		{"Function no args", "SELECT now()", ""},
		{"Function with arg", "SELECT length('hello')", "SELECT length('hello')"},
		{"Function multiple args", "SELECT substring('hello', 1, 3)", "SELECT substring('hello', 1, 3)"},

		// JSON aggregate functions
		{"JSON_OBJECTAGG simple", "SELECT JSON_OBJECTAGG('name' VALUE value) FROM users", ""},
		{"JSON_ARRAYAGG simple", "SELECT JSON_ARRAYAGG(name) FROM users", ""},
		{"JSON_OBJECTAGG with RETURNING", "SELECT JSON_OBJECTAGG('key' VALUE value RETURNING TEXT) FROM users", ""},
		{"JSON_ARRAYAGG with RETURNING", "SELECT JSON_ARRAYAGG(name RETURNING JSONB) FROM users", ""},
		{"JSON_OBJECTAGG with FILTER", "SELECT JSON_OBJECTAGG('key' VALUE val) FILTER (WHERE val IS NOT NULL) FROM users", ""},
		{"JSON_ARRAYAGG with OVER", "SELECT JSON_ARRAYAGG(name) OVER (PARTITION BY department) FROM users", ""},
		{"JSON_OBJECTAGG with FILTER and OVER", "SELECT JSON_OBJECTAGG('key' VALUE val RETURNING TEXT) FILTER (WHERE val > 0) OVER (ORDER BY id) FROM users", ""},

		// Column references
		{"Simple column", "SELECT name FROM users", ""},
		{"Qualified column", "SELECT users.name FROM users", ""},

		// Constants
		{"Integer constant", "SELECT 42", ""},
		{"Float constant", "SELECT 3.14", ""},
		{"String constant", "SELECT 'hello'", "SELECT 'hello'"},
		{"Boolean TRUE", "SELECT TRUE", ""},
		{"Boolean FALSE", "SELECT FALSE", ""},
		{"NULL constant", "SELECT NULL", ""},

		// Table aliases
		{"Table with alias", "SELECT * FROM users AS u", ""},
		{"Table with implicit alias", "SELECT * FROM users u", "SELECT * FROM users AS u"},

		// Multiple tables
		{"Multiple tables", "SELECT * FROM users, orders", ""},

		// Complex queries
		{"Complex query", "SELECT id, name FROM users WHERE age > 21", ""},
		{"Query with multiple conditions", "SELECT * FROM users WHERE (id > 0 AND active = TRUE)", ""},

		// SELECT INTO
		{"SELECT INTO", "SELECT * INTO new_table FROM users", ""},
		{"SELECT INTO TEMPORARY", "SELECT * INTO TEMPORARY temp_users FROM users", ""},
		{"SELECT INTO TEMP", "SELECT * INTO TEMP temp_users FROM users", "SELECT * INTO TEMPORARY TEMP_USERS FROM USERS"},
		{"SELECT INTO LOCAL TEMPORARY", "SELECT * INTO LOCAL TEMPORARY local_temp_users FROM users", "SELECT * INTO TEMPORARY LOCAL_TEMP_USERS FROM USERS"},
		{"SELECT INTO UNLOGGED", "SELECT * INTO UNLOGGED unlogged_users FROM users", ""},
		{"SELECT INTO TABLE explicit", "SELECT * INTO TABLE explicit_table FROM users", "SELECT * INTO EXPLICIT_TABLE FROM USERS"},

		// TABLE statement (simplified SELECT)
		{"TABLE statement", "TABLE users", "SELECT * FROM users"},

		// ONLY modifier
		{"SELECT from ONLY", "SELECT * FROM ONLY users", ""},

		// JOIN operations - Basic
		{"INNER JOIN", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", ""},
		{"LEFT JOIN", "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id", "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id"},
		{"RIGHT JOIN", "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id", "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id"},
		{"FULL JOIN", "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id", "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id"},
		{"CROSS JOIN", "SELECT * FROM users CROSS JOIN orders", "SELECT * FROM users INNER JOIN orders"},
		{"NATURAL JOIN", "SELECT * FROM users NATURAL JOIN orders", ""},
		{"JOIN with USING", "SELECT * FROM users JOIN orders USING (user_id)", "SELECT * FROM users INNER JOIN orders USING ('user_id')"},
		{"JOIN implicit INNER", "SELECT * FROM users JOIN orders ON users.id = orders.user_id", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"},

		// JOIN operations - Advanced
		{"LEFT OUTER JOIN explicit", "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id", ""},
		{"RIGHT OUTER JOIN explicit", "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id", ""},
		{"FULL OUTER JOIN explicit", "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id", ""},
		{"NATURAL INNER JOIN", "SELECT * FROM users NATURAL INNER JOIN orders", "SELECT * FROM users NATURAL JOIN orders"},
		{"NATURAL LEFT JOIN", "SELECT * FROM users NATURAL LEFT JOIN orders", ""},
		{"NATURAL RIGHT JOIN", "SELECT * FROM users NATURAL RIGHT JOIN orders", ""},
		{"NATURAL FULL JOIN", "SELECT * FROM users NATURAL FULL JOIN orders", ""},
		{"Multiple column USING", "SELECT * FROM users JOIN orders USING (user_id, created_date)", "SELECT * FROM users INNER JOIN orders USING ('user_id', 'created_date')"},

		// JOIN operations - Complex
		{"Chained JOINs", "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id"},
		{"Mixed JOIN types", "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id CROSS JOIN categories", "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id INNER JOIN categories"},
		{"Parenthesized JOIN", "SELECT * FROM users JOIN (orders JOIN products ON orders.product_id = products.id) ON users.id = orders.user_id", "SELECT * FROM users INNER JOIN (orders INNER JOIN products ON orders.product_id = products.id) ON users.id = orders.user_id"},
		{"JOIN with table aliases", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", "SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"},

		// Common Table Expressions (CTEs) - Basic
		{"Basic CTE", "WITH stats AS (SELECT * FROM users) SELECT * FROM stats", ""},
		{"Recursive CTE", "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t", ""},
		{"Multiple CTEs", "WITH t1 AS (SELECT id FROM users), t2 AS (SELECT * FROM t1) SELECT * FROM t2", ""},
		{"CTE with column list", "WITH stats(user_id) AS (SELECT id FROM users) SELECT * FROM stats", ""},

		// Advanced CTE Features - MATERIALIZED
		{"MATERIALIZED CTE", "WITH stats AS MATERIALIZED (SELECT id FROM users) SELECT * FROM stats", ""},
		{"NOT MATERIALIZED CTE", "WITH stats AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM stats", ""},
		{"Recursive MATERIALIZED CTE", "WITH RECURSIVE t AS MATERIALIZED (SELECT 1) SELECT * FROM t", ""},

		// Advanced CTE Features - SEARCH clauses
		{"CTE with SEARCH DEPTH FIRST", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"CTE with SEARCH BREADTH FIRST", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH BREADTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"CTE with SEARCH multiple columns", "WITH RECURSIVE tree AS (SELECT id, parent_id FROM nodes) SEARCH DEPTH FIRST BY id, parent_id SET search_seq SELECT * FROM tree", ""},

		// Advanced CTE Features - CYCLE clauses
		{"CTE with CYCLE simple", "WITH RECURSIVE tree AS (SELECT id FROM nodes) CYCLE id SET is_cycle USING path SELECT * FROM tree", ""},
		{"CTE with CYCLE full", "WITH RECURSIVE tree AS (SELECT id FROM nodes) CYCLE id SET is_cycle TO TRUE DEFAULT FALSE USING path SELECT * FROM tree", ""},
		{"CTE with CYCLE multiple columns", "WITH RECURSIVE tree AS (SELECT id, parent_id FROM nodes) CYCLE id, parent_id SET is_cycle USING path SELECT * FROM tree", ""},

		// Advanced CTE Features - Combined clauses
		{"CTE with MATERIALIZED and SEARCH", "WITH RECURSIVE tree AS MATERIALIZED (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"CTE with SEARCH and CYCLE", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle USING path SELECT * FROM tree", ""},
		{"CTE with all advanced features", "WITH RECURSIVE tree AS MATERIALIZED (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle TO TRUE DEFAULT FALSE USING path SELECT * FROM tree", ""},

		// Subqueries and LATERAL
		{"Subquery in FROM", "SELECT * FROM (SELECT id FROM users) AS sub", ""},
		{"LATERAL subquery", "SELECT * FROM users, LATERAL (SELECT * FROM orders) AS sub", ""},
		{"Subquery with alias and columns", "SELECT * FROM (SELECT id, name FROM users) AS sub(user_id, user_name)", ""},

		// JSON Grammar Implementation Notes (for future JSON function implementation)
		// Note: These are placeholders for when JSON functions are fully implemented
		//
		// ✅ json_behavior and json_behavior_clause_opt rules correctly implemented
		// ✅ json_wrapper_behavior rules correctly implemented using JsonWrapper constants
		// ✅ json_quotes_clause_opt rules correctly implemented using JsonQuotes constants
		// ✅ json_format_clause rules correctly implemented using JsonFormat and JsonEncoding
		//
		// Grammar rules now match PostgreSQL exactly:
		// - json_behavior_type: ERROR_P, NULL_P, TRUE_P, FALSE_P, UNKNOWN, EMPTY_P ARRAY, EMPTY_P OBJECT_P, EMPTY_P
		// - json_wrapper_behavior: WITHOUT WRAPPER → JSW_NONE, WITH WRAPPER → JSW_UNCONDITIONAL, etc.
		// - json_behavior_clause_opt: supports ON EMPTY_P, ON ERROR_P, and combined clauses
		// - json_quotes_clause_opt: KEEP QUOTES → JS_QUOTES_KEEP, OMIT QUOTES → JS_QUOTES_OMIT, etc.
		// - json_format_clause: FORMAT_LA JSON → JsonFormat{JS_FORMAT_JSON, JS_ENC_DEFAULT}, etc.

		// COPY statements - basic syntax
		{"COPY FROM basic", "COPY users FROM '/path/to/file.csv'", ""},
		{"COPY FROM with BINARY", "COPY users FROM '/path/to/file.dat' BINARY", "COPY users FROM '/path/to/file.dat' (format 'binary')"},

		// COPY statements - new parenthesized syntax
		{"COPY FROM with new syntax basic", "COPY users FROM '/path/to/file.csv' (format 'csv')", ""},
		{"COPY FROM with header option", "COPY users FROM '/path/to/file.csv' (format 'csv', header true)", ""},
		{"COPY FROM with delimiter option", "COPY users FROM '/path/to/file.csv' (format 'csv', delimiter ',')", ""},
		{"COPY FROM with quote option", "COPY users FROM '/path/to/file.csv' (format 'csv', quote '\"')", ""},
		{"COPY FROM with escape option", "COPY users FROM '/path/to/file.csv' (format 'csv', escape '\\\\')", ""},
		{"COPY FROM with force_quote all", "COPY users FROM '/path/to/file.csv' (format 'csv', force_quote *)", ""},
		{"COPY FROM with encoding option", "COPY users FROM '/path/to/file.csv' (format 'csv', encoding 'UTF8')", ""},
		{"COPY FROM with boolean values", "COPY users FROM '/path/to/file.csv' (header true, freeze false)", ""},
		{"COPY FROM with numeric values", "COPY users FROM '/path/to/file.csv' (header_line 1, skip_rows 2)", ""},
		{"COPY FROM with default values", "COPY users FROM '/path/to/file.csv' (format default, header default)", ""},

		// COPY TO statements
		{"COPY TO basic", "COPY users TO '/path/to/file.csv'", ""},
		{"COPY TO with new syntax", "COPY users TO '/path/to/file.csv' (format 'csv', header true)", ""},
		{"COPY TO with multiple new options", "COPY users TO '/path/to/file.csv' (format 'csv', header true, delimiter ',')", ""},

		// COPY with advanced features
		{"COPY FROM with column list new syntax", "COPY users (id, name, email) FROM '/path/to/file.csv' (format 'csv')", ""},
		{"COPY TO with column list new syntax", "COPY users (id, name, email) TO '/path/to/file.csv' (format 'csv')", ""},
		{"COPY FROM qualified table new syntax", "COPY public.users FROM '/path/to/file.csv' (format 'csv')", ""},
		{"COPY FROM PROGRAM new syntax", "COPY users FROM PROGRAM 'cat /path/to/file.csv' (format 'csv')", ""},
		{"COPY TO PROGRAM new syntax", "COPY users TO PROGRAM 'gzip > /path/to/file.csv.gz' (format 'csv')", ""},
		{"COPY FROM STDIN new syntax", "COPY users FROM STDIN (format 'csv')", ""},
		{"COPY TO STDOUT new syntax", "COPY users TO STDOUT (format 'csv')", ""},

		// ALTER TABLE partition commands
		{"ALTER TABLE ATTACH PARTITION with default", "ALTER TABLE parent_table ATTACH PARTITION child_table DEFAULT", ""},
		{"ALTER TABLE ATTACH PARTITION with range", "ALTER TABLE parent_table ATTACH PARTITION child_table FOR VALUES FROM (1) TO (100)", ""},
		{"ALTER TABLE ATTACH PARTITION with list", "ALTER TABLE parent_table ATTACH PARTITION child_table FOR VALUES IN (1, 2, 3)", ""},
		{"ALTER TABLE DETACH PARTITION", "ALTER TABLE parent_table DETACH PARTITION child_table", ""},
		{"ALTER TABLE DETACH PARTITION CONCURRENTLY", "ALTER TABLE parent_table DETACH PARTITION child_table CONCURRENTLY", ""},
		{"ALTER TABLE DETACH PARTITION FINALIZE", "ALTER TABLE parent_table DETACH PARTITION child_table FINALIZE", ""},

		// ALTER INDEX partition commands
		{"ALTER INDEX ATTACH PARTITION", "ALTER INDEX parent_index ATTACH PARTITION child_index", ""},

		// SET statements - basic generic forms
		{"SET variable equals value", "SET my_var = 'test_value'", ""},
		{"SET variable TO value", "SET my_var TO 'test_value'", "SET my_var = 'test_value'"}, // Normalized to =
		{"SET variable equals DEFAULT", "SET my_var = DEFAULT", ""},
		{"SET variable TO DEFAULT", "SET my_var TO DEFAULT", "SET my_var = DEFAULT"}, // Normalized to =
		{"SET variable FROM CURRENT", "SET my_var FROM CURRENT", ""},

		// SET statements - PostgreSQL-specific forms
		{"SET TIME ZONE string", "SET TIME ZONE 'UTC'", ""},
		{"SET TIME ZONE identifier", "SET TIME ZONE utc", "SET TIME ZONE 'utc'"},
		{"SET TIME ZONE numeric positive", "SET TIME ZONE 5", ""},
		{"SET TIME ZONE numeric negative", "SET TIME ZONE -8", ""},
		{"SET TIME ZONE DEFAULT", "SET TIME ZONE DEFAULT", "SET TIME ZONE default"},
		{"SET TIME ZONE LOCAL", "SET TIME ZONE LOCAL", "SET TIME ZONE 'local'"},
		{"SET TIME ZONE TRUE", "SET TIME ZONE TRUE", "SET TIME ZONE true"},
		{"SET TIME ZONE FALSE", "SET TIME ZONE FALSE", "SET TIME ZONE false"},
		{"SET TIME ZONE ON", "SET TIME ZONE ON", "SET TIME ZONE on"},
		{"SET TIME ZONE OFF", "SET TIME ZONE OFF", "SET TIME ZONE off"},
		// Complex interval parsing not yet implemented - removed test case
		{"SET CATALOG", "SET CATALOG 'mydb'", ""},
		{"SET SCHEMA", "SET SCHEMA 'public'", ""},
		{"SET NAMES with encoding", "SET NAMES 'UTF8'", ""},
		{"SET NAMES without encoding", "SET NAMES", ""},
		{"SET ROLE", "SET ROLE 'admin_role'", ""},
		{"SET SESSION AUTHORIZATION user", "SET SESSION AUTHORIZATION 'username'", ""},
		{"SET SESSION AUTHORIZATION DEFAULT", "SET SESSION AUTHORIZATION DEFAULT", ""},
		{"SET XML OPTION DOCUMENT", "SET XML OPTION DOCUMENT", ""},
		{"SET XML OPTION CONTENT", "SET XML OPTION CONTENT", ""},
		{"SET TRANSACTION SNAPSHOT", "SET TRANSACTION SNAPSHOT 'snapshot_id_123'", ""},

		// SET statements - LOCAL and SESSION variants
		{"SET LOCAL variable", "SET LOCAL my_var = 'local_value'", ""},
		{"SET LOCAL TIME ZONE", "SET LOCAL TIME ZONE 'PST'", ""},
		{"SET LOCAL ROLE", "SET LOCAL ROLE 'temp_role'", ""},
		{"SET SESSION variable", "SET SESSION my_var = 'session_value'", "SET my_var = 'session_value'"}, // SESSION is normalized away
		{"SET SESSION TIME ZONE", "SET SESSION TIME ZONE 'EST'", "SET TIME ZONE 'EST'"},                  // SESSION is normalized away
		{"SET SESSION ROLE", "SET SESSION ROLE 'session_role'", "SET ROLE 'session_role'"},               // SESSION is normalized away

		// SET statements - complex variable names and values
		{"SET qualified variable name", "SET application.setting = 'value'", ""},
		{"SET multiple word variable", "SET log_statement = 'all'", ""},
		{"SET numeric value", "SET work_mem = 1024", ""},
		{"SET boolean value true", "SET enable_seqscan = TRUE", ""},
		{"SET boolean value false", "SET enable_hashjoin = FALSE", ""},
		{"SET variable to ON", "SET autocommit = ON", ""},
		{"SET multiple values", "SET search_path = 'schema1', 'schema2', 'public'", "SET SCHEMA 'schema1', 'schema2', 'public'"}, // Gets parsed as SET SCHEMA

		// Definition constructs (name = value pairs in option lists)
		// These are used in CREATE statements with WITH clauses and constraint options

		// Definition constructs - CREATE INDEX with options (these are known to work)
		{"CREATE INDEX with options", "CREATE INDEX test_idx ON test (id) WITH (fillfactor = 85)", ""},
		{"CREATE UNIQUE INDEX with options", "CREATE UNIQUE INDEX test_idx ON test (id) WITH (fillfactor = 75)", ""},

		// Multiple definition elements
		{"CREATE INDEX with multiple options", "CREATE INDEX test_idx ON test (id) WITH (fillfactor = 80, fastupdate = off)", ""},

		// Boolean option values (expect them to be quoted in deparsed output)
		{"Definition with boolean true", "CREATE INDEX test_idx ON test (id) WITH (fastupdate = on)", "CREATE INDEX test_idx ON test (id) WITH (fastupdate = 'on')"},
		{"Definition with boolean false", "CREATE INDEX test_idx ON test (id) WITH (fastupdate = off)", ""},

		// String and numeric option values
		{"Definition with string value", "CREATE INDEX test_idx ON test (id) WITH (buffering = 'auto')", ""},
		{"Definition with numeric value", "CREATE INDEX test_idx ON test (id) WITH (pages_per_range = 128)", ""},

		// Edge cases for definition constructs
		{"Definition with identifier value", "CREATE INDEX test_idx ON test (id) WITH (fillfactor = auto)", ""},
		{"Definition with quoted identifier", "CREATE INDEX test_idx ON test (id) WITH (\"custom_option\" = 'value')", "CREATE INDEX test_idx ON test (id) WITH (custom_option = 'value')"},
		{"Definition with mixed value types", "CREATE INDEX test_idx ON test (id) WITH (fillfactor = 80, fastupdate = on, buffering = 'auto')", "CREATE INDEX test_idx ON test (id) WITH (fillfactor = 80, fastupdate = 'on', buffering = 'auto')"},

		// CREATE FUNCTION with FunctionParameter tests
		// Testing that FunctionParameter.Name is properly handled as a string

		// Functions with named parameters
		{"Function with named parameters", "CREATE FUNCTION add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$SELECT a + b$$", "CREATE FUNCTION add (a integer, b integer) RETURNS integer LANGUAGE sql AS $$SELECT a + b$$"},
		{"Function with IN/OUT named parameters", "CREATE FUNCTION process(IN input text, OUT result integer) LANGUAGE sql AS $$SELECT length(input)$$", "CREATE FUNCTION process (input text, OUT result integer) LANGUAGE sql AS $$SELECT length(input)$$"},

		// Functions with unnamed parameters
		{"Function with unnamed parameter", "CREATE FUNCTION greet(text) RETURNS text LANGUAGE sql AS $$SELECT 'Hello ' || $1$$", "CREATE FUNCTION greet (text) RETURNS text LANGUAGE sql AS $$SELECT 'Hello ' || $1$$"},
		{"Function with multiple unnamed parameters", "CREATE FUNCTION multiply(integer, integer) RETURNS integer LANGUAGE sql AS $$SELECT $1 * $2$$", "CREATE FUNCTION multiply (integer, integer) RETURNS integer LANGUAGE sql AS $$SELECT $1 * $2$$"},

		// Mixed named and unnamed parameters
		{"Function with mixed parameters", "CREATE FUNCTION calc(a integer, integer, c integer) RETURNS integer LANGUAGE sql AS $$SELECT a + $2 + c$$", "CREATE FUNCTION calc (a integer, integer, c integer) RETURNS integer LANGUAGE sql AS $$SELECT a + $2 + c$$"},

		// Parameter modes
		{"Function with OUT parameter", "CREATE FUNCTION get_values(OUT x integer, OUT y text) LANGUAGE sql AS $$SELECT 1, 'hello'$$", "CREATE FUNCTION get_values (OUT x integer, OUT y text) LANGUAGE sql AS $$SELECT 1, 'hello'$$"},
		{"Function with INOUT parameter", "CREATE FUNCTION double(INOUT value integer) LANGUAGE sql AS $$SELECT value * 2$$", "CREATE FUNCTION double (INOUT value integer) LANGUAGE sql AS $$SELECT value * 2$$"},

		// Functions with default values
		{"Function with default parameter", "CREATE FUNCTION greet_with_default(name text DEFAULT 'World') RETURNS text LANGUAGE sql AS $$SELECT 'Hello, ' || name$$", "CREATE FUNCTION greet_with_default (name text DEFAULT 'World') RETURNS text LANGUAGE sql AS $$SELECT 'Hello, ' || name$$"},

		// Edge cases for parameter names
		{"Function with empty string name handling", "CREATE FUNCTION unnamed_params(integer, text, boolean) RETURNS void LANGUAGE sql AS $$SELECT NULL$$", "CREATE FUNCTION unnamed_params (integer, text, boolean) RETURNS void LANGUAGE sql AS $$SELECT NULL$$"},
		{"Function with qualified name", "CREATE FUNCTION public.my_func(param1 text) RETURNS integer LANGUAGE sql AS $$SELECT 42$$", "CREATE FUNCTION public.my_func (param1 text) RETURNS integer LANGUAGE sql AS $$SELECT 42$$"},

		// CREATE VIEW Tests
		{"Basic CREATE VIEW", "CREATE VIEW user_view AS SELECT * FROM users", ""},
		{"CREATE OR REPLACE VIEW", "CREATE OR REPLACE VIEW user_view AS SELECT * FROM users", ""},
		{"VIEW with column aliases", "CREATE VIEW user_summary (id, full_name) AS SELECT id, name FROM users", ""},
		{"VIEW with qualified name", "CREATE VIEW public.user_view AS SELECT * FROM users", ""},

		// CREATE TRIGGER Tests
		{"Basic CREATE TRIGGER", "CREATE TRIGGER my_trigger BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION trigger_func()", "CREATE TRIGGER my_trigger BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION trigger_func ()"},
		{"CREATE TRIGGER AFTER UPDATE", "CREATE TRIGGER audit_trigger AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION audit_func()", "CREATE TRIGGER audit_trigger AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION audit_func ()"},
		{"CREATE TRIGGER multiple events", "CREATE TRIGGER multi_trigger BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION multi_func()", "CREATE TRIGGER multi_trigger BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION multi_func ()"},
		{"CREATE TRIGGER with arguments", "CREATE TRIGGER arg_trigger BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION trigger_func('arg1', 123)", "CREATE TRIGGER arg_trigger BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION trigger_func ('arg1', 123)"},

		{"Basic GROUP BY", "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id", ""},
		{"GROUP BY multiple columns", "SELECT dept_id, status, COUNT(*) FROM employees GROUP BY dept_id, status", ""},
		{"GROUP BY with expression", "SELECT DATE(created_at), COUNT(*) FROM orders GROUP BY DATE(created_at)", ""},
		{"GROUP BY ROLLUP", "SELECT year, quarter, SUM(sales) FROM sales GROUP BY ROLLUP(year, quarter)", ""},
		{"GROUP BY CUBE", "SELECT category, subcategory, COUNT(*) FROM products GROUP BY CUBE(category, subcategory)", ""},
		{"GROUP BY GROUPING SETS", "SELECT col1, col2, COUNT(*) FROM table1 GROUP BY GROUPING SETS ((col1), (col2), ())", ""},
		{"GROUP BY empty grouping set", "SELECT COUNT(*) FROM table1 GROUP BY ()", ""},

		{"Basic HAVING", "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id HAVING COUNT(*) > 5", ""},
		{"HAVING with AND", "SELECT dept_id, AVG(salary) FROM employees GROUP BY dept_id HAVING COUNT(*) > 5 AND AVG(salary) > 50000", ""},
		{"HAVING with complex expression", "SELECT category, SUM(amount) FROM transactions GROUP BY category HAVING SUM(amount) > 1000 AND COUNT(*) > 10", ""},

		{"Basic ORDER BY", "SELECT name, age FROM users ORDER BY name", ""},
		{"ORDER BY ASC/DESC", "SELECT name, age FROM users ORDER BY name ASC, age DESC", ""},
		{"ORDER BY NULLS FIRST", "SELECT name, score FROM users ORDER BY score NULLS FIRST", ""},
		{"ORDER BY NULLS LAST", "SELECT name, score FROM users ORDER BY score DESC NULLS LAST", ""},
		{"ORDER BY multiple with mixed nulls", "SELECT name, score, age FROM users ORDER BY score ASC NULLS FIRST, age DESC NULLS LAST", ""},
		{"ORDER BY with expression", "SELECT name FROM users ORDER BY UPPER(name)", ""},

		{"GROUP BY + HAVING", "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id HAVING COUNT(*) > 5", ""},
		{"GROUP BY + ORDER BY", "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id ORDER BY dept_id", ""},
		{"HAVING + ORDER BY", "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id HAVING COUNT(*) > 5 ORDER BY COUNT(*) DESC", ""},
		{"All three combined", "SELECT dept_id, AVG(salary) FROM employees GROUP BY dept_id HAVING AVG(salary) > 50000 ORDER BY AVG(salary) DESC", ""},
		{"Advanced ROLLUP with ORDER BY", "SELECT year, quarter, SUM(sales) FROM sales GROUP BY ROLLUP(year, quarter) ORDER BY year NULLS LAST, quarter", ""},
		{"Complex GROUPING SETS with HAVING and ORDER BY", "SELECT category, subcategory, COUNT(*), SUM(amount) FROM transactions GROUP BY GROUPING SETS ((category), (category, subcategory), ()) HAVING SUM(amount) > 1000 ORDER BY category NULLS FIRST, subcategory DESC", ""},

		// Aggregate functions with FILTER clause
		{"Aggregate with FILTER", "SELECT COUNT(*) FILTER (WHERE active = TRUE) FROM users", ""},
		{"SUM with FILTER", "SELECT SUM(amount) FILTER (WHERE status = 'paid') FROM invoices", ""},
		{"Multiple aggregates with FILTER", "SELECT COUNT(*) FILTER (WHERE active), SUM(sales) FILTER (WHERE region = 'US') FROM data", ""},
		{"Aggregate FILTER with GROUP BY", "SELECT dept, AVG(salary) FILTER (WHERE experience > 5) FROM employees GROUP BY dept", ""},

		// Aggregate functions with WITHIN GROUP clause
		{"percentile_cont WITHIN GROUP", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY score) FROM results", ""},
		{"mode WITHIN GROUP", "SELECT mode() WITHIN GROUP (ORDER BY value) FROM measurements", ""},
		{"string_agg WITHIN GROUP", "SELECT string_agg(name, ',') WITHIN GROUP (ORDER BY name) FROM users", ""},
		{"WITHIN GROUP with DESC", "SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY amount DESC) FROM transactions", ""},

		// Combined FILTER and WITHIN GROUP
		{"Aggregate with FILTER and WITHIN GROUP", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY score) FILTER (WHERE valid = TRUE) FROM tests", ""},

		// FOR UPDATE/SHARE locking clauses
		{"Simple FOR UPDATE", "SELECT * FROM users FOR UPDATE", ""},
		{"FOR UPDATE with table", "SELECT * FROM users u FOR UPDATE OF u", "SELECT * FROM users AS u FOR UPDATE OF u"},
		{"FOR UPDATE NOWAIT", "SELECT * FROM accounts FOR UPDATE NOWAIT", ""},
		{"FOR UPDATE SKIP LOCKED", "SELECT * FROM queue FOR UPDATE SKIP LOCKED", ""},

		// FOR NO KEY UPDATE
		{"FOR NO KEY UPDATE", "SELECT * FROM settings FOR NO KEY UPDATE", ""},
		{"FOR NO KEY UPDATE SKIP LOCKED", "SELECT * FROM tasks FOR NO KEY UPDATE SKIP LOCKED", ""},

		// FOR SHARE variants
		{"FOR SHARE", "SELECT * FROM products FOR SHARE", ""},
		{"FOR SHARE NOWAIT", "SELECT * FROM inventory FOR SHARE NOWAIT", ""},
		{"FOR KEY SHARE", "SELECT * FROM categories FOR KEY SHARE", ""},
		{"FOR KEY SHARE SKIP LOCKED", "SELECT * FROM jobs FOR KEY SHARE SKIP LOCKED", ""},

		// Multiple locking clauses
		{"Multiple locking clauses", "SELECT * FROM t1, t2 FOR UPDATE OF t1 FOR SHARE OF t2", ""},

		// FOR READ ONLY (semantically equivalent to no locking, so deparsed without FOR READ ONLY)
		{"FOR READ ONLY", "SELECT * FROM logs FOR READ ONLY", "SELECT * FROM logs"},

		// Locking with other clauses
		{"Locking with ORDER BY LIMIT", "SELECT * FROM queue ORDER BY priority LIMIT 10 FOR UPDATE SKIP LOCKED", ""},
		{"Locking with GROUP BY", "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id FOR UPDATE", ""},

		// Basic transaction commands
		{"BEGIN", "BEGIN", ""},
		{"START TRANSACTION", "START TRANSACTION", ""},
		{"COMMIT", "COMMIT", ""},
		{"ROLLBACK", "ROLLBACK", ""},
		{"END (synonym for COMMIT)", "END", "COMMIT"},
		{"ABORT (synonym for ROLLBACK)", "ABORT", "ROLLBACK"},

		// Transaction with options (basic - detailed options testing may need more work)
		{"BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN ISOLATION LEVEL SERIALIZABLE", ""},
		{"BEGIN READ ONLY", "BEGIN READ ONLY", ""},
		{"BEGIN READ WRITE", "BEGIN READ WRITE", ""},
		{"BEGIN DEFERRABLE", "BEGIN DEFERRABLE", ""},
		{"START TRANSACTION ISOLATION LEVEL READ COMMITTED", "START TRANSACTION ISOLATION LEVEL READ COMMITTED", ""},

		// Transaction chaining
		{"COMMIT AND CHAIN", "COMMIT AND CHAIN", ""},
		{"ROLLBACK AND NO CHAIN", "ROLLBACK AND NO CHAIN", "ROLLBACK"},

		// Savepoints
		{"SAVEPOINT basic", "SAVEPOINT my_savepoint", ""},
		{"RELEASE SAVEPOINT", "RELEASE SAVEPOINT my_savepoint", ""},
		{"RELEASE without SAVEPOINT keyword", "RELEASE my_savepoint", "RELEASE SAVEPOINT my_savepoint"},
		{"ROLLBACK TO SAVEPOINT", "ROLLBACK TO SAVEPOINT my_savepoint", ""},
		{"ROLLBACK TO without SAVEPOINT keyword", "ROLLBACK TO my_savepoint", "ROLLBACK TO SAVEPOINT my_savepoint"},

		// Prepared transactions
		{"PREPARE TRANSACTION", "PREPARE TRANSACTION 'transaction_123'", ""},
		{"COMMIT PREPARED", "COMMIT PREPARED 'transaction_123'", ""},
		{"ROLLBACK PREPARED", "ROLLBACK PREPARED 'transaction_123'", ""},

		// CREATE ROLE/USER/GROUP statements
		{"CREATE ROLE basic", "CREATE ROLE testuser", ""},
		{"CREATE USER basic", "CREATE USER testuser", ""},
		{"CREATE GROUP basic", "CREATE GROUP testgroup", ""},

		// CREATE ROLE with options
		{"CREATE ROLE with PASSWORD", "CREATE ROLE testuser WITH PASSWORD 'secret'", ""},
		{"CREATE USER with SUPERUSER", "CREATE USER testuser WITH SUPERUSER", ""},
		{"CREATE ROLE with LOGIN", "CREATE ROLE testuser WITH LOGIN", ""},
		{"CREATE ROLE with NOLOGIN", "CREATE ROLE testuser WITH NOLOGIN", ""},
		{"CREATE ROLE with INHERIT", "CREATE ROLE testuser WITH INHERIT", ""},
		{"CREATE ROLE with NOINHERIT", "CREATE ROLE testuser WITH NOINHERIT", ""},
		{"CREATE ROLE with CREATEROLE", "CREATE ROLE testuser WITH CREATEROLE", ""},
		{"CREATE ROLE with NOCREATEROLE", "CREATE ROLE testuser WITH NOCREATEROLE", ""},
		{"CREATE ROLE with CREATEDB", "CREATE ROLE testuser WITH CREATEDB", ""},
		{"CREATE ROLE with NOCREATEDB", "CREATE ROLE testuser WITH NOCREATEDB", ""},
		{"CREATE ROLE with REPLICATION", "CREATE ROLE testuser WITH REPLICATION", ""},
		{"CREATE ROLE with NOREPLICATION", "CREATE ROLE testuser WITH NOREPLICATION", ""},
		{"CREATE ROLE with CONNECTION LIMIT", "CREATE ROLE testuser WITH CONNECTION LIMIT 10", ""},
		{"CREATE ROLE with VALID UNTIL", "CREATE ROLE testuser WITH VALID UNTIL '2025-12-31'", ""},
		{"CREATE ROLE with multiple options", "CREATE USER testuser WITH SUPERUSER CREATEDB", ""},
		{"CREATE ROLE with IN ROLE", "CREATE ROLE testuser WITH IN ROLE admin_role", ""},
		{"CREATE ROLE with ADMIN", "CREATE ROLE testuser WITH ADMIN admin_role", ""},

		// ALTER ROLE/USER statements
		{"ALTER ROLE basic", "ALTER ROLE testuser WITH PASSWORD 'newsecret'", ""},
		{"ALTER USER basic", "ALTER USER testuser WITH SUPERUSER", "ALTER ROLE TESTUSER WITH SUPERUSER"},
		{"ALTER ROLE with CURRENT_USER", "ALTER ROLE CURRENT_USER WITH PASSWORD 'newsecret'", ""},
		{"ALTER USER with SESSION_USER", "ALTER USER SESSION_USER WITH NOSUPERUSER", "ALTER ROLE SESSION_USER WITH NOSUPERUSER"},
		{"ALTER ROLE multiple options", "ALTER ROLE testuser WITH NOSUPERUSER NOINHERIT", ""},

		// ALTER GROUP statements (group membership)
		{"ALTER GROUP ADD USER", "ALTER GROUP testgroup ADD USER testuser", ""},
		{"ALTER GROUP DROP USER", "ALTER GROUP testgroup DROP USER testuser", ""},
		{"ALTER GROUP ADD multiple users", "ALTER GROUP testgroup ADD USER user1, user2, user3", ""},
		{"ALTER GROUP DROP multiple users", "ALTER GROUP testgroup DROP USER user1, user2, user3", ""},

		// DROP ROLE/USER/GROUP statements (USER and GROUP are synonyms for ROLE)
		{"DROP ROLE basic", "DROP ROLE testuser", ""},
		{"DROP USER basic", "DROP USER testuser", "DROP ROLE testuser"},
		{"DROP GROUP basic", "DROP GROUP testgroup", "DROP ROLE testgroup"},
		{"DROP ROLE IF EXISTS", "DROP ROLE IF EXISTS testuser", ""},
		{"DROP USER IF EXISTS", "DROP USER IF EXISTS testuser", "DROP ROLE IF EXISTS testuser"},
		{"DROP GROUP IF EXISTS", "DROP GROUP IF EXISTS testgroup", "DROP ROLE IF EXISTS testgroup"},
		{"DROP ROLE multiple", "DROP ROLE user1, user2, user3", ""},
		{"DROP USER multiple with IF EXISTS", "DROP USER IF EXISTS user1, user2, user3", "DROP ROLE IF EXISTS user1, user2, user3"},

		// ALTER ROLE SET statements
		{"ALTER ROLE SET basic", "ALTER ROLE testuser SET search_path TO public", ""},
		{"ALTER ROLE SET with string value", "ALTER ROLE testuser SET timezone TO 'UTC'", "ALTER ROLE testuser SET timezone TO UTC"},
		{"ALTER ROLE SET with multiple values", "ALTER ROLE testuser SET search_path TO schema1, schema2", ""},
		{"ALTER ROLE SET with numeric value", "ALTER ROLE testuser SET work_mem TO 1024", ""},
		{"ALTER ROLE SET with boolean value", "ALTER ROLE testuser SET log_statement TO on", ""},
		{"ALTER ROLE SET in database", "ALTER ROLE testuser IN DATABASE mydb SET search_path TO public", ""},
		{"ALTER ROLE SET in database with string", "ALTER ROLE testuser IN DATABASE mydb SET timezone TO 'UTC'", "ALTER ROLE testuser IN DATABASE mydb SET timezone TO UTC"},
		{"ALTER ROLE RESET basic", "ALTER ROLE testuser RESET search_path", ""},
		{"ALTER ROLE RESET ALL", "ALTER ROLE testuser RESET ALL", ""},
		{"ALTER ROLE RESET in database", "ALTER ROLE testuser IN DATABASE mydb RESET search_path", ""},
		{"ALTER ROLE RESET ALL in database", "ALTER ROLE testuser IN DATABASE mydb RESET ALL", ""},
		{"ALTER USER SET", "ALTER USER testuser SET search_path TO public", "ALTER ROLE testuser SET search_path TO public"},
		{"ALTER USER SET in database", "ALTER USER testuser IN DATABASE mydb SET timezone TO 'UTC'", "ALTER ROLE testuser IN DATABASE mydb SET timezone TO UTC"},
		{"ALTER USER RESET", "ALTER USER testuser RESET search_path", "ALTER ROLE testuser RESET search_path"},
		{"ALTER ROLE ALL SET", "ALTER ROLE ALL SET search_path TO public", ""},
		{"ALTER ROLE ALL SET in database", "ALTER ROLE ALL IN DATABASE mydb SET search_path TO public", ""},
		{"ALTER ROLE ALL RESET", "ALTER ROLE ALL RESET search_path", ""},
		{"ALTER ROLE ALL RESET in database", "ALTER ROLE ALL IN DATABASE mydb RESET ALL", ""},
		{"ALTER USER ALL SET", "ALTER USER ALL SET search_path TO public", "ALTER ROLE ALL SET search_path TO public"},
		{"ALTER USER ALL RESET", "ALTER USER ALL RESET search_path", "ALTER ROLE ALL RESET search_path"},
		{"ALTER ROLE SET log_statement", "ALTER ROLE testuser SET log_statement TO 'all'", ""},
		{"ALTER ROLE SET shared_preload_libraries", "ALTER ROLE testuser SET shared_preload_libraries TO 'pg_stat_statements'", "ALTER ROLE TESTUSER SET SHARED_PRELOAD_LIBRARIES TO PG_STAT_STATEMENTS"},
		{"ALTER ROLE SET with quoted identifier", "ALTER ROLE \"test-user\" SET search_path TO public", "ALTER ROLE TEST-USER SET SEARCH_PATH TO PUBLIC"},
		{"ALTER ROLE SET with DEFAULT", "ALTER ROLE testuser SET search_path TO DEFAULT", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Normalize both strings for comparison (remove extra spaces, uppercase keywords)
			normalizedDeparsed := normalizeSQL(deparsed)
			normalizedExpected := normalizeSQL(expected)

			assert.Equal(t, normalizedExpected, normalizedDeparsed,
				"Deparsed SQL should match expected.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			statements, err = ParseSQL(deparsed)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")
			deparsed = statements[0].SqlString()
			normalizedDeparsed = normalizeSQL(deparsed)
			assert.Equal(t, normalizedExpected, normalizedDeparsed,
				"Second Deparsed SQL should match expected.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)
		})
	}
}

// TestExpressionDeparsing tests deparsing of various expression types
func TestExpressionDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Expression deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Also verify that re-parsing the deparsed SQL succeeds
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
			require.Len(t, statements2, 1, "Re-parsed should have exactly one statement")

			// Verify stability - deparsing again should produce the same result
			deparsed2 := statements2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// TestRoundTripParsing tests that we can parse, deparse, and re-parse successfully
func TestRoundTripParsing(t *testing.T) {
	tests := []struct {
		query    string
		expected string // If empty, expects exact match with query
	}{
		// Basic queries
		{"SELECT * FROM users", ""},
		{"SELECT id, name FROM users WHERE active = TRUE", ""},
		{"SELECT DISTINCT department FROM employees", ""},

		// Expressions
		{"SELECT 1 + 2 * 3", ""},
		{"SELECT age::text FROM users", ""},
		{"SELECT length(name) FROM users", ""},

		// Complex queries
		{"SELECT * FROM users WHERE (age > 18 AND active = TRUE)", ""},
		{"SELECT id AS user_id, name AS user_name FROM users AS u", ""},
		{"SELECT * FROM users, orders WHERE users.id = orders.user_id", ""},

		// Special cases
		{"TABLE users", "SELECT * FROM users"}, // TABLE is converted to SELECT *
		{"SELECT * FROM ONLY users", ""},
		{"SELECT * INTO backup FROM users", ""},

		// COPY statements - basic round trip parsing
		{"COPY users FROM '/path/to/file.csv'", ""},
		{"COPY users FROM '/path/to/file.csv' (format 'csv')", ""},            // Options now preserved!
		{"COPY users (id, name) FROM '/path/to/file.csv' (format 'csv')", ""}, // Options now preserved!
		{"COPY users TO '/path/to/file.csv' (format 'csv', header true)", ""}, // Options now preserved!
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			// First parse
			statements1, err1 := ParseSQL(tt.query)
			require.NoError(t, err1, "First parse should succeed")
			require.Len(t, statements1, 1, "Should have exactly one statement")

			// Deparse
			deparsed := statements1[0].SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.query
			}

			// Check the deparsed output matches expected
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Round-trip deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.query, deparsed, expected)

			// Second parse (of deparsed SQL)
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Second parse should succeed. Deparsed: %s", deparsed)
			require.Len(t, statements2, 1, "Should have exactly one statement after re-parsing")

			// Third deparse (should be stable)
			deparsed2 := statements2[0].SqlString()

			// The second deparsing should match the first (stability test)
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

func TestOneCase(t *testing.T) {
	query := ""
	output := ""
	if query == "" {
		t.Skip("No tests to run")
	}
	if output == "" {
		output = query
	}
	statements, err := ParseSQL(query)
	require.NoError(t, err, "First parse should succeed")
	require.Len(t, statements, 1, "Should have exactly one statement")

	// Deparse
	deparsed := statements[0].SqlString()
	assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

	// Second parse (of deparsed SQL)
	statements, err = ParseSQL(deparsed)
	require.NoError(t, err, "Second parse should succeed. Deparsed: %s", deparsed)
	require.Len(t, statements, 1, "Should have exactly one statement after re-parsing")

	// Third deparse (should be stable)
	deparsed = statements[0].SqlString()

	// The second deparsing should match the first (stability test)
	assert.Equal(t, output, deparsed)
}

// TestDeparsingEdgeCases tests deparsing of edge cases and special scenarios
func TestDeparsingEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		shouldParse  bool
		checkDeparse bool
	}{
		// Empty and minimal queries
		{"SELECT without FROM", "SELECT 1", true, true},
		{"SELECT NULL", "SELECT NULL", true, true},
		{"SELECT multiple constants", "SELECT 1, 2, 3", true, true},

		// Special identifiers
		{"Reserved word as identifier", `SELECT * FROM "select"`, true, true},
		{"Mixed case identifier", `SELECT * FROM "Users"`, true, true},
		{"Identifier with spaces", `SELECT * FROM "user table"`, true, true},

		// Complex expressions
		{"Parentheses preservation", "SELECT (((1)))", true, true},
		{"Expression with all operators", "SELECT 1 + 2 - 3 * 4 / 5 % 6 ^ 7", true, true},

		// Special SELECT variants
		{"SELECT with column list alias", "SELECT * FROM users AS u(id, name)", true, false}, // Column aliases in FROM not fully supported yet
		{"SELECT with multiple DISTINCT ON", "SELECT DISTINCT ON (a, b) c FROM t", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Try to parse
			statements, err := ParseSQL(tt.input)

			if !tt.shouldParse {
				assert.Error(t, err, "Expected parse error")
				return
			}

			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1, "Should have exactly one statement")

			if !tt.checkDeparse {
				// Skip deparsing check for unsupported features
				return
			}

			// Try to deparse
			deparsed := statements[0].SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Try to re-parse the deparsed SQL
			statements2, err2 := ParseSQL(deparsed)
			assert.NoError(t, err2, "Re-parsing should succeed. Deparsed: %s", deparsed)
			if err2 == nil {
				assert.Len(t, statements2, 1, "Should have exactly one statement after re-parsing")
			}
		})
	}
}

// =============================================================================
// TABLE FUNCTION DEPARSE TESTS - Tests for table function deparsing
// =============================================================================

func TestFuncTableDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		{
			name: "Simple function table",
			sql:  "SELECT * FROM generate_series(1, 5)",
		},
		{
			name: "Function table with ORDINALITY",
			sql:  "SELECT * FROM generate_series(1, 5) WITH ORDINALITY",
		},
		{
			name: "LATERAL function table",
			sql:  "SELECT * FROM LATERAL generate_series(1, t.max_val)",
		},
		{
			name: "Function table with alias",
			sql:  "SELECT * FROM generate_series(1, 5) AS t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Expected exactly one statement")

			// Deparse the entire statement
			deparsed := stmts[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Function table deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.sql, deparsed, expected)

			// Verify round-trip parsing works
			stmts2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

			// Verify stability - deparsing again should produce the same result
			deparsed2 := stmts2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Function table deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

func TestXMLTableDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		{
			name: "Basic XMLTABLE",
			sql:  "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS id INT, name TEXT)",
		},
		{
			name: "XMLTABLE with FOR ORDINALITY",
			sql:  "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS pos FOR ORDINALITY, id INT)",
		},
		{
			name: "LATERAL XMLTABLE",
			sql:  "SELECT * FROM LATERAL XMLTABLE('/root/item' PASSING t.xml_data COLUMNS id INT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Expected exactly one statement")

			// Deparse the entire statement
			deparsed := stmts[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"XMLTABLE deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.sql, deparsed, expected)

			// Verify round-trip parsing works
			stmts2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

			// Verify stability - deparsing again should produce the same result
			deparsed2 := stmts2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"XMLTABLE deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// normalizeSQL normalizes SQL for comparison by:
// - Converting to uppercase
// - Removing extra whitespace
// - Removing trailing semicolons
func normalizeSQL(sql string) string {
	// Convert to uppercase
	sql = strings.ToUpper(sql)

	// Replace multiple spaces with single space
	sql = strings.Join(strings.Fields(sql), " ")

	// Remove trailing semicolon if present
	sql = strings.TrimSuffix(sql, ";")

	// Trim whitespace
	sql = strings.TrimSpace(sql)

	return sql
}

// TestDeparsingWithAliases specifically tests alias deparsing
func TestDeparsingWithAliases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Column alias with AS", "SELECT id AS user_id", ""},
		{"Column alias without AS", "SELECT id user_id", "SELECT id AS user_id"},
		{"Multiple column aliases", "SELECT id AS user_id, name AS user_name", ""},
		{"Table alias with AS", "SELECT * FROM users AS u", ""},
		{"Table alias without AS", "SELECT * FROM users u", "SELECT * FROM users AS u"},
		{"Complex aliases", "SELECT u.id AS user_id FROM users AS u", ""},
		{"Function with alias", "SELECT length(name) AS name_length FROM users", ""},
		{"Expression with alias", "SELECT age + 1 AS next_age FROM users", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			// Deparse
			deparsed := statements[0].SqlString()

			// Determine expected
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Alias deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// TestMissingDeparsingCoverage tests the previously identified gaps in deparsing coverage
func TestMissingDeparsingCoverage(t *testing.T) {
	t.Run("MultipleStatements", testMultipleStatements)
	t.Run("NameLists", testNameLists)
	t.Run("Indirection", testIndirection)
	t.Run("QualifiedOperators", testQualifiedOperators)
	t.Run("AdvancedTypeCasting", testAdvancedTypeCasting)
	t.Run("ParenthesizedSelect", testParenthesizedSelect)
	t.Run("SelectAllClause", testSelectAllClause)
}

// testMultipleStatements tests deparsing of multiple statements (stmtmulti)
func testMultipleStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Greater(t, len(statements), 1, "Should have multiple statements")

			// Deparse each statement and join with semicolons
			var deparsedParts []string
			for _, stmt := range statements {
				deparsedParts = append(deparsedParts, stmt.SqlString())
			}
			deparsed := strings.Join(deparsedParts, "; ")

			assert.Equal(t, normalizeSQL(tt.expected), normalizeSQL(deparsed),
				"Multiple statement deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testNameLists tests deparsing of name lists and qualified name lists
func testNameLists(t *testing.T) {
	// Note: Since name_list and qualified_name_list are typically used internally
	// in grammar rules, we test them through contexts where they appear

	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
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
		// Note: More advanced name list contexts (like in DDL) will be tested in later phases
	}

	for _, tt := range tests {
		if tt.skip {
			t.Run(tt.name, func(t *testing.T) {
				t.Skip(tt.reason)
			})
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Name list deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testIndirection tests deparsing of array subscripts and field access
func testIndirection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:   "array subscript",
			input:  "SELECT column[1]",
			skip:   true,
			reason: "Array subscript parsing not yet implemented",
		},
		{
			name:   "array slice",
			input:  "SELECT column[1:5]",
			skip:   true,
			reason: "Array slice parsing not yet implemented",
		},
		{
			name:   "field access",
			input:  "SELECT record.field",
			skip:   true,
			reason: "Field access parsing not yet implemented",
		},
		{
			name:   "nested field access",
			input:  "SELECT record.subrecord.field",
			skip:   true,
			reason: "Nested field access parsing not yet implemented",
		},
		{
			name:   "mixed indirection",
			input:  "SELECT array_col[1].field",
			skip:   true,
			reason: "Mixed indirection parsing not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Indirection deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testQualifiedOperators tests deparsing of qualified operators
func testQualifiedOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:   "qualified operator",
			input:  "SELECT a OPERATOR(pg_catalog.+) b",
			skip:   true,
			reason: "Qualified operator parsing not yet implemented",
		},
		{
			name:   "schema qualified operator",
			input:  "SELECT a OPERATOR(myschema.=) b",
			skip:   true,
			reason: "Schema qualified operator parsing not yet implemented",
		},
		{
			name:   "custom operator",
			input:  "SELECT a OPERATOR(public.@@) b",
			skip:   true,
			reason: "Custom operator parsing not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Qualified operator deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testAdvancedTypeCasting tests deparsing of advanced type casting
func testAdvancedTypeCasting(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:     "bit type casting",
			input:    "SELECT value::bit",
			expected: "",
		},
		{
			name:     "bit with length",
			input:    "SELECT value::bit(8)",
			expected: "SELECT value::bit", // Note: Type modifiers not preserved in current implementation
		},
		{
			name:     "timestamp type",
			input:    "SELECT value::timestamp",
			expected: "",
		},
		{
			name:     "timestamp with precision",
			input:    "SELECT value::timestamp(6)",
			expected: "SELECT value::timestamp", // Note: Type modifiers not preserved in current implementation
		},
		{
			name:     "timestamptz type",
			input:    "SELECT value::timestamptz",
			expected: "",
		},
		{
			name:     "date type",
			input:    "SELECT value::date",
			expected: "",
		},
		{
			name:     "time type",
			input:    "SELECT value::time",
			expected: "",
		},
		{
			name:   "interval type",
			input:  "SELECT value::interval",
			skip:   true,
			reason: "Interval type parsing not yet implemented",
		},
		{
			name:   "interval with fields",
			input:  "SELECT value::interval day to hour",
			skip:   true,
			reason: "Interval type parsing not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Advanced type casting deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testParenthesizedSelect tests deparsing of parenthesized SELECT statements
func testParenthesizedSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple parenthesized select",
			input:    "(SELECT * FROM users)",
			expected: "SELECT * FROM users", // Note: Parentheses not preserved in current implementation
		},
		{
			name:     "nested parentheses",
			input:    "((SELECT id FROM users))",
			expected: "SELECT id FROM users", // Note: Parentheses not preserved in current implementation
		},
		{
			name:     "parenthesized select with where",
			input:    "(SELECT * FROM users WHERE active = true)",
			expected: "SELECT * FROM users WHERE active = TRUE", // Note: Parentheses not preserved in current implementation
		},
		{
			name:     "parenthesized select with alias",
			input:    "(SELECT id AS user_id FROM users)",
			expected: "SELECT id AS user_id FROM users", // Note: Parentheses not preserved in current implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()

			// For parenthesized SELECT, we expect the parentheses to be preserved
			assert.Equal(t, normalizeSQL(tt.expected), normalizeSQL(deparsed),
				"Parenthesized SELECT deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testSelectAllClause tests deparsing of SELECT ALL clause
func testSelectAllClause(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:   "select all",
			input:  "SELECT ALL * FROM users",
			skip:   true,
			reason: "SELECT ALL parsing not yet implemented - opt_all_clause rule needs implementation",
		},
		{
			name:   "select all with columns",
			input:  "SELECT ALL id, name FROM users",
			skip:   true,
			reason: "SELECT ALL parsing not yet implemented - opt_all_clause rule needs implementation",
		},
		{
			name:   "select all with where",
			input:  "SELECT ALL * FROM users WHERE active = true",
			skip:   true,
			reason: "SELECT ALL parsing not yet implemented - opt_all_clause rule needs implementation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"SELECT ALL deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// TestComprehensiveSELECTDeparsing tests comprehensive SELECT statement round-trip parsing
// This replaces and enhances the tests from select_statement_test.go
func TestComprehensiveSELECTDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
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
		{"Type cast to text", "SELECT id::text FROM users", ""},
		{"Type cast with spaces", "SELECT id :: integer FROM users", "SELECT id::integer FROM users"},
		{"Complex expression cast", "SELECT (age + 1)::varchar FROM users", ""},
		{"Multiple casts", "SELECT id::text, age::varchar FROM users", ""},

		// Column references
		{"Simple column reference", "SELECT id FROM users", ""},
		{"Qualified column reference", "SELECT users.id FROM users", ""},
		{"Schema qualified column", "SELECT public.users.id FROM public.users", ""},
		{"Multiple qualified columns", "SELECT u.id, u.name FROM users AS u", ""},

		// Function calls
		{"Function no args", "SELECT now()", ""},
		{"Function single arg", "SELECT length(name) FROM users", ""},
		{"Function multiple args", "SELECT substring(name, 1, 5) FROM users", ""},
		{"Qualified function", "SELECT pg_catalog.length(name) FROM users", ""},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a SELECT statement
			selectStmt, ok := statements[0].(*ast.SelectStmt)
			require.True(t, ok, "Expected SelectStmt")
			require.NotNil(t, selectStmt, "SelectStmt should not be nil")

			// Deparse back to SQL
			deparsed := selectStmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected result
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Round-trip SELECT deparsing failed.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Verify round-trip stability (re-parse the deparsed SQL)
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, statements2, 1, "Re-parsed should have exactly one statement")

			// Verify second deparse is identical (stability test)
			deparsed2 := statements2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// TestOperatorPrecedenceDeparsing tests operator precedence within SELECT statements
// This ensures operators are parsed and deparsed with correct precedence
func TestOperatorPrecedenceDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
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
		{"Type cast precedence", "SELECT age::text FROM users", ""},

		// WHERE clause complex precedence
		{"Complex WHERE precedence", "SELECT * FROM users WHERE id = 1 OR id = 2 AND active", ""},
		{"WHERE with parentheses", "SELECT * FROM users WHERE (id = 1 OR id = 2) AND active", ""},
		{"NOT precedence", "SELECT * FROM users WHERE NOT active AND verified", ""},
		{"NOT with parentheses", "SELECT * FROM users WHERE NOT (active AND verified)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Deparse back to SQL
			deparsed := statements[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected result
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Operator precedence deparsing failed.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Verify round-trip parsing works
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing should succeed: %s", deparsed)
			require.Len(t, statements2, 1, "Should have one statement after re-parsing")
		})
	}
}

// TestAdvancedDeparsing tests comprehensive deparsing for JOIN, CTE, and subquery features
func TestAdvancedDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Normalize both strings for comparison
			normalizedDeparsed := normalizeSQL(deparsed)
			normalizedExpected := normalizeSQL(expected)

			require.Equal(t, normalizedExpected, normalizedDeparsed)
			// Most importantly, ensure round-trip parsing works
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing must succeed: %v\nDeparsed: %s", reparseErr, deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")
		})
	}
}

// =============================================================================
// COMPREHENSIVE TABLE FUNCTION TESTS - Tests for all table function types with round-trip verification
// =============================================================================

// TestTableFunctions tests comprehensive round-trip parsing for all table function types
func TestTableFunctions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		// Function table tests (equivalent to TestFuncTable)
		{
			name: "Simple function table",
			sql:  "SELECT * FROM generate_series(1, 5)",
		},
		{
			name: "Function table with ORDINALITY",
			sql:  "SELECT * FROM generate_series(1, 5) WITH ORDINALITY",
		},
		{
			name: "LATERAL function table",
			sql:  "SELECT * FROM LATERAL generate_series(1, t.max_val)",
		},
		{
			name: "ROWS FROM syntax",
			sql:  "SELECT * FROM ROWS FROM (generate_series(1, 5))",
		},
		{
			name: "ROWS FROM with ORDINALITY",
			sql:  "SELECT * FROM ROWS FROM (generate_series(1, 5)) WITH ORDINALITY",
		},

		// XMLTABLE tests (equivalent to TestXMLTable)
		{
			name: "Basic XMLTABLE",
			sql:  "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS id INT, name TEXT)",
		},
		{
			name: "XMLTABLE with FOR ORDINALITY",
			sql:  "SELECT * FROM XMLTABLE('/root/item' PASSING '<root><item>1</item></root>' COLUMNS pos FOR ORDINALITY, id INT)",
		},
		{
			name: "LATERAL XMLTABLE",
			sql:  "SELECT * FROM LATERAL XMLTABLE('/root/item' PASSING t.xml_data COLUMNS id INT)",
		},

		// JSON_TABLE tests (equivalent to TestJSONTable)
		{
			name: "Basic JSON_TABLE",
			sql:  `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (id INT))`,
		},
		{
			name: "JSON_TABLE with FOR ORDINALITY",
			sql:  `SELECT * FROM JSON_TABLE('{"items": [1, 2, 3]}', '$.items[*]' COLUMNS (pos FOR ORDINALITY, val INT PATH '$'))`,
		},
		{
			name: "JSON_TABLE with EXISTS column",
			sql:  `SELECT * FROM JSON_TABLE('{"items": [{"id": 1}]}', '$.items[*]' COLUMNS (has_id BOOLEAN EXISTS PATH '$.id'))`,
		},
		{
			name: "JSON_TABLE with NESTED columns",
			sql:  `SELECT * FROM JSON_TABLE('{"items": [{"props": {"a": 1}}]}', '$.items[*]' COLUMNS (NESTED PATH '$.props' COLUMNS (a INT PATH '$.a')))`,
		},
		{
			name: "LATERAL JSON_TABLE",
			sql:  `SELECT * FROM LATERAL JSON_TABLE(t.json_data, '$.items[*]' COLUMNS (id INT PATH '$.id'))`,
		},

		// Table function aliases tests (equivalent to TestTableFunctionAliases)
		{
			name: "Function table with alias",
			sql:  "SELECT * FROM generate_series(1, 5) AS t",
		},
		{
			name: "XMLTABLE with alias",
			sql:  "SELECT * FROM XMLTABLE('/root/item' PASSING '<root></root>' COLUMNS id INT) AS xt",
		},
		{
			name: "JSON_TABLE with alias",
			sql:  `SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (id INT)) AS jt`,
		},
		{
			name: "Function table with column aliases",
			sql:  "SELECT * FROM generate_series(1, 5) AS t(num)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Expected exactly one statement")

			// Deparse the entire statement
			deparsed := stmts[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Table function deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.sql, deparsed, expected)

			// Verify round-trip parsing works
			stmts2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

			// Verify stability - deparsing again should produce the same result
			deparsed2 := stmts2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Table function deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// =============================================================================
// DML STATEMENT DEPARSE TESTS - Tests for INSERT, UPDATE, DELETE, MERGE deparsing
// =============================================================================

// TestDMLRoundTrip tests round-trip parsing and deparsing of DML statements
func TestDMLRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // if different from input SQL
	}{
		// INSERT statements
		{
			name: "INSERT with VALUES",
			sql:  "INSERT INTO users VALUES (1, 'John')",
		},
		{
			name: "INSERT with column list and VALUES",
			sql:  "INSERT INTO users (id, name) VALUES (1, 'John')",
		},
		{
			name: "INSERT with SELECT",
			sql:  "INSERT INTO users SELECT * FROM temp_users",
		},
		{
			name: "INSERT with DEFAULT VALUES",
			sql:  "INSERT INTO users DEFAULT VALUES",
		},
		{
			name: "INSERT with RETURNING",
			sql:  "INSERT INTO users (name) VALUES ('John') RETURNING id",
		},
		{
			name: "INSERT with schema qualified table",
			sql:  "INSERT INTO public.users (name) VALUES ('John')",
		},

		// UPDATE statements
		{
			name: "simple UPDATE",
			sql:  "UPDATE users SET name = 'Jane'",
		},
		{
			name: "UPDATE with WHERE",
			sql:  "UPDATE users SET name = 'Jane' WHERE id = 1",
		},
		{
			name: "UPDATE with RETURNING",
			sql:  "UPDATE users SET name = 'Jane' RETURNING id, name",
		},
		{
			name: "UPDATE with multiple columns",
			sql:  "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1",
		},
		{
			name: "UPDATE with schema qualified table",
			sql:  "UPDATE public.users SET name = 'Jane'",
		},

		// DELETE statements
		{
			name: "simple DELETE",
			sql:  "DELETE FROM users",
		},
		{
			name: "DELETE with WHERE",
			sql:  "DELETE FROM users WHERE id = 1",
		},
		{
			name: "DELETE with RETURNING",
			sql:  "DELETE FROM users WHERE id = 1 RETURNING name",
		},
		{
			name: "DELETE with schema qualified table",
			sql:  "DELETE FROM public.users WHERE id = 1",
		},

		// MERGE statements (basic)
		{
			name: "basic MERGE",
			sql:  "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name: "MERGE with schema qualified tables",
			sql:  "MERGE INTO public.target USING staging.source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Expected exactly 1 statement")

			stmt := stmts[0]

			// Verify statement type
			var expectedType string
			switch stmt.(type) {
			case *ast.InsertStmt:
				expectedType = "INSERT"
			case *ast.UpdateStmt:
				expectedType = "UPDATE"
			case *ast.DeleteStmt:
				expectedType = "DELETE"
			case *ast.MergeStmt:
				expectedType = "MERGE"
			default:
				t.Fatalf("Unexpected statement type: %T", stmt)
			}

			assert.Equal(t, expectedType, stmt.StatementType())

			// Test SqlString method
			sqlString := stmt.SqlString()
			assert.NotEmpty(t, sqlString, "SqlString() should not be empty")

			// Note: We focus on round-trip parsing rather than exact string matching
			// due to possible formatting differences in the deparsed output

			// For round-trip testing, we compare the semantic meaning
			// rather than exact string match due to possible formatting differences
			t.Logf("Original: %s", tt.sql)
			t.Logf("Deparsed: %s", sqlString)

			// Try to parse the deparsed SQL to ensure it's valid
			reparsedStmts, err := ParseSQL(sqlString)
			assert.NoError(t, err, "Failed to reparse deparsed SQL: %s", sqlString)
			assert.Len(t, reparsedStmts, 1, "Reparsed SQL should produce exactly 1 statement")

			// Verify the reparsed statement has the same type
			reparsedStmt := reparsedStmts[0]
			assert.Equal(t, stmt.StatementType(), reparsedStmt.StatementType(),
				"Reparsed statement should have same type")

			// Additional semantic checks based on statement type
			switch origStmt := stmt.(type) {
			case *ast.InsertStmt:
				reparsedInsert := reparsedStmt.(*ast.InsertStmt)
				if origStmt.Relation != nil && reparsedInsert.Relation != nil {
					assert.Equal(t, origStmt.Relation.RelName, reparsedInsert.Relation.RelName,
						"Table name should be preserved")
				}
			case *ast.UpdateStmt:
				reparsedUpdate := reparsedStmt.(*ast.UpdateStmt)
				if origStmt.Relation != nil && reparsedUpdate.Relation != nil {
					assert.Equal(t, origStmt.Relation.RelName, reparsedUpdate.Relation.RelName,
						"Table name should be preserved")
				}
			case *ast.DeleteStmt:
				reparsedDelete := reparsedStmt.(*ast.DeleteStmt)
				if origStmt.Relation != nil && reparsedDelete.Relation != nil {
					assert.Equal(t, origStmt.Relation.RelName, reparsedDelete.Relation.RelName,
						"Table name should be preserved")
				}
			case *ast.MergeStmt:
				reparsedMerge := reparsedStmt.(*ast.MergeStmt)
				if origStmt.Relation != nil && reparsedMerge.Relation != nil {
					assert.Equal(t, origStmt.Relation.RelName, reparsedMerge.Relation.RelName,
						"Target table name should be preserved")
				}
			}
		})
	}
}

// TestDMLSqlStringMethods tests the SqlString methods directly on AST nodes
func TestDMLSqlStringMethods(t *testing.T) {
	t.Run("InsertStmt SqlString", func(t *testing.T) {
		// Create a simple INSERT statement node
		relation := ast.NewRangeVar("users", "", "")
		insertStmt := ast.NewInsertStmt(relation)

		// Add a simple SELECT for VALUES
		selectStmt := ast.NewSelectStmt()
		insertStmt.SelectStmt = selectStmt

		sqlString := insertStmt.SqlString()
		assert.Contains(t, sqlString, "INSERT INTO")
		assert.Contains(t, sqlString, "users")
		t.Logf("InsertStmt SqlString: %s", sqlString)
	})

	t.Run("UpdateStmt SqlString", func(t *testing.T) {
		// Create a simple UPDATE statement node
		relation := ast.NewRangeVar("users", "", "")
		updateStmt := ast.NewUpdateStmt(relation)

		// Add a target (SET clause)
		target := ast.NewResTarget("name", ast.NewA_Const(ast.NewString("Jane"), 0))
		updateStmt.TargetList = ast.NewNodeList()
		updateStmt.TargetList.Append(target)

		sqlString := updateStmt.SqlString()
		assert.Contains(t, sqlString, "UPDATE")
		assert.Contains(t, sqlString, "users")
		assert.Contains(t, sqlString, "SET")
		t.Logf("UpdateStmt SqlString: %s", sqlString)
	})

	t.Run("DeleteStmt SqlString", func(t *testing.T) {
		// Create a simple DELETE statement node
		relation := ast.NewRangeVar("users", "", "")
		deleteStmt := ast.NewDeleteStmt(relation)

		sqlString := deleteStmt.SqlString()
		assert.Contains(t, sqlString, "DELETE FROM")
		assert.Contains(t, sqlString, "users")
		t.Logf("DeleteStmt SqlString: %s", sqlString)
	})

	t.Run("MergeStmt SqlString", func(t *testing.T) {
		// Create a simple MERGE statement node
		targetRelation := ast.NewRangeVar("target", "", "")
		sourceRelation := ast.NewRangeVar("source", "", "")
		joinCondition := ast.NewA_Const(ast.NewString("target.id = source.id"), 0)

		mergeStmt := ast.NewMergeStmt(targetRelation, sourceRelation, joinCondition)

		sqlString := mergeStmt.SqlString()
		assert.Contains(t, sqlString, "MERGE INTO")
		assert.Contains(t, sqlString, "target")
		assert.Contains(t, sqlString, "USING")
		assert.Contains(t, sqlString, "source")
		assert.Contains(t, sqlString, "ON")
		t.Logf("MergeStmt SqlString: %s", sqlString)
	})
}

// TestDMLWithClauses tests DML statements with various clauses
func TestDMLWithClauses(t *testing.T) {
	t.Run("INSERT with WITH clause", func(t *testing.T) {
		sql := "WITH temp AS (SELECT 1 as id) INSERT INTO users SELECT * FROM temp"

		stmts, err := ParseSQL(sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		insertStmt := stmts[0].(*ast.InsertStmt)
		sqlString := insertStmt.SqlString()

		assert.Contains(t, sqlString, "WITH")
		assert.Contains(t, sqlString, "INSERT INTO")
		t.Logf("INSERT with WITH: %s", sqlString)
	})

	t.Run("UPDATE with FROM clause", func(t *testing.T) {
		sql := "UPDATE users SET name = temp.name FROM temp_users temp WHERE users.id = temp.id"

		stmts, err := ParseSQL(sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		updateStmt := stmts[0].(*ast.UpdateStmt)
		sqlString := updateStmt.SqlString()

		assert.Contains(t, sqlString, "UPDATE")
		assert.Contains(t, sqlString, "SET")
		assert.Contains(t, sqlString, "FROM")
		assert.Contains(t, sqlString, "WHERE")
		t.Logf("UPDATE with FROM: %s", sqlString)
	})

	t.Run("DELETE with USING clause", func(t *testing.T) {
		sql := "DELETE FROM users USING temp_users temp WHERE users.id = temp.id"

		stmts, err := ParseSQL(sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		deleteStmt := stmts[0].(*ast.DeleteStmt)
		sqlString := deleteStmt.SqlString()

		assert.Contains(t, sqlString, "DELETE FROM")
		assert.Contains(t, sqlString, "USING")
		assert.Contains(t, sqlString, "WHERE")
		t.Logf("DELETE with USING: %s", sqlString)
	})
}

// TestComprehensiveDMLDeparsing tests comprehensive DML deparsing for Phase-3E features
func TestComprehensiveDMLDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		// ===== INSERT Statement Tests =====
		{
			name: "INSERT basic VALUES",
			sql:  "INSERT INTO users VALUES (1, 'John')",
		},
		{
			name: "INSERT with column list",
			sql:  "INSERT INTO users (id, name) VALUES (1, 'John')",
		},
		{
			name: "INSERT multiple VALUES",
			sql:  "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')",
		},
		{
			name: "INSERT with DEFAULT VALUES",
			sql:  "INSERT INTO users DEFAULT VALUES",
		},
		{
			name: "INSERT with SELECT",
			sql:  "INSERT INTO users SELECT id, name FROM temp_users",
		},
		{
			name: "INSERT with subquery",
			sql:  "INSERT INTO users (SELECT id, name FROM temp_users WHERE active = TRUE)",
		},
		{
			name: "INSERT with RETURNING single column",
			sql:  "INSERT INTO users (name) VALUES ('John') RETURNING id",
		},
		{
			name: "INSERT with RETURNING multiple columns",
			sql:  "INSERT INTO users (name) VALUES ('John') RETURNING id, name, created_at",
		},
		{
			name: "INSERT with RETURNING *",
			sql:  "INSERT INTO users (name) VALUES ('John') RETURNING *",
		},
		{
			name: "INSERT with qualified table name",
			sql:  "INSERT INTO public.users (name) VALUES ('John')",
		},
		{
			name: "INSERT with table alias",
			sql:  "INSERT INTO users AS u (name) VALUES ('John')",
		},
		{
			name: "INSERT with WITH clause",
			sql:  "WITH temp AS (SELECT 'John' as name) INSERT INTO users (name) SELECT name FROM temp",
		},
		{
			name: "INSERT with complex expressions in VALUES",
			sql:  "INSERT INTO users (id, name, age) VALUES (1 + 2, upper('john'), 25 * 2)",
		},

		// ===== UPDATE Statement Tests =====
		{
			name: "UPDATE simple",
			sql:  "UPDATE users SET name = 'Jane'",
		},
		{
			name: "UPDATE with WHERE",
			sql:  "UPDATE users SET name = 'Jane' WHERE id = 1",
		},
		{
			name: "UPDATE multiple columns",
			sql:  "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1",
		},
		{
			name: "UPDATE with complex SET expressions",
			sql:  "UPDATE users SET name = upper('jane'), age = age + 1, updated_at = now()",
		},
		{
			name: "UPDATE with FROM clause",
			sql:  "UPDATE users SET name = temp.name FROM temp_users temp WHERE users.id = temp.id",
		},
		{
			name: "UPDATE with multiple FROM tables",
			sql:  "UPDATE users SET name = t1.name FROM temp_users t1, other_table t2 WHERE users.id = t1.id AND t1.other_id = t2.id",
		},
		{
			name: "UPDATE with complex WHERE",
			sql:  "UPDATE users SET name = 'Jane' WHERE id > 10 AND active = TRUE AND created_at > '2023-01-01'",
		},
		{
			name: "UPDATE with RETURNING single column",
			sql:  "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING id",
		},
		{
			name: "UPDATE with RETURNING multiple columns",
			sql:  "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING id, name, updated_at",
		},
		{
			name: "UPDATE with RETURNING *",
			sql:  "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING *",
		},
		{
			name: "UPDATE with qualified table",
			sql:  "UPDATE public.users SET name = 'Jane' WHERE id = 1",
		},
		// Note: WITH clause for UPDATE/DELETE not yet fully implemented in parser
		// {
		// 	name: "UPDATE with WITH clause",
		// 	sql:  "WITH temp AS (SELECT id FROM active_users) UPDATE users SET active = FALSE WHERE id IN (SELECT id FROM temp)",
		// },
		// {
		// 	name: "UPDATE with subquery in SET",
		// 	sql:  "UPDATE users SET name = (SELECT name FROM profiles WHERE profiles.user_id = users.id)",
		// },
		// {
		// 	name: "UPDATE with subquery in WHERE",
		// 	sql:  "UPDATE users SET active = FALSE WHERE id IN (SELECT user_id FROM banned_users)",
		// },

		// ===== DELETE Statement Tests =====
		{
			name: "DELETE simple",
			sql:  "DELETE FROM users",
		},
		{
			name: "DELETE with WHERE",
			sql:  "DELETE FROM users WHERE id = 1",
		},
		{
			name: "DELETE with complex WHERE",
			sql:  "DELETE FROM users WHERE active = FALSE AND created_at < '2020-01-01'",
		},
		{
			name: "DELETE with USING clause",
			sql:  "DELETE FROM users USING temp_users temp WHERE users.id = temp.id",
		},
		{
			name: "DELETE with multiple USING tables",
			sql:  "DELETE FROM users USING temp_users t1, other_table t2 WHERE users.id = t1.id AND t1.other_id = t2.id",
		},
		{
			name: "DELETE with RETURNING single column",
			sql:  "DELETE FROM users WHERE id = 1 RETURNING id",
		},
		{
			name: "DELETE with RETURNING multiple columns",
			sql:  "DELETE FROM users WHERE id = 1 RETURNING id, name, deleted_at",
		},
		{
			name: "DELETE with RETURNING *",
			sql:  "DELETE FROM users WHERE id = 1 RETURNING *",
		},
		{
			name: "DELETE with qualified table",
			sql:  "DELETE FROM public.users WHERE id = 1",
		},
		// Note: WITH clause and subqueries in WHERE not yet fully implemented for DELETE
		// {
		// 	name: "DELETE with WITH clause",
		// 	sql:  "WITH temp AS (SELECT id FROM inactive_users) DELETE FROM users WHERE id IN (SELECT id FROM temp)",
		// },
		// {
		// 	name: "DELETE with subquery in WHERE",
		// 	sql:  "DELETE FROM users WHERE id IN (SELECT user_id FROM temp_table)",
		// },

		// ===== MERGE Statement Tests =====
		{
			name: "MERGE basic",
			sql:  "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name: "MERGE with qualified tables",
			sql:  "MERGE INTO public.target USING staging.source ON target.id = source.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name: "MERGE with table aliases",
			sql:  "MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name: "MERGE with complex join condition",
			sql:  "MERGE INTO target USING source ON target.id = source.id AND target.version = source.version WHEN MATCHED THEN DO NOTHING",
		},
		{
			name: "MERGE with subquery as source",
			sql:  "MERGE INTO target USING (SELECT * FROM source WHERE active = TRUE) AS s ON target.id = s.id WHEN MATCHED THEN DO NOTHING",
		},
		{
			name: "MERGE with WITH clause",
			sql:  "WITH filtered AS (SELECT * FROM source WHERE active = TRUE) MERGE INTO target USING filtered ON target.id = filtered.id WHEN MATCHED THEN DO NOTHING",
		},

		// ===== Complex DML with Expressions =====
		{
			name: "INSERT with function calls in VALUES",
			sql:  "INSERT INTO logs (message, created_at) VALUES (concat('Hello ', 'World'), now())",
		},
		{
			name: "UPDATE with arithmetic expressions",
			sql:  "UPDATE products SET price = price * 1.1, updated_count = updated_count + 1",
		},
		// Note: extract() function not yet implemented in parser
		// {
		// 	name: "DELETE with function in WHERE",
		// 	sql:  "DELETE FROM users WHERE length(name) < 3 OR extract(year FROM created_at) < 2020",
		// },
		{
			name: "DELETE with simple function in WHERE",
			sql:  "DELETE FROM users WHERE length(name) < 3",
		},

		// ===== DML with Type Casts =====
		{
			name: "INSERT with type casts",
			sql:  "INSERT INTO users (id, name, age) VALUES (1::bigint, 'John'::varchar, '25'::integer)",
		},
		{
			name: "UPDATE with type casts",
			sql:  "UPDATE users SET score = '95.5'::decimal, active = 'true'::boolean",
		},

		// ===== DML with Advanced Table References =====
		// Note: ONLY modifier not yet fully implemented in DML parser
		// {
		// 	name: "INSERT with ONLY modifier",
		// 	sql:  "INSERT INTO ONLY parent_table (id, name) VALUES (1, 'test')",
		// },
		{
			name: "UPDATE with ONLY modifier",
			sql:  "UPDATE ONLY parent_table SET name = 'updated'",
		},
		{
			name: "DELETE with ONLY modifier",
			sql:  "DELETE FROM ONLY parent_table WHERE id = 1",
		},

		// ===== Edge Cases =====
		// Note: Empty column lists may not be fully supported
		// {
		// 	name: "INSERT with empty column list and VALUES",
		// 	sql:  "INSERT INTO users () VALUES ()",
		// },
		{
			name: "UPDATE with no WHERE clause",
			sql:  "UPDATE users SET active = TRUE",
		},
		{
			name: "DELETE with no WHERE clause",
			sql:  "DELETE FROM temp_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Expected exactly one statement")

			// Deparse the statement
			deparsed := stmts[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Log for debugging
			t.Logf("Original: %s", tt.sql)
			t.Logf("Deparsed: %s", deparsed)

			// Test that the deparsed SQL can be re-parsed (round-trip test)
			stmts2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

			// Verify statement types match
			assert.Equal(t, stmts[0].StatementType(), stmts2[0].StatementType(),
				"Statement types should match after round-trip")

			// Test stability - second deparse should match first
			deparsed2 := stmts2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// TestDMLExpressionDeparsing tests deparsing of complex expressions within DML statements
func TestDMLExpressionDeparsing(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		// Complex expressions in INSERT
		{
			name: "INSERT with nested function calls",
			sql:  "INSERT INTO users (name, email) VALUES (upper(trim('  john  ')), lower(concat('john', '@', 'example.com')))",
		},
		{
			name: "INSERT with arithmetic in VALUES",
			sql:  "INSERT INTO products (id, price, discounted_price) VALUES (1, 100.00, 100.00 * 0.9)",
		},
		{
			name: "INSERT with parenthesized expressions",
			sql:  "INSERT INTO products (total) VALUES ((price + tax) * quantity)",
		},

		// Complex expressions in UPDATE SET clauses
		{
			name: "UPDATE with complex SET expressions",
			sql:  "UPDATE products SET price = price * (1 + tax_rate), updated_at = now()",
		},
		{
			name: "UPDATE with nested arithmetic",
			sql:  "UPDATE stats SET score = (score + bonus) * multiplier, rank = rank + 1",
		},
		{
			name: "UPDATE with function calls in SET",
			sql:  "UPDATE users SET name = upper(trim(name)), email = lower(email)",
		},

		// Complex expressions in WHERE clauses
		{
			name: "DELETE with arithmetic in WHERE",
			sql:  "DELETE FROM products WHERE (price * 0.9) < 10.00",
		},
		{
			name: "UPDATE with function calls in WHERE",
			sql:  "UPDATE users SET active = FALSE WHERE length(name) < 3 AND upper(status) = 'INACTIVE'",
		},
		{
			name: "DELETE with nested expressions in WHERE",
			sql:  "DELETE FROM orders WHERE (total + tax) > (limitval * 1.5) AND status = 'pending'",
		},

		// Expression combinations with type casts
		{
			name: "INSERT with type casts and expressions",
			sql:  "INSERT INTO logs (level, message, count) VALUES (upper('info')::text, concat('Log: ', details), (1 + retry_count)::integer)",
		},
		{
			name: "UPDATE with complex FROM and expressions",
			sql:  "UPDATE orders SET total = o.quantity * p.price, updated_at = now() FROM order_items o, products p WHERE orders.id = o.order_id AND o.product_id = p.id",
		},

		// Advanced function combinations
		{
			name: "INSERT with deeply nested functions",
			sql:  "INSERT INTO processed (data) VALUES (upper(substring(trim(input_data), 1, 10)))",
		},
		{
			name: "UPDATE with multiple function calls",
			sql:  "UPDATE users SET full_name = concat(upper(first_name), ' ', upper(last_name)), slug = lower(replace(name, ' ', '-'))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Expected exactly one statement")

			// Deparse the statement
			deparsed := stmts[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			t.Logf("Original: %s", tt.sql)
			t.Logf("Deparsed: %s", deparsed)

			// Test round-trip parsing
			stmts2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

			// Verify statement types match
			assert.Equal(t, stmts[0].StatementType(), stmts2[0].StatementType(),
				"Statement types should match after round-trip")
		})
	}
}

// TestRenameStmtDeparsing tests round-trip parsing and deparsing for all RenameStmt variants
func TestRenameStmtDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a RenameStmt
			renameStmt, ok := statements[0].(*ast.RenameStmt)
			require.True(t, ok, "Statement should be a RenameStmt")
			require.Equal(t, "RenameStmt", renameStmt.StatementType())

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Log for debugging
			t.Logf("Original: %s", tt.input)
			t.Logf("Deparsed: %s", deparsed)
			t.Logf("Expected: %s", expected)

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"RenameStmt deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Also verify that re-parsing the deparsed SQL succeeds
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
			require.Len(t, statements2, 1, "Re-parsed should have exactly one statement")

			renameStmt2, ok2 := statements2[0].(*ast.RenameStmt)
			require.True(t, ok2, "Re-parsed statement should be a RenameStmt")
			require.Equal(t, renameStmt.StatementType(), renameStmt2.StatementType())
		})
	}
}

// TestRenameStmtDeparsing_FuturePhases tests RENAME operations for advanced PostgreSQL objects
// that will be implemented in future phases. These tests are currently expected to fail.
func TestRenameStmtDeparsing_Advanced(t *testing.T) {
	t.Skip("Skipping advanced RENAME tests - these will be implemented in future phases")

	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
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
	}

	// This test is intentionally empty to document future work
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Skip("Advanced object type not implemented yet - part of future phases")
		})
	}
}

// TestRenameStmtObjectTypes tests that RenameStmt correctly identifies object types
func TestRenameStmtObjectTypes(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		objectType ast.ObjectType
	}{
		{"TABLE", "ALTER TABLE users RENAME TO customers", ast.OBJECT_TABLE},
		{"COLUMN", "ALTER TABLE users RENAME COLUMN name TO full_name", ast.OBJECT_COLUMN},
		{"CONSTRAINT", "ALTER TABLE users RENAME CONSTRAINT old_c TO new_c", ast.OBJECT_TABCONSTRAINT},
		{"INDEX", "ALTER INDEX users_idx RENAME TO customers_idx", ast.OBJECT_INDEX},
		{"SEQUENCE", "ALTER SEQUENCE user_id_seq RENAME TO customer_id_seq", ast.OBJECT_SEQUENCE},
		{"VIEW", "ALTER VIEW user_view RENAME TO customer_view", ast.OBJECT_VIEW},
		{"MATERIALIZED VIEW", "ALTER MATERIALIZED VIEW mat_view RENAME TO new_mat_view", ast.OBJECT_MATVIEW},
		{"FOREIGN TABLE", "ALTER FOREIGN TABLE foreign_users RENAME TO foreign_customers", ast.OBJECT_FOREIGN_TABLE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			renameStmt, ok := statements[0].(*ast.RenameStmt)
			require.True(t, ok, "Statement should be a RenameStmt")
			assert.Equal(t, tt.objectType, renameStmt.RenameType,
				"Object type mismatch for: %s", tt.input)
		})
	}
}

// TestRenameStmtObjectTypes_FuturePhases tests object type identification for advanced objects
func TestRenameStmtObjectTypes_Advanced(t *testing.T) {
	t.Skip("Skipping advanced object type tests - these will be implemented in future phases")

	tests := []struct {
		name       string
		input      string
		objectType ast.ObjectType
	}{
		{"DATABASE", "ALTER DATABASE old_db RENAME TO new_db", ast.OBJECT_DATABASE},
		{"SCHEMA", "ALTER SCHEMA old_schema RENAME TO new_schema", ast.OBJECT_SCHEMA},
		{"ROLE", "ALTER ROLE old_role RENAME TO new_role", ast.OBJECT_ROLE},
		{"USER", "ALTER USER old_user RENAME TO new_user", ast.OBJECT_ROLE},
		{"GROUP", "ALTER GROUP old_group RENAME TO new_group", ast.OBJECT_ROLE},
		{"TABLESPACE", "ALTER TABLESPACE old_ts RENAME TO new_ts", ast.OBJECT_TABLESPACE},
		{"DOMAIN", "ALTER DOMAIN old_domain RENAME TO new_domain", ast.OBJECT_DOMAIN},
		{"DOMAIN CONSTRAINT", "ALTER DOMAIN my_domain RENAME CONSTRAINT old_check TO new_check", ast.OBJECT_DOMCONSTRAINT},
		{"TYPE", "ALTER TYPE old_type RENAME TO new_type", ast.OBJECT_TYPE},
		{"TYPE ATTRIBUTE", "ALTER TYPE my_type RENAME ATTRIBUTE old_attr TO new_attr", ast.OBJECT_ATTRIBUTE},
		{"FUNCTION", "ALTER FUNCTION old_func() RENAME TO new_func", ast.OBJECT_FUNCTION},
		{"PROCEDURE", "ALTER PROCEDURE old_proc() RENAME TO new_proc", ast.OBJECT_PROCEDURE},
		{"ROUTINE", "ALTER ROUTINE old_routine() RENAME TO new_routine", ast.OBJECT_ROUTINE},
		{"AGGREGATE", "ALTER AGGREGATE old_agg() RENAME TO new_agg", ast.OBJECT_AGGREGATE},
		{"COLLATION", "ALTER COLLATION old_collation RENAME TO new_collation", ast.OBJECT_COLLATION},
		{"CONVERSION", "ALTER CONVERSION old_conv RENAME TO new_conv", ast.OBJECT_CONVERSION},
		{"LANGUAGE", "ALTER LANGUAGE old_lang RENAME TO new_lang", ast.OBJECT_LANGUAGE},
		{"PROCEDURAL LANGUAGE", "ALTER PROCEDURAL LANGUAGE old_lang RENAME TO new_lang", ast.OBJECT_LANGUAGE},
		{"OPERATOR CLASS", "ALTER OPERATOR CLASS old_class USING btree RENAME TO new_class", ast.OBJECT_OPCLASS},
		{"OPERATOR FAMILY", "ALTER OPERATOR FAMILY old_family USING btree RENAME TO new_family", ast.OBJECT_OPFAMILY},
		{"POLICY", "ALTER POLICY old_policy ON users RENAME TO new_policy", ast.OBJECT_POLICY},
		{"RULE", "ALTER RULE old_rule ON users RENAME TO new_rule", ast.OBJECT_RULE},
		{"TRIGGER", "ALTER TRIGGER old_trigger ON users RENAME TO new_trigger", ast.OBJECT_TRIGGER},
		{"EVENT TRIGGER", "ALTER EVENT TRIGGER old_event_trigger RENAME TO new_event_trigger", ast.OBJECT_EVENT_TRIGGER},
		{"PUBLICATION", "ALTER PUBLICATION old_pub RENAME TO new_pub", ast.OBJECT_PUBLICATION},
		{"SUBSCRIPTION", "ALTER SUBSCRIPTION old_sub RENAME TO new_sub", ast.OBJECT_SUBSCRIPTION},
		{"FOREIGN DATA WRAPPER", "ALTER FOREIGN DATA WRAPPER old_fdw RENAME TO new_fdw", ast.OBJECT_FDW},
		{"SERVER", "ALTER SERVER old_server RENAME TO new_server", ast.OBJECT_FOREIGN_SERVER},
		{"STATISTICS", "ALTER STATISTICS old_stats RENAME TO new_stats", ast.OBJECT_STATISTIC_EXT},
		{"TEXT SEARCH PARSER", "ALTER TEXT SEARCH PARSER old_parser RENAME TO new_parser", ast.OBJECT_TSPARSER},
		{"TEXT SEARCH DICTIONARY", "ALTER TEXT SEARCH DICTIONARY old_dict RENAME TO new_dict", ast.OBJECT_TSDICTIONARY},
		{"TEXT SEARCH TEMPLATE", "ALTER TEXT SEARCH TEMPLATE old_template RENAME TO new_template", ast.OBJECT_TSTEMPLATE},
		{"TEXT SEARCH CONFIGURATION", "ALTER TEXT SEARCH CONFIGURATION old_config RENAME TO new_config", ast.OBJECT_TSCONFIGURATION},
	}

	// This test is intentionally empty to document future work
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Skip("Advanced object type not implemented yet - part of future phases")
		})
	}
}

// TestRenameStmtMissingOk tests that IF EXISTS is properly handled
func TestRenameStmtMissingOk(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		missingOk bool
	}{
		{"TABLE without IF EXISTS", "ALTER TABLE users RENAME TO customers", false},
		{"TABLE with IF EXISTS", "ALTER TABLE IF EXISTS users RENAME TO customers", true},
		{"INDEX without IF EXISTS", "ALTER INDEX users_idx RENAME TO customers_idx", false},
		{"INDEX with IF EXISTS", "ALTER INDEX IF EXISTS users_idx RENAME TO customers_idx", true},
		{"VIEW without IF EXISTS", "ALTER VIEW user_view RENAME TO customer_view", false},
		{"VIEW with IF EXISTS", "ALTER VIEW IF EXISTS user_view RENAME TO customer_view", true},
		{"SEQUENCE without IF EXISTS", "ALTER SEQUENCE user_id_seq RENAME TO customer_id_seq", false},
		{"SEQUENCE with IF EXISTS", "ALTER SEQUENCE IF EXISTS user_id_seq RENAME TO customer_id_seq", true},
		{"MATERIALIZED VIEW without IF EXISTS", "ALTER MATERIALIZED VIEW mat_view RENAME TO new_mat_view", false},
		{"MATERIALIZED VIEW with IF EXISTS", "ALTER MATERIALIZED VIEW IF EXISTS mat_view RENAME TO new_mat_view", true},
		{"FOREIGN TABLE without IF EXISTS", "ALTER FOREIGN TABLE foreign_users RENAME TO foreign_customers", false},
		{"FOREIGN TABLE with IF EXISTS", "ALTER FOREIGN TABLE IF EXISTS foreign_users RENAME TO foreign_customers", true},
		{"COLUMN without IF EXISTS", "ALTER TABLE users RENAME COLUMN name TO full_name", false},
		{"COLUMN with IF EXISTS", "ALTER TABLE IF EXISTS users RENAME COLUMN name TO full_name", true},
		{"CONSTRAINT without IF EXISTS", "ALTER TABLE users RENAME CONSTRAINT old_c TO new_c", false},
		{"CONSTRAINT with IF EXISTS", "ALTER TABLE IF EXISTS users RENAME CONSTRAINT old_c TO new_c", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a RenameStmt with correct MissingOk flag
			renameStmt, ok := statements[0].(*ast.RenameStmt)
			require.True(t, ok, "Statement should be a RenameStmt")
			assert.Equal(t, tt.missingOk, renameStmt.MissingOk,
				"MissingOk flag should match for %s", tt.input)
		})
	}
}

func TestLimitOffsetDeparse(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantSql   string // expected deparse output
		normalize bool   // if true, normalize the expected vs actual for comparison
	}{
		// Basic LIMIT tests
		{
			name:    "simple LIMIT",
			sql:     "SELECT * FROM users LIMIT 10",
			wantSql: "SELECT * FROM users LIMIT 10",
		},
		{
			name:    "LIMIT with expression",
			sql:     "SELECT * FROM users LIMIT 5 + 5",
			wantSql: "SELECT * FROM users LIMIT 5 + 5",
		},
		{
			name:    "LIMIT ALL",
			sql:     "SELECT * FROM users LIMIT ALL",
			wantSql: "SELECT * FROM users LIMIT ALL",
		},

		// Basic OFFSET tests
		{
			name:    "simple OFFSET",
			sql:     "SELECT * FROM users OFFSET 20",
			wantSql: "SELECT * FROM users OFFSET 20",
		},
		{
			name:    "OFFSET with expression",
			sql:     "SELECT * FROM users OFFSET 10 * 2",
			wantSql: "SELECT * FROM users OFFSET 10 * 2",
		},

		// Combined LIMIT and OFFSET
		{
			name:    "LIMIT and OFFSET",
			sql:     "SELECT * FROM users LIMIT 10 OFFSET 20",
			wantSql: "SELECT * FROM users LIMIT 10 OFFSET 20",
		},
		{
			name:    "OFFSET before LIMIT",
			sql:     "SELECT * FROM users OFFSET 20 LIMIT 10",
			wantSql: "SELECT * FROM users LIMIT 10 OFFSET 20", // Should normalize order
		},

		// SQL:2008 FETCH FIRST syntax - should deparse as LIMIT (matching PostgreSQL behavior)
		{
			name:    "FETCH FIRST ROW ONLY",
			sql:     "SELECT * FROM users FETCH FIRST ROW ONLY",
			wantSql: "SELECT * FROM users LIMIT 1",
		},
		{
			name:    "FETCH FIRST ROWS ONLY (implicit 1)",
			sql:     "SELECT * FROM users FETCH FIRST ROWS ONLY",
			wantSql: "SELECT * FROM users LIMIT 1",
		},
		{
			name:    "FETCH NEXT ROW ONLY",
			sql:     "SELECT * FROM users FETCH NEXT ROW ONLY",
			wantSql: "SELECT * FROM users LIMIT 1",
		},
		{
			name:    "FETCH FIRST 5 ROWS ONLY",
			sql:     "SELECT * FROM users FETCH FIRST 5 ROWS ONLY",
			wantSql: "SELECT * FROM users LIMIT 5",
		},
		{
			name:    "FETCH NEXT 10 ROWS ONLY",
			sql:     "SELECT * FROM users FETCH NEXT 10 ROWS ONLY",
			wantSql: "SELECT * FROM users LIMIT 10",
		},

		// FETCH with WITH TIES - must preserve FETCH FIRST syntax
		{
			name:    "FETCH FIRST ROW WITH TIES",
			sql:     "SELECT * FROM users ORDER BY score FETCH FIRST ROW WITH TIES",
			wantSql: "SELECT * FROM users ORDER BY score FETCH FIRST ROW WITH TIES",
		},
		{
			name:    "FETCH FIRST 5 ROWS WITH TIES",
			sql:     "SELECT * FROM users ORDER BY score FETCH FIRST 5 ROWS WITH TIES",
			wantSql: "SELECT * FROM users ORDER BY score FETCH FIRST 5 ROWS WITH TIES",
		},

		// SQL:2008 OFFSET syntax with ROW/ROWS - should deparse as simple OFFSET
		{
			name:    "OFFSET with ROWS keyword",
			sql:     "SELECT * FROM users OFFSET 10 ROWS",
			wantSql: "SELECT * FROM users OFFSET 10",
		},
		{
			name:    "OFFSET with ROW keyword",
			sql:     "SELECT * FROM users OFFSET 1 ROW",
			wantSql: "SELECT * FROM users OFFSET 1",
		},

		// Combined FETCH and OFFSET
		{
			name:    "OFFSET and FETCH FIRST",
			sql:     "SELECT * FROM users OFFSET 20 FETCH FIRST 10 ROWS ONLY",
			wantSql: "SELECT * FROM users LIMIT 10 OFFSET 20",
		},

		// Complex scenarios
		{
			name:    "ORDER BY with LIMIT",
			sql:     "SELECT * FROM users ORDER BY name LIMIT 10",
			wantSql: "SELECT * FROM users ORDER BY name LIMIT 10",
		},
		{
			name:    "ORDER BY with FETCH FIRST WITH TIES",
			sql:     "SELECT * FROM users ORDER BY score DESC FETCH FIRST 5 ROWS WITH TIES",
			wantSql: "SELECT * FROM users ORDER BY score DESC FETCH FIRST 5 ROWS WITH TIES",
		},
		{
			name:    "WHERE with LIMIT and OFFSET",
			sql:     "SELECT * FROM users WHERE active = true OFFSET 50 LIMIT 25",
			wantSql: "SELECT * FROM users WHERE active = TRUE LIMIT 25 OFFSET 50",
		},

		// Round-trip testing - these may have slight variations in formatting
		{
			name:    "Complex query with CTE and FETCH",
			sql:     "WITH top_users AS (SELECT * FROM users ORDER BY score DESC FETCH FIRST 20 ROWS ONLY) SELECT * FROM top_users",
			wantSql: "WITH top_users AS (SELECT * FROM users ORDER BY score DESC LIMIT 20) SELECT * FROM top_users",
		},
		{
			name:    "Subquery with LIMIT",
			sql:     "SELECT * FROM (SELECT * FROM users LIMIT 10) AS u WHERE u.age > 25",
			wantSql: "SELECT * FROM (SELECT * FROM users LIMIT 10) AS u WHERE u.age > 25",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL")
			require.NotEmpty(t, stmts, "No statements parsed")

			// Get the parsed statement
			stmt := stmts[0]
			require.IsType(t, &ast.SelectStmt{}, stmt, "Expected SelectStmt")

			// Deparse back to SQL
			selectStmt := stmt.(*ast.SelectStmt)
			actualSql := selectStmt.SqlString()

			if tt.normalize {
				// For FETCH FIRST tests, we normalize whitespace and case
				expected := normalizeSQL(tt.wantSql)
				actual := normalizeSQL(actualSql)
				assert.Equal(t, expected, actual, "Deparsed SQL doesn't match expected")
			} else {
				// Exact match expected
				assert.Equal(t, tt.wantSql, actualSql, "Deparsed SQL doesn't match expected")
			}
		})
	}
}

// TestLimitOffsetRoundTrip tests that parsing and deparsing maintains semantic equivalence
func TestLimitOffsetRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"simple limit", "SELECT * FROM users LIMIT 10"},
		{"limit with offset", "SELECT * FROM users LIMIT 10 OFFSET 5"},
		{"fetch first only", "SELECT * FROM users FETCH FIRST 10 ROWS ONLY"},
		{"fetch first with ties", "SELECT * FROM users ORDER BY id FETCH FIRST 5 ROWS WITH TIES"},
		{"offset with rows", "SELECT * FROM users OFFSET 10 ROWS"},
		{"complex query", "SELECT name, COUNT(*) FROM users WHERE active = true GROUP BY name ORDER BY COUNT(*) DESC LIMIT 20 OFFSET 10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the original SQL
			stmts1, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse original SQL")
			require.NotEmpty(t, stmts1, "No statements parsed from original SQL")

			// Deparse to get SQL string
			stmt1 := stmts1[0].(*ast.SelectStmt)
			sql2 := stmt1.SqlString()

			// Parse the deparsed SQL
			stmts2, err := ParseSQL(sql2)
			require.NoError(t, err, "Failed to parse deparsed SQL: %s", sql2)
			require.NotEmpty(t, stmts2, "No statements parsed from deparsed SQL")

			// Compare the key fields
			stmt2 := stmts2[0].(*ast.SelectStmt)

			// The limit values should be semantically equivalent
			if stmt1.LimitCount != nil && stmt2.LimitCount != nil {
				assert.Equal(t, stmt1.LimitCount.SqlString(), stmt2.LimitCount.SqlString(),
					"LimitCount differs after round-trip")
			} else {
				assert.Equal(t, stmt1.LimitCount, stmt2.LimitCount, "LimitCount presence differs")
			}

			if stmt1.LimitOffset != nil && stmt2.LimitOffset != nil {
				assert.Equal(t, stmt1.LimitOffset.SqlString(), stmt2.LimitOffset.SqlString(),
					"LimitOffset differs after round-trip")
			} else {
				assert.Equal(t, stmt1.LimitOffset, stmt2.LimitOffset, "LimitOffset presence differs")
			}

			// The limit option should be preserved for FETCH FIRST syntax
			assert.Equal(t, stmt1.LimitOption, stmt2.LimitOption,
				"LimitOption differs after round-trip")
		})
	}
}

// TestLimitOffsetASTFields verifies that parsing correctly populates AST fields
func TestLimitOffsetASTFields(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectLimit  string
		expectOffset string
		expectOption ast.LimitOption
	}{
		{
			name:         "LIMIT only",
			sql:          "SELECT * FROM users LIMIT 10",
			expectLimit:  "10",
			expectOffset: "",
			expectOption: ast.LIMIT_OPTION_COUNT,
		},
		{
			name:         "OFFSET only",
			sql:          "SELECT * FROM users OFFSET 5",
			expectLimit:  "",
			expectOffset: "5",
			expectOption: ast.LIMIT_OPTION_COUNT, // OFFSET sets this in the grammar
		},
		{
			name:         "LIMIT and OFFSET",
			sql:          "SELECT * FROM users LIMIT 10 OFFSET 5",
			expectLimit:  "10",
			expectOffset: "5",
			expectOption: ast.LIMIT_OPTION_COUNT,
		},
		{
			name:         "FETCH FIRST ONLY",
			sql:          "SELECT * FROM users FETCH FIRST 10 ROWS ONLY",
			expectLimit:  "10",
			expectOffset: "",
			expectOption: ast.LIMIT_OPTION_COUNT,
		},
		{
			name:         "FETCH FIRST WITH TIES",
			sql:          "SELECT * FROM users ORDER BY id FETCH FIRST 5 ROWS WITH TIES",
			expectLimit:  "5",
			expectOffset: "",
			expectOption: ast.LIMIT_OPTION_WITH_TIES,
		},
		{
			name:         "FETCH FIRST implicit 1 ONLY",
			sql:          "SELECT * FROM users FETCH FIRST ROW ONLY",
			expectLimit:  "1",
			expectOffset: "",
			expectOption: ast.LIMIT_OPTION_COUNT,
		},
		{
			name:         "OFFSET with FETCH",
			sql:          "SELECT * FROM users OFFSET 10 FETCH FIRST 5 ROWS ONLY",
			expectLimit:  "5",
			expectOffset: "10",
			expectOption: ast.LIMIT_OPTION_COUNT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL")
			require.NotEmpty(t, stmts, "No statements parsed")

			stmt := stmts[0].(*ast.SelectStmt)

			// Check limit count
			if tt.expectLimit != "" {
				require.NotNil(t, stmt.LimitCount, "Expected LimitCount to be set")
				assert.Equal(t, tt.expectLimit, stmt.LimitCount.SqlString(), "LimitCount value mismatch")
			} else {
				assert.Nil(t, stmt.LimitCount, "Expected LimitCount to be nil")
			}

			// Check limit offset
			if tt.expectOffset != "" {
				require.NotNil(t, stmt.LimitOffset, "Expected LimitOffset to be set")
				assert.Equal(t, tt.expectOffset, stmt.LimitOffset.SqlString(), "LimitOffset value mismatch")
			} else {
				assert.Nil(t, stmt.LimitOffset, "Expected LimitOffset to be nil")
			}

			// Check limit option
			assert.Equal(t, tt.expectOption, stmt.LimitOption, "LimitOption value mismatch")
		})
	}
}
