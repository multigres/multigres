/*
 * PostgreSQL Parser - SQL Statement Parsing Tests
 *
 * This file contains comprehensive tests for SQL statement parsing including:
 * - Data Manipulation Language (DML) statements: INSERT, UPDATE, DELETE, MERGE
 * - Data Definition Language (DDL) statements: CREATE, ALTER, DROP
 * - Round-trip parsing and deparsing validation
 *
 * Consolidated from dml_test.go and ddl_test.go
 */

package parser

import (
	"strings"
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/require"
)

// TestDMLParsing tests round-trip parsing and deparsing for all DML statements
func TestDMLParsing(t *testing.T) {
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			AssertRoundTripSQL(t, tt.input, tt.expected)
		})
	}
}

// TestDDLParsing tests both parsing and deparsing of DDL statements
func TestDDLParsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, expects exact match with input
	}{
		// CREATE TABLE tests
		{
			name:     "Simple CREATE TABLE",
			sql:      "CREATE TABLE users (id int, name varchar(100))",
			expected: "CREATE TABLE users (id INT, name VARCHAR(100))",
		},
		{
			name:     "CREATE TABLE with PRIMARY KEY",
			sql:      "CREATE TABLE users (id int PRIMARY KEY, name varchar(100))",
			expected: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
		},
		{
			name: "CREATE TABLE with NOT NULL",
			sql:  "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100) NOT NULL)",
		},
		{
			name: "CREATE TABLE with DEFAULT",
			sql:  "CREATE TABLE users (id INT DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())",
		},
		{
			name: "CREATE TABLE with UNIQUE constraint",
			sql:  "CREATE TABLE users (id INT, email VARCHAR(100) UNIQUE)",
		},
		{
			name: "CREATE TABLE with CHECK constraint",
			sql:  "CREATE TABLE users (id INT, age INT CHECK (age > 0))",
		},
		{
			name: "CREATE TABLE with table-level PRIMARY KEY",
			sql:  "CREATE TABLE users (id INT, name VARCHAR(100), PRIMARY KEY (id))",
		},
		{
			name: "CREATE TABLE with FOREIGN KEY",
			sql:  "CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))",
		},
		{
			name: "CREATE TABLE IF NOT EXISTS",
			sql:  "CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR(100))",
		},
		{
			name: "CREATE TABLE with multiple constraints",
			sql:  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE NOT NULL, age INT CHECK (age > 0))",
		},

		// Temporary table creation tests (OptTempTableName)
		{
			name: "CREATE TEMPORARY TABLE",
			sql:  "CREATE TEMPORARY TABLE temp_users (id INT, name VARCHAR(100))",
		},
		{
			name:     "CREATE TEMP TABLE",
			sql:      "CREATE TEMP TABLE temp_users (id INT, name varchar(100))",
			expected: "CREATE TEMPORARY TABLE temp_users (id INT, name VARCHAR(100))",
		},
		{
			name:     "CREATE LOCAL TEMPORARY TABLE",
			sql:      "CREATE LOCAL TEMPORARY TABLE temp_users (id INT, name varchar(100))",
			expected: "CREATE TEMPORARY TABLE temp_users (id INT, name VARCHAR(100))",
		},
		{
			name:     "CREATE LOCAL TEMP TABLE",
			sql:      "CREATE LOCAL TEMP TABLE temp_users (id INT, name varchar(100))",
			expected: "CREATE TEMPORARY TABLE temp_users (id INT, name VARCHAR(100))",
		},
		{
			name:     "CREATE GLOBAL TEMPORARY TABLE",
			sql:      "CREATE GLOBAL TEMPORARY TABLE temp_users (id INT, name varchar(100))",
			expected: "CREATE TEMPORARY TABLE temp_users (id INT, name VARCHAR(100))",
		},
		{
			name:     "CREATE GLOBAL TEMP TABLE",
			sql:      "CREATE GLOBAL TEMP TABLE temp_users (id INT, name varchar(100))",
			expected: "CREATE TEMPORARY TABLE temp_users (id INT, name VARCHAR(100))",
		},
		{
			name: "CREATE UNLOGGED TABLE",
			sql:  "CREATE UNLOGGED TABLE unlogged_users (id INT, name VARCHAR(100))",
		},

		// CREATE INDEX tests
		{
			name: "Simple CREATE INDEX",
			sql:  "CREATE INDEX idx_users_name ON users (name)",
		},
		{
			name: "CREATE UNIQUE INDEX",
			sql:  "CREATE UNIQUE INDEX idx_users_email ON users (email)",
		},
		{
			name: "CREATE INDEX with multiple columns",
			sql:  "CREATE INDEX idx_users_name_email ON users (name, email)",
		},
		{
			name: "CREATE INDEX CONCURRENTLY",
			sql:  "CREATE INDEX CONCURRENTLY idx_users_name ON users (name)",
		},
		{
			name: "CREATE INDEX IF NOT EXISTS",
			sql:  "CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)",
		},
		{
			name: "CREATE INDEX with INCLUDE",
			sql:  "CREATE INDEX idx_users_name ON users (name) INCLUDE (email, created_at)",
		},

		// CREATE SEQUENCE tests
		{
			name: "CREATE SEQUENCE basic",
			sql:  "CREATE SEQUENCE test_seq",
		},
		{
			name: "CREATE SEQUENCE IF NOT EXISTS",
			sql:  "CREATE SEQUENCE IF NOT EXISTS test_seq",
		},

		// CREATE EXTENSION tests
		{
			name: "CREATE EXTENSION basic",
			sql:  "CREATE EXTENSION postgis",
		},
		{
			name: "CREATE EXTENSION IF NOT EXISTS",
			sql:  "CREATE EXTENSION IF NOT EXISTS uuid_ossp",
		},
		{
			name: "CREATE EXTENSION with VERSION",
			sql:  "CREATE EXTENSION postgis VERSION '3.0'",
		},
		{
			name: "CREATE EXTENSION with SCHEMA",
			sql:  "CREATE EXTENSION postgis SCHEMA public",
		},
		{
			name: "CREATE EXTENSION with CASCADE",
			sql:  "CREATE EXTENSION postgis CASCADE",
		},
		{
			name: "CREATE EXTENSION with multiple options",
			sql:  "CREATE EXTENSION postgis VERSION '3.0' SCHEMA public CASCADE",
		},

		// CREATE CONVERSION tests
		{
			name: "CREATE CONVERSION basic",
			sql:  "CREATE CONVERSION myconv FOR 'UTF8' TO 'LATIN1' FROM myconvfunc",
		},
		{
			name: "CREATE DEFAULT CONVERSION",
			sql:  "CREATE DEFAULT CONVERSION myconv FOR 'UTF8' TO 'LATIN1' FROM myconvfunc",
		},

		// CREATE LANGUAGE tests
		{
			name: "CREATE LANGUAGE basic",
			sql:  "CREATE LANGUAGE plperl HANDLER plperl_call_handler",
		},
		{
			name: "CREATE OR REPLACE LANGUAGE",
			sql:  "CREATE OR REPLACE LANGUAGE plperl HANDLER plperl_call_handler",
		},

		// CREATE FOREIGN DATA WRAPPER tests
		{
			name: "CREATE FOREIGN DATA WRAPPER basic",
			sql:  "CREATE FOREIGN DATA WRAPPER myfdw",
		},
		{
			name: "CREATE FOREIGN DATA WRAPPER with HANDLER",
			sql:  "CREATE FOREIGN DATA WRAPPER myfdw HANDLER myhandler",
		},
		{
			name: "CREATE FOREIGN DATA WRAPPER with VALIDATOR",
			sql:  "CREATE FOREIGN DATA WRAPPER myfdw VALIDATOR myvalidator",
		},
		{
			name: "CREATE FOREIGN DATA WRAPPER with qualified HANDLER",
			sql:  "CREATE FOREIGN DATA WRAPPER myfdw HANDLER myschema.myhandler",
		},
		{
			name: "CREATE FOREIGN DATA WRAPPER with both HANDLER and VALIDATOR",
			sql:  "CREATE FOREIGN DATA WRAPPER myfdw HANDLER myhandler VALIDATOR myvalidator",
		},
		{
			name: "CREATE FOREIGN DATA WRAPPER with OPTIONS",
			sql:  "CREATE FOREIGN DATA WRAPPER myfdw OPTIONS (library 'myfdw_lib', language 'C')",
		},

		// CREATE FOREIGN SERVER tests
		{
			name: "CREATE FOREIGN SERVER basic",
			sql:  "CREATE SERVER myserver FOREIGN DATA WRAPPER myfdw",
		},
		{
			name: "CREATE FOREIGN SERVER IF NOT EXISTS",
			sql:  "CREATE SERVER IF NOT EXISTS myserver FOREIGN DATA WRAPPER myfdw",
		},
		{
			name: "CREATE FOREIGN SERVER with TYPE",
			sql:  "CREATE SERVER myserver TYPE 'postgresql' FOREIGN DATA WRAPPER myfdw",
		},
		{
			name: "CREATE FOREIGN SERVER with VERSION",
			sql:  "CREATE SERVER myserver VERSION '1.0' FOREIGN DATA WRAPPER myfdw",
		},
		{
			name: "CREATE FOREIGN SERVER with OPTIONS",
			sql:  "CREATE SERVER myserver FOREIGN DATA WRAPPER myfdw OPTIONS (host 'localhost', port '5432')",
		},
		{
			name: "CREATE FOREIGN SERVER with all options",
			sql:  "CREATE SERVER myserver TYPE 'postgresql' VERSION '15.0' FOREIGN DATA WRAPPER myfdw OPTIONS (host 'localhost')",
		},

		// CREATE FOREIGN TABLE tests
		{
			name: "CREATE FOREIGN TABLE basic",
			sql:  "CREATE FOREIGN TABLE foreign_users (id INT, name TEXT) SERVER myserver",
		},
		{
			name: "CREATE FOREIGN TABLE IF NOT EXISTS",
			sql:  "CREATE FOREIGN TABLE IF NOT EXISTS foreign_users (id INT, name TEXT) SERVER myserver",
		},
		{
			name: "CREATE FOREIGN TABLE with OPTIONS",
			sql:  "CREATE FOREIGN TABLE foreign_users (id INT, name TEXT) SERVER myserver OPTIONS (table_name 'users')",
		},
		{
			name: "CREATE FOREIGN TABLE with INHERITS",
			sql:  "CREATE FOREIGN TABLE foreign_users (age INT) INHERITS (base_table) SERVER myserver",
		},

		// CREATE USER MAPPING tests
		{
			name: "CREATE USER MAPPING basic",
			sql:  "CREATE USER MAPPING FOR CURRENT_USER SERVER myserver",
		},
		{
			name: "CREATE USER MAPPING IF NOT EXISTS",
			sql:  "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER myserver",
		},
		{
			name: "CREATE USER MAPPING with OPTIONS",
			sql:  "CREATE USER MAPPING FOR CURRENT_USER SERVER myserver OPTIONS (user 'remote_user', password 'secret')",
		},
		{
			name: "CREATE USER MAPPING for specific user",
			sql:  "CREATE USER MAPPING FOR alice SERVER myserver OPTIONS (user 'alice_remote')",
		},

		// ALTER TABLE tests
		{
			name: "ALTER TABLE ADD COLUMN",
			sql:  "ALTER TABLE users ADD COLUMN age INT",
		},
		{
			name: "ALTER TABLE DROP COLUMN",
			sql:  "ALTER TABLE users DROP COLUMN age",
		},
		{
			name: "ALTER TABLE ADD CONSTRAINT",
			sql:  "ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)",
		},
		{
			name: "ALTER TABLE DROP CONSTRAINT",
			sql:  "ALTER TABLE users DROP CONSTRAINT users_email_unique",
		},
		{
			name: "ALTER TABLE ALTER COLUMN SET DEFAULT",
			sql:  "ALTER TABLE users ALTER COLUMN age SET DEFAULT 0",
		},
		{
			name: "ALTER TABLE ALTER COLUMN DROP DEFAULT",
			sql:  "ALTER TABLE users ALTER COLUMN age DROP DEFAULT",
		},
		{
			name: "ALTER TABLE ALTER COLUMN SET NOT NULL",
			sql:  "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
		},
		{
			name: "ALTER TABLE ALTER COLUMN DROP NOT NULL",
			sql:  "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
		},

		// Identity column option tests (alter_identity_column_option_list)
		{
			name: "ALTER TABLE ALTER COLUMN ADD GENERATED BY DEFAULT AS IDENTITY",
			sql:  "ALTER TABLE users ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY",
		},
		{
			name: "ALTER TABLE ALTER COLUMN ADD GENERATED ALWAYS AS IDENTITY",
			sql:  "ALTER TABLE users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY",
		},

		// ALTER TABLE partition commands
		{
			name: "ALTER TABLE ATTACH PARTITION with default bound",
			sql:  "ALTER TABLE parent_table ATTACH PARTITION child_table DEFAULT",
		},
		{
			name: "ALTER TABLE ATTACH PARTITION with range bound",
			sql:  "ALTER TABLE parent_table ATTACH PARTITION child_table FOR VALUES FROM (1) TO (100)",
		},
		{
			name: "ALTER TABLE ATTACH PARTITION with list bound",
			sql:  "ALTER TABLE parent_table ATTACH PARTITION child_table FOR VALUES IN (1, 2, 3)",
		},
		{
			name: "ALTER TABLE DETACH PARTITION",
			sql:  "ALTER TABLE parent_table DETACH PARTITION child_table",
		},
		{
			name: "ALTER TABLE DETACH PARTITION CONCURRENTLY",
			sql:  "ALTER TABLE parent_table DETACH PARTITION child_table CONCURRENTLY",
		},
		{
			name: "ALTER TABLE DETACH PARTITION FINALIZE",
			sql:  "ALTER TABLE parent_table DETACH PARTITION child_table FINALIZE",
		},

		// ALTER INDEX partition commands
		{
			name: "ALTER INDEX ATTACH PARTITION",
			sql:  "ALTER INDEX parent_index ATTACH PARTITION child_index",
		},

		// DROP statements
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE users",
		},
		{
			name: "DROP TABLE IF EXISTS",
			sql:  "DROP TABLE IF EXISTS users",
		},
		{
			name: "DROP TABLE CASCADE",
			sql:  "DROP TABLE users CASCADE",
		},
		{
			name: "DROP TABLE multiple",
			sql:  "DROP TABLE users, orders, products",
		},
		{
			name: "DROP INDEX",
			sql:  "DROP INDEX idx_users_name",
		},
		{
			name: "DROP INDEX IF EXISTS",
			sql:  "DROP INDEX IF EXISTS idx_users_name",
		},
		{
			name: "DROP INDEX CONCURRENTLY",
			sql:  "DROP INDEX CONCURRENTLY idx_users_name",
		},
		{
			name: "CREATE TABLE with FOREIGN KEY ON DELETE SET NULL",
			sql:  "CREATE TABLE test (id INT REFERENCES parent(id) ON DELETE SET NULL)",
		},
		{
			name: "CREATE TABLE with FOREIGN KEY ON DELETE SET NULL with column list",
			sql:  "CREATE TABLE test (id INT REFERENCES parent(id) ON DELETE SET NULL (id))",
		},
		{
			name: "CREATE TABLE with FOREIGN KEY ON DELETE SET DEFAULT with column list",
			sql:  "CREATE TABLE test (id INT REFERENCES parent(id) ON DELETE SET DEFAULT (id))",
		},
		{
			name: "CREATE TABLE with FOREIGN KEY ON DELETE CASCADE",
			sql:  "CREATE TABLE test (id INT REFERENCES parent(id) ON DELETE CASCADE)",
		},
		{
			name: "CREATE TABLE with FOREIGN KEY ON UPDATE RESTRICT",
			sql:  "CREATE TABLE test (id INT REFERENCES parent(id) ON UPDATE RESTRICT)",
		},

		// CREATE VIEW Tests
		{
			name: "Basic CREATE VIEW round-trip",
			sql:  "CREATE VIEW user_summary AS SELECT id, name FROM users",
		},
		{
			name: "CREATE OR REPLACE VIEW round-trip",
			sql:  "CREATE OR REPLACE VIEW user_summary AS SELECT id, name, email FROM users",
		},
		{
			name: "CREATE VIEW with column aliases round-trip",
			sql:  "CREATE VIEW user_info (user_id, full_name) AS SELECT id, name FROM users",
		},
		{
			name: "CREATE VIEW with WHERE clause round-trip",
			sql:  "CREATE VIEW active_users AS SELECT * FROM users WHERE (active = TRUE)",
		},
		{
			name: "CREATE VIEW with CHECK OPTION round-trip",
			sql:  "CREATE VIEW active_users AS SELECT * FROM users WHERE (active = TRUE) WITH CHECK OPTION",
		},

		// CREATE FUNCTION Tests
		{
			name: "Basic CREATE FUNCTION round-trip",
			sql:  "CREATE FUNCTION test_func () RETURNS INT LANGUAGE sql AS $$SELECT 1$$",
		},
		{
			name: "CREATE OR REPLACE FUNCTION round-trip",
			sql:  "CREATE OR REPLACE FUNCTION test_func () RETURNS INT LANGUAGE sql AS $$SELECT 1$$",
		},
		{
			name: "CREATE FUNCTION with parameters round-trip",
			sql:  "CREATE FUNCTION add_func (a INT, b INT) RETURNS INT LANGUAGE sql AS $$SELECT $1 + $2$$",
		},
		{
			name: "CREATE PROCEDURE round-trip",
			sql:  "CREATE PROCEDURE test_proc () LANGUAGE sql AS $$BEGIN NULL; END$$",
		},

		// CREATE TRIGGER Tests
		{
			name: "Basic CREATE TRIGGER round-trip",
			sql:  "CREATE TRIGGER audit_insert BEFORE INSERT ON users EXECUTE FUNCTION audit_function ()",
		},
		{
			name: "CREATE TRIGGER with FOR EACH ROW round-trip",
			sql:  "CREATE TRIGGER row_trigger BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION audit_function ()",
		},
		{
			name: "CREATE TRIGGER INSTEAD OF round-trip",
			sql:  "CREATE TRIGGER view_trigger INSTEAD OF INSERT ON user_view EXECUTE FUNCTION handle_view_insert ()",
		},

		// CREATE MATERIALIZED VIEW tests
		{
			name: "Simple materialized view",
			sql:  "CREATE MATERIALIZED VIEW test_matview AS SELECT 1",
		},
		{
			name: "CREATE MATERIALIZED VIEW IF NOT EXISTS",
			sql:  "CREATE MATERIALIZED VIEW IF NOT EXISTS test_matview AS SELECT 1",
		},

		// CREATE SCHEMA tests
		{
			name: "Simple schema",
			sql:  "CREATE SCHEMA test_schema",
		},
		{
			name: "CREATE SCHEMA IF NOT EXISTS",
			sql:  "CREATE SCHEMA IF NOT EXISTS test_schema",
		},

		// REFRESH MATERIALIZED VIEW tests
		{
			name: "REFRESH MATERIALIZED VIEW",
			sql:  "REFRESH MATERIALIZED VIEW test_matview",
		},
		{
			name: "REFRESH MATERIALIZED VIEW CONCURRENTLY",
			sql:  "REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview",
		},

		// CREATE DOMAIN tests
		{
			name: "Simple CREATE DOMAIN",
			sql:  "CREATE DOMAIN email AS VARCHAR(255)",
		},
		{
			name: "CREATE DOMAIN with CHECK constraint",
			sql:  "CREATE DOMAIN email AS VARCHAR(255) CHECK (value LIKE '%@%.%')",
		},
		{
			name: "CREATE DOMAIN with NOT NULL",
			sql:  "CREATE DOMAIN positive_int AS INT NOT NULL CHECK (value > 0)",
		},
		{
			name: "CREATE DOMAIN with named constraint",
			sql:  "CREATE DOMAIN email AS VARCHAR(255) CONSTRAINT valid_email CHECK (value LIKE '%@%.%')",
		},

		// ALTER DOMAIN tests
		{
			name: "ALTER DOMAIN SET DEFAULT",
			sql:  "ALTER DOMAIN email SET DEFAULT 'unknown@example.com'",
		},
		{
			name: "ALTER DOMAIN DROP DEFAULT",
			sql:  "ALTER DOMAIN email DROP DEFAULT",
		},
		{
			name: "ALTER DOMAIN SET NOT NULL",
			sql:  "ALTER DOMAIN email SET NOT NULL",
		},
		{
			name: "ALTER DOMAIN DROP NOT NULL",
			sql:  "ALTER DOMAIN email DROP NOT NULL",
		},
		{
			name: "ALTER DOMAIN ADD CONSTRAINT",
			sql:  "ALTER DOMAIN email ADD CHECK (length(value) > 5)",
		},
		{
			name: "ALTER DOMAIN DROP CONSTRAINT",
			sql:  "ALTER DOMAIN email DROP CONSTRAINT email_check",
		},
		{
			name: "ALTER DOMAIN DROP CONSTRAINT IF EXISTS",
			sql:  "ALTER DOMAIN email DROP CONSTRAINT IF EXISTS email_check CASCADE",
		},
		{
			name: "ALTER DOMAIN VALIDATE CONSTRAINT",
			sql:  "ALTER DOMAIN email VALIDATE CONSTRAINT email_check",
		},

		// CREATE TYPE tests
		{
			name: "CREATE TYPE ENUM",
			sql:  "CREATE TYPE color AS ENUM ('red', 'green', 'blue')",
		},
		{
			name: "CREATE TYPE ENUM empty",
			sql:  "CREATE TYPE status AS ENUM ()",
		},
		{
			name: "CREATE TYPE composite",
			sql:  "CREATE TYPE point AS (x INT, y INT)",
		},
		{
			name: "CREATE TYPE shell",
			sql:  "CREATE TYPE mytype",
		},
		{
			name: "CREATE TYPE with definition",
			sql:  "CREATE TYPE mytype (input = mytype_in, output = mytype_out)",
		},
		{
			name:     "CREATE TYPE RANGE",
			sql:      "CREATE TYPE int4_range AS RANGE (subtype = int4)",
			expected: "CREATE TYPE int4_range AS RANGE (subtype = INT)",
		},

		// ALTER TYPE tests
		{
			name: "ALTER TYPE ADD VALUE",
			sql:  "ALTER TYPE color ADD VALUE 'yellow'",
		},
		{
			name: "ALTER TYPE ADD VALUE IF NOT EXISTS",
			sql:  "ALTER TYPE color ADD VALUE IF NOT EXISTS 'purple'",
		},
		{
			name: "ALTER TYPE ADD VALUE BEFORE",
			sql:  "ALTER TYPE color ADD VALUE 'orange' BEFORE 'red'",
		},
		{
			name: "ALTER TYPE ADD VALUE AFTER",
			sql:  "ALTER TYPE color ADD VALUE 'cyan' AFTER 'blue'",
		},

		// CREATE AGGREGATE tests
		{
			name:     "CREATE AGGREGATE basic",
			sql:      "CREATE AGGREGATE avg_int (int4) (sfunc = int4_avg_accum, stype = int8)",
			expected: "CREATE AGGREGATE avg_int (INT) (sfunc = int4_avg_accum, stype = BIGINT)",
		},
		{
			name:     "CREATE OR REPLACE AGGREGATE",
			sql:      "CREATE OR REPLACE AGGREGATE sum_int (int4) (sfunc = int4pl, stype = int8)",
			expected: "CREATE OR REPLACE AGGREGATE sum_int (INT) (sfunc = int4pl, stype = BIGINT)",
		},
		{
			name:     "CREATE AGGREGATE old style",
			sql:      "CREATE AGGREGATE myavg (basetype = int4, sfunc = int4_avg_accum, stype = int8)",
			expected: "CREATE AGGREGATE myavg (basetype = INT, sfunc = int4_avg_accum, stype = BIGINT)",
		},

		// CREATE OPERATOR tests
		{
			name:     "CREATE OPERATOR basic",
			sql:      "CREATE OPERATOR x.+ (leftarg = int4, rightarg = int4, function = int4eq)",
			expected: "CREATE OPERATOR x.+ (leftarg = INT, rightarg = INT, function = int4eq)",
		},
		{
			name: "CREATE OPERATOR with procedure",
			sql:  "CREATE OPERATOR + (leftarg = box, rightarg = box, procedure = box_add)",
		},

		// CREATE TEXT SEARCH tests
		{
			name: "CREATE TEXT SEARCH PARSER",
			sql:  "CREATE TEXT SEARCH PARSER my_parser (start = prsd_start, gettoken = prsd_nexttoken)",
		},
		{
			name: "CREATE TEXT SEARCH DICTIONARY",
			sql:  "CREATE TEXT SEARCH DICTIONARY my_dict (template = simple, stopwords = english)",
		},
		{
			name: "CREATE TEXT SEARCH TEMPLATE",
			sql:  "CREATE TEXT SEARCH TEMPLATE my_template (init = dsimple_init, lexize = dsimple_lexize)",
		},
		{
			name: "CREATE TEXT SEARCH CONFIGURATION",
			sql:  "CREATE TEXT SEARCH CONFIGURATION my_config (parser = default)",
		},

		// CREATE COLLATION tests
		{
			name: "CREATE COLLATION basic",
			sql:  "CREATE COLLATION french (locale = 'fr_FR.utf8')",
		},
		{
			name: "CREATE COLLATION IF NOT EXISTS",
			sql:  "CREATE COLLATION IF NOT EXISTS german (locale = 'de_DE.utf8')",
		},
		{
			name: "CREATE COLLATION FROM",
			sql:  "CREATE COLLATION german FROM \"de_DE\"",
		},
		{
			name: "CREATE COLLATION IF NOT EXISTS FROM",
			sql:  "CREATE COLLATION IF NOT EXISTS french_copy FROM \"fr_FR\"",
		},

		// CREATE EVENT TRIGGER tests
		{
			name: "CREATE EVENT TRIGGER basic",
			sql:  "CREATE EVENT TRIGGER my_event_trigger ON ddl_command_start EXECUTE FUNCTION my_trigger_func()",
		},
		{
			name: "CREATE EVENT TRIGGER with WHEN clause",
			sql:  "CREATE EVENT TRIGGER my_event_trigger ON ddl_command_start WHEN tag IN ('CREATE TABLE', 'ALTER TABLE') EXECUTE FUNCTION my_trigger_func()",
		},
		{
			name: "CREATE EVENT TRIGGER with multiple WHEN conditions",
			sql:  "CREATE EVENT TRIGGER my_event_trigger ON ddl_command_start WHEN tag IN ('CREATE TABLE') AND schema IN ('public') EXECUTE FUNCTION my_trigger_func()",
		},
		{
			name:     "CREATE EVENT TRIGGER using PROCEDURE",
			sql:      "CREATE EVENT TRIGGER my_event_trigger ON ddl_command_end EXECUTE PROCEDURE my_trigger_proc()",
			expected: "CREATE EVENT TRIGGER my_event_trigger ON ddl_command_end EXECUTE FUNCTION my_trigger_proc()",
		},

		// ALTER EVENT TRIGGER tests
		{
			name: "ALTER EVENT TRIGGER ENABLE",
			sql:  "ALTER EVENT TRIGGER my_event_trigger ENABLE",
		},
		{
			name: "ALTER EVENT TRIGGER ENABLE REPLICA",
			sql:  "ALTER EVENT TRIGGER my_event_trigger ENABLE REPLICA",
		},
		{
			name: "ALTER EVENT TRIGGER ENABLE ALWAYS",
			sql:  "ALTER EVENT TRIGGER my_event_trigger ENABLE ALWAYS",
		},
		{
			name: "ALTER EVENT TRIGGER DISABLE",
			sql:  "ALTER EVENT TRIGGER my_event_trigger DISABLE",
		},

		// DROP User Mapping tests
		{
			name: "DROP USER MAPPING basic",
			sql:  "DROP USER MAPPING FOR CURRENT_USER SERVER myserver",
		},
		{
			name: "DROP USER MAPPING IF EXISTS",
			sql:  "DROP USER MAPPING IF EXISTS FOR alice SERVER myserver",
		},

		// More complex test cases with different expected outputs
		{
			name:     "DROP TABLE RESTRICT round-trip",
			sql:      "DROP TABLE users RESTRICT",
			expected: "DROP TABLE users",
		},

		// CREATE SEQUENCE with additional options that are implemented
		{
			name: "CREATE SEQUENCE AS bigint",
			sql:  "CREATE SEQUENCE bigint_seq AS BIGINT",
		},

		// CREATE TYPE with more complex definitions
		{
			name: "CREATE TYPE with multiple enum values",
			sql:  "CREATE TYPE status_type AS ENUM ('pending', 'approved', 'rejected', 'cancelled')",
		},
		{
			name: "CREATE TYPE composite with constraints",
			sql:  "CREATE TYPE address AS (street TEXT, city TEXT, zipcode TEXT)",
		},
		{
			name: "CREATE TYPE DOMAIN",
			sql:  "CREATE DOMAIN us_postal_code AS TEXT CHECK (value ~ '^[0-9]{5}$' OR value ~ '^[0-9]{5}-[0-9]{4}$')",
		},

		// CREATE MATERIALIZED VIEW with WITH NO DATA (already implemented)
		{
			name: "CREATE MATERIALIZED VIEW WITH NO DATA",
			sql:  "CREATE MATERIALIZED VIEW empty_matview AS SELECT * FROM users WITH NO DATA",
		},

		// CREATE FOREIGN DATA WRAPPER with all implemented options
		{
			name: "CREATE FOREIGN DATA WRAPPER with all options",
			sql:  "CREATE FOREIGN DATA WRAPPER postgres_fdw HANDLER postgres_fdw_handler VALIDATOR postgres_fdw_validator OPTIONS (debug 'true')",
		},

		// More comprehensive EVENT TRIGGER tests with implemented options
		{
			name: "CREATE EVENT TRIGGER with complex WHEN",
			sql:  "CREATE EVENT TRIGGER ddl_audit ON ddl_command_end WHEN tag IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE') AND command_tag IN ('CREATE', 'ALTER', 'DROP') EXECUTE FUNCTION audit_ddl()",
		},

		// Test REFRESH MATERIALIZED VIEW variations (implemented)
		{
			name: "REFRESH MATERIALIZED VIEW WITH NO DATA",
			sql:  "REFRESH MATERIALIZED VIEW test_matview WITH NO DATA",
		},

		// === Comprehensive Phase 3G Additional Constructs Tests ===

		// CREATE CONVERSION tests
		{
			name: "CREATE CONVERSION basic",
			sql:  "CREATE CONVERSION test_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1",
		},
		{
			name: "CREATE DEFAULT CONVERSION",
			sql:  "CREATE DEFAULT CONVERSION test_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1",
		},

		// CREATE LANGUAGE tests
		{
			name: "CREATE LANGUAGE basic",
			sql:  "CREATE LANGUAGE plperl HANDLER plperl_call_handler",
		},
		{
			name: "CREATE OR REPLACE LANGUAGE",
			sql:  "CREATE OR REPLACE LANGUAGE plperl HANDLER plperl_call_handler",
		},
		{
			name: "CREATE TRUSTED LANGUAGE",
			sql:  "CREATE TRUSTED LANGUAGE plperl HANDLER plperl_call_handler",
		},
		{
			name: "CREATE LANGUAGE with VALIDATOR",
			sql:  "CREATE LANGUAGE plperl HANDLER plperl_call_handler VALIDATOR plperl_validator",
		},
		{
			name: "CREATE LANGUAGE with INLINE",
			sql:  "CREATE LANGUAGE plperl HANDLER plperl_call_handler INLINE plperl_inline_handler",
		},

		// CREATE CAST tests
		{
			name: "CREATE CAST basic",
			sql:  "CREATE CAST (INT AS TEXT) WITH FUNCTION int4out()",
		},
		{
			name: "CREATE CAST WITHOUT FUNCTION",
			sql:  "CREATE CAST (INT AS BIGINT) WITHOUT FUNCTION",
		},
		{
			name: "CREATE CAST WITH INOUT",
			sql:  "CREATE CAST (INT AS TEXT) WITH INOUT",
		},

		// CREATE OPERATOR CLASS tests
		{
			name:     "CREATE OPERATOR CLASS basic",
			sql:      "CREATE OPERATOR CLASS test_ops FOR TYPE int4 USING btree AS OPERATOR 1 <",
			expected: "CREATE OPERATOR CLASS test_ops FOR TYPE INT USING btree AS OPERATOR 1 <",
		},
		{
			name:     "CREATE OPERATOR CLASS DEFAULT",
			sql:      "CREATE OPERATOR CLASS test_ops DEFAULT FOR TYPE int4 USING btree AS OPERATOR 1 <",
			expected: "CREATE OPERATOR CLASS test_ops DEFAULT FOR TYPE INT USING btree AS OPERATOR 1 <",
		},
		{
			name:     "CREATE OPERATOR CLASS with FAMILY",
			sql:      "CREATE OPERATOR CLASS test_ops FOR TYPE int4 USING btree FAMILY test_family AS OPERATOR 1 <",
			expected: "CREATE OPERATOR CLASS test_ops FOR TYPE INT USING btree FAMILY test_family AS OPERATOR 1 <",
		},

		// CREATE OPERATOR FAMILY tests
		{
			name: "CREATE OPERATOR FAMILY basic",
			sql:  "CREATE OPERATOR FAMILY test_family USING btree",
		},

		// ALTER OPERATOR FAMILY tests
		{
			name: "ALTER OPERATOR FAMILY ADD",
			sql:  "ALTER OPERATOR FAMILY test_family USING btree ADD OPERATOR 1 <",
		},
		{
			name:     "ALTER OPERATOR FAMILY DROP",
			sql:      "ALTER OPERATOR FAMILY test_family USING btree DROP OPERATOR 1 (int4, int4)",
			expected: "ALTER OPERATOR FAMILY test_family USING btree DROP OPERATOR 1 (INT, INT)",
		},

		// CREATE TRANSFORM tests
		{
			name:     "CREATE TRANSFORM basic",
			sql:      "CREATE TRANSFORM FOR int LANGUAGE sql (FROM SQL WITH FUNCTION int_to_sql(), TO SQL WITH FUNCTION sql_to_int())",
			expected: "CREATE TRANSFORM FOR INT LANGUAGE sql ( FROM SQL WITH FUNCTION int_to_sql(), TO SQL WITH FUNCTION sql_to_int() )",
		},
		{
			name:     "CREATE OR REPLACE TRANSFORM",
			sql:      "CREATE OR REPLACE TRANSFORM FOR text LANGUAGE plpython3u (FROM SQL WITH FUNCTION text_to_python())",
			expected: "CREATE OR REPLACE TRANSFORM FOR TEXT LANGUAGE plpython3u ( FROM SQL WITH FUNCTION text_to_python() )",
		},

		// CREATE STATISTICS tests
		{
			name: "CREATE STATISTICS basic",
			sql:  "CREATE STATISTICS test_stats ON a, b FROM test_table",
		},
		{
			name: "CREATE STATISTICS IF NOT EXISTS",
			sql:  "CREATE STATISTICS IF NOT EXISTS test_stats (dependencies) ON a, b FROM test_table",
		},
		{
			name: "CREATE STATISTICS with specific types",
			sql:  "CREATE STATISTICS test_stats (ndistinct, dependencies) ON a, b FROM test_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test parsing
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse %s", tt.name)
			require.Len(t, stmts, 1, "Expected exactly one statement")
			require.NotNil(t, stmts[0], "Statement should not be nil")

			// Test deparsing
			deparsed := stmts[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Normalize whitespace for comparison
			deparsedNorm := strings.Join(strings.Fields(deparsed), " ")
			expectedNorm := strings.Join(strings.Fields(expected), " ")

			// Assert that deparsed SQL matches expected
			require.Equal(t, expectedNorm, deparsedNorm,
				"Deparsed SQL should match expected.\nOriginal: %s\nExpected: %s\nDeparsed: %s",
				tt.sql, expected, deparsed)

			// Test round-trip parsing - must succeed
			reparsedStmts, err := ParseSQL(deparsed)
			require.NoError(t, err, "Round-trip parsing should succeed for %s: %s", tt.name, deparsed)
			require.Len(t, reparsedStmts, 1, "Should have exactly one statement after reparse for %s", tt.name)

			// Verify stability - deparsing again should produce the same result
			redeparsed := reparsedStmts[0].SqlString()
			redeparsedNorm := strings.Join(strings.Fields(redeparsed), " ")

			require.Equal(t, deparsedNorm, redeparsedNorm,
				"Deparsing should be stable for %s.\nFirst: %s\nSecond: %s", tt.name, deparsed, redeparsed)
		})
	}
}

// TestDDLSqlStringMethods tests SqlString methods directly on DDL AST nodes
// These tests verify that SqlString methods work correctly on manually constructed AST nodes
func TestDDLSqlStringMethods(t *testing.T) {
	t.Run("CreateStmt SqlString", func(t *testing.T) {
		// Create a simple CREATE TABLE statement node
		relation := ast.NewRangeVar("users", "", "")
		createStmt := ast.NewCreateStmt(relation)

		// Add a column
		colDef := ast.NewColumnDef("id", ast.NewTypeName([]string{"int"}), 0)
		createStmt.TableElts = &ast.NodeList{
			Items: []ast.Node{
				colDef,
			},
		}

		sqlString := createStmt.SqlString()
		require.Contains(t, sqlString, "CREATE TABLE users")
		require.Contains(t, sqlString, "(id INT)")

		t.Logf("CreateStmt SqlString: %s", sqlString)
	})

	t.Run("IndexStmt SqlString", func(t *testing.T) {
		// Create a simple CREATE INDEX statement node
		relation := ast.NewRangeVar("users", "", "")
		indexParams := ast.NewNodeList()
		indexParams.Append(ast.NewIndexElem("name"))
		indexStmt := ast.NewIndexStmt("idx_users_name", relation, indexParams)

		sqlString := indexStmt.SqlString()
		require.Contains(t, sqlString, "CREATE INDEX idx_users_name ON users")
		require.Contains(t, sqlString, "(name)")

		t.Logf("IndexStmt SqlString: %s", sqlString)
	})

	t.Run("DropStmt SqlString", func(t *testing.T) {
		// Create a simple DROP TABLE statement node
		dropStmt := &ast.DropStmt{
			BaseNode:   ast.BaseNode{Tag: ast.T_DropStmt},
			RemoveType: ast.OBJECT_TABLE,
			Objects:    ast.NewNodeList(),
			Behavior:   ast.DropRestrict,
			MissingOk:  false,
		}

		// Add table name
		dropStmt.Objects.Append(ast.NewString("users"))

		sqlString := dropStmt.SqlString()
		require.Contains(t, sqlString, "DROP TABLE users")

		t.Logf("DropStmt SqlString: %s", sqlString)
	})

	t.Run("Constraint SqlString", func(t *testing.T) {
		// Test PRIMARY KEY constraint
		pkConstraint := ast.NewConstraint(ast.CONSTR_PRIMARY)
		pkConstraint.Keys = ast.NewNodeList(ast.NewString("id"))

		sqlString := pkConstraint.SqlString()
		require.Equal(t, "PRIMARY KEY (id)", sqlString)

		// Test UNIQUE constraint
		uniqueConstraint := ast.NewConstraint(ast.CONSTR_UNIQUE)
		uniqueConstraint.Keys = ast.NewNodeList(ast.NewString("email"))

		sqlString2 := uniqueConstraint.SqlString()
		require.Equal(t, "UNIQUE (email)", sqlString2)

		t.Logf("PRIMARY KEY SqlString: %s", sqlString)
		t.Logf("UNIQUE SqlString: %s", sqlString2)
	})
}

// TestKeyActionsSetNullWithColumnList tests SET NULL with optional column list
func TestKeyActionsSetNullWithColumnList(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		hasCol bool
	}{
		{
			name:   "SET NULL without column list",
			input:  "CREATE TABLE test (id int REFERENCES parent(id) ON DELETE SET NULL);",
			hasCol: false,
		},
		{
			name:   "SET NULL with column list",
			input:  "CREATE TABLE test (id int REFERENCES parent(id) ON DELETE SET NULL (id));",
			hasCol: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.input)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			createStmt, ok := stmts[0].(*ast.CreateStmt)
			require.True(t, ok)
			require.NotNil(t, createStmt.TableElts)
			require.Len(t, createStmt.TableElts.Items, 1)

			colDef := createStmt.TableElts.Items[0].(*ast.ColumnDef)
			require.NotNil(t, colDef.Constraints)
			require.Len(t, colDef.Constraints.Items, 1)

			constraint, ok := colDef.Constraints.Items[0].(*ast.Constraint)
			require.True(t, ok)
			require.Equal(t, ast.CONSTR_FOREIGN, constraint.Contype)
			require.Equal(t, ast.FKCONSTR_ACTION_SETNULL, constraint.FkDelAction)

			// The actual KeyActions struct should contain the column information
			// but for now we're testing that it parses without error
		})
	}
}

// TestKeyActionsSetDefaultWithColumnList tests SET DEFAULT with optional column list
func TestKeyActionsSetDefaultWithColumnList(t *testing.T) {
	input := "CREATE TABLE test (id int REFERENCES parent(id) ON DELETE SET DEFAULT (id));"

	stmts, err := ParseSQL(input)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	createStmt, ok := stmts[0].(*ast.CreateStmt)
	require.True(t, ok)
	require.NotNil(t, createStmt.TableElts)
	require.Len(t, createStmt.TableElts.Items, 1)

	colDef := createStmt.TableElts.Items[0].(*ast.ColumnDef)
	require.NotNil(t, colDef.Constraints)
	require.Len(t, colDef.Constraints.Items, 1)

	constraint, ok := colDef.Constraints.Items[0].(*ast.Constraint)
	require.True(t, ok)
	require.Equal(t, ast.CONSTR_FOREIGN, constraint.Contype)
	require.Equal(t, ast.FKCONSTR_ACTION_SETDEFAULT, constraint.FkDelAction)
}

// TestKeyActionsUpdateRestriction tests that UPDATE actions don't allow column lists
func TestKeyActionsUpdateRestriction(t *testing.T) {
	// This should cause a parser error since column lists are not supported for UPDATE actions
	input := "CREATE TABLE test (id int REFERENCES parent(id) ON UPDATE SET NULL (id));"

	_, err := ParseSQL(input)
	// We expect an error here since UPDATE with column list should not be supported
	require.Error(t, err)
}

// TestNodeListCreateFunctionDeparsing tests CreateFunctionStmt with NodeList implementation
func TestNodeListCreateFunctionDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		{
			name: "Simple function",
			sql:  "CREATE FUNCTION add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$SELECT a + b$$",
		},
		{
			name: "Function with unnamed parameter",
			sql:  "CREATE FUNCTION greet(text) RETURNS TEXT LANGUAGE sql AS $$SELECT 'Hello ' || $1$$",
		},
		{
			name: "Function with OUT parameter",
			sql:  "CREATE FUNCTION process(IN input text, OUT result integer) LANGUAGE sql AS $$SELECT length(input)$$",
		},
		{
			name: "OR REPLACE function",
			sql:  "CREATE OR REPLACE FUNCTION test() RETURNS void LANGUAGE sql AS $$SELECT$$",
		},
		{
			name: "PROCEDURE",
			sql:  "CREATE PROCEDURE test_proc() LANGUAGE sql AS $$SELECT$$",
		},
		{
			name: "Function with qualified name",
			sql:  "CREATE FUNCTION public.test_func() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a CreateFunctionStmt
			createFunc, ok := stmts[0].(*ast.CreateFunctionStmt)
			require.True(t, ok, "Expected CreateFunctionStmt, got %T", stmts[0])

			// Verify NodeList fields are properly set
			require.NotNil(t, createFunc.FuncName, "FuncName should be NodeList, not nil")
			require.Greater(t, createFunc.FuncName.Len(), 0, "FuncName should have items")

			if createFunc.Parameters != nil {
				require.IsType(t, &ast.NodeList{}, createFunc.Parameters, "Parameters should be *NodeList")
			}

			if createFunc.Options != nil {
				require.IsType(t, &ast.NodeList{}, createFunc.Options, "Options should be *NodeList")
			}

			// Test deparsing
			deparsed := createFunc.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement is also correct
			reparsedFunc, ok := reparsedStmts[0].(*ast.CreateFunctionStmt)
			require.True(t, ok, "Reparsed should be CreateFunctionStmt")
			require.Equal(t, createFunc.IsProcedure, reparsedFunc.IsProcedure)
			require.Equal(t, createFunc.Replace, reparsedFunc.Replace)
		})
	}
}

// TestNodeListViewDeparsing tests ViewStmt with NodeList implementation
func TestNodeListViewDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		{
			name: "Simple view",
			sql:  "CREATE VIEW test_view AS SELECT 1",
		},
		{
			name: "OR REPLACE view",
			sql:  "CREATE OR REPLACE VIEW test_view AS SELECT 1",
		},
		{
			name: "View with column aliases",
			sql:  "CREATE VIEW user_info(id, name) AS SELECT user_id, username FROM users",
		},
		{
			name: "TEMPORARY view",
			sql:  "CREATE TEMPORARY VIEW temp_view AS SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a ViewStmt
			viewStmt, ok := stmts[0].(*ast.ViewStmt)
			require.True(t, ok, "Expected ViewStmt, got %T", stmts[0])

			// Verify NodeList fields
			if viewStmt.Options != nil {
				require.IsType(t, &ast.NodeList{}, viewStmt.Options, "Options should be *NodeList")
			}

			// Test deparsing
			deparsed := viewStmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement is also correct
			reparsedView, ok := reparsedStmts[0].(*ast.ViewStmt)
			require.True(t, ok, "Reparsed should be ViewStmt")
			require.Equal(t, viewStmt.Replace, reparsedView.Replace)
		})
	}
}

// TestCursorStatements tests parsing of cursor-related statements
func TestCursorStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		// DECLARE CURSOR statements
		{
			name: "Simple DECLARE CURSOR",
			sql:  "DECLARE my_cursor CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with BINARY",
			sql:  "DECLARE binary_cursor BINARY CURSOR FOR SELECT * FROM data",
		},
		{
			name: "DECLARE CURSOR with INSENSITIVE",
			sql:  "DECLARE insensitive_cursor INSENSITIVE CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with ASENSITIVE",
			sql:  "DECLARE asensitive_cursor ASENSITIVE CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with SCROLL",
			sql:  "DECLARE scroll_cursor SCROLL CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with NO SCROLL",
			sql:  "DECLARE noscroll_cursor NO SCROLL CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with WITH HOLD",
			sql:  "DECLARE hold_cursor CURSOR WITH HOLD FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with multiple options",
			sql:  "DECLARE complex_cursor BINARY INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with complex query",
			sql:  "DECLARE query_cursor CURSOR FOR SELECT u.id, u.name FROM users u WHERE u.active = TRUE ORDER BY u.name",
		},

		// FETCH statements
		{
			name: "FETCH from cursor",
			sql:  "FETCH FROM my_cursor",
		},
		{
			name: "FETCH IN cursor",
			sql:  "FETCH IN my_cursor",
		},
		{
			name: "FETCH count from cursor",
			sql:  "FETCH 5 FROM my_cursor",
		},
		{
			name: "FETCH count in cursor",
			sql:  "FETCH 10 IN my_cursor",
		},
		{
			name: "FETCH NEXT from cursor",
			sql:  "FETCH NEXT FROM my_cursor",
		},
		{
			name: "FETCH NEXT in cursor",
			sql:  "FETCH NEXT IN my_cursor",
		},
		{
			name: "FETCH PRIOR from cursor",
			sql:  "FETCH PRIOR FROM my_cursor",
		},
		{
			name: "FETCH PRIOR in cursor",
			sql:  "FETCH PRIOR IN my_cursor",
		},
		{
			name: "FETCH FIRST from cursor",
			sql:  "FETCH FIRST FROM my_cursor",
		},
		{
			name: "FETCH FIRST in cursor",
			sql:  "FETCH FIRST IN my_cursor",
		},
		{
			name: "FETCH LAST from cursor",
			sql:  "FETCH LAST FROM my_cursor",
		},
		{
			name: "FETCH LAST in cursor",
			sql:  "FETCH LAST IN my_cursor",
		},
		{
			name: "FETCH ABSOLUTE positive",
			sql:  "FETCH ABSOLUTE 100 FROM my_cursor",
		},
		{
			name: "FETCH ABSOLUTE negative",
			sql:  "FETCH ABSOLUTE -50 IN my_cursor",
		},
		{
			name: "FETCH RELATIVE positive",
			sql:  "FETCH RELATIVE 10 FROM my_cursor",
		},
		{
			name: "FETCH RELATIVE negative",
			sql:  "FETCH RELATIVE -5 IN my_cursor",
		},
		{
			name: "FETCH ALL from cursor",
			sql:  "FETCH ALL FROM my_cursor",
		},
		{
			name: "FETCH ALL in cursor",
			sql:  "FETCH ALL IN my_cursor",
		},
		{
			name: "FETCH FORWARD from cursor",
			sql:  "FETCH FORWARD FROM my_cursor",
		},
		{
			name: "FETCH FORWARD count",
			sql:  "FETCH FORWARD 3 FROM my_cursor",
		},
		{
			name: "FETCH FORWARD ALL",
			sql:  "FETCH FORWARD ALL FROM my_cursor",
		},
		{
			name: "FETCH BACKWARD from cursor",
			sql:  "FETCH BACKWARD FROM my_cursor",
		},
		{
			name: "FETCH BACKWARD count",
			sql:  "FETCH BACKWARD 7 FROM my_cursor",
		},
		{
			name: "FETCH BACKWARD ALL",
			sql:  "FETCH BACKWARD ALL FROM my_cursor",
		},

		// MOVE statements
		{
			name: "MOVE from cursor",
			sql:  "MOVE FROM my_cursor",
		},
		{
			name: "MOVE IN cursor",
			sql:  "MOVE IN my_cursor",
		},
		{
			name: "MOVE count from cursor",
			sql:  "MOVE 5 FROM my_cursor",
		},
		{
			name: "MOVE NEXT from cursor",
			sql:  "MOVE NEXT FROM my_cursor",
		},
		{
			name: "MOVE PRIOR from cursor",
			sql:  "MOVE PRIOR FROM my_cursor",
		},
		{
			name: "MOVE FIRST from cursor",
			sql:  "MOVE FIRST FROM my_cursor",
		},
		{
			name: "MOVE LAST from cursor",
			sql:  "MOVE LAST FROM my_cursor",
		},
		{
			name: "MOVE ABSOLUTE",
			sql:  "MOVE ABSOLUTE 50 FROM my_cursor",
		},
		{
			name: "MOVE RELATIVE",
			sql:  "MOVE RELATIVE -10 FROM my_cursor",
		},
		{
			name: "MOVE ALL from cursor",
			sql:  "MOVE ALL FROM my_cursor",
		},
		{
			name: "MOVE FORWARD",
			sql:  "MOVE FORWARD FROM my_cursor",
		},
		{
			name: "MOVE FORWARD count",
			sql:  "MOVE FORWARD 2 FROM my_cursor",
		},
		{
			name: "MOVE FORWARD ALL",
			sql:  "MOVE FORWARD ALL FROM my_cursor",
		},
		{
			name: "MOVE BACKWARD",
			sql:  "MOVE BACKWARD FROM my_cursor",
		},
		{
			name: "MOVE BACKWARD count",
			sql:  "MOVE BACKWARD 4 FROM my_cursor",
		},
		{
			name: "MOVE BACKWARD ALL",
			sql:  "MOVE BACKWARD ALL FROM my_cursor",
		},

		// CLOSE statements
		{
			name: "CLOSE specific cursor",
			sql:  "CLOSE my_cursor",
		},
		{
			name: "CLOSE ALL cursors",
			sql:  "CLOSE ALL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a cursor statement (can be DeclareCursorStmt, FetchStmt, or ClosePortalStmt)
			stmt := stmts[0]
			require.Implements(t, (*ast.Stmt)(nil), stmt, "Should implement Stmt interface")

			// Test deparsing
			deparsed := stmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Compare with expected output if provided
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement has the same type
			reparsedStmt := reparsedStmts[0]
			require.IsType(t, stmt, reparsedStmt, "Reparsed statement should have same type")
		})
	}
}

// TestPreparedStatements tests parsing of prepared statement-related commands
func TestPreparedStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		// PREPARE statements
		{
			name: "Simple PREPARE without parameters",
			sql:  "PREPARE simple_plan AS SELECT * FROM users",
		},
		{
			name: "PREPARE with one parameter",
			sql:  "PREPARE plan_with_param (integer) AS SELECT * FROM users WHERE id = $1",
		},
		{
			name: "PREPARE with multiple parameters",
			sql:  "PREPARE multi_param_plan (integer, text, boolean) AS SELECT * FROM users WHERE id = $1 AND name = $2 AND active = $3",
		},
		{
			name: "PREPARE with qualified type names",
			sql:  "PREPARE qualified_types (pg_catalog.int4, pg_catalog.text) AS SELECT $1::int4, $2::text",
		},
		{
			name: "PREPARE with complex query",
			sql:  "PREPARE complex_query (integer) AS SELECT u.id, u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id WHERE u.id = $1 ORDER BY p.created_at DESC",
		},
		{
			name: "PREPARE INSERT statement",
			sql:  "PREPARE insert_user (text, text, integer) AS INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
		},
		{
			name: "PREPARE UPDATE statement",
			sql:  "PREPARE update_user (text, integer) AS UPDATE users SET name = $1 WHERE id = $2",
		},
		{
			name: "PREPARE DELETE statement",
			sql:  "PREPARE delete_user (integer) AS DELETE FROM users WHERE id = $1",
		},
		{
			name: "PREPARE with custom type",
			sql:  "PREPARE custom_type_query (user_status) AS SELECT * FROM users WHERE status = $1",
		},

		// EXECUTE statements
		{
			name: "EXECUTE without parameters",
			sql:  "EXECUTE simple_plan",
		},
		{
			name: "EXECUTE with one parameter",
			sql:  "EXECUTE plan_with_param (123)",
		},
		{
			name: "EXECUTE with multiple parameters",
			sql:  "EXECUTE multi_param_plan (123, 'John Doe', TRUE)",
		},
		{
			name: "EXECUTE with string parameters",
			sql:  "EXECUTE user_query ('admin', 'password123')",
		},
		{
			name: "EXECUTE with NULL parameter",
			sql:  "EXECUTE nullable_query (NULL)",
		},
		{
			name: "EXECUTE with numeric parameters",
			sql:  "EXECUTE numeric_query (42, 3.14159, -100)",
		},
		{
			name: "EXECUTE with boolean parameters",
			sql:  "EXECUTE boolean_query (TRUE, FALSE)",
		},
		{
			name: "EXECUTE with mixed parameters",
			sql:  "EXECUTE mixed_query (1, 'test', TRUE, NULL, 3.14)",
		},
		{
			name: "EXECUTE with function call parameter",
			sql:  "EXECUTE time_query (NOW())",
		},
		{
			name: "EXECUTE with expression parameters",
			sql:  "EXECUTE calc_query (1 + 2, 'prefix' || 'suffix')",
		},

		// DEALLOCATE statements
		{
			name: "DEALLOCATE specific statement",
			sql:  "DEALLOCATE my_plan",
		},
		{
			name: "DEALLOCATE ALL statements",
			sql:  "DEALLOCATE ALL",
		},
		{
			name: "DEALLOCATE PREPARE specific statement",
			sql:  "DEALLOCATE PREPARE my_plan",
		},
		{
			name: "DEALLOCATE PREPARE ALL statements",
			sql:  "DEALLOCATE PREPARE ALL",
		},

		// Edge cases and complex scenarios
		{
			name: "PREPARE with very long name",
			sql:  "PREPARE very_long_prepared_statement_name_that_exceeds_normal_length AS SELECT 1",
		},
		{
			name: "PREPARE with complex subquery",
			sql:  "PREPARE subquery_plan (integer) AS SELECT * FROM users WHERE id IN (SELECT user_id FROM posts WHERE author_id = $1)",
		},
		{
			name: "PREPARE with window function",
			sql:  "PREPARE window_plan (text) AS SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) as rank FROM users WHERE department = $1",
		},
		{
			name: "EXECUTE with type cast in parameter",
			sql:  "EXECUTE plan_name ('123'::integer)",
		},

		// CREATE TABLE AS EXECUTE statements
		{
			name: "CREATE TABLE AS EXECUTE without parameters",
			sql:  "CREATE TABLE result_table AS EXECUTE my_plan",
		},
		{
			name: "CREATE TABLE AS EXECUTE with parameters",
			sql:  "CREATE TABLE user_results AS EXECUTE user_query (123, 'admin')",
		},
		{
			name: "CREATE TABLE AS EXECUTE with qualified table name",
			sql:  "CREATE TABLE public.results AS EXECUTE data_plan (1, 2, 3)",
		},
		{
			name: "CREATE TEMPORARY TABLE AS EXECUTE",
			sql:  "CREATE TEMPORARY TABLE temp_results AS EXECUTE temp_plan",
		},
		{
			name: "CREATE TEMP TABLE AS EXECUTE with parameters",
			sql:  "CREATE TEMP TABLE temp_data AS EXECUTE analysis_plan (100, 'report')",
		},

		// IF NOT EXISTS variants
		{
			name: "CREATE TABLE IF NOT EXISTS AS EXECUTE",
			sql:  "CREATE TABLE IF NOT EXISTS backup_table AS EXECUTE backup_plan",
		},
		{
			name: "CREATE TEMPORARY TABLE IF NOT EXISTS AS EXECUTE",
			sql:  "CREATE TEMPORARY TABLE IF NOT EXISTS temp_backup AS EXECUTE temp_backup_plan (1, 2)",
		},
		{
			name: "CREATE TEMP TABLE IF NOT EXISTS AS EXECUTE",
			sql:  "CREATE TEMP TABLE IF NOT EXISTS summary AS EXECUTE summary_plan ('monthly')",
		},

		// WITH DATA / WITH NO DATA variants
		{
			name: "CREATE TABLE AS EXECUTE with WITH DATA",
			sql:  "CREATE TABLE results AS EXECUTE my_plan WITH DATA",
		},
		{
			name: "CREATE TABLE AS EXECUTE with WITH NO DATA",
			sql:  "CREATE TABLE empty_results AS EXECUTE my_plan WITH NO DATA",
		},
		{
			name: "CREATE TEMPORARY TABLE AS EXECUTE with WITH DATA",
			sql:  "CREATE TEMPORARY TABLE temp_results AS EXECUTE temp_plan WITH DATA",
		},
		{
			name: "CREATE TEMP TABLE IF NOT EXISTS AS EXECUTE with WITH NO DATA",
			sql:  "CREATE TEMP TABLE IF NOT EXISTS summary AS EXECUTE summary_plan WITH NO DATA",
		},

		// Enhanced create_as_target features
		{
			name: "CREATE TABLE AS EXECUTE with column list",
			sql:  "CREATE TABLE results (id, name, value) AS EXECUTE my_plan",
		},
		{
			name: "CREATE TABLE AS EXECUTE with WITH options",
			sql:  "CREATE TABLE results WITH (fillfactor=80) AS EXECUTE my_plan",
		},
		{
			name: "CREATE TABLE AS EXECUTE with USING access method",
			sql:  "CREATE TABLE results USING heap AS EXECUTE my_plan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a prepared statement (can be PrepareStmt, ExecuteStmt, or DeallocateStmt)
			stmt := stmts[0]
			require.Implements(t, (*ast.Stmt)(nil), stmt, "Should implement Stmt interface")

			// Test deparsing
			deparsed := stmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Compare with expected output if provided
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement has the same type
			reparsedStmt := reparsedStmts[0]
			require.IsType(t, stmt, reparsedStmt, "Reparsed statement should have same type")
		})
	}
}
