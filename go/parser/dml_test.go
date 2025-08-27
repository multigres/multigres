package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		{"INSERT with subquery", "INSERT INTO users (SELECT id, name FROM temp_users WHERE active = TRUE)", "INSERT INTO users SELECT id, name FROM temp_users WHERE active = TRUE"},
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
		{"INSERT with type casts", "INSERT INTO users (id, name, age) VALUES (1::bigint, 'John'::varchar, '25'::integer)", ""},

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
		{"UPDATE with type casts", "UPDATE users SET score = '95.5'::decimal, active = 'true'::boolean", ""},
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
		{"DELETE with nested expressions in WHERE", "DELETE FROM orders WHERE (total + tax) > (limit * 1.5) AND status = 'pending'", ""},

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
		{"INSERT with type casts and expressions", "INSERT INTO logs (level, message, count) VALUES (upper('info')::text, concat('Log: ', details), (1 + retry_count)::integer)", ""},
		{"INSERT with deeply nested functions", "INSERT INTO processed (data) VALUES (upper(substring(trim(input_data), 1, 10)))", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input
			stmts, err := ParseSQL(tt.input)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.input)
			require.Len(t, stmts, 1, "Expected exactly one statement")

			// Deparse the statement
			deparsed := stmts[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Check if output matches expected
			assert.Equal(t, expected, deparsed, "Deparsed SQL should match expected output")

			// Test round-trip parsing (re-parse the deparsed SQL)
			stmts2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

			// Verify statement types match
			assert.Equal(t, stmts[0].StatementType(), stmts2[0].StatementType(),
				"Statement types should match after round-trip")

			// Test stability - second deparse should match first
			deparsed2 := stmts2[0].SqlString()
			assert.Equal(t, deparsed, deparsed2,
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}
