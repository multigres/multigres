// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// BenchmarkMultigresParser benchmarks our parser using PostgreSQL test queries
func BenchmarkMultigresParser(b *testing.B) {
	// Load all test queries from PostgreSQL test files
	queries := loadPostgresTestQueries(b)

	// Variables to prevent compiler optimization
	var totalStatements int
	var parseErrors int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			// Parse using our parser
			asts, err := ParseSQL(query)
			if err != nil {
				parseErrors++
				continue // Skip queries that fail to parse
			}

			// Minimal operation to prevent compiler optimization
			// Just count the number of statements parsed
			totalStatements += len(asts)
		}
	}

	// Use the results to prevent dead code elimination
	b.Logf("Parsed %d total statements with %d errors", totalStatements, parseErrors)
}

// BenchmarkPgQueryGo benchmarks pg-query-go using the same PostgreSQL test queries
func BenchmarkPgQueryGo(b *testing.B) {
	// Load all test queries from PostgreSQL test files
	queries := loadPostgresTestQueries(b)

	// Variables to prevent compiler optimization
	var totalStatements int
	var parseErrors int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			// Parse using pg-query-go
			result, err := pg_query.Parse(query)
			if err != nil {
				parseErrors++
				continue // Skip queries that fail to parse
			}

			// Minimal operation to prevent compiler optimization
			// Just count the number of statements parsed
			if result.Stmts != nil {
				totalStatements += len(result.Stmts)
			}
		}
	}

	// Use the results to prevent dead code elimination
	b.Logf("Parsed %d total statements with %d errors", totalStatements, parseErrors)
}

// loadPostgresTestQueries loads all queries from PostgreSQL test JSON files
func loadPostgresTestQueries(b *testing.B) []string {
	b.Helper()

	var allQueries []string
	var skippedCount int

	// Read all JSON files from the postgres directory
	postgresDir := "testdata/postgres"
	files, err := os.ReadDir(postgresDir)
	if err != nil {
		b.Fatalf("Failed to read postgres test directory: %v", err)
	}

	// Load queries from each JSON file
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			filepath := filepath.Join("postgres", file.Name())
			tests := readJSONTests(filepath)

			for _, test := range tests {
				// Skip queries that are expected to fail
				if test.Error != "" {
					skippedCount++
					continue
				}

				if test.Query != "" {
					allQueries = append(allQueries, test.Query)
				}
			}
		}
	}

	if len(allQueries) == 0 {
		b.Fatal("No test queries found")
	}

	b.Logf("Loaded %d test queries for benchmarking (skipped %d queries with expected errors)", len(allQueries), skippedCount)
	return allQueries
}

// Comparative benchmarks for individual operations

// BenchmarkMultigresParserSimpleSelect benchmarks a simple SELECT with our parser
func BenchmarkMultigresParserSimpleSelect(b *testing.B) {
	query := "SELECT * FROM users WHERE id = 1"

	var astCount int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		asts, err := ParseSQL(query)
		if err != nil {
			b.Fatal(err)
		}
		astCount += len(asts)
	}

	// Prevent compiler optimization
	if astCount == 0 {
		b.Fatal("No ASTs generated")
	}
}

// BenchmarkPgQueryGoSimpleSelect benchmarks a simple SELECT with pg-query-go
func BenchmarkPgQueryGoSimpleSelect(b *testing.B) {
	query := "SELECT * FROM users WHERE id = 1"

	var stmtCount int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := pg_query.Parse(query)
		if err != nil {
			b.Fatal(err)
		}
		stmtCount += len(result.Stmts)
	}

	// Prevent compiler optimization
	if stmtCount == 0 {
		b.Fatal("No statements generated")
	}
}

// BenchmarkMultigresParserComplexJoin benchmarks a complex JOIN with our parser
func BenchmarkMultigresParserComplexJoin(b *testing.B) {
	query := `
		SELECT u.id, u.name, o.order_id, o.total, p.product_name
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		INNER JOIN order_items oi ON o.order_id = oi.order_id
		INNER JOIN products p ON oi.product_id = p.product_id
		WHERE u.created_at > '2024-01-01'
		  AND o.status = 'completed'
		ORDER BY o.created_at DESC
		LIMIT 100
	`

	var astCount int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		asts, err := ParseSQL(query)
		if err != nil {
			b.Fatal(err)
		}
		astCount += len(asts)
	}

	// Prevent compiler optimization
	if astCount == 0 {
		b.Fatal("No ASTs generated")
	}
}

// BenchmarkPgQueryGoComplexJoin benchmarks a complex JOIN with pg-query-go
func BenchmarkPgQueryGoComplexJoin(b *testing.B) {
	query := `
		SELECT u.id, u.name, o.order_id, o.total, p.product_name
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		INNER JOIN order_items oi ON o.order_id = oi.order_id
		INNER JOIN products p ON oi.product_id = p.product_id
		WHERE u.created_at > '2024-01-01'
		  AND o.status = 'completed'
		ORDER BY o.created_at DESC
		LIMIT 100
	`

	var stmtCount int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := pg_query.Parse(query)
		if err != nil {
			b.Fatal(err)
		}
		stmtCount += len(result.Stmts)
	}

	// Prevent compiler optimization
	if stmtCount == 0 {
		b.Fatal("No statements generated")
	}
}

// BenchmarkMultigresParserDDL benchmarks DDL statement parsing with our parser
func BenchmarkMultigresParserDDL(b *testing.B) {
	query := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			is_active BOOLEAN DEFAULT true,
			metadata JSONB,
			CONSTRAINT email_check CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
		)
	`

	var astCount int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		asts, err := ParseSQL(query)
		if err != nil {
			b.Fatal(err)
		}
		astCount += len(asts)
	}

	// Prevent compiler optimization
	if astCount == 0 {
		b.Fatal("No ASTs generated")
	}
}

// BenchmarkPgQueryGoDDL benchmarks DDL statement parsing with pg-query-go
func BenchmarkPgQueryGoDDL(b *testing.B) {
	query := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			is_active BOOLEAN DEFAULT true,
			metadata JSONB,
			CONSTRAINT email_check CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
		)
	`

	var stmtCount int

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := pg_query.Parse(query)
		if err != nil {
			b.Fatal(err)
		}
		stmtCount += len(result.Stmts)
	}

	// Prevent compiler optimization
	if stmtCount == 0 {
		b.Fatal("No statements generated")
	}
}
