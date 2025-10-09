// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fakepgdb

import (
	"testing"
)

func TestBasicQuery(t *testing.T) {
	db := New(t)
	db.AddQuery("SELECT 1", &ExpectedResult{
		Columns: []string{"?column?"},
		Rows:    [][]interface{}{{int64(1)}},
	})

	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	var result int
	err := sqlDB.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if result != 1 {
		t.Errorf("expected 1, got %d", result)
	}

	// Verify query was called
	if db.GetQueryCalledNum("SELECT 1") != 1 {
		t.Errorf("expected query to be called once, got %d", db.GetQueryCalledNum("SELECT 1"))
	}
}

func TestQueryPattern(t *testing.T) {
	db := New(t)
	db.AddQueryPattern("SELECT \\* FROM users WHERE id = .*", &ExpectedResult{
		Columns: []string{"id", "name"},
		Rows:    [][]interface{}{{int64(1), "John"}},
	})

	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	var id int
	var name string
	err := sqlDB.QueryRow("SELECT * FROM users WHERE id = 1").Scan(&id, &name)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if id != 1 || name != "John" {
		t.Errorf("expected (1, 'John'), got (%d, '%s')", id, name)
	}
}

func TestRejectedQuery(t *testing.T) {
	db := New(t)
	db.AddRejectedQuery("SELECT * FROM forbidden", &FakeError{msg: "access denied"})

	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	var result int
	err := sqlDB.QueryRow("SELECT * FROM forbidden").Scan(&result)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err.Error() != "access denied" {
		t.Errorf("expected 'access denied', got '%v'", err)
	}
}

func TestOrderedQueries(t *testing.T) {
	db := New(t)
	db.OrderMatters()

	db.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: "SELECT 1",
		QueryResult: &ExpectedResult{
			Columns: []string{"?column?"},
			Rows:    [][]interface{}{{int64(1)}},
		},
	})

	db.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: "SELECT 2",
		QueryResult: &ExpectedResult{
			Columns: []string{"?column?"},
			Rows:    [][]interface{}{{int64(2)}},
		},
	})

	db.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: "SELECT 3",
		QueryResult: &ExpectedResult{
			Columns: []string{"?column?"},
			Rows:    [][]interface{}{{int64(3)}},
		},
	})

	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	// Execute queries in order
	var result int
	err := sqlDB.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}
	if result != 1 {
		t.Errorf("expected 1, got %d", result)
	}

	err = sqlDB.QueryRow("SELECT 2").Scan(&result)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	if result != 2 {
		t.Errorf("expected 2, got %d", result)
	}

	err = sqlDB.QueryRow("SELECT 3").Scan(&result)
	if err != nil {
		t.Fatalf("query 3 failed: %v", err)
	}
	if result != 3 {
		t.Errorf("expected 3, got %d", result)
	}

	db.VerifyAllExecutedOrFail()
}

func TestMultipleRows(t *testing.T) {
	db := New(t)
	db.AddQuery("SELECT * FROM users", &ExpectedResult{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
			{int64(3), "Charlie"},
		},
	})

	sqlDB := db.OpenDB()
	defer sqlDB.Close()

	rows, err := sqlDB.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id   int64
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	i := 0
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("too many rows returned")
		}

		if id != expected[i].id || name != expected[i].name {
			t.Errorf("row %d: expected (%d, '%s'), got (%d, '%s')",
				i, expected[i].id, expected[i].name, id, name)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), i)
	}
}

// FakeError is a simple error type for testing
type FakeError struct {
	msg string
}

func (e *FakeError) Error() string {
	return e.msg
}
