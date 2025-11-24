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

// Package fakepgdb provides a fake PostgreSQL server for tests.
// It is inspired by Vitess's fakesqldb but adapted for PostgreSQL.
package fakepgdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	appendEntry = -1
)

// DB is a fake PostgreSQL database. All methods are thread-safe.
// It implements driver.Connector to be used with sql.OpenDB.
type DB struct {
	// t is our testing.TB instance
	t testing.TB

	// name is the name of this DB
	name string

	// orderMatters controls whether query order matters
	orderMatters atomic.Bool

	// mu protects all the following fields
	mu sync.Mutex

	// data maps tolower(query) to a result
	data map[string]*ExpectedResult

	// rejectedData maps tolower(query) to an error
	rejectedData map[string]error

	// patternData is a map of regexp queries to results
	patternData map[string]exprResult

	// queryCalled keeps track of how many times a query was called
	queryCalled map[string]int

	// querylog keeps track of all called queries
	querylog []string

	// expectedExecuteFetch is the array of expected queries (for ordered mode)
	expectedExecuteFetch []ExpectedExecuteFetch

	// expectedExecuteFetchIndex is the current index of the query
	expectedExecuteFetchIndex int

	// neverFail makes unmatched queries return empty results instead of errors
	neverFail atomic.Bool

	// allowAll returns empty result for all queries (for benchmarking)
	allowAll atomic.Bool

	// queryPatternUserCallback stores optional callbacks when a query with a pattern is called
	queryPatternUserCallback map[*regexp.Regexp]func(string)
}

// ExpectedResult holds the data for a matched query.
type ExpectedResult struct {
	Columns []string
	Rows    [][]any
	// BeforeFunc() is synchronously called before the server returns the result.
	BeforeFunc func()
}

type exprResult struct {
	queryPattern string
	expr         *regexp.Regexp
	result       *ExpectedResult
	err          string
}

// ExpectedExecuteFetch defines for an expected query the to be faked output.
// It is used for ordered expected output.
type ExpectedExecuteFetch struct {
	Query       string
	QueryResult *ExpectedResult
	Error       error
	// AfterFunc is a callback which is executed while the query
	// is executed i.e., before the fake responds to the client.
	AfterFunc func()
}

// New creates a new fake PostgreSQL database for testing.
func New(t testing.TB) *DB {
	db := &DB{
		t:                        t,
		name:                     "fakepgdb",
		data:                     make(map[string]*ExpectedResult),
		rejectedData:             make(map[string]error),
		queryCalled:              make(map[string]int),
		queryPatternUserCallback: make(map[*regexp.Regexp]func(string)),
		patternData:              make(map[string]exprResult),
	}

	return db
}

// Name returns the name of the DB.
func (db *DB) Name() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.name
}

// SetName sets the name of the DB.
func (db *DB) SetName(name string) *DB {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.name = name
	return db
}

// OrderMatters sets the orderMatters flag.
func (db *DB) OrderMatters() {
	db.orderMatters.Store(true)
}

// Connect returns a driver.Conn implementation.
func (db *DB) Connect(ctx context.Context) (driver.Conn, error) {
	return &fakeConn{db: db}, nil
}

// Driver returns a driver.Driver implementation.
func (db *DB) Driver() driver.Driver {
	return &fakeDriver{db: db}
}

// OpenDB returns a *sql.DB connected to this fake database.
func (db *DB) OpenDB() *sql.DB {
	return sql.OpenDB(db)
}

//
// Methods to add expected queries and results.
//

// AddQuery adds a query and its expected result.
func (db *DB) AddQuery(query string, expectedResult *ExpectedResult) *ExpectedResult {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	r := &ExpectedResult{
		Columns:    expectedResult.Columns,
		Rows:       expectedResult.Rows,
		BeforeFunc: expectedResult.BeforeFunc,
	}
	db.data[key] = r
	db.queryCalled[key] = 0
	return r
}

// SetBeforeFunc sets the BeforeFunc field for the previously registered "query".
func (db *DB) SetBeforeFunc(query string, f func()) {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	r, ok := db.data[key]
	if !ok {
		db.t.Fatalf("BUG: no query registered for: %v", query)
	}
	r.BeforeFunc = f
}

// AddQueryPattern adds an expected result for a set of queries.
// These patterns are checked if no exact matches from AddQuery() are found.
// This function forces the addition of begin/end anchors (^$) and turns on
// case-insensitive matching mode.
func (db *DB) AddQueryPattern(queryPattern string, expectedResult *ExpectedResult) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	db.mu.Lock()
	defer db.mu.Unlock()
	db.patternData[queryPattern] = exprResult{
		queryPattern: queryPattern,
		expr:         expr,
		result:       expectedResult,
	}
}

// RemoveQueryPattern removes a query pattern that was previously added.
func (db *DB) RemoveQueryPattern(queryPattern string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.patternData, queryPattern)
}

// RejectQueryPattern allows a query pattern to be rejected with an error
func (db *DB) RejectQueryPattern(queryPattern, error string) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	db.mu.Lock()
	defer db.mu.Unlock()
	db.patternData[queryPattern] = exprResult{
		queryPattern: queryPattern,
		expr:         expr,
		err:          error,
	}
}

// ClearQueryPattern removes all query patterns set up
func (db *DB) ClearQueryPattern() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.patternData = make(map[string]exprResult)
}

// AddQueryPatternWithCallback is similar to AddQueryPattern: in addition it calls the provided callback function
func (db *DB) AddQueryPatternWithCallback(queryPattern string, expectedResult *ExpectedResult, callback func(string)) {
	db.AddQueryPattern(queryPattern, expectedResult)
	db.queryPatternUserCallback[db.patternData[queryPattern].expr] = callback
}

// DeleteQuery deletes query from the fake DB.
func (db *DB) DeleteQuery(query string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := strings.ToLower(query)
	delete(db.data, key)
	delete(db.queryCalled, key)
}

// DeleteAllQueries deletes all expected queries from the fake DB.
func (db *DB) DeleteAllQueries() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data = make(map[string]*ExpectedResult)
	db.patternData = make(map[string]exprResult)
	db.queryCalled = make(map[string]int)
}

// AddRejectedQuery adds a query which will be rejected at execution time.
func (db *DB) AddRejectedQuery(query string, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rejectedData[strings.ToLower(query)] = err
}

// DeleteRejectedQuery deletes query from the fake DB.
func (db *DB) DeleteRejectedQuery(query string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.rejectedData, strings.ToLower(query))
}

// GetQueryCalledNum returns how many times db executes a certain query.
func (db *DB) GetQueryCalledNum(query string) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	num, ok := db.queryCalled[strings.ToLower(query)]
	if !ok {
		return 0
	}
	return num
}

// QueryLog returns the query log as a semicolon separated string
func (db *DB) QueryLog() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return strings.Join(db.querylog, ";")
}

// ResetQueryLog resets the query log
func (db *DB) ResetQueryLog() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.querylog = nil
}

//
// Methods for ordered expected queries.
//

// AddExpectedExecuteFetch adds an ExpectedExecuteFetch directly.
func (db *DB) AddExpectedExecuteFetch(entry ExpectedExecuteFetch) {
	db.AddExpectedExecuteFetchAtIndex(appendEntry, entry)
}

// AddExpectedExecuteFetchAtIndex inserts a new entry at index.
func (db *DB) AddExpectedExecuteFetchAtIndex(index int, entry ExpectedExecuteFetch) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.expectedExecuteFetch == nil || index < 0 || index >= len(db.expectedExecuteFetch) {
		index = appendEntry
	}
	if index == appendEntry {
		db.expectedExecuteFetch = append(db.expectedExecuteFetch, entry)
	} else {
		// Grow the slice by one element
		if cap(db.expectedExecuteFetch) == len(db.expectedExecuteFetch) {
			db.expectedExecuteFetch = append(db.expectedExecuteFetch, make([]ExpectedExecuteFetch, 1)...)
		} else {
			db.expectedExecuteFetch = db.expectedExecuteFetch[0 : len(db.expectedExecuteFetch)+1]
		}
		// Use copy to move the upper part of the slice out of the way and open a hole
		copy(db.expectedExecuteFetch[index+1:], db.expectedExecuteFetch[index:])
		// Store the new value
		db.expectedExecuteFetch[index] = entry
	}
}

// AddExpectedQuery adds a single query with no result.
func (db *DB) AddExpectedQuery(query string, err error) {
	db.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query:       query,
		QueryResult: &ExpectedResult{},
		Error:       err,
	})
}

// DeleteAllEntries removes all ordered entries.
func (db *DB) DeleteAllEntries() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.expectedExecuteFetch = make([]ExpectedExecuteFetch, 0)
	db.expectedExecuteFetchIndex = 0
}

// VerifyAllExecutedOrFail checks that all expected queries were actually executed.
func (db *DB) VerifyAllExecutedOrFail() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.expectedExecuteFetchIndex != len(db.expectedExecuteFetch) {
		db.t.Errorf("%v: not all expected queries were executed. leftovers: %v", db.name, db.expectedExecuteFetch[db.expectedExecuteFetchIndex:])
	}
}

// SetAllowAll makes all queries return empty results.
func (db *DB) SetAllowAll(allowAll bool) {
	db.allowAll.Store(allowAll)
}

// SetNeverFail makes unmatched queries return empty results instead of errors.
func (db *DB) SetNeverFail(neverFail bool) {
	db.neverFail.Store(neverFail)
}

// handleQuery handles a query and returns the result.
func (db *DB) handleQuery(query string) (*ExpectedResult, error) {
	if db.allowAll.Load() {
		return &ExpectedResult{}, nil
	}

	if db.orderMatters.Load() {
		return db.handleQueryOrdered(query)
	}

	key := strings.ToLower(query)
	db.mu.Lock()
	db.queryCalled[key]++
	db.querylog = append(db.querylog, key)

	// Check if we should reject it
	if err, ok := db.rejectedData[key]; ok {
		db.mu.Unlock()
		return nil, err
	}

	// Check explicit queries from AddQuery()
	result, ok := db.data[key]
	if ok {
		db.mu.Unlock()
		if f := result.BeforeFunc; f != nil {
			f()
		}
		return result, nil
	}

	// Check query patterns from AddQueryPattern()
	for _, pat := range db.patternData {
		if pat.expr.MatchString(query) {
			userCallback, ok := db.queryPatternUserCallback[pat.expr]
			db.mu.Unlock()
			if ok {
				userCallback(query)
			}
			if pat.err != "" {
				return nil, errors.New(pat.err)
			}
			return pat.result, nil
		}
	}

	db.mu.Unlock()

	if db.neverFail.Load() {
		return &ExpectedResult{}, nil
	}

	// Nothing matched
	return nil, fmt.Errorf("fakepgdb: query '%s' is not supported on %v", query, db.name)
}

func (db *DB) handleQueryOrdered(query string) (*ExpectedResult, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	index := db.expectedExecuteFetchIndex

	if index >= len(db.expectedExecuteFetch) {
		if db.neverFail.Load() {
			return &ExpectedResult{}, nil
		}
		db.t.Errorf("%v: got unexpected out of bound fetch: %v >= %v (%s)", db.name, index, len(db.expectedExecuteFetch), query)
		return nil, errors.New("unexpected out of bound fetch")
	}

	entry := db.expectedExecuteFetch[index]

	if entry.AfterFunc != nil {
		defer entry.AfterFunc()
	}

	expected := entry.Query

	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(query, expected[0:len(expected)-1]) {
			if db.neverFail.Load() {
				return &ExpectedResult{}, nil
			}
			db.t.Errorf("%v: got unexpected query start (index=%v): %v != %v", db.name, index, query, expected)
			return nil, errors.New("unexpected query")
		}
	} else {
		if query != expected {
			if db.neverFail.Load() {
				return &ExpectedResult{}, nil
			}
			db.t.Errorf("%v: got unexpected query (index=%v): %v != %v", db.name, index, query, expected)
			return nil, errors.New("unexpected query")
		}
	}

	db.expectedExecuteFetchIndex++
	db.t.Logf("ExecuteFetch: %v: %v", db.name, query)

	if entry.Error != nil {
		return nil, entry.Error
	}

	return entry.QueryResult, nil
}
