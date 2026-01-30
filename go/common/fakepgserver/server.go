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

// Package fakepgserver provides a fake PostgreSQL server for testing.
// It speaks the PostgreSQL wire protocol and returns pre-configured results.
// The API mirrors go/tools/fakepgdb for consistency.
package fakepgserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// Server is a fake PostgreSQL server for testing.
// All methods are thread-safe.
type Server struct {
	// t is our testing.TB instance.
	t testing.TB

	// listener is the PostgreSQL protocol listener.
	listener *server.Listener

	// address is the server's listening address (host:port).
	address string

	// name is the name of this server (for debugging).
	name string

	// orderMatters controls whether query order matters.
	orderMatters atomic.Bool

	// mu protects all the following fields.
	mu sync.Mutex

	// data maps tolower(query) to a result.
	data map[string]*sqltypes.Result

	// rejectedData maps tolower(query) to an error.
	rejectedData map[string]error

	// patternData is a map of regexp queries to results.
	patternData map[string]exprResult

	// patternCalled keeps track of how many times each pattern was matched.
	patternCalled map[string]int

	// queryCalled keeps track of how many times a query was called.
	queryCalled map[string]int

	// querylog keeps track of all called queries.
	querylog []string

	// expectedExecuteFetch is the array of expected queries (for ordered mode).
	expectedExecuteFetch []ExpectedExecuteFetch

	// expectedExecuteFetchIndex is the current index of the query.
	expectedExecuteFetchIndex int

	// neverFail makes unmatched queries return empty results instead of errors.
	neverFail atomic.Bool

	// queryPatternUserCallback stores optional callbacks when a query with a pattern is called.
	queryPatternUserCallback map[*regexp.Regexp]func(string)
}

type exprResult struct {
	queryPattern string
	expr         *regexp.Regexp
	result       *sqltypes.Result
	err          string
}

// trustAllProvider implements server.TrustAuthProvider for testing.
// It allows all connections without password authentication,
// simulating Unix socket trust authentication.
type trustAllProvider struct{}

// AllowTrustAuth always returns true, allowing all connections without password.
func (p *trustAllProvider) AllowTrustAuth(_ context.Context, _, _ string) bool {
	return true
}

// ExpectedExecuteFetch defines for an expected query the to be faked output.
// It is used for ordered expected output.
type ExpectedExecuteFetch struct {
	Query       string
	QueryResult *sqltypes.Result
	Error       error
}

// New creates a new fake PostgreSQL server for testing.
// The server listens on a random available TCP port.
func New(t testing.TB) *Server {
	s := &Server{
		t:                        t,
		name:                     "fakepgserver",
		data:                     make(map[string]*sqltypes.Result),
		rejectedData:             make(map[string]error),
		queryCalled:              make(map[string]int),
		patternCalled:            make(map[string]int),
		queryPatternUserCallback: make(map[*regexp.Regexp]func(string)),
		patternData:              make(map[string]exprResult),
	}

	// Create the handler.
	handler := &fakeHandler{server: s}

	// Create listener on random port with trust auth (simulates Unix socket trust auth).
	var err error
	s.listener, err = server.NewListener(server.ListenerConfig{
		Address:           "127.0.0.1:0", // Random available port.
		Handler:           handler,
		TrustAuthProvider: &trustAllProvider{},
		Logger:            slog.Default(),
	})
	if err != nil {
		t.Fatalf("fakepgserver: failed to create listener: %v", err)
	}

	// Get the actual address.
	s.address = s.listener.Addr().String()

	// Start serving in background.
	go func() {
		if err := s.listener.Serve(); err != nil {
			// Don't log errors if the listener was closed intentionally.
			if !errors.Is(err, net.ErrClosed) {
				t.Logf("fakepgserver: serve error: %v", err)
			}
		}
	}()

	t.Logf("fakepgserver: listening on %s", s.address)

	return s
}

// Name returns the name of the server.
func (s *Server) Name() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.name
}

// SetName sets the name of the server.
func (s *Server) SetName(name string) *Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.name = name
	return s
}

// Address returns the server's listening address.
func (s *Server) Address() string {
	return s.address
}

// ClientConfig returns a client.Config for connecting to this server.
// No password is needed since fakepgserver uses trust authentication.
func (s *Server) ClientConfig() *client.Config {
	host, port, err := net.SplitHostPort(s.address)
	if err != nil {
		s.t.Fatalf("fakepgserver: failed to parse address: %v", err)
	}

	var portNum int
	_, _ = fmt.Sscanf(port, "%d", &portNum)

	return &client.Config{
		Host:     host,
		Port:     portNum,
		User:     "test",
		Database: "testdb",
	}
}

// Close closes the server and stops accepting connections.
func (s *Server) Close() {
	if err := s.listener.Close(); err != nil {
		s.t.Logf("fakepgserver: close error: %v", err)
	}
}

// OrderMatters sets the orderMatters flag.
func (s *Server) OrderMatters() {
	s.orderMatters.Store(true)
}

//
// Methods to add expected queries and results.
//

// AddQuery adds a query and its expected result.
func (s *Server) AddQuery(q string, result *sqltypes.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(q)
	s.data[key] = result
	s.queryCalled[key] = 0
}

// AddQueryPattern adds an expected result for a set of queries.
// These patterns are checked if no exact matches from AddQuery() are found.
// This function forces the addition of begin/end anchors (^$) and turns on
// case-insensitive matching mode.
func (s *Server) AddQueryPattern(queryPattern string, result *sqltypes.Result) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patternData[queryPattern] = exprResult{
		queryPattern: queryPattern,
		expr:         expr,
		result:       result,
	}
}

// RemoveQueryPattern removes a query pattern that was previously added.
func (s *Server) RemoveQueryPattern(queryPattern string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.patternData, queryPattern)
	delete(s.patternCalled, queryPattern)
}

// RejectQueryPattern allows a query pattern to be rejected with an error.
func (s *Server) RejectQueryPattern(queryPattern, errMsg string) {
	expr := regexp.MustCompile("(?is)^" + queryPattern + "$")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patternData[queryPattern] = exprResult{
		queryPattern: queryPattern,
		expr:         expr,
		err:          errMsg,
	}
}

// ClearQueryPattern removes all query patterns set up.
func (s *Server) ClearQueryPattern() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patternData = make(map[string]exprResult)
	s.patternCalled = make(map[string]int)
}

// AddQueryPatternWithCallback is similar to AddQueryPattern: in addition it calls the provided callback function.
func (s *Server) AddQueryPatternWithCallback(queryPattern string, result *sqltypes.Result, callback func(string)) {
	s.AddQueryPattern(queryPattern, result)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryPatternUserCallback[s.patternData[queryPattern].expr] = callback
}

// DeleteQuery deletes query from the fake server.
func (s *Server) DeleteQuery(query string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(query)
	delete(s.data, key)
	delete(s.queryCalled, key)
}

// DeleteAllQueries deletes all expected queries from the fake server.
func (s *Server) DeleteAllQueries() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]*sqltypes.Result)
	s.patternData = make(map[string]exprResult)
	s.queryCalled = make(map[string]int)
	s.patternCalled = make(map[string]int)
}

// AddRejectedQuery adds a query which will be rejected at execution time.
func (s *Server) AddRejectedQuery(query string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejectedData[strings.ToLower(query)] = err
}

// DeleteRejectedQuery deletes query from the fake server.
func (s *Server) DeleteRejectedQuery(query string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rejectedData, strings.ToLower(query))
}

// GetQueryCalledNum returns how many times the server executed a certain query.
func (s *Server) GetQueryCalledNum(query string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	num, ok := s.queryCalled[strings.ToLower(query)]
	if !ok {
		return 0
	}
	return num
}

// QueryLog returns the query log as a semicolon separated string.
func (s *Server) QueryLog() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.querylog, ";")
}

// ResetQueryLog resets the query log.
func (s *Server) ResetQueryLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.querylog = nil
}

//
// Methods for ordered expected queries.
//

// AddExpectedExecuteFetch appends an ExpectedExecuteFetch to the end.
func (s *Server) AddExpectedExecuteFetch(entry ExpectedExecuteFetch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expectedExecuteFetch = append(s.expectedExecuteFetch, entry)
}

// AddExpectedQuery adds a single query with no result.
func (s *Server) AddExpectedQuery(q string, err error) {
	s.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query:       q,
		QueryResult: &sqltypes.Result{},
		Error:       err,
	})
}

// DeleteAllEntries removes all ordered entries.
func (s *Server) DeleteAllEntries() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expectedExecuteFetch = make([]ExpectedExecuteFetch, 0)
	s.expectedExecuteFetchIndex = 0
}

// VerifyAllExecutedOrFail checks that all expected queries were actually executed.
func (s *Server) VerifyAllExecutedOrFail() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expectedExecuteFetchIndex != len(s.expectedExecuteFetch) {
		s.t.Errorf("%v: not all expected queries were executed. leftovers: %v", s.name, s.expectedExecuteFetch[s.expectedExecuteFetchIndex:])
	}
}

// GetPatternCalledNum returns how many times a pattern was matched.
func (s *Server) GetPatternCalledNum(pattern string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.patternCalled[pattern]
}

// VerifyAllPatternsUsedOrFail checks that all registered patterns were matched at least once.
func (s *Server) VerifyAllPatternsUsedOrFail() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var unused []string
	for pattern := range s.patternData {
		if s.patternCalled[pattern] == 0 {
			unused = append(unused, pattern)
		}
	}
	if len(unused) > 0 {
		s.t.Errorf("%v: not all query patterns were used. unused patterns: %v", s.name, unused)
	}
}

// SetNeverFail makes unmatched queries return empty results instead of errors.
func (s *Server) SetNeverFail(neverFail bool) {
	s.neverFail.Store(neverFail)
}

// handleQuery handles a query and returns the result.
// This is called by the handler.
func (s *Server) handleQuery(q string) (*sqltypes.Result, error) {
	if s.orderMatters.Load() {
		return s.handleQueryOrdered(q)
	}

	key := strings.ToLower(q)
	s.mu.Lock()
	s.queryCalled[key]++
	s.querylog = append(s.querylog, key)

	// Check if we should reject it.
	if err, ok := s.rejectedData[key]; ok {
		s.mu.Unlock()
		return nil, err
	}

	// Check explicit queries from AddQuery().
	result, ok := s.data[key]
	if ok {
		s.mu.Unlock()
		return result, nil
	}

	// Check query patterns from AddQueryPattern().
	for _, pat := range s.patternData {
		if pat.expr.MatchString(q) {
			s.patternCalled[pat.queryPattern]++
			userCallback, ok := s.queryPatternUserCallback[pat.expr]
			s.mu.Unlock()
			if ok {
				userCallback(q)
			}
			if pat.err != "" {
				return nil, errors.New(pat.err)
			}
			return pat.result, nil
		}
	}

	s.mu.Unlock()

	if s.neverFail.Load() {
		return &sqltypes.Result{CommandTag: "SELECT 0"}, nil
	}

	// Nothing matched.
	return nil, fmt.Errorf("fakepgserver: query '%s' is not supported on %v", q, s.name)
}

func (s *Server) handleQueryOrdered(q string) (*sqltypes.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := s.expectedExecuteFetchIndex

	if index >= len(s.expectedExecuteFetch) {
		if s.neverFail.Load() {
			return &sqltypes.Result{CommandTag: "SELECT 0"}, nil
		}
		s.t.Errorf("%v: got unexpected out of bound fetch: %v >= %v (%s)", s.name, index, len(s.expectedExecuteFetch), q)
		return nil, errors.New("unexpected out of bound fetch")
	}

	entry := s.expectedExecuteFetch[index]
	expected := entry.Query

	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(q, expected[0:len(expected)-1]) {
			if s.neverFail.Load() {
				return &sqltypes.Result{CommandTag: "SELECT 0"}, nil
			}
			s.t.Errorf("%v: got unexpected query start (index=%v): %v != %v", s.name, index, q, expected)
			return nil, errors.New("unexpected query")
		}
	} else {
		if q != expected {
			if s.neverFail.Load() {
				return &sqltypes.Result{CommandTag: "SELECT 0"}, nil
			}
			s.t.Errorf("%v: got unexpected query (index=%v): %v != %v", s.name, index, q, expected)
			return nil, errors.New("unexpected query")
		}
	}

	s.expectedExecuteFetchIndex++
	s.t.Logf("ExecuteFetch: %v: %v", s.name, q)

	if entry.Error != nil {
		return nil, entry.Error
	}

	return entry.QueryResult, nil
}

// MakeResult creates a simple sqltypes.Result from column names and row values.
// This is a convenience function for tests. All values are converted to text format.
func MakeResult(columns []string, rows [][]any) *sqltypes.Result {
	fields := make([]*query.Field, len(columns))
	for i, col := range columns {
		fields[i] = &query.Field{
			Name:         col,
			DataTypeOid:  25, // TEXT type OID
			DataTypeSize: -1, // Variable length
		}
	}

	sqlRows := make([]*sqltypes.Row, len(rows))
	for i, row := range rows {
		values := make([]sqltypes.Value, len(row))
		for j, val := range row {
			if val == nil {
				values[j] = nil // NULL
			} else {
				values[j] = fmt.Appendf(nil, "%v", val)
			}
		}
		sqlRows[i] = &sqltypes.Row{Values: values}
	}

	return &sqltypes.Result{
		Fields:     fields,
		Rows:       sqlRows,
		CommandTag: fmt.Sprintf("SELECT %d", len(rows)),
	}
}
