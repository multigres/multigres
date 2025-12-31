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

// Package mock provides mock implementations for testing.
package mock

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/pb/query"
)

// QueryService is a mock implementation of executor.InternalQueryService for testing.
type QueryService struct {
	mu       sync.Mutex
	patterns []queryPattern
}

type queryPattern struct {
	pattern     *regexp.Regexp
	result      *sqltypes.Result
	err         error
	callback    func(string)
	ctxCallback func(context.Context, string) // callback that receives context for blocking tests
	consumeOnce bool                          // if true, pattern is removed after first match
}

// NewQueryService creates a new mock query service for testing.
func NewQueryService() *QueryService {
	return &QueryService{}
}

// Compile-time check that QueryService implements InternalQueryService.
var _ executor.InternalQueryService = (*QueryService)(nil)

// AddQueryPattern adds a query pattern with an expected result.
func (m *QueryService) AddQueryPattern(pattern string, result *sqltypes.Result) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern: regexp.MustCompile(pattern),
		result:  result,
	})
}

// AddQueryPatternWithCallback adds a query pattern with a callback.
func (m *QueryService) AddQueryPatternWithCallback(pattern string, result *sqltypes.Result, callback func(string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern:  regexp.MustCompile(pattern),
		result:   result,
		callback: callback,
	})
}

// AddQueryPatternWithError adds a query pattern that returns an error.
func (m *QueryService) AddQueryPatternWithError(pattern string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern: regexp.MustCompile(pattern),
		err:     err,
	})
}

// AddQueryPatternWithContextCallback adds a query pattern with a context-aware callback.
// This is useful for testing blocking queries that should respond to context cancellation.
func (m *QueryService) AddQueryPatternWithContextCallback(pattern string, result *sqltypes.Result, callback func(context.Context, string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern:     regexp.MustCompile(pattern),
		result:      result,
		ctxCallback: callback,
	})
}

// AddQueryPatternOnce adds a query pattern that is consumed after the first match.
// This is useful when you need different results for subsequent calls to the same query.
func (m *QueryService) AddQueryPatternOnce(pattern string, result *sqltypes.Result) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern:     regexp.MustCompile(pattern),
		result:      result,
		consumeOnce: true,
	})
}

// AddQueryPatternOnceWithError adds a query pattern that returns an error and is consumed after the first match.
func (m *QueryService) AddQueryPatternOnceWithError(pattern string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern:     regexp.MustCompile(pattern),
		err:         err,
		consumeOnce: true,
	})
}

// ExpectationsWereMet returns an error if any consumeOnce patterns were not matched.
// This is useful for verifying that all expected queries were executed.
func (m *QueryService) ExpectationsWereMet() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var unmet []string
	for _, p := range m.patterns {
		if p.consumeOnce {
			unmet = append(unmet, p.pattern.String())
		}
	}
	if len(unmet) > 0 {
		return fmt.Errorf("expected queries were not executed: %v", unmet)
	}
	return nil
}

// Query implements executor.InternalQueryService.
func (m *QueryService) Query(ctx context.Context, queryStr string) (*sqltypes.Result, error) {
	m.mu.Lock()
	matchedIndex := -1
	for i := range m.patterns {
		if m.patterns[i].pattern.MatchString(queryStr) {
			matchedIndex = i
			break
		}
	}

	if matchedIndex == -1 {
		m.mu.Unlock()
		return nil, fmt.Errorf("no matching query pattern for: %s", queryStr)
	}

	// Copy the matched pattern's data before potentially modifying the slice
	matched := m.patterns[matchedIndex]

	// Remove the pattern if it should only be used once
	if matched.consumeOnce {
		m.patterns = append(m.patterns[:matchedIndex], m.patterns[matchedIndex+1:]...)
	}
	m.mu.Unlock()

	if matched.callback != nil {
		matched.callback(queryStr)
	}
	if matched.ctxCallback != nil {
		matched.ctxCallback(ctx, queryStr)
	}
	if matched.err != nil {
		return nil, matched.err
	}
	return matched.result, nil
}

// QueryArgs implements executor.InternalQueryService.
// For the mock, arguments are ignored and matching is done solely on the query string.
func (m *QueryService) QueryArgs(ctx context.Context, queryStr string, args ...any) (*sqltypes.Result, error) {
	return m.Query(ctx, queryStr)
}

// MakeQueryResult creates a sqltypes.Result from columns and rows.
func MakeQueryResult(columns []string, rows [][]any) *sqltypes.Result {
	result := &sqltypes.Result{
		Fields: make([]*query.Field, len(columns)),
		Rows:   make([]*sqltypes.Row, len(rows)),
	}

	for i, col := range columns {
		result.Fields[i] = &query.Field{Name: col}
	}

	for i, row := range rows {
		values := make([]sqltypes.Value, len(row))
		for j, val := range row {
			if val == nil {
				values[j] = nil
			} else {
				values[j] = fmt.Appendf(nil, "%v", val)
			}
		}
		result.Rows[i] = &sqltypes.Row{Values: values}
	}

	return result
}
