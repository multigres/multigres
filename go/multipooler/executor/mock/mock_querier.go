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

	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/pb/query"
)

// Querier is a mock implementation of executor.InternalQuerier for testing.
type Querier struct {
	mu       sync.Mutex
	patterns []queryPattern
}

type queryPattern struct {
	pattern     *regexp.Regexp
	result      *query.QueryResult
	err         error
	callback    func(string)
	ctxCallback func(context.Context, string) // callback that receives context for blocking tests
}

// NewQuerier creates a new mock querier for testing.
func NewQuerier() *Querier {
	return &Querier{}
}

// Compile-time check that Querier implements InternalQuerier.
var _ executor.InternalQuerier = (*Querier)(nil)

// AddQueryPattern adds a query pattern with an expected result.
func (m *Querier) AddQueryPattern(pattern string, result *query.QueryResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern: regexp.MustCompile(pattern),
		result:  result,
	})
}

// AddQueryPatternWithCallback adds a query pattern with a callback.
func (m *Querier) AddQueryPatternWithCallback(pattern string, result *query.QueryResult, callback func(string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern:  regexp.MustCompile(pattern),
		result:   result,
		callback: callback,
	})
}

// AddQueryPatternWithError adds a query pattern that returns an error.
func (m *Querier) AddQueryPatternWithError(pattern string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern: regexp.MustCompile(pattern),
		err:     err,
	})
}

// AddQueryPatternWithContextCallback adds a query pattern with a context-aware callback.
// This is useful for testing blocking queries that should respond to context cancellation.
func (m *Querier) AddQueryPatternWithContextCallback(pattern string, result *query.QueryResult, callback func(context.Context, string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, queryPattern{
		pattern:     regexp.MustCompile(pattern),
		result:      result,
		ctxCallback: callback,
	})
}

// Query implements executor.InternalQuerier.
func (m *Querier) Query(ctx context.Context, queryStr string) (*query.QueryResult, error) {
	m.mu.Lock()
	var matched *queryPattern
	for i := range m.patterns {
		if m.patterns[i].pattern.MatchString(queryStr) {
			matched = &m.patterns[i]
			break
		}
	}
	m.mu.Unlock()

	if matched == nil {
		return nil, fmt.Errorf("no matching query pattern for: %s", queryStr)
	}

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

// MakeQueryResult creates a query.QueryResult from columns and rows.
func MakeQueryResult(columns []string, rows [][]any) *query.QueryResult {
	result := &query.QueryResult{
		Fields: make([]*query.Field, len(columns)),
		Rows:   make([]*query.Row, len(rows)),
	}

	for i, col := range columns {
		result.Fields[i] = &query.Field{Name: col}
	}

	for i, row := range rows {
		values := make([][]byte, len(row))
		for j, val := range row {
			if val == nil {
				values[j] = nil
			} else {
				values[j] = fmt.Appendf(nil, "%v", val)
			}
		}
		result.Rows[i] = &query.Row{Values: values}
	}

	return result
}
