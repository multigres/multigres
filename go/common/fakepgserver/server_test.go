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

package fakepgserver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
)

func TestServer_BasicQuery(t *testing.T) {
	s := New(t)
	defer s.Close()

	s.AddQuery("SELECT 1", MakeResult(
		[]string{"?column?"},
		[][]any{{int64(1)}},
	))

	// Connect to the server.
	conn, err := client.Connect(context.Background(), s.ClientConfig())
	require.NoError(t, err)
	defer conn.Close()

	// Execute query.
	results, err := conn.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)

	result := results[0]
	require.Len(t, result.Fields, 1)
	assert.Equal(t, "?column?", result.Fields[0].Name)

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "1", string(result.Rows[0].Values[0]))
}

func TestServer_QueryPattern(t *testing.T) {
	s := New(t)
	defer s.Close()

	s.AddQueryPattern(`SELECT \* FROM users WHERE id = \d+`, MakeResult(
		[]string{"id", "name"},
		[][]any{{int64(1), "Alice"}},
	))

	conn, err := client.Connect(context.Background(), s.ClientConfig())
	require.NoError(t, err)
	defer conn.Close()

	// Should match pattern.
	results, err := conn.Query(context.Background(), "SELECT * FROM users WHERE id = 123")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0].Rows, 1)
}

func TestServer_RejectedQuery(t *testing.T) {
	s := New(t)
	defer s.Close()

	s.AddRejectedQuery("SELECT * FROM forbidden", errors.New("access denied"))

	conn, err := client.Connect(context.Background(), s.ClientConfig())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Query(context.Background(), "SELECT * FROM forbidden")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access denied")
}

func TestServer_QueryCallCount(t *testing.T) {
	s := New(t)
	defer s.Close()

	s.AddQuery("SELECT 1", MakeResult(
		[]string{"?column?"},
		[][]any{{int64(1)}},
	))

	conn, err := client.Connect(context.Background(), s.ClientConfig())
	require.NoError(t, err)
	defer conn.Close()

	assert.Equal(t, 0, s.GetQueryCalledNum("SELECT 1"))

	_, err = conn.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, 1, s.GetQueryCalledNum("SELECT 1"))

	_, err = conn.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, 2, s.GetQueryCalledNum("SELECT 1"))
}

func TestServer_NeverFail(t *testing.T) {
	s := New(t)
	defer s.Close()
	s.SetNeverFail(true)

	conn, err := client.Connect(context.Background(), s.ClientConfig())
	require.NoError(t, err)
	defer conn.Close()

	// Unregistered query should return empty result instead of error.
	results, err := conn.Query(context.Background(), "SELECT * FROM unknown_table")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0].Rows, 0)
}

func TestServer_OrderedQueries(t *testing.T) {
	s := New(t)
	defer s.Close()
	s.OrderMatters()

	s.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: "SELECT 1",
		QueryResult: MakeResult(
			[]string{"?column?"},
			[][]any{{int64(1)}},
		),
	})
	s.AddExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: "SELECT 2",
		QueryResult: MakeResult(
			[]string{"?column?"},
			[][]any{{int64(2)}},
		),
	})

	conn, err := client.Connect(context.Background(), s.ClientConfig())
	require.NoError(t, err)
	defer conn.Close()

	// Execute in order.
	results, err := conn.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)

	results, err = conn.Query(context.Background(), "SELECT 2")
	require.NoError(t, err)
	require.Len(t, results, 1)

	s.VerifyAllExecutedOrFail()
}
