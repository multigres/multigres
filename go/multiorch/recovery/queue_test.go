// Copyright 2025 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package recovery

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/viperutil"
)

func TestQueue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	reg := viperutil.NewRegistry()
	cfg := config.NewConfig(reg)
	q := NewQueue(logger, cfg)
	require.Zero(t, q.QueueLen())

	// Push
	q.Push(t.Name())
	require.Equal(t, 1, q.QueueLen())
	_, found := q.enqueued[t.Name()]
	require.True(t, found)

	// Push duplicate
	q.Push(t.Name())
	require.Equal(t, 1, q.QueueLen())

	// Consume
	key, release, ok := q.Consume(context.Background())
	require.True(t, ok)
	require.Equal(t, t.Name(), key)
	require.Equal(t, 1, q.QueueLen())
	_, found = q.enqueued[t.Name()]
	require.True(t, found)

	// Release
	release()
	require.Zero(t, q.QueueLen())
	_, found = q.enqueued[t.Name()]
	require.False(t, found)
}

type testQueue interface {
	QueueLen() int
	Push(string)
	Consume(context.Context) (string, func(), bool)
}

func BenchmarkQueues(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	reg := viperutil.NewRegistry()
	cfg := config.NewConfig(reg)

	tests := []struct {
		name  string
		queue testQueue
	}{
		{"Current", NewQueue(logger, cfg)},
	}
	for _, test := range tests {
		q := test.queue
		b.Run(test.name, func(b *testing.B) {
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				for i := range 1000 {
					q.Push(b.Name() + strconv.Itoa(i))
				}
				q.QueueLen()
				for range 1000 {
					_, release, _ := q.Consume(ctx)
					release()
				}
			}
		})
	}
}
