// Copyright 2026 Supabase, Inc.
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

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/pb/query"
)

func TestWithStatementTimeout(t *testing.T) {
	ctx := context.Background()

	for _, opts := range []*query.ExecuteOptions{nil, {}, {StatementTimeout: durationpb.New(0)}} {
		got, cancel := withStatementTimeout(ctx, opts)
		cancel()
		_, hasDeadline := got.Deadline()
		require.False(t, hasDeadline, "no budget must mean no deadline: %v", opts)
	}

	got, cancel := withStatementTimeout(ctx, &query.ExecuteOptions{StatementTimeout: durationpb.New(50 * time.Millisecond)})
	defer cancel()
	deadline, hasDeadline := got.Deadline()
	require.True(t, hasDeadline)
	require.WithinDuration(t, time.Now().Add(50*time.Millisecond), deadline, 20*time.Millisecond)
	<-got.Done()
	require.ErrorIs(t, got.Err(), context.DeadlineExceeded)
}
