// Copyright 2026 Supabase, Inc.
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

package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryContextError_NilError(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, queryContextError(ctx, nil))
}

func TestQueryContextError_CancelTakesPriority(t *testing.T) {
	// Simulate BeginQueryCancel + CancelQuery: context canceled with errQueryCanceled cause.
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errQueryCanceled)

	// Even though we pass a different error, cancel cause wins.
	err := queryContextError(ctx, errors.New("some gRPC error"))
	assert.Equal(t, errQueryCanceled, err)
}

func TestQueryContextError_DeadlineExceeded(t *testing.T) {
	ctx := context.Background()
	err := queryContextError(ctx, context.DeadlineExceeded)
	assert.Equal(t, errStatementTimeout, err)
}

func TestQueryContextError_WrappedDeadlineExceeded(t *testing.T) {
	ctx := context.Background()
	wrapped := fmt.Errorf("query failed: %w", context.DeadlineExceeded)
	err := queryContextError(ctx, wrapped)
	assert.Equal(t, errStatementTimeout, err)
}

func TestQueryContextError_UnrelatedError(t *testing.T) {
	ctx := context.Background()
	original := errors.New("connection refused")
	err := queryContextError(ctx, original)
	assert.Equal(t, original, err)
}

func TestQueryContextError_CancelOverDeadline(t *testing.T) {
	// When both cancel cause and DeadlineExceeded are present, cancel wins.
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errQueryCanceled)

	err := queryContextError(ctx, context.DeadlineExceeded)
	assert.Equal(t, errQueryCanceled, err)
}
