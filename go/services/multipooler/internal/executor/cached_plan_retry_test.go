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

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/query"
)

// cachedPlanError builds a synthetic SQLSTATE 0A000 diagnostic matching what
// PostgreSQL sends for a stale prepared statement, for tests that need to
// force the retry path without actually orchestrating a real DDL race.
func cachedPlanError() error {
	return mterrors.NewPgError("ERROR", mterrors.PgSSFeatureNotSupported, "cached plan must not change result type", "")
}

// TestBindExecuteWithCachedPlanRetry_HealsOnCachedPlanError verifies the
// retry helper closes the stale statement, re-Parses (via ensurePrepared),
// and retries bindExecute exactly once on a 0A000 error.
func TestBindExecuteWithCachedPlanRetry_HealsOnCachedPlanError(t *testing.T) {
	e, _, rconn := newDeadReservedConnTestExecutor(t)
	ctx := context.Background()
	conn := rconn.Conn()
	stmt := &query.PreparedStatement{Query: "SELECT 1"}

	canonicalName, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)

	calls := 0
	bindExecute := func(name string) (bool, error) {
		calls++
		if calls == 1 {
			return false, cachedPlanError()
		}
		assert.NotEmpty(t, name, "retry must be called with a canonical name")
		return true, nil
	}

	completed, err := cachedPlanRetry(ctx, e, conn, stmt, canonicalName, bindExecute)
	require.NoError(t, err)
	assert.True(t, completed)
	assert.Equal(t, 2, calls, "must retry exactly once after healing")
}

// TestBindExecuteWithCachedPlanRetry_DoesNotRetryOtherErrors verifies a
// non-cached-plan error is returned as-is, with no retry attempted.
func TestBindExecuteWithCachedPlanRetry_DoesNotRetryOtherErrors(t *testing.T) {
	e, _, rconn := newDeadReservedConnTestExecutor(t)
	ctx := context.Background()
	conn := rconn.Conn()
	stmt := &query.PreparedStatement{Query: "SELECT 1"}

	canonicalName, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)

	calls := 0
	unrelatedErr := errors.New("connection reset")
	bindExecute := func(name string) (bool, error) {
		calls++
		return false, unrelatedErr
	}

	_, err = cachedPlanRetry(ctx, e, conn, stmt, canonicalName, bindExecute)
	require.ErrorIs(t, err, unrelatedErr)
	assert.Equal(t, 1, calls, "must not retry an error that isn't a stale cached plan")
}

// TestDescribeWithCachedPlanRetry_HealsOnCachedPlanError is describe's
// counterpart to TestBindExecuteWithCachedPlanRetry_HealsOnCachedPlanError:
// PostgreSQL revalidates a prepared statement's result shape on Describe
// too, not just Bind/Execute (exec_describe_statement_message ->
// CachedPlanGetTargetList -> RevalidateCachedQuery), so this must heal the
// same way.
func TestDescribeWithCachedPlanRetry_HealsOnCachedPlanError(t *testing.T) {
	e, _, rconn := newDeadReservedConnTestExecutor(t)
	ctx := context.Background()
	conn := rconn.Conn()
	stmt := &query.PreparedStatement{Query: "SELECT 1"}

	canonicalName, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)

	calls := 0
	wantDesc := &query.StatementDescription{HasFields: true}
	describe := func(name string) (*query.StatementDescription, error) {
		calls++
		if calls == 1 {
			return nil, cachedPlanError()
		}
		assert.NotEmpty(t, name, "retry must be called with a canonical name")
		return wantDesc, nil
	}

	desc, err := cachedPlanRetry(ctx, e, conn, stmt, canonicalName, describe)
	require.NoError(t, err)
	assert.Same(t, wantDesc, desc)
	assert.Equal(t, 2, calls, "must retry exactly once after healing")
}

// TestDescribeWithCachedPlanRetry_DoesNotRetryOtherErrors verifies a
// non-cached-plan error is returned as-is, with no retry attempted.
func TestDescribeWithCachedPlanRetry_DoesNotRetryOtherErrors(t *testing.T) {
	e, _, rconn := newDeadReservedConnTestExecutor(t)
	ctx := context.Background()
	conn := rconn.Conn()
	stmt := &query.PreparedStatement{Query: "SELECT 1"}

	canonicalName, err := e.ensurePrepared(ctx, conn, stmt)
	require.NoError(t, err)

	calls := 0
	unrelatedErr := errors.New("connection reset")
	describe := func(name string) (*query.StatementDescription, error) {
		calls++
		return nil, unrelatedErr
	}

	_, err = cachedPlanRetry(ctx, e, conn, stmt, canonicalName, describe)
	require.ErrorIs(t, err, unrelatedErr)
	assert.Equal(t, 1, calls, "must not retry an error that isn't a stale cached plan")
}
