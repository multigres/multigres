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

package utils

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RequirePgError asserts that err is a *pgconn.PgError carrying the given
// SQLSTATE code, and returns it so callers can make further assertions (e.g. on
// Message or Severity). It fails the test immediately if err is nil or not a
// PgError.
func RequirePgError(t *testing.T, err error, code string) *pgconn.PgError {
	t.Helper()
	var pgErr *pgconn.PgError
	require.Truef(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T: %v", err, err)
	assert.Equalf(t, code, pgErr.Code, "unexpected SQLSTATE (message: %s)", pgErr.Message)
	return pgErr
}
