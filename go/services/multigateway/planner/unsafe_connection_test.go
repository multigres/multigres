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

package planner

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

// TestPlanUnsafeConnectionSet covers the `SET multigres.unsafe_connection`
// path: `= on` plans the latch primitive, while turning it off / resetting it /
// a non-boolean value are rejected (the latch is one-way). Each case runs against
// both the current name and the deprecated multigres.direct_connection alias,
// which must be planned identically.
func TestPlanUnsafeConnectionSet(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{})

	assertErrCode := func(t *testing.T, err error, code string) {
		t.Helper()
		require.Error(t, err)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Equal(t, code, diag.Code)
	}

	// The current name and its deprecated alias must behave identically.
	for _, param := range []string{"multigres.unsafe_connection", "multigres.direct_connection"} {
		t.Run(param, func(t *testing.T) {
			t.Run("enables", func(t *testing.T) {
				sql := "SET " + param + " = on"
				plan, err := p.Plan(sql, parseOne(t, sql), conn.Conn, PlanOptions{})
				require.NoError(t, err)
				require.NotNil(t, plan)
			})

			t.Run("turning off rejected (one-way latch)", func(t *testing.T) {
				sql := "SET " + param + " = off"
				_, err := p.Plan(sql, parseOne(t, sql), conn.Conn, PlanOptions{})
				assertErrCode(t, err, mterrors.PgSSFeatureNotSupported)
			})

			t.Run("reset rejected (one-way latch)", func(t *testing.T) {
				sql := "RESET " + param
				_, err := p.Plan(sql, parseOne(t, sql), conn.Conn, PlanOptions{})
				assertErrCode(t, err, mterrors.PgSSFeatureNotSupported)
			})

			t.Run("non-boolean value rejected", func(t *testing.T) {
				sql := "SET " + param + " = maybe"
				_, err := p.Plan(sql, parseOne(t, sql), conn.Conn, PlanOptions{})
				assertErrCode(t, err, mterrors.PgSSInvalidParameterValue)
			})
		})
	}
}

// TestPlanUnsafeConnectionSuppressesRejections confirms that a statement the
// enforcing path rejects (a blocklisted expression call) is accepted when the
// connection is an unsafe connection — i.e. Plan reads the per-connection flag.
func TestPlanUnsafeConnectionSuppressesRejections(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	sql := "SELECT dblink('host=x', 'SELECT 1')"

	// Enforcing connection: rejected.
	enforcing := server.NewTestConn(&bytes.Buffer{})
	_, err := p.Plan(sql, parseOne(t, sql), enforcing.Conn, PlanOptions{})
	require.Error(t, err)

	// Unsafe connection: accepted.
	direct := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection())
	plan, err := p.Plan(sql, parseOne(t, sql), direct.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)
}
