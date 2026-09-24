// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errDDL = errors.New("ddl boom")

// scriptedDDLConn injects failures into a ddlrepl helper: exec/execArgs fail
// when their SQL contains the matching substring, queryCount fails when its SQL
// contains failCountOn, and otherwise queryCount reports counts(sql).
type scriptedDDLConn struct {
	failExecOn     string
	failExecArgsOn string
	failCountOn    string
	counts         func(sql string) int64
}

func (c scriptedDDLConn) exec(_ context.Context, sql string) error {
	if c.failExecOn != "" && strings.Contains(sql, c.failExecOn) {
		return errDDL
	}
	return nil
}

func (c scriptedDDLConn) execArgs(_ context.Context, sql string, _ ...any) error {
	if c.failExecArgsOn != "" && strings.Contains(sql, c.failExecArgsOn) {
		return errDDL
	}
	return nil
}

func (c scriptedDDLConn) queryCount(_ context.Context, sql string, _ ...any) (int64, error) {
	if c.failCountOn != "" && strings.Contains(sql, c.failCountOn) {
		return 0, errDDL
	}
	if c.counts != nil {
		return c.counts(sql), nil
	}
	return 0, nil
}

func ctx() context.Context { return context.Background() }

func TestSetupDDLCaptureErrors(t *testing.T) {
	// A shared-object exec fails.
	err := setupDDLCapture(ctx(), scriptedDDLConn{failExecOn: "CREATE SCHEMA"}, "m1", nil)
	assert.ErrorContains(t, err, "set up ddl capture")

	// The per-table registration INSERT fails.
	err = setupDDLCapture(ctx(), scriptedDDLConn{failExecArgsOn: "ddl_capture_tables"}, "m1", []string{"public.orders"})
	assert.ErrorContains(t, err, "register ddl capture table")
}

func TestArmDDLCaptureErrors(t *testing.T) {
	// The trigger-existence probe fails.
	err := armDDLCapture(ctx(), scriptedDDLConn{failCountOn: "pg_event_trigger"})
	assert.ErrorContains(t, err, "check ddl capture trigger")

	// The trigger is absent and creating it fails.
	err = armDDLCapture(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "CREATE",
	})
	assert.ErrorContains(t, err, "arm ddl capture trigger")
}

func TestTeardownDDLCaptureErrors(t *testing.T) {
	// Deregister fails.
	err := teardownDDLCapture(ctx(), scriptedDDLConn{failExecArgsOn: "DELETE FROM multigres.ddl_capture_tables"}, "m1")
	assert.ErrorContains(t, err, "deregister ddl capture tables")

	// ddl_log clear fails.
	err = teardownDDLCapture(ctx(), scriptedDDLConn{failExecArgsOn: "DELETE FROM multigres.ddl_log"}, "m1")
	assert.ErrorContains(t, err, "clear ddl_log rows")

	// The remaining-count probe fails.
	err = teardownDDLCapture(ctx(), scriptedDDLConn{failCountOn: "count(*)"}, "m1")
	assert.ErrorContains(t, err, "count ddl capture tables")

	// No registrations remain, and dropping a shared object fails.
	err = teardownDDLCapture(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "DROP",
	}, "m1")
	assert.ErrorContains(t, err, "drop shared ddl capture objects")
}

func TestSetupDDLApplyErrors(t *testing.T) {
	// A shared-object exec fails.
	err := setupDDLApply(ctx(), scriptedDDLConn{failExecOn: "CREATE SCHEMA"}, "m1")
	assert.ErrorContains(t, err, "set up ddl apply")

	// The apply-trigger probe fails.
	err = setupDDLApply(ctx(), scriptedDDLConn{failCountOn: "mg_apply_ddl"}, "m1")
	assert.ErrorContains(t, err, "check ddl apply trigger")

	// The trigger is absent and creating it fails.
	err = setupDDLApply(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "CREATE TRIGGER",
	}, "m1")
	assert.ErrorContains(t, err, "set up ddl apply trigger")

	// Registering the migration fails.
	err = setupDDLApply(ctx(), scriptedDDLConn{
		counts:         func(string) int64 { return 1 }, // trigger already present
		failExecArgsOn: "ddl_apply",
	}, "m1")
	assert.ErrorContains(t, err, "register ddl apply")
}

func TestTeardownDDLApplyErrors(t *testing.T) {
	// Deregister fails.
	err := teardownDDLApply(ctx(), scriptedDDLConn{failExecArgsOn: "DELETE FROM multigres.ddl_apply"}, "m1")
	assert.ErrorContains(t, err, "deregister ddl apply")

	// ddl_log clear fails.
	err = teardownDDLApply(ctx(), scriptedDDLConn{failExecArgsOn: "DELETE FROM multigres.ddl_log"}, "m1")
	assert.ErrorContains(t, err, "clear ddl_log rows")

	// The apply-refs count fails.
	err = teardownDDLApply(ctx(), scriptedDDLConn{failCountOn: "ddl_apply"}, "m1")
	assert.ErrorContains(t, err, "count ddl apply refs")

	// Apply refs remain: early return, no drops.
	require.NoError(t, teardownDDLApply(ctx(), scriptedDDLConn{
		counts: func(sql string) int64 {
			if strings.Contains(sql, "ddl_apply") {
				return 2
			}
			return 0
		},
	}, "m1"))

	// No apply refs remain, and dropping the refs table fails.
	err = teardownDDLApply(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "ddl_apply",
	}, "m1")
	assert.ErrorContains(t, err, "drop ddl apply refs")

	// The capture-table existence probe fails.
	err = teardownDDLApply(ctx(), scriptedDDLConn{failCountOn: "ddl_capture_tables"}, "m1")
	assert.ErrorContains(t, err, "check ddl_capture_tables")

	// This server also publishes (capture table present, rows remain): drop the
	// apply trigger rather than ddl_log, and that drop fails.
	err = teardownDDLApply(ctx(), scriptedDDLConn{
		counts: func(sql string) int64 {
			switch {
			case strings.Contains(sql, "ddl_apply"):
				return 0 // no apply refs remain
			case strings.Contains(sql, "pg_class"):
				return 1 // ddl_capture_tables exists
			case strings.Contains(sql, "ddl_capture_tables"):
				return 3 // still publishing
			}
			return 0
		},
		failExecOn: dropApplyTriggerSQL,
	}, "m1")
	assert.ErrorContains(t, err, "drop ddl apply trigger")

	// Subscribe-only server (no capture table): drop ddl_log, and that fails.
	err = teardownDDLApply(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: dropDDLLogTableSQL,
	}, "m1")
	assert.ErrorContains(t, err, "drop ddl_log")

	// Finally, dropping the apply function fails after everything else succeeds.
	err = teardownDDLApply(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: dropApplyFunctionSQL,
	}, "m1")
	assert.ErrorContains(t, err, "drop ddl apply function")
}
