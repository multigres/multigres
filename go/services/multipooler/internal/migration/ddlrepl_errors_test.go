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
)

var errDDL = errors.New("ddl boom")

// scriptedDDLConn injects failures into a ddlrepl helper: exec/execArgs fail
// when their SQL contains the matching substring, queryCount fails when its SQL
// contains failCountOn, and otherwise queryCount reports counts(sql). execErr
// overrides the error a matched exec/execArgs returns (default errDDL) — used
// to simulate a specific Postgres error (e.g. an "already exists" race) rather
// than a generic failure.
type scriptedDDLConn struct {
	failExecOn     string
	failExecArgsOn string
	failCountOn    string
	counts         func(sql string) int64
	execErr        error
}

func (c scriptedDDLConn) err() error {
	if c.execErr != nil {
		return c.execErr
	}
	return errDDL
}

func (c scriptedDDLConn) exec(_ context.Context, sql string) error {
	if c.failExecOn != "" && strings.Contains(sql, c.failExecOn) {
		return c.err()
	}
	return nil
}

func (c scriptedDDLConn) execArgs(_ context.Context, sql string, _ ...any) error {
	if c.failExecArgsOn != "" && strings.Contains(sql, c.failExecArgsOn) {
		return c.err()
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
	err := setupDDLCapture(ctx(), scriptedDDLConn{failExecOn: "CREATE SCHEMA"}, nil)
	assert.ErrorContains(t, err, "set up ddl capture")

	// The per-table registration INSERT fails.
	err = setupDDLCapture(ctx(), scriptedDDLConn{failExecArgsOn: "ddl_capture_tables"}, []string{"public.orders"})
	assert.ErrorContains(t, err, "register ddl capture table")
}

func TestArmDDLCaptureErrors(t *testing.T) {
	// The trigger-existence probe fails.
	err := armDDLCapture(ctx(), scriptedDDLConn{failCountOn: "pg_event_trigger"})
	assert.ErrorContains(t, err, "check ddl capture trigger")

	// The trigger is absent and creating it fails with something other than an
	// "already exists" race.
	err = armDDLCapture(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "CREATE",
	})
	assert.ErrorContains(t, err, "arm ddl capture trigger")
}

func TestTeardownDDLCaptureErrors(t *testing.T) {
	err := teardownDDLCapture(ctx(), scriptedDDLConn{failExecOn: "mg_capture_ddl"})
	assert.ErrorContains(t, err, "tear down ddl capture")
}

func TestSetupDDLApplyErrors(t *testing.T) {
	// A shared-object exec fails.
	err := setupDDLApply(ctx(), scriptedDDLConn{failExecOn: "CREATE SCHEMA"})
	assert.ErrorContains(t, err, "set up ddl apply")

	// The apply-trigger probe fails.
	err = setupDDLApply(ctx(), scriptedDDLConn{failCountOn: "mg_apply_ddl"})
	assert.ErrorContains(t, err, "check ddl apply trigger")

	// The trigger is absent and creating it fails with something other than an
	// "already exists" race.
	err = setupDDLApply(ctx(), scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "CREATE TRIGGER",
	})
	assert.ErrorContains(t, err, "set up ddl apply trigger")
}

func TestTeardownDDLApplyErrors(t *testing.T) {
	err := teardownDDLApply(ctx(), scriptedDDLConn{failExecOn: "apply_ddl()"})
	assert.ErrorContains(t, err, "tear down ddl apply")
}
