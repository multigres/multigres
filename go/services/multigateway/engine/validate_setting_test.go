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

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
)

// setConfigResultRow builds the single-row, single-column result
// set_config(...) returns, as mockIExecute's StreamExecute would hand back to
// ValidateSetting's capture callback.
func setConfigResultRow(value string) *sqltypes.Result {
	return &sqltypes.Result{
		Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte(value)}},
		},
	}
}

func TestValidateSetting_ValidateSQL(t *testing.T) {
	tests := []struct {
		name     string
		varName  string
		varValue string
		want     string
	}{
		{
			name:     "plain",
			varName:  "extra_float_digits",
			varValue: "100",
			want:     "SELECT pg_catalog.set_config('extra_float_digits', '100', TRUE)",
		},
		{
			name:     "is_local true so the validation reverts after the statement",
			varName:  "search_path",
			varValue: "myschema",
			want:     "SELECT pg_catalog.set_config('search_path', 'myschema', TRUE)",
		},
		{
			name:     "single quote in value is doubled (no breakout)",
			varName:  "search_path",
			varValue: "a'); DROP TABLE t; --",
			want:     "SELECT pg_catalog.set_config('search_path', 'a''); DROP TABLE t; --', TRUE)",
		},
		{
			name:     "single quote in name is doubled too",
			varName:  "mtinject.it's",
			varValue: "value",
			want:     "SELECT pg_catalog.set_config('mtinject.it''s', 'value', TRUE)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewValidateSetting("default", "0-inf", tt.varName, tt.varValue, "SET ...")
			assert.Equal(t, tt.want, v.validateSQL())
		})
	}
}

// TestValidateSetting_PropagatesError confirms the primitive surfaces the
// backend's error (e.g. an out-of-range value) up to the caller — this is what
// makes a bad SET fail at SET time. The Sequence relies on this to stop before
// the tracking step.
func TestValidateSetting_PropagatesError(t *testing.T) {
	wantErr := errors.New("100 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)")
	mockExec := &mockIExecute{streamExecuteErr: wantErr}

	v := NewValidateSetting("default", "0-inf", "extra_float_digits", "100", "SET extra_float_digits = 100")
	err := v.StreamExecute(context.Background(), mockExec, nil, nil, nil, PlanExecInfo{}, nil)
	require.ErrorIs(t, err, wantErr, "validation error must propagate so the SET fails at SET time")
}

// TestValidateSetting_SuccessReturnsNil confirms a successful validation returns
// nil (the trailing ApplySessionState then tracks the setting and emits the SET
// tag). Row discarding itself is exercised end-to-end in the session-settings
// e2e, where the client sees CommandComplete("SET") rather than a result row.
func TestValidateSetting_SuccessReturnsNil(t *testing.T) {
	v := NewValidateSetting("default", "0-inf", "work_mem", "64MB", "SET work_mem = '64MB'")
	err := v.StreamExecute(context.Background(), &mockIExecute{}, nil, nil, nil, PlanExecInfo{}, nil)
	require.NoError(t, err)
}

// TestValidateSetting_CapturesConfirmedValueForNonReportableGUC verifies the
// core Step 1a fix: a GUC PostgreSQL does NOT report via ParameterStatus
// (e.g. work_mem, or any planner GUC) still gets its set_config-confirmed
// value captured onto the exchange for SessionSettings to use; it must not
// be silently dropped just because it isn't one of the 8 reportable names.
// It must also NOT appear in ReportedSettings, since that map drives the
// client-facing ParameterStatus echo and non-reportable GUCs must never be
// announced there.
func TestValidateSetting_CapturesConfirmedValueForNonReportableGUC(t *testing.T) {
	mockExec := &mockIExecute{streamExecuteResult: setConfigResultRow("4MB")}
	exchange := &SequenceExchange{}

	v := NewValidateSetting("default", "0-inf", "work_mem", "4MB", "SET work_mem = '4MB'")
	err := v.StreamExecute(context.Background(), mockExec, nil, nil, nil, PlanExecInfo{Exchange: exchange}, nil)

	require.NoError(t, err)
	assert.True(t, mockExec.lastStreamKeepStructured, "must always request structured rows now, not just for reportable GUCs")
	require.True(t, exchange.HasConfirmedValue)
	assert.Equal(t, "4MB", exchange.ConfirmedValue)
	assert.Empty(t, exchange.ReportedSettings, "a non-reportable GUC must never be added to the client-facing ParameterStatus echo")
}

// TestValidateSetting_CapturesConfirmedValueAndReportsForReportableGUC
// verifies a reportable GUC gets BOTH: the confirmed value on the exchange
// (for SessionSettings, new behavior) and the ParameterStatus-display-name
// entry in ReportedSettings (for the client echo, unchanged behavior), and
// that PostgreSQL's resolved value (not the client's literal) is what's
// captured in both places.
func TestValidateSetting_CapturesConfirmedValueAndReportsForReportableGUC(t *testing.T) {
	mockExec := &mockIExecute{streamExecuteResult: setConfigResultRow("ISO, MDY")}
	exchange := &SequenceExchange{}

	v := NewValidateSetting("default", "0-inf", "datestyle", "ISO", "SET DateStyle = 'ISO'")
	err := v.StreamExecute(context.Background(), mockExec, nil, nil, nil, PlanExecInfo{Exchange: exchange}, nil)

	require.NoError(t, err)
	require.True(t, exchange.HasConfirmedValue)
	assert.Equal(t, "ISO, MDY", exchange.ConfirmedValue)
	assert.Equal(t, map[string]string{"DateStyle": "ISO, MDY"}, exchange.ReportedSettings,
		"reportable GUC must still drive the client echo, keyed by its ParameterStatus display name")
}

// TestValidateSetting_NilExchangeDoesNotPanic confirms validate is safe to
// call with no exchange (e.g. a plan built outside a Sequence).
func TestValidateSetting_NilExchangeDoesNotPanic(t *testing.T) {
	mockExec := &mockIExecute{streamExecuteResult: setConfigResultRow("4MB")}
	v := NewValidateSetting("default", "0-inf", "work_mem", "4MB", "SET work_mem = '4MB'")
	err := v.StreamExecute(context.Background(), mockExec, nil, nil, nil, PlanExecInfo{}, nil)
	require.NoError(t, err)
}
