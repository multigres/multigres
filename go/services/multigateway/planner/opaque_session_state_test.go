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

package planner

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

func TestPlan_BackendLocalSessionStateAffinity(t *testing.T) {
	tests := []struct {
		sql        string
		wantOpaque bool
		wantTemp   bool
	}{
		{sql: "SELECT setseed(0.5)", wantOpaque: true},
		{sql: "SELECT random()"},
		{sql: "SELECT current_schema()", wantTemp: true},
		{sql: "DO $$ BEGIN SET enable_bitmapscan = off; END $$", wantOpaque: true},
		{sql: "DO $$ BEGIN NULL; END $$"},
		{sql: "CALL p()", wantOpaque: true},
	}

	planner := NewPlanner("default", slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)), nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			plan, err := planner.Plan(tt.sql, parseOne(t, tt.sql), conn, PlanOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantOpaque, plan.ExecInfo.OpaqueSessionState)
			assert.Equal(t, tt.wantTemp, plan.ExecInfo.TempTable)
		})
	}
}
