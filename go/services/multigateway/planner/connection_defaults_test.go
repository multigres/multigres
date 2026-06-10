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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func TestStatementChangesConnectionDefaults(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{"alter database set value", "ALTER DATABASE postgres SET search_path = public, topology", true},
		{"alter database reset", "ALTER DATABASE postgres RESET search_path", true},
		{"alter role set value", "ALTER ROLE app SET search_path = public", true},
		{"alter role in database set", "ALTER ROLE app IN DATABASE postgres SET work_mem = '64MB'", true},
		{"alter role reset", "ALTER ROLE app RESET search_path", true},
		{"create extension on allowlist", "CREATE EXTENSION postgis_topology", true},
		{"create extension on allowlist if not exists", "CREATE EXTENSION IF NOT EXISTS postgis_topology", true},
		{"create extension off allowlist", "CREATE EXTENSION postgis", false},
		{"create extension hstore", "CREATE EXTENSION hstore", false},
		{"plain select", "SELECT 1", false},
		{"session set is not a per-db default", "SET search_path = public", false},
		{"alter table is unrelated", "ALTER TABLE t ADD COLUMN c int", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := statementChangesConnectionDefaults(parseOne(t, tt.sql))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPlan_FlagsConnectionDefaultsOnRoute verifies the planner surfaces the
// detection on the Route primitive so ScatterConn can mark the request.
func TestPlan_FlagsConnectionDefaultsOnRoute(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{"alter database set", "ALTER DATABASE postgres SET search_path = public, topology", true},
		{"alter role set", "ALTER ROLE app SET search_path = public", true},
		{"create extension postgis_topology", "CREATE EXTENSION postgis_topology", true},
		{"create extension postgis (off allowlist)", "CREATE EXTENSION postgis", false},
		{"plain select routes without the flag", "SELECT 1", false},
	}
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPlanner("default", logger, nil)
			conn := server.NewTestConn(&bytes.Buffer{}).Conn

			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn)
			require.NoError(t, err)
			require.NotNil(t, plan)

			route, ok := plan.Primitive.(*engine.Route)
			require.True(t, ok, "expected Route, got %T", plan.Primitive)
			assert.Equal(t, tt.want, route.InvalidatesConnectionDefaults)
		})
	}
}
