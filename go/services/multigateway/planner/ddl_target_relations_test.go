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
)

// TestPlan_DDLTargetRelations verifies that planDefault stamps
// ExecInfo.DDLTargetRelations for DDL that can change a table's result
// shape, so the multipooler can invalidate cached prepared statements
// against those tables once the statement executes. Statements that don't
// affect an existing prepared statement's result shape carry nil.
func TestPlan_DDLTargetRelations(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "alter table alter column type", sql: "ALTER TABLE orders ALTER COLUMN amount TYPE numeric", want: []string{"orders"}},
		{name: "drop table", sql: "DROP TABLE orders", want: []string{"orders"}},
		{name: "drop table multiple", sql: "DROP TABLE orders, public.customers", want: []string{"orders", "public.customers"}},
		{name: "rename table", sql: "ALTER TABLE orders RENAME TO purchase_orders", want: []string{"orders"}},
		{name: "rename column", sql: "ALTER TABLE orders RENAME COLUMN amount TO total", want: []string{"orders"}},
		{name: "alter index unaffected", sql: "ALTER INDEX orders_pkey RENAME TO orders_pk", want: nil},
		{name: "create table unaffected", sql: "CREATE TABLE orders (id int)", want: nil},
		{name: "plain select unaffected", sql: "SELECT * FROM orders", want: nil},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPlanner("default", logger, nil)
			conn := server.NewTestConn(&bytes.Buffer{}).Conn

			plan, err := p.Plan(tt.sql, parseOne(t, tt.sql), conn, PlanOptions{})
			require.NoError(t, err)
			require.NotNil(t, plan)

			assert.ElementsMatch(t, tt.want, plan.ExecInfo.DDLTargetRelations, "DDL target relations for %q", tt.sql)
		})
	}
}
