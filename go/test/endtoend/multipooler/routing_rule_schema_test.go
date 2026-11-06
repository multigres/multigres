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

package multipooler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestCatalogSchema_RoutingRuleConstraints checks the constraints that
// multigres.routing_rule declares itself: the scope pins which source and
// target columns are set, and every id references its catalog row. Everything
// else about a rule is validated by the pooler before it writes one. Each case
// runs in a transaction that is rolled back, so the shared catalog is left as
// it was.
func TestCatalogSchema_RoutingRuleConstraints(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	db := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.PrimaryPgctld.PoolerDir), setup.PrimaryPgctld.PgPort)
	defer db.Close()

	// fixture creates an import operation, a srctg tablegroup with its 0-inf
	// shard and a table registered in it; the rule columns refer to them as
	// op, def (the default tablegroup), src, srcshard and tbl.
	const fixture = `
CREATE TEMP TABLE ids ON COMMIT DROP AS SELECT
	'33333333-3333-3333-3333-333333333333'::uuid AS op,
	(SELECT tablegroup_id FROM multigres.tablegroup WHERE name = 'default') AS def;
INSERT INTO multigres.operation (operation_id, kind, state) SELECT op, 'import', 'preparing' FROM ids;
INSERT INTO multigres.tablegroup (name, state) VALUES ('routing_rule_test_src', 'active');
INSERT INTO multigres.shard (tablegroup_id, shard_name, state)
	SELECT tablegroup_id, '0-inf', 'active' FROM multigres.tablegroup WHERE name = 'routing_rule_test_src';
INSERT INTO multigres.tablegroup_table (schema_name, table_name, tablegroup_id)
	SELECT 'public', 'routing_rule_test_t', tablegroup_id FROM multigres.tablegroup WHERE name = 'routing_rule_test_src';
ALTER TABLE ids ADD COLUMN src bigint, ADD COLUMN srcshard bigint, ADD COLUMN tbl bigint;
UPDATE ids SET
	src = (SELECT tablegroup_id FROM multigres.tablegroup WHERE name = 'routing_rule_test_src'),
	srcshard = (SELECT s.shard_id FROM multigres.shard s JOIN multigres.tablegroup tg USING (tablegroup_id)
		WHERE tg.name = 'routing_rule_test_src'),
	tbl = (SELECT table_id FROM multigres.tablegroup_table WHERE table_name = 'routing_rule_test_t')`

	tests := []struct {
		name    string
		insert  string
		wantErr string
	}{
		{
			name: "shard rule",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, to_tablegroup_id, to_shard_id, created_version)
				SELECT op, 'tablegroup', def, src, srcshard, 1 FROM ids`,
		},
		{
			name: "table rule",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, from_table_id, to_tablegroup_id, created_version)
				SELECT op, 'table', src, tbl, def, 1 FROM ids`,
		},
		{
			name: "shard rule without a target shard",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, to_tablegroup_id, created_version)
				SELECT op, 'tablegroup', def, src, 1 FROM ids`,
			wantErr: "routing_rule_check1",
		},
		{
			name: "shard rule naming a table",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, from_table_id, to_tablegroup_id, to_shard_id, created_version)
				SELECT op, 'tablegroup', src, tbl, def, srcshard, 1 FROM ids`,
			wantErr: "routing_rule_check",
		},
		{
			name: "table rule without a table",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, to_tablegroup_id, created_version)
				SELECT op, 'table', src, def, 1 FROM ids`,
			wantErr: "routing_rule_check",
		},
		{
			name: "unknown scope",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, to_tablegroup_id, created_version)
				SELECT op, 'shard', def, src, 1 FROM ids`,
			wantErr: "routing_rule_scope_check",
		},
		{
			name: "unknown operation",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, to_tablegroup_id, to_shard_id, created_version)
				SELECT '44444444-4444-4444-4444-444444444444', 'tablegroup', def, src, srcshard, 1 FROM ids`,
			wantErr: "routing_rule_operation_id_fkey",
		},
		{
			name: "unknown target shard",
			insert: `INSERT INTO multigres.routing_rule
				(operation_id, scope, from_tablegroup_id, to_tablegroup_id, to_shard_id, created_version)
				SELECT op, 'tablegroup', def, src, -1, 1 FROM ids`,
			wantErr: "routing_rule_to_shard_id_fkey",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()

			_, err = tx.Exec(fixture)
			require.NoError(t, err, "fixture")
			_, err = tx.Exec(tt.insert)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
