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

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

const connectionsProbePattern = `to_regclass\('multigres\.connections'\)`

// TestEnsureMigrationSchema_MissingConnectionsTable: EnsureSchema reports a missing
// connections table. Off the catalog shard that is expected and the schema counts as
// ensured; on the catalog shard it means the catalog ensure did not complete, so it is
// retried instead of leaving stat_migration uncreated until a restart.
func TestEnsureMigrationSchema_MissingConnectionsTable(t *testing.T) {
	for _, tc := range []struct {
		name, tableGroup, shard string
		wantEnsured             bool
	}{
		{"catalog shard retries", constants.DefaultTableGroup, constants.DefaultShard, false},
		{"another shard is done", "commerce", constants.DefaultShard, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pm, m := newTestManagerWithMock(t, tc.tableGroup, tc.shard)
			m.AddQueryPattern(connectionsProbePattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{false}}))
			addShardRefEnsure(m, nil)
			m.AddQueryPattern(".*", mock.MakeQueryResult(nil, nil))

			pm.ensureMigrationSchema(context.Background(), pm.migrationCoordinator())

			pm.mu.Lock()
			defer pm.mu.Unlock()
			assert.Equal(t, tc.wantEnsured, pm.migrationSchemaEnsured)
		})
	}

	t.Run("the connections table exists", func(t *testing.T) {
		pm, m := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
		m.AddQueryPattern(connectionsProbePattern, mock.MakeQueryResult([]string{"exists"}, [][]any{{true}}))
		addShardRefEnsure(m, nil)
		m.AddQueryPattern(".*", mock.MakeQueryResult(nil, nil))

		pm.ensureMigrationSchema(context.Background(), pm.migrationCoordinator())

		pm.mu.Lock()
		defer pm.mu.Unlock()
		assert.True(t, pm.migrationSchemaEnsured)
	})
}
