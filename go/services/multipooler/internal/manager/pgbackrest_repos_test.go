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

package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestInsertInitialPgBackRestRepo(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(m *mock.QueryService)
		errorContains string
	}{
		{
			name: "seeds the generation-1 row",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
			},
		},
		{
			name: "insert failure is surfaced",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.pgbackrest_repos", errors.New("insert failed"))
			},
			errorContains: "failed to seed pgbackrest_repos",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
			tt.setupMock(mockQueryService)

			err := pm.insertInitialPgBackRestRepo(context.Background())

			if tt.errorContains != "" {
				require.ErrorContains(t, err, tt.errorContains)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestRequireInitialRepoEncryptionError(t *testing.T) {
	required := &clustermetadatapb.BackupLocation{RequireInitialRepoEncryption: true}
	optional := &clustermetadatapb.BackupLocation{}

	assert.NoError(t, requireInitialRepoEncryptionError(optional, nil))
	assert.NoError(t, requireInitialRepoEncryptionError(required, backup.CipherKeys{1: "pass"}))
	assert.ErrorContains(t, requireInitialRepoEncryptionError(required, nil), "no cipher key for generation 1")
	assert.ErrorContains(t, requireInitialRepoEncryptionError(required, backup.CipherKeys{1: ""}), "declared unencrypted")
}
