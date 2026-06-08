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

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestVerifyBackups(t *testing.T) {
	// VerifyBackups requires a running pgbackrest with a valid stanza.
	// The actual invocation, including the corrupt-backup negative path, is
	// tested by integration tests (endtoend/multipooler) and the backup engine
	// unit tests. This test validates the manager handler's precondition checks.

	t.Run("fails when not ready", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		_, err := pm.VerifyBackups(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "manager is in unknown state")
	})

	t.Run("fails when config not yet generated", func(t *testing.T) {
		// Ready, but topology hasn't been loaded so no config path exists.
		pm := createTestManager(t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
		_, err := pm.VerifyBackups(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "pgbackrest config not found")
	})
}
