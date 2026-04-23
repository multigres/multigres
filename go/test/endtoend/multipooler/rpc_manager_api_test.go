// Copyright 2025 Supabase, Inc.
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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// TestMultipoolerPrimaryLSN verifies that a primary pooler reports a valid WAL position
// via the consensus Status RPC.
func TestMultipoolerPrimaryLSN(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithoutReplication())
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	primaryClient, err := shardsetup.NewMultipoolerClient(setup.PrimaryMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { primaryClient.Close() })

	ctx := utils.WithTimeout(t, 5*time.Second)
	statusResp, err := primaryClient.Consensus.Status(ctx, &consensusdatapb.StatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResp.WalPosition, "WAL position should not be nil on primary")
	assert.NotEmpty(t, statusResp.WalPosition.CurrentLsn, "Primary LSN should not be empty")
	assert.Contains(t, statusResp.WalPosition.CurrentLsn, "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")
}
