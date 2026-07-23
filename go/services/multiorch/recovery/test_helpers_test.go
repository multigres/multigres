// Copyright 2025 Supabase, Inc.
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

package recovery

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
)

// newTestCoordinator creates a coordinator for tests that need one but manage their own Engine creation.
func newTestCoordinator(ts topoclient.Store, rpcClient rpcclient.MultipoolerClient, cell string) *consensus.Coordinator {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	coordID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIORCH,
		Cell:      cell,
		Name:      "test-coordinator",
	}
	return consensus.NewCoordinator(coordID, ts, rpcClient, logger)
}
