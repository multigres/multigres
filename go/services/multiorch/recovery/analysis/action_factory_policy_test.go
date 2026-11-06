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

package analysis

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestRecoveryActionFactory_BuildsAppointLeaderFromThePolicy(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	rpcClient := &rpcclient.FakeClient{}
	coord := consensus.NewCoordinator(nil, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, store.NewTestCache(t), rpcClient, ts, coord, slog.Default())

	metadata := factory.NewAppointLeaderAction().Metadata()

	assert.Equal(t, DefaultAvailabilityPolicy().PreRecheckRefreshTimeout, metadata.RefreshBeforeRecheckTimeout,
		"the failover's refresh budget must come from the availability policy")
	assert.Positive(t, metadata.RefreshBeforeRecheckTimeout)
}
