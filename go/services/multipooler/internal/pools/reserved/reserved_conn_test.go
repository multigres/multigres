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

package reserved

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

func TestConn_RefreshDefaultsIfStale(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddQuery("SELECT 1", &sqltypes.Result{})

	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     4,
				MaxIdleCount: 4,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn.Release(ReleaseCommit, nil)

	pidBefore := conn.ProcessID()

	pool.InvalidateDefaults()

	require.NoError(t, conn.RefreshDefaultsIfStale(ctx))
	pidAfter := conn.ProcessID()
	assert.NotEqual(t, pidBefore, pidAfter, "checked-out reserved backend must reconnect after invalidation")

	_, err = conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)
}
