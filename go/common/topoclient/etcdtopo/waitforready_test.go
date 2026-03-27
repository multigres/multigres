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

package etcdtopo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

func TestWaitForReady(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	ctx := utils.LeakCheckContext(t)

	t.Run("healthy etcd returns nil", func(t *testing.T) {
		clientAddr, _ := StartEtcd(t)
		// StartEtcd already called WaitForReady internally, but call it again
		// explicitly to confirm the /health endpoint continues to return 200.
		readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err := WaitForReady(readyCtx, clientAddr)
		require.NoError(t, err)
	})

	t.Run("wrong address times out", func(t *testing.T) {
		freePort := utils.GetFreePort(t)
		badAddr := fmt.Sprintf("http://localhost:%d", freePort)
		readyCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		err := WaitForReady(readyCtx, badAddr)
		require.Error(t, err)
	})

	t.Run("already-cancelled context returns error immediately", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := WaitForReady(cancelledCtx, "http://localhost:1")
		require.Error(t, err)
	})
}
