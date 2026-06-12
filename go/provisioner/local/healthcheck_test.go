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

package local

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"
)

func TestMain(m *testing.M) {
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add directories to PATH: %v\n", err)
		os.Exit(1) //nolint:forbidigo // TestMain() is allowed to call os.Exit
	}
	os.Setenv("MULTIGRES_TEST_PARENT_PID", strconv.Itoa(os.Getpid()))
	exitCode := m.Run()
	os.Unsetenv("MULTIGRES_TEST_PARENT_PID")
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// TestCheckEtcdHealth verifies that checkEtcdHealth works against a real etcd instance.
// This is the only test coverage for checkEtcdHealth — without it, a regression like
// accidentally removing --listen-metrics-urls (which would cause /readyz to return 404
// on the client port and make waitForServiceReady time out after 60 attempts) would go
// undetected.
func TestCheckEtcdHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping etcd integration tests in short mode")
	}
	ctx := utils.LeakCheckContext(t)

	_, metricsAddr, _ := etcdtopo.StartEtcd(t)

	// Strip the "http://" prefix — checkEtcdHealth expects a bare host:port address.
	address := metricsAddr[len("http://"):]

	p := &localProvisioner{}

	t.Run("healthy etcd returns nil", func(t *testing.T) {
		err := p.checkEtcdHealth(ctx, address)
		require.NoError(t, err)
	})

	t.Run("wrong address returns error", func(t *testing.T) {
		freePort := utils.GetFreePort(t)
		err := p.checkEtcdHealth(ctx, fmt.Sprintf("localhost:%d", freePort))
		require.Error(t, err)
	})

	t.Run("cancelled context returns error", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := p.checkEtcdHealth(cancelledCtx, address)
		require.Error(t, err)
	})
}
