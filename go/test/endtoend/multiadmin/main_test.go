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

// Package multiadmin contains end-to-end tests that exercise the multiadmin
// service started via shardsetup's WithMultiadmin() option, covering its
// gRPC API, HTTP/REST grpc-gateway, and the multigres CLI that talks to it.
package multiadmin

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// setupManager manages a shared test cluster (multipoolers + multigateway +
// multiadmin) so all tests in this package share the same multiadmin process.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultiadmin(),
	)
})

func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared cluster setup for tests in this package.
func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return setupManager.Get(t)
}
