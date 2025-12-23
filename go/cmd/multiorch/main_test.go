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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInit_TopoMissingAddresses verifies that Init() returns an error when
// topo-global-server-addresses is not configured. This was previously an
// os.Exit() call that couldn't be tested.
func TestInit_TopoMissingAddresses(t *testing.T) {
	cmd, _ := CreateMultiOrchCommand()

	cmd.SetArgs([]string{
		"--watch-targets", "db1/shard1",
		"--config-file-not-found-handling", "ignore",
	})

	err := cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "topo-global-server-addresses must be configured")
}

// TestInit_TopoMissingRoot verifies that Init() returns an error when
// topo-global-root is not configured.
func TestInit_TopoMissingRoot(t *testing.T) {
	cmd, _ := CreateMultiOrchCommand()

	cmd.SetArgs([]string{
		"--watch-targets", "db1/shard1",
		"--topo-global-server-addresses", "localhost:2379",
		"--config-file-not-found-handling", "ignore",
	})

	err := cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "topo-global-root must be non-empty")
}

// Note: Testing watch-targets validation requires a working topo connection.
// The multiorch/init.go validates watch-targets after topo.Open() succeeds,
// so we can't test "watch-targets is required" error without a running topo server.
// That validation is tested indirectly through integration tests.
