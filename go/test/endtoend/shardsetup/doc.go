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

// Package shardsetup provides shared test infrastructure for end-to-end tests of a single shard.
//
// # Overview
//
// This package provides a ShardSetup struct that manages the infrastructure for testing
// a PostgreSQL shard: multipoolers (pgctld + multipooler pairs) and optionally multiorch instances.
//
// # Usage
//
// To use this package, follow these steps:
//
// 1. Create a main_test.go file in your test package with TestMain:
//
//	package yourpackage
//
//	import (
//		"os"
//		"testing"
//
//		"github.com/multigres/multigres/go/test/endtoend/shardsetup"
//	)
//
//	var sharedSetup *shardsetup.ShardSetup
//
//	func TestMain(m *testing.M) {
//		exitCode := shardsetup.RunTestMain(m, func(t *testing.T) *shardsetup.ShardSetup {
//			sharedSetup = shardsetup.New(t,
//				shardsetup.WithMultipoolerCount(2), // primary + standby
//				shardsetup.WithMultiorchCount(0),   // no multiorch for basic tests
//			)
//			return sharedSetup
//		})
//		os.Exit(exitCode)
//	}
//
// 2. In your tests, use the shared setup:
//
//	func TestSomething(t *testing.T) {
//		sharedSetup.SetupTest(t) // Validates clean state and registers cleanup
//
//		// Your test code here
//		client := sharedSetup.NewPrimaryClient(t)
//		defer client.Close()
//		// ...
//	}
//
// # Naming Convention
//
// Multipooler instances are named by index:
//   - Index 0: "primary"
//   - Index 1: "standby"
//   - Index 2+: "standby2", "standby3", etc.
//
// Access instances by name:
//
//	setup.GetMultipoolerInstance("primary")
//	setup.GetMultipoolerInstance("standby")
//	setup.PrimaryMultipooler() // shorthand for GetMultipooler("primary")
//	setup.StandbyMultipooler() // shorthand for GetMultipooler("standby")
//
// # Test Isolation
//
// The SetupTest method provides test isolation:
//   - Validates all nodes are in expected clean state before test
//   - Registers cleanup handler to reset state after test
//   - Clean state means: terms=1, types=PRIMARY/REPLICA, GUCs reset, WAL replay active
//
// # Replication
//
// By default, standbys are created in recovery mode but NOT actively replicating.
// To configure replication during a test:
//
//	setup.ConfigureReplication(t, "standby")
//
// This allows tests to set up replication from scratch if needed.
package shardsetup
