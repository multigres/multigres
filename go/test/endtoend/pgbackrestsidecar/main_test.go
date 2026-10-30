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

// Package pgbackrestsidecar holds the end-to-end tests of pgBackRest running
// in a pgbackrestd sidecar per pooler.
//
// What runs where:
//   - Linux with Landlock: the sidecar runs under --sandbox=required, so a test
//     that depends on confinement is real. This is the run that counts.
//   - macOS: there is no Landlock, the provisioner starts the sidecar with
//     --sandbox=off, and the tests prove the wiring (sockets, credentials,
//     repository, restore ordering), not confinement. Tests that need /proc or
//     real confinement skip with a reason.
package pgbackrestsidecar

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

func TestMain(m *testing.M) {
	os.Exit(shardsetup.RunTestMain(m)) //nolint:forbidigo // TestMain is allowed to call os.Exit
}
