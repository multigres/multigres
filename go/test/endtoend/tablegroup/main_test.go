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

// Package tablegroup contains end-to-end tests for creating tablegroups
// through the sharding catalog: the catalog primary's operations and
// multiadmin's ListDesiredTableGroups. The test plays the provisioner itself,
// the role the Kubernetes operator plays in a real deployment.
package tablegroup

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

func TestMain(m *testing.M) {
	os.Exit(shardsetup.RunTestMain(m)) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}
