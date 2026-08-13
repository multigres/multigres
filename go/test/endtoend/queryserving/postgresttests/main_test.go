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

package postgresttests

import (
	"os"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestMain sets up the shared test environment (notably PATH, so the cluster can
// find etcd and run_in_test.sh). buildPostgres later prepends the built
// PostgreSQL bin ahead of the repo bin, so pgctld uses the PostGIS-enabled build
// while etcd/run_in_test.sh still resolve from the repo bin.
func TestMain(m *testing.M) {
	os.Exit(shardsetup.RunTestMain(m)) //nolint:forbidigo // TestMain may call os.Exit
}
