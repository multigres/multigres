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

package topoclient_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/multigres/multigres/go/tools/pathutil"
)

// TestMain puts the repository's bin/ and go/test/endtoend/ on PATH, because
// the tests that start a real etcd run it through run_in_test.sh, which lives
// there; go test does not set PATH for them. The parent PID lets
// run_in_test.sh stop etcd if this test binary dies.
func TestMain(m *testing.M) {
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add directories to PATH: %v\n", err)
		os.Exit(1) //nolint:forbidigo // TestMain() is allowed to call os.Exit
	}
	os.Setenv("MULTIGRES_TEST_PARENT_PID", strconv.Itoa(os.Getpid()))
	os.Exit(m.Run()) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}
