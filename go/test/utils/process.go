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

package utils

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// CommandWithOrphanProtection creates a command wrapped in run_in_test.sh for orphan
// process protection. The process uses context.Background() for its lifetime, avoiding
// premature SIGKILL when the monitoring context is cancelled. Instead, Stop() is called
// when monitorCtx is cancelled, giving run_in_test.sh time to send SIGTERM to its child
// process before exiting.
//
// Callers should set any required environment variables (e.g. MULTIGRES_TESTDATA_DIR)
// on the returned Cmd before calling Start().
func CommandWithOrphanProtection(monitorCtx context.Context, name string, args ...string) *executil.Cmd {
	allArgs := append([]string{name}, args...)
	cmd := executil.Command(context.Background(), "run_in_test.sh", allArgs...)
	go func() {
		<-monitorCtx.Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = cmd.Stop(stopCtx)
		cancel()
	}()
	return cmd
}
