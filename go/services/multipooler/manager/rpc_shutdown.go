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

package manager

import (
	"context"
	"log/slog"
	"os"
	"syscall"

	"github.com/multigres/multigres/go/tools/ctxutil"
)

// InitiateGracefulShutdown runs the graceful shutdown sequence and then sends
// SIGTERM to trigger the normal servenv exit path. Both steps happen in a
// goroutine so the RPC response is returned to the caller first.
//
// performGracefulShutdown is protected by sync.Once: if OnTermSync fires
// concurrently (because SIGTERM arrives before performGracefulShutdown
// completes), the second call is a no-op.
func (pm *MultiPoolerManager) InitiateGracefulShutdown(ctx context.Context, attr slog.Attr) {
	pm.logger.InfoContext(ctx, "Shutdown RPC received. Initiating graceful shutdown", attr)
	go func() {
		pm.performGracefulShutdown(ctxutil.Detach(ctx))
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			// Kill(self, SIGTERM) should never fail in practice. Panic so the
			// process terminates rather than staying alive in an unexpected state.
			panic("Shutdown RPC: failed to send SIGTERM to self: " + err.Error())
		}
	}()
}
