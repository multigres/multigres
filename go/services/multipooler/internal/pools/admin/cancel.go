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

package admin

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// cancelRequestTimeout bounds sending a cancel request when a query's context
// expires. It sits on the timeout path, so keep it short.
const cancelRequestTimeout = time.Second

// cancelFailureReminder is how often a run of failed cancels is logged again while
// it lasts.
const cancelFailureReminder = 10 * time.Minute

// cancelFailures tracks failed cancel requests since the last one that succeeded.
var cancelFailures failureRun

// failureRun tracks a run of failures so it can be logged when it starts, every
// cancelFailureReminder while it lasts, and when it ends, rather than once per
// failure.
//
// TODO: this is a one-off log throttle. If other code needs to log a recurring
// problem when it starts, while it lasts and when it clears, extract a shared
// helper into go/tools, likely keyed per problem.
type failureRun struct {
	mu         sync.Mutex
	count      int
	lastLogged time.Time
}

// fail records a failure at now. It reports whether to log it, and how many failures
// the run has had.
func (r *failureRun) fail(now time.Time) (log bool, count int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	if r.count == 1 || now.Sub(r.lastLogged) >= cancelFailureReminder {
		r.lastLogged = now
		return true, r.count
	}
	return false, r.count
}

// succeed ends the run and returns how many failures it had.
func (r *failureRun) succeed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.count
	r.count = 0
	return count
}

// cancelOnServer sends a cancel request with send, best effort. Failures are logged
// as runs (see failureRun), not each one.
func cancelOnServer(ctx context.Context, send func(context.Context) error) {
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelRequestTimeout)
	defer cancel()
	if err := send(cancelCtx); err != nil {
		if log, count := cancelFailures.fail(time.Now()); log {
			slog.WarnContext(ctx, "failed to cancel a timed-out query on the server; its backend may keep running",
				"error", err, "failed_cancels", count)
		}
		return
	}
	if failed := cancelFailures.succeed(); failed > 0 {
		slog.InfoContext(ctx, "cancelling timed-out queries on the server works again", "failed_cancels", failed)
	}
}
