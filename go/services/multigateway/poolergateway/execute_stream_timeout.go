// Copyright 2026 Supabase, Inc.
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

package poolergateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
)

// Allow the pooler's backend cancel (up to 5s) and drain (up to 2s) to
// finish. Failure to acknowledge completion within this bound ends the session.
const operationCleanupGrace = 10 * time.Second

// watchOperation preserves the transport after a statement deadline so the
// pooler can acknowledge cleanup. Explicit cancellation still closes it
// immediately. The returned stop function also stops any cleanup timer.
func watchOperation(ctx context.Context, cancel context.CancelFunc, grace time.Duration) func() bool {
	var mu sync.Mutex
	var timer *time.Timer
	stopped := false
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if stopped {
			return
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			timer = time.AfterFunc(grace, cancel)
		} else {
			cancel()
		}
	})
	return func() bool {
		ok := stop()
		mu.Lock()
		stopped = true
		if timer != nil {
			timer.Stop()
		}
		mu.Unlock()
		return ok
	}
}

func completionUnconfirmed() error {
	return mterrors.ToGRPC(mterrors.NewPgError("FATAL", mterrors.PgSSConnectionFailure,
		"statement timed out before backend cleanup could be confirmed", ""))
}

// A legacy RPC deadline ends the transport without acknowledging backend
// cleanup. Do not report a recoverable timeout for that ambiguous outcome.
func legacyTimeoutError(ctx context.Context, err error) error {
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return completionUnconfirmed()
	}
	return err
}

// finish also covers callbacks that stop consuming results at the deadline.
// Release alone cancels the transport and cannot establish backend readiness.
func (r *streamResponses) finish(err error) error {
	defer r.Release()
	if !errors.Is(r.ctx.Err(), context.DeadlineExceeded) || r.done {
		return err
	}
	for {
		_, completionErr := r.recv()
		if completionErr != nil {
			return completionErr
		}
	}
}

// Public query-service callbacks return application errors, not gRPC statuses.
// Preserve their identity unless timeout recovery supplies a new outcome.
func (r *streamResponses) finishQuery(err error) error {
	if !errors.Is(r.ctx.Err(), context.DeadlineExceeded) || r.done {
		r.Release()
		return err
	}
	return mterrors.FromGRPC(r.finish(nil))
}

func completedTimeout(ctx context.Context, err error) error {
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) || isFatalRPC(err) {
		return err
	}
	// Keep status details, including authoritative transaction reservation state.
	st := status.Convert(err).Proto()
	if st == nil {
		st = status.New(codes.DeadlineExceeded, "statement timeout").Proto()
	}
	st.Code = int32(codes.DeadlineExceeded)
	st.Message = "statement timeout"
	return status.FromProto(st).Err()
}

func isFatalRPC(err error) bool {
	var diagnostic *mterrors.PgDiagnostic
	return errors.As(mterrors.FromGRPC(err), &diagnostic) && diagnostic.IsFatal()
}
