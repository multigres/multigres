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

package command

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxFencedRewindRuns bounds how many fenced run ids are remembered. Ids are
// fenced only after a failed or interrupted rewind, so this is far more than
// are ever live at once.
const maxFencedRewindRuns = 256

// rewindRuns tracks the mutating pg_rewind run pgctld is executing and the run
// ids that have been fenced. A caller whose PgRewind call ended early cannot tell
// a request that was never handled from one that is about to start; fencing the
// id answers that: after abort returns, a run with that id is not running and
// cannot start. begin and abort share one mutex so that a request is either
// registered before it is fenced, and then terminated, or refused.
//
// The zero value is ready to use.
type rewindRuns struct {
	mu      sync.Mutex
	current *rewindRun
	fenced  map[string]struct{}
	order   []string
}

// rewindRun is the registered run. done is closed once the run has finished
// cleaning up after itself, which includes removing the running record.
type rewindRun struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{}
}

// begin registers the run id and returns a context that abort cancels, and a
// function the caller must call once the run is over. It refuses an id that has
// been fenced and a second run while one is registered. A request without an id
// (a caller that predates it) is registered under one generated here, so it
// cannot overlap a tracked run and a tracked run cannot be overlapped by it, but
// nothing can fence it.
func (r *rewindRuns) begin(ctx context.Context, id string) (context.Context, func(), error) {
	if id == "" {
		id = "unfenceable-" + uuid.NewString()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, fenced := r.fenced[id]; fenced {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "pg_rewind run %s was aborted before it started", id)
	}
	if r.current != nil {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "pg_rewind run %s is already running", r.current.id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &rewindRun{id: id, cancel: cancel, done: make(chan struct{})}
	r.current = run
	return runCtx, func() {
		cancel()
		r.mu.Lock()
		if r.current == run {
			r.current = nil
		}
		r.mu.Unlock()
		close(run.done)
	}, nil
}

// abort fences the id and, if that run is executing, cancels it and waits until
// it has exited. It reports whether a running pg_rewind was terminated. It
// returns an error only if the run is still executing when ctx ends.
func (r *rewindRuns) abort(ctx context.Context, id string) (bool, error) {
	r.mu.Lock()
	r.fence(id)
	run := r.current
	r.mu.Unlock()
	if run == nil || run.id != id {
		return false, nil
	}
	run.cancel()
	select {
	case <-run.done:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// fence records the id, forgetting the oldest once the bound is reached. The
// caller holds r.mu.
func (r *rewindRuns) fence(id string) {
	if _, ok := r.fenced[id]; ok {
		return
	}
	if r.fenced == nil {
		r.fenced = make(map[string]struct{})
	}
	if len(r.order) >= maxFencedRewindRuns {
		delete(r.fenced, r.order[0])
		r.order = r.order[1:]
	}
	r.fenced[id] = struct{}{}
	r.order = append(r.order, id)
}
