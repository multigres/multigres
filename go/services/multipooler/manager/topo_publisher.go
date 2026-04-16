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

package manager

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const topoPublisherRetryInterval = 30 * time.Second

// topoRegistrar is the subset of topoclient.Store used by topoPublisher.
type topoRegistrar interface {
	RegisterMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler, allowUpdate bool) error
}

// topoPublisher eventually-consistently reflects the multipooler's in-memory state
// to etcd. Callers invoke Notify with the current state whenever it changes; a
// background goroutine wakes immediately to write it and also retries periodically
// so that writes missed during an etcd outage are recovered automatically.
type topoPublisher struct {
	logger     *slog.Logger
	topoClient topoRegistrar

	// wakeup is a size-1 buffered channel. A non-blocking send schedules a
	// publish without accumulating multiple pending signals.
	wakeup chan struct{}

	mu            sync.Mutex
	desired       *clustermetadatapb.MultiPooler // latest state that should be in etcd
	lastPublished *clustermetadatapb.MultiPooler // last state successfully written; nil if never written
}

func newTopoPublisher(logger *slog.Logger, topoClient topoRegistrar) *topoPublisher {
	return &topoPublisher{
		logger:     logger,
		topoClient: topoClient,
		wakeup:     make(chan struct{}, 1),
	}
}

// Notify records mp as the desired topology state and schedules an immediate
// publish attempt. mp is cloned so the caller may reuse the original.
//
// ctx must carry an action lock (see AssertActionLockHeld). The action lock
// serialises state transitions, preventing concurrent calls from racing to
// overwrite each other's desired state.
func (tp *topoPublisher) Notify(ctx context.Context, mp *clustermetadatapb.MultiPooler) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	tp.mu.Lock()
	tp.desired = proto.Clone(mp).(*clustermetadatapb.MultiPooler)
	tp.mu.Unlock()

	// Non-blocking send: if the channel is already full, a publish is already
	// pending and will pick up the latest desired state.
	select {
	case tp.wakeup <- struct{}{}:
	default:
	}
	return nil
}

// Run is the background loop. It blocks until ctx is cancelled. Call it in a
// goroutine: go tp.Run(ctx).
func (tp *topoPublisher) Run(ctx context.Context) {
	ticker := time.NewTicker(topoPublisherRetryInterval)
	defer ticker.Stop()
	tp.run(ctx, ticker.C)
}

// run is the internal loop, accepting an injectable ticker channel so tests can
// drive retries without real clock time.
func (tp *topoPublisher) run(ctx context.Context, tickC <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tp.wakeup:
			tp.publishIfNeeded(ctx)
		case <-tickC:
			tp.publishIfNeeded(ctx)
		}
	}
}

// publishIfNeeded writes the desired state to etcd if it differs from the last
// successfully published state. A no-op when state is already current.
func (tp *topoPublisher) publishIfNeeded(ctx context.Context) {
	tp.mu.Lock()
	desired := tp.desired
	lastPublished := tp.lastPublished
	tp.mu.Unlock()

	if desired == nil {
		return
	}

	if proto.Equal(desired, lastPublished) {
		return
	}

	tp.logger.InfoContext(ctx, "Publishing multipooler state to topology",
		"type", desired.Type,
		"serving_status", desired.ServingStatus)

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := tp.topoClient.RegisterMultiPooler(publishCtx, desired, true); err != nil {
		tp.logger.ErrorContext(ctx, "Failed to publish multipooler state to topology; will retry",
			"error", err,
			"type", desired.Type,
			"serving_status", desired.ServingStatus)
		return
	}

	tp.mu.Lock()
	tp.lastPublished = proto.Clone(desired).(*clustermetadatapb.MultiPooler)
	tp.mu.Unlock()

	tp.logger.InfoContext(ctx, "Published multipooler state to topology",
		"type", desired.Type,
		"serving_status", desired.ServingStatus)
}
