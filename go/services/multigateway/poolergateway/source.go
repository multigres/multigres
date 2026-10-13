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

package poolergateway

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerSource is where the gateway finds its poolers (events for its pooler
// cache) and how it reaches each one (Target). The gateway builds the gRPC
// client from Target, so client settings are the same for every source.
type PoolerSource interface {
	poolerwatch.Source

	// Target returns the gRPC target and dial options for pooler.
	Target(pooler *clustermetadatapb.Multipooler) (target string, opts []grpc.DialOption, err error)
}

type remoteSource struct {
	*poolerwatch.TopoSource
	creds grpc.DialOption
}

// NewRemoteSource watches the topology for poolers in every cell and reaches
// each at its record's address with creds. Multigres uses it.
func NewRemoteSource(store topoclient.ConnProvider, creds grpc.DialOption) PoolerSource {
	return remoteSource{TopoSource: poolerwatch.NewTopoSource(store), creds: creds}
}

func (s remoteSource) Target(pooler *clustermetadatapb.Multipooler) (string, []grpc.DialOption, error) {
	poolerInfo := &topoclient.MultipoolerInfo{Multipooler: pooler}
	return poolerInfo.Addr(), []grpc.DialOption{s.creds}, nil
}

// RecordFeed follows one pooler's topology record (Multipooler.WatchRecord):
// fn gets the record right away and after every change, until stop.
type RecordFeed interface {
	WatchRecord(fn func(*clustermetadatapb.Multipooler)) (stop func())
}

type localSource struct {
	feed     RecordFeed
	endpoint servenv.Endpoint

	mu   sync.Mutex
	stop func()
}

// NewLocalSource follows the pooler in the same process through feed, without
// the topology, and reaches it at endpoint. The gateway then never sees other
// processes' poolers. minigres uses it.
func NewLocalSource(feed RecordFeed, endpoint servenv.Endpoint) PoolerSource {
	return &localSource{feed: feed, endpoint: endpoint}
}

// Start delivers the first record as a snapshot of its cell and each change as
// an upsert, as the topology source does.
func (s *localSource) Start(_ context.Context, _ *slog.Logger, handlers poolerwatch.Handlers) {
	// The feed calls back one change at a time, so first needs no lock.
	first := true
	stop := s.feed.WatchRecord(func(record *clustermetadatapb.Multipooler) {
		if first {
			first = false
			handlers.OnSnapshot(record.GetId().GetCell(), []*clustermetadatapb.Multipooler{record})
			return
		}
		handlers.OnUpsert(record)
	})
	s.mu.Lock()
	s.stop = stop
	s.mu.Unlock()
}

func (s *localSource) Stop() {
	s.mu.Lock()
	stop := s.stop
	s.stop = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (s *localSource) Target(*clustermetadatapb.Multipooler) (string, []grpc.DialOption, error) {
	target, opts := s.endpoint.Target()
	return target, opts, nil
}
