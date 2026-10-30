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
package poolerserver

import (
	"context"
	"errors"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// RoutingProvider serves the routing the manager holds in the catalog of the
// default tablegroup's primary.
type RoutingProvider interface {
	GetRoutingSnapshot(ctx context.Context, req *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error)
}

// RoutingProvider returns the manager's routing service, or an error when the
// health provider (the manager) does not implement one.
func (s *QueryPoolerServer) RoutingProvider() (RoutingProvider, error) {
	p, ok := s.healthProvider.(RoutingProvider)
	if !ok {
		return nil, errors.New("routing is unavailable")
	}
	return p, nil
}
