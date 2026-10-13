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
	"errors"
	"fmt"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerDialer decides where and how the gateway connects to one pooler. The
// gateway builds the gRPC client itself from what the dialer returns, so
// telemetry and every other client setting are the same whichever dialer is
// used.
type PoolerDialer interface {
	// Target returns the gRPC target and dial options for pooler. An error
	// means the gateway must not connect to it.
	Target(pooler *clustermetadatapb.Multipooler) (target string, opts []grpc.DialOption, err error)
}

// remoteDialer connects to a pooler over the network, at the address in its
// topology record.
type remoteDialer struct {
	creds grpc.DialOption
}

// NewRemoteDialer returns the dialer Multigres uses: it connects to each
// pooler at the address in its topology record, with the given transport
// credentials (TLS or plaintext).
func NewRemoteDialer(creds grpc.DialOption) PoolerDialer {
	return remoteDialer{creds: creds}
}

func (d remoteDialer) Target(pooler *clustermetadatapb.Multipooler) (string, []grpc.DialOption, error) {
	poolerInfo := &topoclient.MultipoolerInfo{Multipooler: pooler}
	return poolerInfo.Addr(), []grpc.DialOption{d.creds}, nil
}

// ErrNotLocalPooler is returned by a local dialer for any pooler other than
// the one running in the same process.
var ErrNotLocalPooler = errors.New("not the pooler of this process")

// localDialer connects to the pooler running in the same process, at the
// local endpoint its query service is served on.
type localDialer struct {
	self     func() *clustermetadatapb.ID
	endpoint servenv.Endpoint
}

// NewLocalDialer returns a dialer for the pooler in the same process
// (minigres). self returns that pooler's ID; it is called on every Target, so
// it may depend on flags parsed after the dialer is built. endpoint is where
// the pooler's query service is served; the dialer connects the way the
// endpoint says, whether it is in memory or, later, a Unix socket. Every other
// pooler gets ErrNotLocalPooler: with a shared topology the gateway also sees
// other processes' poolers, and must never route to them, nor treat them as
// its own.
func NewLocalDialer(self func() *clustermetadatapb.ID, endpoint servenv.Endpoint) PoolerDialer {
	return localDialer{self: self, endpoint: endpoint}
}

func (d localDialer) Target(pooler *clustermetadatapb.Multipooler) (string, []grpc.DialOption, error) {
	if !sameComponent(pooler.GetId(), d.self()) {
		return "", nil, fmt.Errorf("pooler %s: %w", topoclient.ComponentIDString(pooler.GetId()), ErrNotLocalPooler)
	}
	target, opts := d.endpoint.Target()
	return target, opts, nil
}

// sameComponent reports whether a and b identify the same component.
func sameComponent(a, b *clustermetadatapb.ID) bool {
	return a.GetComponent() == b.GetComponent() &&
		a.GetCell() == b.GetCell() &&
		a.GetName() == b.GetName()
}
