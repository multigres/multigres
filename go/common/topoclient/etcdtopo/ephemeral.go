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

package etcdtopo

import (
	"context"
	"errors"
	"path"
	"sync"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/multigres/multigres/go/tools/ctxutil"
)

// Ephemeral files are bound to a lease shared by every ephemeral file on this
// connection and renewed for the connection's lifetime. When the process dies,
// renewals stop and etcd deletes the files within one TTL
// (--topo-etcd-lease-ttl) — that is what keeps a killed component from leaving
// a permanent registration behind.
//
// This layer deliberately does not remember which files it wrote. If the lease
// ends while the process is alive (etcd unreachable for longer than the TTL),
// the files are gone and only their owner knows they should exist; re-creating
// them is the owner's job, via toporeg's re-assertion loop. All this layer
// does is forget the dead lease so the next write grants a fresh one.

// ephemeralState holds the lease shared by this connection's ephemeral files.
type ephemeralState struct {
	mu      sync.Mutex
	leaseID clientv3.LeaseID
}

// PutEphemeral is part of the topoclient.Conn interface.
func (s *etcdtopo) PutEphemeral(ctx context.Context, filePath string, contents []byte) error {
	nodePath := path.Join(s.root, filePath)

	leaseID, err := s.ephemeralLease(ctx)
	if err != nil {
		return convertError(err, nodePath)
	}

	if _, err := s.cli.Put(ctx, nodePath, string(contents), clientv3.WithLease(leaseID)); err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// The lease expired between the lookup and the write. Drop it
			// so the caller's next attempt grants a fresh one.
			s.clearEphemeralLease(leaseID)
		}
		return convertError(err, nodePath)
	}
	return nil
}

// ephemeralLease returns the connection's lease, granting and starting
// keepalive for it on first use.
func (s *etcdtopo) ephemeralLease(ctx context.Context) (clientv3.LeaseID, error) {
	s.eph.mu.Lock()
	defer s.eph.mu.Unlock()

	if s.eph.leaseID != 0 {
		return s.eph.leaseID, nil
	}

	lease, err := s.cli.Grant(ctx, int64(leaseTTL))
	if err != nil {
		return 0, err
	}
	// Renewals must outlive the caller's context: they run for the
	// connection's lifetime. Conn.Close tears them down via cli.Close.
	ka, err := s.cli.KeepAlive(ctxutil.Detach(ctx), lease.ID)
	if err != nil {
		return 0, err
	}
	s.eph.leaseID = lease.ID
	go s.watchEphemeralLease(lease.ID, ka)
	return lease.ID, nil
}

// watchEphemeralLease drains keepalive responses — the etcd client stops
// renewing if the channel fills up — and clears the lease once it ends, so
// the next write grants a fresh one.
func (s *etcdtopo) watchEphemeralLease(id clientv3.LeaseID, ka <-chan *clientv3.LeaseKeepAliveResponse) {
	for range ka {
	}
	s.clearEphemeralLease(id)
}

// clearEphemeralLease forgets the lease if it is still the current one.
func (s *etcdtopo) clearEphemeralLease(id clientv3.LeaseID) {
	s.eph.mu.Lock()
	defer s.eph.mu.Unlock()
	if s.eph.leaseID == id {
		s.eph.leaseID = 0
	}
}
