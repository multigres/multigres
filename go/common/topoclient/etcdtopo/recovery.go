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
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// recoveryState holds the channel behind Recoveries. The watcher goroutine
// starts on first use, so connections nobody asks about pay nothing.
type recoveryState struct {
	once sync.Once
	ch   chan struct{}
}

// Recoveries is part of the topoclient.ConnRecovery interface.
//
// It reports each time the gRPC connection to etcd leaves READY and comes
// back. The etcd client reconnects on its own, so this is the only point where
// a component learns that the server may have restarted, or come back empty.
//
// A connection that idles out and wakes up looks the same as an outage and
// produces one extra signal. That costs the consumer one cheap check.
func (s *etcdtopo) Recoveries() <-chan struct{} {
	s.recovery.once.Do(func() {
		s.recovery.ch = make(chan struct{}, 1)
		go s.watchConnState(s.cli.ActiveConnection(), s.recovery.ch)
	})
	return s.recovery.ch
}

func (s *etcdtopo) watchConnState(conn *grpc.ClientConn, out chan<- struct{}) {
	defer close(out)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	go func() {
		select {
		case <-s.running:
			cancel()
		case <-ctx.Done():
		}
	}()

	lost := false
	state := conn.GetState()
	for {
		switch state {
		case connectivity.Ready:
			if lost {
				lost = false
				select {
				case out <- struct{}{}:
				default:
				}
			}
		case connectivity.Shutdown:
			return
		default:
			lost = true
		}
		if !conn.WaitForStateChange(ctx, state) {
			return
		}
		state = conn.GetState()
	}
}
