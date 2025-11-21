// Copyright 2025 Supabase, Inc.
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

package utils

import (
	"net"
	"sync"
	"testing"
)

// portCache tracks ports currently allocated to tests to prevent duplicates.
// Ports are added when allocated and removed via t.Cleanup() when tests complete.
var portCache sync.Map

// GetFreePort returns a port number that was verified free by the OS and not
// currently allocated to another test. It uses a global cache to prevent the
// same port from being returned multiple times within the same test process.
// Ports are automatically released from the cache when the test completes via
// t.Cleanup(), preventing memory leaks.
//
// If the OS returns a port that's already in the cache, GetFreePort will keep
// the listener open (to prevent OS reuse) and retry until it gets a unique port.
func GetFreePort(t *testing.T) int {
	t.Helper()

	for {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("failed to allocate free port: %v", err)
		}

		port := lis.Addr().(*net.TCPAddr).Port

		// Try to claim this port atomically
		_, alreadyExists := portCache.LoadOrStore(port, true)
		if !alreadyExists {
			// Successfully claimed this port
			lis.Close()
			t.Cleanup(func() {
				portCache.Delete(port)
			})
			return port
		}

		// Port already in cache - hold this listener open to prevent OS reuse
		// and try again. The defer ensures cleanup when we return with a good port.
		defer lis.Close()
	}
}
