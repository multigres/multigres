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

package shardsetup

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWaitForPortFree exercises the three branches of waitForPortFree: a port
// that is already free, a port that becomes free during the wait, and a port
// that stays in use until the timeout.
func TestWaitForPortFree(t *testing.T) {
	// Grab an ephemeral port, then release it so it is free. Loopback bind
	// avoids gosec G102 (the literal all-interfaces ":0" form is flagged). The
	// occupied-path holders below bind the same ":port" waitForPortFree probes,
	// since on BSD/macOS a loopback listener does not collide with an
	// all-interfaces bind.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	// Already free: returns promptly.
	start := time.Now()
	waitForPortFree(t, port, 2*time.Second)
	require.Less(t, time.Since(start), time.Second, "should return promptly when the port is already free")

	// Occupied, then freed mid-wait: returns once the listener closes.
	occupied, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	require.NoError(t, err)
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = occupied.Close()
	}()
	start = time.Now()
	waitForPortFree(t, port, 5*time.Second)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "should wait until the port is released")
	require.Less(t, elapsed, 5*time.Second, "should return well before the timeout once freed")

	// Stays in use: returns after the timeout (warning path) rather than hanging.
	held, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	require.NoError(t, err)
	defer held.Close()
	start = time.Now()
	waitForPortFree(t, port, 300*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond, "should wait out the timeout when the port never frees")
}
