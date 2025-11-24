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
	"testing"
)

// GetFreePort returns a port number that was verified free by the OS.
// It binds to localhost:0, lets the OS choose a free port, then closes
// the listener and returns the port number. There's a small race window
// between closing and actual use, but this is much more reliable than
// counter-based allocation, especially with parallel test processes.
func GetFreePort(t *testing.T) int {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}

	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()

	return port
}
