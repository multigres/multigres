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
	"os"
	"sync"
)

const (
	// BasePort is the starting port for all tests
	BasePort    = 6700
	portsPerPid = 500
)

var (
	// Global state for port allocation
	portMutex      sync.Mutex
	portCounter    int
	basePortOffset int
)

func init() {
	// Use process ID to create unique port ranges for different test processes
	// This prevents conflicts when running tests from different packages simultaneously
	pid := os.Getpid()
	// Give each test process enough ports for multiple zones. Tests assume that each
	// zone can use up to 100 ports.
	//
	// This generates a max port number of 6700 + (99 * 500) + 200 = 56400 (< 65535)
	basePortOffset = (pid % 100) * portsPerPid
}

// GetNextPort returns the next available port for tests
func GetNextPort() int {
	portMutex.Lock()
	defer portMutex.Unlock()

	// Simple deterministic approach: just increment and return the next port
	portCounter++
	port := BasePort + basePortOffset + portCounter
	return port
}

// GetNextEtcd2Port returns the next available port for tests
func GetNextEtcd2Port() int {
	port := GetNextPort()
	// Let's skip two ports as they will be use by etcd.
	// This way other services will not conflict with etcd.
	GetNextPort()
	GetNextPort()
	return port
}
