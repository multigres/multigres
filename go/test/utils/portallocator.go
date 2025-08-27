// Copyright 2025 The Multigres Authors.
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
	"sync"
)

const (
	// BasePort is the starting port for all tests
	BasePort = 6700
)

var (
	// Global state for port allocation
	portMutex   sync.Mutex
	portCounter int
)

// GetNextPort returns the next available port for tests
func GetNextPort() int {
	portMutex.Lock()
	defer portMutex.Unlock()

	// Simple deterministic approach: just increment and return the next port
	portCounter++
	port := BasePort + portCounter

	return port
}

// GetCounter returns the current port counter (useful for debugging)
func GetCounter() int {
	portMutex.Lock()
	defer portMutex.Unlock()
	return portCounter
}
