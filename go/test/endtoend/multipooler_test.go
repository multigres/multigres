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

package endtoend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMultiPoolerClientBasic tests the multipooler client utilities
func TestMultiPoolerClientBasic(t *testing.T) {
	// This is a basic test of the client utilities without requiring a full cluster
	// It mainly tests the client creation and error handling

	// Test creating client with invalid address
	_, err := NewMultiPoolerTestClient("invalid-address")
	assert.Error(t, err, "Should fail with invalid address")

	// Test creating client with non-existent server
	_, err = NewMultiPoolerTestClient("localhost:99999")
	assert.Error(t, err, "Should fail when server is not running")
}
