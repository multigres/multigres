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

//go:build linux

package sandbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every failure below returns before anything is confined, so it is safe in
// the test process itself.

func TestRestrictAt_RefusesARulePathThatDoesNotExist(t *testing.T) {
	requireLandlock(t)
	err := restrictAt(Policy{Rules: []Rule{{Path: "/nonexistent-sandbox-rule-path", Access: Read}}}, ABI())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rule path")
}
