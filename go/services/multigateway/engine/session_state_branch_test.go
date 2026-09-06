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

package engine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

// TestSessionPinned_UnsafeConnection confirms an unsafe connection is always
// treated as pinned (its backend is reserved+quarantined per statement), while a
// plain connection with no transaction/reservation is not.
func TestSessionPinned_UnsafeConnection(t *testing.T) {
	plain := server.NewTestConn(&bytes.Buffer{}).Conn
	assert.False(t, SessionPinned(plain, nil, "tg", "0"),
		"a plain idle connection is not pinned")

	direct := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn
	assert.True(t, SessionPinned(direct, nil, "tg", "0"),
		"an unsafe connection is always pinned")
}
