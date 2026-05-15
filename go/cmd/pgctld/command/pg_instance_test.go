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

package command

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewPgInstance_RejectsEmptyPassword verifies that newPgInstance refuses to
// build an instance without a password. pg_hba.conf requires scram-sha-256 for
// every connection, so an empty password would silently produce a pgInstance
// whose psql calls fail later with a confusing auth error.
func TestNewPgInstance_RejectsEmptyPassword(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := PgCtldServiceConfig{
		Port:     5432,
		User:     "postgres",
		Password: "",
	}
	pg, err := newPgInstance(logger, "/tmp/does-not-matter", "/tmp/does-not-matter", cfg)
	require.Error(t, err)
	assert.Nil(t, pg)
	assert.Contains(t, err.Error(), "non-empty password")
}
