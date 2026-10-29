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

package multipooler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestResolveAdoptedValues(t *testing.T) {
	flagDefaults := adoptedPgctldValues{pgPort: 5432, poolerDir: ""}
	status := &pgctldpb.StatusResponse{Port: 25432, PoolerDir: "/data/pooler-1"}

	t.Run("nothing explicit adopts everything", func(t *testing.T) {
		got := resolveAdoptedValues(flagDefaults, map[string]bool{}, status)
		assert.Equal(t, adoptedPgctldValues{pgPort: 25432, poolerDir: "/data/pooler-1"}, got)
	})

	t.Run("explicit flags win per value", func(t *testing.T) {
		flags := flagDefaults
		flags.pgPort = 5433
		got := resolveAdoptedValues(flags, map[string]bool{"pg-port": true}, status)
		assert.Equal(t, 5433, got.pgPort)
		// Non-explicit values still adopt.
		assert.Equal(t, "/data/pooler-1", got.poolerDir)
	})

	t.Run("empty status values keep flag values", func(t *testing.T) {
		got := resolveAdoptedValues(flagDefaults, map[string]bool{}, &pgctldpb.StatusResponse{})
		assert.Equal(t, flagDefaults, got)
	})
}

func TestRequireSidecarSockets(t *testing.T) {
	const control, wal = "/run/pgbackrest-control/control.sock", "/run/pgbackrest-wal/wal.sock"
	assert.NoError(t, requireSidecarSockets(control, wal))
	assert.ErrorContains(t, requireSidecarSockets("", wal), "--pgbackrest-control-socket")
	assert.ErrorContains(t, requireSidecarSockets(control, ""), "--pgbackrest-wal-socket")
}
