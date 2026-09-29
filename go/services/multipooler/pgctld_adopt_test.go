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
	flagDefaults := adoptedPgctldValues{
		pgPort:             5432,
		poolerDir:          "",
		pgBackRestPort:     8432,
		pgBackRestCertFile: "/certs/pgbackrest.crt",
		pgBackRestKeyFile:  "/certs/pgbackrest.key",
		pgBackRestCAFile:   "/certs/ca.crt",
	}
	status := &pgctldpb.StatusResponse{
		Port:              25432,
		PoolerDir:         "/data/pooler-1",
		PgbackrestPort:    43227,
		PgbackrestCertDir: "/tmp/certs",
	}

	t.Run("nothing explicit adopts everything", func(t *testing.T) {
		got := resolveAdoptedValues(flagDefaults, map[string]bool{}, status)
		assert.Equal(t, adoptedPgctldValues{
			pgPort:             25432,
			poolerDir:          "/data/pooler-1",
			pgBackRestPort:     43227,
			pgBackRestCertFile: "/tmp/certs/pgbackrest.crt",
			pgBackRestKeyFile:  "/tmp/certs/pgbackrest.key",
			pgBackRestCAFile:   "/tmp/certs/ca.crt",
		}, got)
	})

	t.Run("explicit flags win per value", func(t *testing.T) {
		flags := flagDefaults
		flags.pgPort = 5433
		flags.pgBackRestCAFile = "/my/ca.crt"
		got := resolveAdoptedValues(flags, map[string]bool{"pg-port": true, "pgbackrest-ca-file": true}, status)
		assert.Equal(t, 5433, got.pgPort)
		assert.Equal(t, "/my/ca.crt", got.pgBackRestCAFile)
		// Non-explicit values still adopt.
		assert.Equal(t, "/data/pooler-1", got.poolerDir)
		assert.Equal(t, "/tmp/certs/pgbackrest.crt", got.pgBackRestCertFile)
	})

	t.Run("empty status values keep flag values", func(t *testing.T) {
		got := resolveAdoptedValues(flagDefaults, map[string]bool{}, &pgctldpb.StatusResponse{})
		assert.Equal(t, flagDefaults, got)
	})
}
