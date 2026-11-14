// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn"
)

// A connection with no password is refused before anything is created (see Validate).
func TestCreateMigrationRefusesAConnectionWithoutAPassword(t *testing.T) {
	tc := newTestCoord(t)
	tc.store.conns[defaultConnID].ConnInfo = infoOf("host=attacker.example.com port=5432 user=postgres dbname=app")
	_, err := tc.c.CreateMigration(context.Background(), CreateParams{
		ConnectionName: "src", TargetDatabase: "app", Tables: []string{"public.orders"},
	})
	require.ErrorIs(t, err, ErrInvalidConnection)
	require.ErrorContains(t, err, "password is required")
	require.Empty(t, tc.dialed, "the source is not even dialed")
	require.Empty(t, tc.store.migs, "and nothing is recorded")
	require.NotContains(t, tc.log, "target.CreateSubscription")
}

// Setup and the switch back to IMPORT refuse it too, before anything is created or torn down.
func TestSubscribingRefusesAConnectionWithoutAPassword(t *testing.T) {
	for name, phase := range map[string]Phase{"setup": PhaseCreated, "switch back to IMPORT": PhaseExporting} {
		t.Run(name, func(t *testing.T) {
			tc := newTestCoord(t)
			tc.store.conns[defaultConnID].ConnInfo = infoOf("host=attacker.example.com port=5432 user=postgres dbname=app")
			tc.seed(&Migration{Phase: phase})
			_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
			require.ErrorContains(t, err, "password is required")
			require.Empty(t, tc.dialed)
			require.NotContains(t, tc.log, "source.CreatePublication")
			require.NotContains(t, tc.log, "target.DropTables")
			require.NotContains(t, tc.log, "target.DropLogicalSlot", "nothing is torn down")
			require.NotContains(t, tc.log, "target.CreateSubscription")
		})
	}
}

// What the local postgres is handed is the conninfo rendered from the validated connection, with
// the pins of params.go, and the same connection is what the migrator's own client and pg_dump
// dial. A connection that names a password file, a service or a client certificate is refused
// rather than passed on.
func TestSubscriptionIsGivenTheRenderedValidatedConnInfo(t *testing.T) {
	t.Run("rendered", func(t *testing.T) {
		tc := newTestCoord(t)
		conn, err := backingconn.FromDSN("postgres://repl:p%40ss@src.example.com:5433/appdb?sslmode=disable&connect_timeout=5")
		require.NoError(t, err)
		tc.store.conns[defaultConnID].ConnInfo = conn
		tc.seed(&Migration{Phase: PhaseCreated})
		tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1}

		_, err = tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
		require.NoError(t, err)

		want := conn.LibpqString()
		require.Equal(t, []string{want}, tc.tgt.subscribedWith)
		require.NotEmpty(t, tc.dialed)
		for _, dialed := range tc.dialed {
			require.Equal(t, conn.DSN(), dialed, "the migrator's own client is given the same connection")
		}
		for _, pinned := range []string{"port='5433'", "sslmode='disable'", "connect_timeout='5'", "options=''", "sslcertmode='disable'"} {
			require.Contains(t, want, pinned)
		}
	})
	t.Run("one that names a file is refused", func(t *testing.T) {
		for _, param := range []string{"passfile=/etc/pgbackrest.pgpass", "service=s", "sslcert=/c.pem", "sslkey=/k.pem"} {
			tc := newTestCoord(t)
			tc.store.conns[defaultConnID].ConnInfo = infoOf("host=attacker.example.com port=5432 user=postgres password=pw dbname=app " + param)
			tc.seed(&Migration{Phase: PhaseCreated})
			_, err := tc.c.SetMigrationDirection(context.Background(), Ref{ID: testMigID}, DirectionImport, ActivateOptions{})
			require.ErrorIs(t, err, ErrInvalidConnection, param)
			require.Empty(t, tc.tgt.subscribedWith, param)
			require.Empty(t, tc.dialed, param)
		}
	})
}
