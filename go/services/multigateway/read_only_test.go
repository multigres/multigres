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

package multigateway

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multigateway/readonly"
)

func TestDatabaseFromRecordPath(t *testing.T) {
	for path, want := range map[string]string{
		"databases/postgres/Database":       "postgres",
		"/root/databases/app/Database":      "app",
		"databases/postgres/other":          "",
		"databases/postgres/shards/0/Shard": "",
		"databases//Database":               "",
		"cells/zone1/Cell":                  "",
	} {
		got, ok := databaseFromRecordPath(path)
		require.Equal(t, want != "", ok, path)
		require.Equal(t, want, got, path)
	}
}

func TestWatchReadOnly_MirrorsTopoRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	require.NoError(t, ts.CreateDatabase(ctx, "app", &clustermetadatapb.Database{Name: "app"}))
	require.NoError(t, ts.UpdateDatabaseFields(ctx, "app", func(db *clustermetadatapb.Database) error {
		db.ReadOnly = true
		return nil
	}))

	mg := &Multigateway{ts: ts, readOnly: readonly.New()}
	go mg.watchReadOnly(ctx, slog.Default())

	// The initial snapshot seeds the mode, so a restarted gateway inherits it.
	require.Eventually(t, func() bool { return mg.readOnly.Get("app").Enabled }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, ts.UpdateDatabaseFields(ctx, "app", func(db *clustermetadatapb.Database) error {
		db.ReadOnly = false
		return nil
	}))
	require.Eventually(t, func() bool { return !mg.readOnly.Get("app").Enabled }, 5*time.Second, 10*time.Millisecond)

	// A database created later is picked up by the recursive watch.
	require.NoError(t, ts.CreateDatabase(ctx, "late", &clustermetadatapb.Database{Name: "late", ReadOnly: true}))
	require.Eventually(t, func() bool { return mg.readOnly.Get("late").Enabled }, 5*time.Second, 10*time.Millisecond)
	require.False(t, mg.readOnly.Get("app").Enabled, "modes are independent per database")
}
