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
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn/backingconntest"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// newTestMultipooler builds a Multipooler with just enough servenv wiring for flag and
// configuration tests; it is never started.
func newTestMultipooler() *Multipooler {
	reg := viperutil.NewRegistry()
	grpcServer := servenv.NewGrpcServer(reg)
	return NewMultipooler(telemetry.NewTelemetry(), reg, servenv.ProcessResources{
		ServEnv:         servenv.NewServEnv(reg),
		GrpcServer:      grpcServer,
		QueryGrpcServer: grpcServer,
	}, "/", false)
}

func TestLeadershipModeFor(t *testing.T) {
	for _, tc := range []struct {
		backingConnection string
		staticLeader      bool
		want              clustermetadatapb.LeadershipMode
	}{
		{"", false, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS},
		{"", true, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC},
		{"src", false, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED},
	} {
		got, err := leadershipModeFor(tc.backingConnection, tc.staticLeader)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
	_, err := leadershipModeFor("src", true)
	require.ErrorContains(t, err, "static leader")
}

func TestValidateBackendFlags(t *testing.T) {
	none := func(string) bool { return false }
	only := func(name string) func(string) bool { return func(n string) bool { return n == name } }

	// Managed poolers may set any of the local-postgres flags.
	require.NoError(t, validateBackendFlags("", none))
	require.NoError(t, validateBackendFlags("", only("pg-port")))

	// An unmanaged pooler has no pgctld, no local postgres and no backups: every flag that
	// belongs to them is refused, the ones pgctld's Status is adopted into included.
	require.NoError(t, validateBackendFlags("src", none))
	for _, name := range managedPostgresFlags() {
		require.ErrorContains(t, validateBackendFlags("src", only(name)), "--"+name, name)
	}
	require.ElementsMatch(t, []string{
		"socket-file", "pooler-dir", "pg-port", "pgctld-addr", "pgbackrest-port",
		"pgbackrest-cert-file", "pgbackrest-key-file", "pgbackrest-ca-file", "pgbackrest-cipher-key-file",
	}, managedPostgresFlags())
}

// TestManagedPostgresFlagsAreRegisteredFlags: the list is of real flags, so that renaming one
// cannot leave it silently unchecked.
func TestManagedPostgresFlagsAreRegisteredFlags(t *testing.T) {
	mp := newTestMultipooler()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	for _, name := range managedPostgresFlags() {
		require.NotNil(t, flags.Lookup(name), name)
	}
}

func TestBackingConnectionValidate(t *testing.T) {
	for _, tc := range backingconntest.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			c := backingConnection{Host: tc.Host, Port: tc.Port, Database: tc.Database, User: tc.User}
			if tc.Refused == "" {
				require.NoError(t, c.Validate())
				return
			}
			require.ErrorContains(t, c.Validate(), tc.Refused)
		})
	}
}

type fakeSource struct {
	conn *backingConnection
}

func (f fakeSource) BackingConnection(_ context.Context, name string) (*backingConnection, error) {
	if name != "src" {
		return nil, errors.New("unknown connection")
	}
	return f.conn, nil
}

func TestConnectionSourceIsAnExplicitSeam(t *testing.T) {
	// With nothing configured an unmanaged pooler refuses to start, saying why.
	mp := newTestMultipooler()
	_, err := mp.connections.BackingConnection(t.Context(), "src")
	require.ErrorContains(t, err, "no source of backing connections")

	want := &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "app", User: "u"}
	mp.connections = fakeSource{conn: want}
	got, err := mp.connections.BackingConnection(t.Context(), "src")
	require.NoError(t, err)
	require.Same(t, want, got)
}

// TestAConnectionIsUsedOnlyIfTheSourceGaveAUsableOne: whatever the source is, nothing, an
// incomplete connection and a connection that names several servers are refused, and the
// refusal is permanent.
func TestAConnectionIsUsedOnlyIfTheSourceGaveAUsableOne(t *testing.T) {
	good := &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "app", User: "u"}
	got, err := fetchBackingConnection(t.Context(), fakeSource{conn: good}, "src")
	require.NoError(t, err)
	require.Same(t, good, got)

	_, err = fetchBackingConnection(t.Context(), fakeSource{}, "src")
	require.ErrorIs(t, err, errInvalidBackingConnection, "a source that answers with nothing")

	incomplete := *good
	incomplete.User = ""
	_, err = fetchBackingConnection(t.Context(), fakeSource{conn: &incomplete}, "src")
	require.ErrorIs(t, err, errInvalidBackingConnection)

	hostList := *good
	hostList.Host = "a.example.com,b.example.com"
	_, err = fetchBackingConnection(t.Context(), fakeSource{conn: &hostList}, "src")
	require.ErrorIs(t, err, errInvalidBackingConnection)

	withCredentials := *good
	withCredentials.Host = "alice@db.example.com"
	_, err = fetchBackingConnection(t.Context(), fakeSource{conn: &withCredentials}, "src")
	require.ErrorIs(t, err, errInvalidBackingConnection)

	_, err = fetchBackingConnection(t.Context(), fakeSource{conn: good}, "other")
	require.ErrorContains(t, err, "unknown connection")
	require.NotErrorIs(t, err, errInvalidBackingConnection, "a source's own failure is not a verdict on the connection")
}

// TestIPv6HostsAreValidAndKeepTheirColons: a literal IPv6 address is a host, and is dialed as
// one (see the pgprotocol client).
func TestIPv6HostsAreValidAndKeepTheirColons(t *testing.T) {
	c := backingConnection{Host: "::1", Port: 5432, Database: "app", User: "u"}
	require.NoError(t, c.Validate())
}

func TestResolveLeadershipMode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		static  bool
		want    clustermetadatapb.LeadershipMode
		wantErr string
	}{
		{name: "managed by default", want: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS},
		{name: "managed keeps local flags", args: []string{"--pg-port=5433", "--pooler-dir=/tmp/p"}, want: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS},
		{name: "static leader", static: true, want: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC},
		{name: "backing connection", args: []string{"--backing-connection=src"}, want: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED},
		{name: "static leader with backing connection", args: []string{"--backing-connection=src"}, static: true, wantErr: "static leader"},
		{name: "socket file", args: []string{"--backing-connection=src", "--socket-file=/tmp/s"}, wantErr: "--socket-file"},
		{name: "pooler dir", args: []string{"--backing-connection=src", "--pooler-dir=/tmp/p"}, wantErr: "--pooler-dir"},
		{name: "pg port", args: []string{"--backing-connection=src", "--pg-port=5432"}, wantErr: "--pg-port"},
		{name: "pgctld address", args: []string{"--backing-connection=src", "--pgctld-addr=localhost:1"}, wantErr: "--pgctld-addr"},
		{name: "pgbackrest port", args: []string{"--backing-connection=src", "--pgbackrest-port=8432"}, wantErr: "--pgbackrest-port"},
		{name: "pgbackrest cipher key file", args: []string{"--backing-connection=src", "--pgbackrest-cipher-key-file=/tmp/k"}, wantErr: "--pgbackrest-cipher-key-file"},
		{name: "managed keeps pgbackrest flags", args: []string{"--pgbackrest-port=8432", "--pgctld-addr=localhost:1"}, want: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := newTestMultipooler()
			mp.staticLeader = tc.static
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			mp.RegisterFlags(flags)
			require.NoError(t, flags.Parse(tc.args))
			got, err := mp.resolveLeadershipMode()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestAPoolerStaysManagedOrUnmanaged: a pooler id stays managed or unmanaged for life. A
// restart that would flip a registered pooler between managed and unmanaged is refused; a
// first start, or a restart that stays on the same side, is not.
func TestAPoolerStaysManagedOrUnmanaged(t *testing.T) {
	const (
		consensus   = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS
		static      = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
		unmanaged   = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED
		unspecified = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNSPECIFIED
		unknown     = clustermetadatapb.LeadershipMode(99)
	)
	for name, tc := range map[string]struct {
		registered *clustermetadatapb.LeadershipMode
		starting   clustermetadatapb.LeadershipMode
		refused    bool
	}{
		"first start, consensus":        {nil, consensus, false},
		"first start, unmanaged":        {nil, unmanaged, false},
		"restart, consensus":            {new(consensus), consensus, false},
		"restart, unmanaged":            {new(unmanaged), unmanaged, false},
		"legacy record, consensus":      {new(unspecified), consensus, false},
		"static restarted consensus":    {new(static), consensus, false},
		"consensus restarted unmanaged": {new(consensus), unmanaged, true},
		"legacy record, unmanaged":      {new(unspecified), unmanaged, true},
		"unmanaged restarted consensus": {new(unmanaged), consensus, true},
		"unknown mode, consensus":       {new(unknown), consensus, true},
		"unknown mode, unmanaged":       {new(unknown), unmanaged, false},
	} {
		t.Run(name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			t.Cleanup(func() { _ = ts.Close() })
			id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p"}
			if tc.registered != nil {
				require.NoError(t, ts.CreateMultipooler(t.Context(), &clustermetadatapb.Multipooler{Id: id, LeadershipMode: *tc.registered}))
			}
			err := requireUnchangedManagement(t.Context(), ts, id, tc.starting, slog.Default())
			if tc.refused {
				require.ErrorContains(t, err, "cannot start as")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAnUnreadableRecordRefusesOnlyAnUnmanagedPooler: a managed pooler still starts when its
// topology record cannot be read, as it does without the managed-or-unmanaged check; an unmanaged
// one, whose bootstrap needs the topology anyway, does not.
func TestAnUnreadableRecordRefusesOnlyAnUnmanagedPooler(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p"}
	factory.SetError(errors.New("topology unreachable"))

	require.NoError(t, requireUnchangedManagement(t.Context(), ts, id,
		clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS, slog.Default()))
	require.Error(t, requireUnchangedManagement(t.Context(), ts, id,
		clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, slog.Default()))
}

// countingSource counts the fetches it is asked for.
type countingSource struct{ fetches int }

func (c *countingSource) BackingConnection(context.Context, string) (*backingConnection, error) {
	c.fetches++
	return nil, errors.New("not reached")
}

// TestInitRefusesAShardItCannotServeBeforeFetchingTheBackingConnection: a bad shard name, a
// missing one, and the default tablegroup, which an unmanaged pooler never serves, are all
// refused before the pooler asks anything for its backing connection.
func TestInitRefusesAShardItCannotServeBeforeFetchingTheBackingConnection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tableGroup string
		shard      string
		wantErr    string
	}{
		{"bad shard name", "migrateTG", "foo", "foo"},
		{"no shard", "migrateTG", "", "shard is required"},
		{"no tablegroup", "", "0-inf", "table group is required"},
		{"default tablegroup", "default", "0-inf", "default tablegroup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			t.Cleanup(func() { _ = ts.Close() })
			source := &countingSource{}
			mp := newTestMultipooler()
			mp.connections = source
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			mp.RegisterFlags(flags)
			require.NoError(t, flags.Parse([]string{
				"--backing-connection=src", "--database=db", "--cell=zone1", "--service-id=ext",
				"--table-group=" + tc.tableGroup, "--shard=" + tc.shard,
			}))
			err := mp.Init(t.Context(), ts)
			require.ErrorContains(t, err, tc.wantErr)
			require.Zero(t, source.fetches)
		})
	}
}

// TestAPoolerRefusesABackingConnectionThatServesAnotherDatabase: the pooler advertises
// --database to the cluster and dials the connection's, so the two must agree, and a pooler
// whose connection serves another database refuses to start.
func TestAPoolerRefusesABackingConnectionThatServesAnotherDatabase(t *testing.T) {
	backend := &manager.ExternalBackend{Host: "db.example.com", Port: 5432, Database: "other"}
	require.NoError(t, checkBackingDatabase("src", backend, "other"))
	require.EqualError(t, checkBackingDatabase("src", backend, "db"),
		`backing connection "src" serves database "other", which does not match --database "db"`)

	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	mp := newTestMultipooler()
	mp.connections = fakeSource{conn: &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "other", User: "u"}}
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	require.NoError(t, flags.Parse([]string{
		"--backing-connection=src", "--database=db", "--cell=zone1", "--service-id=ext",
		"--table-group=migrateTG", "--shard=0-inf",
	}))
	err := mp.Init(t.Context(), ts)
	require.EqualError(t, err, `backing connection "src" serves database "other", which does not match --database "db"`)
}
