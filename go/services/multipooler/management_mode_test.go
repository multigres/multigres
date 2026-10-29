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

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

func TestManagementModeFor(t *testing.T) {
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED, managementModeFor(""))
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, managementModeFor("src"))
}

func TestValidateBackendFlags(t *testing.T) {
	none := func(string) bool { return false }
	only := func(name string) func(string) bool { return func(n string) bool { return n == name } }

	// Managed poolers may set any of the local-postgres flags.
	require.NoError(t, validateBackendFlags("", none))
	require.NoError(t, validateBackendFlags("", only("pg-port")))

	require.NoError(t, validateBackendFlags("src", none))
	for _, name := range []string{"socket-file", "pooler-dir", "pg-port"} {
		require.ErrorContains(t, validateBackendFlags("src", only(name)), "--"+name, name)
	}
}

func TestBackingConnectionValidate(t *testing.T) {
	good := backingConnection{Host: "db.example.com", Port: 5432, Database: "app", User: "u"}
	require.NoError(t, good.Validate())
	for name, mutate := range map[string]func(*backingConnection){
		"empty host":      func(c *backingConnection) { c.Host = "" },
		"host with path":  func(c *backingConnection) { c.Host = "/tmp/socket" },
		"host with creds": func(c *backingConnection) { c.Host = "u@db" },
		"host list":       func(c *backingConnection) { c.Host = "a.example.com,b.example.com" },
		"port zero":       func(c *backingConnection) { c.Port = 0 },
		"port too large":  func(c *backingConnection) { c.Port = 65536 },
		"no database":     func(c *backingConnection) { c.Database = "" },
		"no user":         func(c *backingConnection) { c.User = "" },
	} {
		c := good
		mutate(&c)
		require.Error(t, c.Validate(), name)
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

func TestResolveManagementMode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    clustermetadatapb.PoolerManagementMode
		wantErr string
	}{
		{name: "managed by default", want: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED},
		{name: "managed keeps local flags", args: []string{"--pg-port=5433", "--pooler-dir=/tmp/p"}, want: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED},
		{name: "backing connection", args: []string{"--backing-connection=src"}, want: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED},
		{name: "socket file", args: []string{"--backing-connection=src", "--socket-file=/tmp/s"}, wantErr: "--socket-file"},
		{name: "pooler dir", args: []string{"--backing-connection=src", "--pooler-dir=/tmp/p"}, wantErr: "--pooler-dir"},
		{name: "pg port", args: []string{"--backing-connection=src", "--pg-port=5432"}, wantErr: "--pg-port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := newTestMultipooler()
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			mp.RegisterFlags(flags)
			require.NoError(t, flags.Parse(tc.args))
			got, err := mp.resolveManagementMode()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestAPoolerKeepsTheManagementModeItWasRegisteredWith: a pooler id keeps its management mode
// for life. A restart that would flip a registered pooler between managed and unmanaged is
// refused; a first start, or a restart in the same mode, is not.
func TestAPoolerKeepsTheManagementModeItWasRegisteredWith(t *testing.T) {
	const (
		managed     = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED
		unmanaged   = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
		unspecified = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNSPECIFIED
	)
	for name, tc := range map[string]struct {
		registered *clustermetadatapb.PoolerManagementMode
		starting   clustermetadatapb.PoolerManagementMode
		refused    bool
	}{
		"first start, managed":        {nil, managed, false},
		"first start, unmanaged":      {nil, unmanaged, false},
		"restart, managed":            {new(managed), managed, false},
		"restart, unmanaged":          {new(unmanaged), unmanaged, false},
		"legacy record, managed":      {new(unspecified), managed, false},
		"managed restarted unmanaged": {new(managed), unmanaged, true},
		"legacy record, unmanaged":    {new(unspecified), unmanaged, true},
		"unmanaged restarted managed": {new(unmanaged), managed, true},
	} {
		t.Run(name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			t.Cleanup(func() { _ = ts.Close() })
			id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p"}
			if tc.registered != nil {
				require.NoError(t, ts.CreateMultipooler(t.Context(), &clustermetadatapb.Multipooler{Id: id, ManagementMode: *tc.registered}))
			}
			err := requireUnchangedManagementMode(t.Context(), ts, id, tc.starting, slog.Default())
			if tc.refused {
				require.ErrorContains(t, err, "cannot start as")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAnUnreadableRecordRefusesOnlyAnUnmanagedPooler: a managed pooler still starts when its
// topology record cannot be read, as it does without the management mode check; an unmanaged
// one, whose bootstrap needs the topology anyway, does not.
func TestAnUnreadableRecordRefusesOnlyAnUnmanagedPooler(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p"}
	factory.SetError(errors.New("topology unreachable"))

	require.NoError(t, requireUnchangedManagementMode(t.Context(), ts, id,
		clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED, slog.Default()))
	require.Error(t, requireUnchangedManagementMode(t.Context(), ts, id,
		clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, slog.Default()))
}
