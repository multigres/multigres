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

package reserved

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pgserver "github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
)

// fakeReplicationCredentialProvider is a test CredentialProvider that
// reports every role as having the REPLICATION attribute. Used to let
// fakepgserver accept replication-mode startup parameters.
type fakeReplicationCredentialProvider struct{}

func (fakeReplicationCredentialProvider) GetCredentials(_ context.Context, _, _ string) (*pgserver.Credentials, error) {
	return &pgserver.Credentials{
		Hash:              &scram.ScramHash{},
		IsReplicationRole: true,
	}, nil
}

func TestLogicalReplicationConfigAddsStartupParameter(t *testing.T) {
	base := client.Config{
		Parameters: map[string]string{"application_name": "multipooler"},
	}

	cfg := logicalReplicationClientConfig(base)

	assert.Equal(t, "database", cfg.Parameters["replication"])
	assert.Equal(t, "multipooler", cfg.Parameters["application_name"],
		"existing parameters must be preserved")

	_, baseHas := base.Parameters["replication"]
	assert.False(t, baseHas, "must not mutate caller's config")
}

func TestLogicalReplicationConfigHandlesNilParameters(t *testing.T) {
	base := client.Config{}

	cfg := logicalReplicationClientConfig(base)

	assert.Equal(t, "database", cfg.Parameters["replication"])
	assert.Nil(t, base.Parameters, "must not mutate caller's config")
}

func TestNewLogicalReplicationConnTagsReasonAndStartupParam(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	defer conn.Release(ReleaseError) // Pooled wrapper has pool=nil → closes the underlying socket.

	assert.True(t, protoutil.HasLogicalReplicationReason(conn.RemainingReasons()),
		"replication conn must be tagged with ReasonLogicalReplication")
	assert.Equal(t, pgserver.ReplicationLogical, server.LastReplicationMode(),
		"replication=database must have been parsed by the server")
}

func TestNewLogicalReplicationConnIsExemptFromIdleKiller(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	defer conn.Release(ReleaseError)

	assert.Zero(t, conn.InactivityTimeout(),
		"replication conn must carry inactivityTimeout=0 so the idle killer skips it; "+
			"idle teardown is Postgres' wal_sender_timeout's job")
	assert.False(t, conn.IsTimedOut())
}
