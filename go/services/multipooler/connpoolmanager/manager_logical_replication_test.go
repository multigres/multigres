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

package connpoolmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pgserver "github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
)

// fakeReplicationCredentialProvider returns IsReplicationRole=true for every
// role so fakepgserver accepts replication-mode startup parameters.
type fakeReplicationCredentialProvider struct{}

func (fakeReplicationCredentialProvider) GetCredentials(_ context.Context, _, _ string) (*pgserver.Credentials, error) {
	return &pgserver.Credentials{
		Hash:              &scram.ScramHash{},
		IsReplicationRole: true,
	}, nil
}

func TestManagerNewLogicalReplicationConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	mgr := newTestManager(t, server)
	defer mgr.Close()

	const user = "test_user"
	conn, err := mgr.NewLogicalReplicationConn(context.Background(), user, nil, nil)
	require.NoError(t, err)
	defer conn.Release(reserved.ReleaseError)

	assert.True(t, protoutil.HasLogicalReplicationReason(conn.RemainingReasons()),
		"replication conn must be tagged with ReasonLogicalReplication")
	assert.Equal(t, pgserver.ReplicationLogical, server.LastReplicationMode(),
		"replication=database must have been sent in the startup message")

	// The factory must create a per-user pool, not place the replication conn
	// on the shared admin pool.
	assert.True(t, mgr.HasUserPool(user),
		"replication conn must be checked out from the user's pool, not the admin pool")
}
