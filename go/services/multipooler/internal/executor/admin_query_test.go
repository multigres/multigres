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

package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
)

// The QueryAdmin* methods are the access path for the locked-down multigres
// sidecar schema. These tests exercise them against a real connpoolmanager.Manager
// (whose admin pool dials the fake server), the same wiring production uses.

func TestQueryAdmin(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	res, err := e.QueryAdmin(ctx, "SELECT 1 FROM multigres.heartbeat")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 1, server.GetQueryCalledNum("SELECT 1 FROM multigres.heartbeat"))
}

func TestQueryAdminArgs(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	res, err := e.QueryAdminArgs(ctx, "SELECT oid FROM multigres.tablegroup WHERE name = $1", "default")
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestQueryAdminMultiStatement(t *testing.T) {
	server := fakepgserver.New(t)
	server.SetNeverFail(true)

	e := newInternalQueryTestExecutor(t, server)
	ctx := context.Background()

	err := e.QueryAdminMultiStatement(ctx, "CREATE SCHEMA multigres; GRANT USAGE ON SCHEMA multigres TO PUBLIC")
	require.NoError(t, err)
	assert.Equal(t, 1, server.GetQueryCalledNum("CREATE SCHEMA multigres; GRANT USAGE ON SCHEMA multigres TO PUBLIC"))
}
