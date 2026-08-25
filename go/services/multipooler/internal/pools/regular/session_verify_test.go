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

package regular

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
)

// addSessionState scripts the session-source probe to report the given
// name/value pairs as the backend's real session GUC state.
func addSessionState(server *fakepgserver.Server, vars [][]any) {
	server.AddQuery(sessionSourceQuery, fakepgserver.MakeResult([]string{"name", "current_setting"}, vars))
}

func TestVerifySessionStateClean(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	addSessionState(server, nil)

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifySessionStateMatchingLabel(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	addSessionState(server, [][]any{{"work_mem", "64MB"}, {"search_path", "app, public"}})

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{
		"work_mem":    "64MB",
		"search_path": "app, public",
	}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifySessionStateUntracked(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// A hidden set_config left work_mem session state on a backend whose
	// label only knows search_path.
	addSessionState(server, [][]any{{"work_mem", "123MB"}, {"search_path", "app"}})

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"search_path": "app"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"work_mem"}, div.Untracked)
	assert.Empty(t, div.Phantom)
	assert.Empty(t, div.Mismatched)
}

func TestVerifySessionStateUntrackedOnCleanLabel(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	addSessionState(server, [][]any{{"work_mem", "123MB"}})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"work_mem"}, div.Untracked)
}

func TestVerifySessionStatePhantom(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	addSessionState(server, nil)

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"app.tenant": "acme"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, div.Untracked)
	assert.Equal(t, []string{"app.tenant"}, div.Phantom)
	assert.Empty(t, div.Mismatched)
}

func TestVerifySessionStateNormalizationEqual(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// Tracked '65536' vs displayed '64MB' is a spelling difference, not
	// divergence: the normalization probe maps the tracked value to the same
	// display form.
	addSessionState(server, [][]any{{"work_mem", "64MB"}})
	server.AddQuery(
		"SELECT n, pg_catalog.set_config(n, v, true) FROM (VALUES ('work_mem', '65536')) AS t(n, v)",
		fakepgserver.MakeResult([]string{"n", "set_config"}, [][]any{{"work_mem", "64MB"}}),
	)

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"work_mem": "65536"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifySessionStateValueMismatch(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	addSessionState(server, [][]any{{"work_mem", "123MB"}})
	server.AddQuery(
		"SELECT n, pg_catalog.set_config(n, v, true) FROM (VALUES ('work_mem', '64MB')) AS t(n, v)",
		fakepgserver.MakeResult([]string{"n", "set_config"}, [][]any{{"work_mem", "64MB"}}),
	)

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"work_mem": "64MB"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, div.Untracked)
	assert.Empty(t, div.Phantom)
	assert.Equal(t, []string{"work_mem"}, div.Mismatched)
}

func TestVerifySessionStateIdentityMismatchSkipsProbe(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// role/session_authorization never go through the normalization probe:
	// a differing role name is divergence outright. No set_config query is
	// scripted, so reaching the probe would fail the test.
	addSessionState(server, [][]any{{"role", "bob"}})

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"role": "alice"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"role"}, div.Mismatched)
}

func TestVerifySessionStateNormalizationProbeFailureFailsClosed(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	addSessionState(server, [][]any{{"temp_buffers", "16MB"}})
	// The backend rejects re-applying the tracked value (e.g. the
	// temp_buffers freeze). The probe cannot prove equivalence, so the name
	// is reported as mismatched rather than silently cleared.
	server.AddRejectedQuery(
		"SELECT n, pg_catalog.set_config(n, v, true) FROM (VALUES ('temp_buffers', '8MB')) AS t(n, v)",
		errors.New("invalid value for parameter"),
	)

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"temp_buffers": "8MB"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"temp_buffers"}, div.Mismatched)
}

func TestVerifySessionStateProbeErrorPropagates(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.AddRejectedQuery(sessionSourceQuery, errors.New("backend on fire"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.VerifySessionState(context.Background())
	require.Error(t, err)
}
