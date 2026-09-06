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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
)

// identityRows is the backend's identity state for a connection with no SET
// ROLE / SET SESSION AUTHORIZATION in effect: role reports 'none' and
// session_user is the fakepgserver login user ("test").
func identityRows() [][]any {
	return [][]any{
		{"role", "none", "identity"},
		{"session_authorization", "test", "identity"},
	}
}

// scriptProbe scripts the session-state probe for the given tracked custom
// GUC names, reporting the given (name, value, src) rows.
func scriptProbe(server *fakepgserver.Server, customNames []string, rows [][]any) {
	server.AddQuery(sessionStateQuery(customNames),
		fakepgserver.MakeResult([]string{"name", "current_setting", "src"}, rows))
}

func TestVerifySessionStateClean(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, identityRows())

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifySessionStateMatchingLabel(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, append([][]any{
		{"work_mem", "64MB", "session"},
		{"search_path", "app, public", "session"},
	}, identityRows()...))

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
	scriptProbe(server, nil, append([][]any{
		{"work_mem", "123MB", "session"},
		{"search_path", "app", "session"},
	}, identityRows()...))

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
	scriptProbe(server, nil, append([][]any{
		{"work_mem", "123MB", "session"},
	}, identityRows()...))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"work_mem"}, div.Untracked)
}

func TestVerifySessionStatePhantom(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, identityRows())

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"work_mem": "64MB"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, div.Untracked)
	assert.Equal(t, []string{"work_mem"}, div.Phantom)
	assert.Empty(t, div.Mismatched)
}

func TestVerifySessionStateNormalizationEqual(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// Tracked '65536' vs displayed '64MB' is a spelling difference, not
	// divergence: the normalization probe maps the tracked value to the same
	// display form.
	scriptProbe(server, nil, append([][]any{
		{"work_mem", "64MB", "session"},
	}, identityRows()...))
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

func TestVerifySessionStateBackslashValueQuotedAsEscapeString(t *testing.T) {
	// Tracked values are client-controlled. A backslash before a quote would
	// break out of a quote-only-escaped literal under
	// standard_conforming_strings=off, so the normalization probe must emit
	// an E'...' literal with the backslash doubled.
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, append([][]any{
		{"application_name", "x", "session"},
	}, identityRows()...))
	server.AddQuery(
		`SELECT n, pg_catalog.set_config(n, v, true) FROM (VALUES ('application_name', E'a\\''; SELECT 1--')) AS t(n, v)`,
		fakepgserver.MakeResult([]string{"n", "set_config"}, [][]any{{"application_name", "x"}}),
	)

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"application_name": `a\'; SELECT 1--`}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestSessionStateQueryQuotesCustomNames(t *testing.T) {
	assert.Equal(t,
		constants.SessionSourceProbeSQL+
			` UNION ALL SELECT E'my\\''x', pg_catalog.current_setting(E'my\\''x', true), 'custom'`+
			` UNION ALL SELECT 'my.tenant', pg_catalog.current_setting('my.tenant', true), 'custom'`,
		sessionStateQuery([]string{`my\'x`, "my.tenant"}))
}

func TestVerifySessionStateValueMismatch(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, append([][]any{
		{"work_mem", "123MB", "session"},
	}, identityRows()...))
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

func TestVerifySessionStateNormalizationProbeFailureFailsClosed(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, append([][]any{
		{"temp_buffers", "16MB", "session"},
	}, identityRows()...))
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
	server.AddRejectedQuery(sessionStateQuery(nil), errors.New("backend on fire"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.VerifySessionState(context.Background())
	require.Error(t, err)
}

// --- Identity (role / session_authorization) ---
//
// Both are GUC_NO_SHOW_ALL: they never appear in pg_settings, so the probe
// reads them explicitly and compares them against the label without the
// phantom logic used for ordinary GUCs.

func TestVerifySessionStateTrackedRoleMatches(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, [][]any{
		{"role", "alice", "identity"},
		{"session_authorization", "test", "identity"},
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"role": "alice"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifySessionStateUntrackedRole(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// A hidden SET ROLE on a clean-labeled backend.
	scriptProbe(server, nil, [][]any{
		{"role", "sneaky", "identity"},
		{"session_authorization", "test", "identity"},
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"role"}, div.Untracked)
}

func TestVerifySessionStatePhantomRole(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// The label claims a role but the backend has none in effect.
	scriptProbe(server, nil, identityRows())

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"role": "alice"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"role"}, div.Phantom)
}

func TestVerifySessionStateRoleValueMismatch(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, [][]any{
		{"role", "bob", "identity"},
		{"session_authorization", "test", "identity"},
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"role": "alice"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"role"}, div.Mismatched)
}

func TestVerifySessionStateSessionAuthDivergence(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// session_user differs from the login user with nothing tracked: a
	// hidden SET SESSION AUTHORIZATION.
	scriptProbe(server, nil, [][]any{
		{"role", "none", "identity"},
		{"session_authorization", "other", "identity"},
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"session_authorization"}, div.Untracked)
}

func TestVerifySessionStateTrackedSessionAuthMatches(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, nil, [][]any{
		{"role", "none", "identity"},
		{"session_authorization", "alice", "identity"},
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"session_authorization": "alice"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

// --- Custom (placeholder) GUCs ---
//
// Placeholder GUCs are hidden from pg_settings until an extension defines
// them, so tracked custom names are probed explicitly with
// current_setting(name, missing_ok := true).

func TestVerifySessionStateCustomGucMatches(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, []string{"my.tenant"}, append(identityRows(),
		[]any{"my.tenant", "acme", "custom"}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"my.tenant": "acme"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifySessionStateCustomGucPhantom(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// NULL from current_setting(name, true): the session never saw the GUC.
	scriptProbe(server, []string{"my.tenant"}, append(identityRows(),
		[]any{"my.tenant", nil, "custom"}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"my.tenant": "acme"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"my.tenant"}, div.Phantom)
}

func TestVerifySessionStateCustomGucMismatch(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptProbe(server, []string{"my.tenant"}, append(identityRows(),
		[]any{"my.tenant", "evil", "custom"}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"my.tenant": "acme"}, 1))

	div, err := conn.VerifySessionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"my.tenant"}, div.Mismatched)
}
