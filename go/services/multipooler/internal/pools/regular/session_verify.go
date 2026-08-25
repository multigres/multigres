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
	"sort"
	"strings"

	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
)

// sessionSourceQuery reads the backend's real session-level GUC state: every
// variable whose current value was installed by this session (SET, SET ROLE,
// SET SESSION AUTHORIZATION, set_config(..., false) — including any hidden
// inside routine bodies). current_setting() is used instead of
// pg_settings.setting because it returns the SHOW-style display form, which is
// also what set_config returns in the normalization probe below, keeping the
// two comparisons apples-to-apples.
const sessionSourceQuery = "SELECT name, current_setting(name) FROM pg_settings WHERE source = 'session'"

// VerifySessionState compares this connection's tracked settings label
// against the backend's real session GUC state and reports any divergence.
//
// Under the gateway-authoritative session model the label is trusted
// absolutely: pooled reuse with a pointer-equal label and clean-stack reuse
// both run zero SQL. Anything that mutates session state without being
// tracked (a set_config hidden in a routine body, a tracking bug) therefore
// leaks to the next borrower. This probe is the detection net for that class
// of failure; it runs on idle pooled connections only.
//
// The probe is side-effect-free. The main query only reads pg_settings. The
// secondary normalization probe uses set_config(..., is_local := true) in a
// single autocommit statement, so every assignment reverts when the
// statement's implicit transaction ends.
//
// The returned divergence carries GUC names only, never values.
func (c *Conn) VerifySessionState(ctx context.Context) (connpool.SessionDivergence, error) {
	var div connpool.SessionDivergence

	// Idle pool connections are never mid-transaction; refuse to probe one
	// that is, rather than misread transaction-local state as session state.
	if !c.IsIdle() {
		return div, errors.New("connection is not idle")
	}

	var tracked map[string]string
	if s := c.State().GetSettings(); s != nil {
		tracked = s.Vars
	}

	// Plain Query, never QueryWithRetry: a retry reconnects the socket, which
	// resets the very session state being inspected and would mask divergence.
	results, err := c.Query(ctx, sessionSourceQuery)
	if err != nil {
		return div, err
	}
	if len(results) != 1 {
		return div, errors.New("session state probe returned unexpected result count")
	}

	real := make(map[string]string, results[0].RowCount())
	for _, row := range results[0].StructuredRows() {
		if len(row.Values) != 2 || row.Values[0].IsNull() || row.Values[1].IsNull() {
			return div, errors.New("session state probe returned malformed row")
		}
		real[connstate.CanonicalGUCName(string(row.Values[0]))] = string(row.Values[1])
	}

	// Tracked keys are already canonical (canonicalizeGUCVars at Settings
	// construction), so name comparison is a direct map lookup.
	var candidates []string
	for name, realValue := range real {
		trackedValue, ok := tracked[name]
		switch {
		case !ok:
			div.Untracked = append(div.Untracked, name)
		case trackedValue != realValue:
			candidates = append(candidates, name)
		}
	}
	for name := range tracked {
		if _, ok := real[name]; !ok {
			div.Phantom = append(div.Phantom, name)
		}
	}

	if len(candidates) > 0 {
		mismatched, err := c.confirmValueMismatches(ctx, candidates, tracked, real)
		if err != nil {
			return connpool.SessionDivergence{}, err
		}
		div.Mismatched = mismatched
	}

	sort.Strings(div.Untracked)
	sort.Strings(div.Phantom)
	sort.Strings(div.Mismatched)
	return div, nil
}

// confirmValueMismatches filters exact-string value mismatches down to real
// divergence by asking PostgreSQL to normalize the tracked value: set_config
// returns the SHOW-style display form, so a tracked '65536' whose backend
// shows '64MB' compares equal after normalization. Identity GUCs skip the
// probe (role names have no display normalization, and set_config on them can
// fail on permissions rather than value), and a failed probe fails closed:
// its candidates are reported as mismatched rather than silently cleared.
func (c *Conn) confirmValueMismatches(ctx context.Context, candidates []string, tracked, real map[string]string) ([]string, error) {
	var mismatched, probeNames []string
	for _, name := range candidates {
		switch name {
		case "role", "session_authorization":
			mismatched = append(mismatched, name)
		default:
			probeNames = append(probeNames, name)
		}
	}
	if len(probeNames) == 0 {
		return mismatched, nil
	}
	sort.Strings(probeNames)

	// Single autocommit statement: each set_config applies is_local := true,
	// so every assignment reverts when the statement's implicit transaction
	// ends. Single quotes are escaped by doubling, as in Settings.ApplyQuery.
	var b strings.Builder
	b.WriteString("SELECT n, pg_catalog.set_config(n, v, true) FROM (VALUES ")
	for i, name := range probeNames {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("('")
		b.WriteString(strings.ReplaceAll(name, "'", "''"))
		b.WriteString("', '")
		b.WriteString(strings.ReplaceAll(tracked[name], "'", "''"))
		b.WriteString("')")
	}
	b.WriteString(") AS t(n, v)")

	results, err := c.Query(ctx, b.String())
	if err != nil {
		// A dead connection can't be judged; surface the error so the caller
		// treats this as a probe failure, not divergence.
		if c.IsClosed() {
			return nil, err
		}
		// The statement itself failed (e.g. a value the backend now rejects):
		// fail closed and report every probed name as mismatched.
		return append(mismatched, probeNames...), nil
	}
	if len(results) != 1 {
		return nil, errors.New("normalization probe returned unexpected result count")
	}

	normalized := make(map[string]string, results[0].RowCount())
	for _, row := range results[0].StructuredRows() {
		if len(row.Values) != 2 || row.Values[0].IsNull() || row.Values[1].IsNull() {
			return nil, errors.New("normalization probe returned malformed row")
		}
		normalized[connstate.CanonicalGUCName(string(row.Values[0]))] = string(row.Values[1])
	}
	for _, name := range probeNames {
		norm, ok := normalized[name]
		if !ok || norm != real[name] {
			mismatched = append(mismatched, name)
		}
	}
	return mismatched, nil
}
