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

// The session-state probe reads the backend's real session GUC state in one
// round trip and compares it against the connection's tracked settings label.
// Three sources are combined, because pg_settings alone cannot see everything
// (verified on PostgreSQL 17):
//
//   - 'session' rows: pg_settings WHERE source = 'session' — every defined
//     GUC whose current value was installed by this session (SET,
//     set_config(..., false), including any hidden inside routine bodies).
//     current_setting() is used instead of pg_settings.setting because it
//     returns the SHOW-style display form, which is also what set_config
//     returns in the normalization probe, keeping comparisons
//     apples-to-apples.
//   - 'identity' rows: role and session_authorization are GUC_NO_SHOW_ALL —
//     they NEVER appear in pg_settings — so they are read explicitly.
//     current_setting('role') reports 'none' when no SET ROLE is in effect;
//     session_user is the current session authorization.
//   - 'custom' rows: placeholder GUCs (names with a dot, e.g. 'my.tenant')
//     are also hidden from pg_settings until an extension defines them, so
//     every custom name in the tracked label is read explicitly with
//     current_setting(name, missing_ok := true), which returns NULL when the
//     session has never seen the GUC.
//
// Known blind spot: a custom GUC set behind tracking's back on a connection
// whose label does not contain it is undetectable — placeholder GUCs cannot
// be enumerated from SQL. The creation-time rejection gates remain the
// defense for that class.
const sessionSourceProbe = "SELECT name, current_setting(name), 'session' FROM pg_settings WHERE source = 'session'" +
	" UNION ALL SELECT 'role', pg_catalog.current_setting('role'), 'identity'" +
	" UNION ALL SELECT 'session_authorization', session_user::text, 'identity'"

// sessionStateQuery returns the probe SQL, extending the constant part with
// one 'custom' row per tracked placeholder GUC. customNames must be sorted
// for deterministic SQL. Single quotes are escaped by doubling, as in
// Settings.ApplyQuery.
func sessionStateQuery(customNames []string) string {
	var b strings.Builder
	b.WriteString(sessionSourceProbe)
	for _, name := range customNames {
		escaped := strings.ReplaceAll(name, "'", "''")
		b.WriteString(" UNION ALL SELECT '")
		b.WriteString(escaped)
		b.WriteString("', pg_catalog.current_setting('")
		b.WriteString(escaped)
		b.WriteString("', true), 'custom'")
	}
	return b.String()
}

// SessionStateChecker is the connpool.ConnChecker that verifies a
// connection's session GUC state; it delegates to VerifySessionState. It is
// registered on every regular pool at construction — the pool's
// ScrubInterval decides whether the scrub worker actually runs.
type SessionStateChecker struct{}

// Name implements connpool.ConnChecker.
func (SessionStateChecker) Name() string { return "session_state" }

// Check implements connpool.ConnChecker.
func (SessionStateChecker) Check(ctx context.Context, conn *Conn) (connpool.Divergence, error) {
	return conn.VerifySessionState(ctx)
}

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
// The probe is side-effect-free. The main query only reads state. The
// secondary normalization probe uses set_config(..., is_local := true) in a
// single autocommit statement, so every assignment reverts when the
// statement's implicit transaction ends.
//
// The returned divergence carries GUC names only, never values.
func (c *Conn) VerifySessionState(ctx context.Context) (connpool.Divergence, error) {
	var div connpool.Divergence

	// Idle pool connections are never mid-transaction; refuse to probe one
	// that is, rather than misread transaction-local state as session state.
	if !c.IsIdle() {
		return div, errors.New("connection is not idle")
	}

	var tracked map[string]string
	if s := c.State().GetSettings(); s != nil {
		tracked = s.Vars
	}

	// Custom (placeholder) GUCs in the label must be probed by name; see the
	// probe documentation above. Tracked keys are already canonical
	// (canonicalizeGUCVars at Settings construction).
	var customNames []string
	for name := range tracked {
		if strings.Contains(name, ".") {
			customNames = append(customNames, name)
		}
	}
	sort.Strings(customNames)

	// Plain Query, never QueryWithRetry: a retry reconnects the socket, which
	// resets the very session state being inspected and would mask divergence.
	results, err := c.Query(ctx, sessionStateQuery(customNames))
	if err != nil {
		return div, err
	}
	if len(results) != 1 {
		return div, errors.New("session state probe returned unexpected result count")
	}

	sessionVars := make(map[string]string)
	identity := make(map[string]string)
	customVars := make(map[string]string) // only names the session has a value for
	for _, row := range results[0].StructuredRows() {
		if len(row.Values) != 3 || row.Values[0].IsNull() || row.Values[2].IsNull() {
			return div, errors.New("session state probe returned malformed row")
		}
		name := connstate.CanonicalGUCName(string(row.Values[0]))
		switch src := string(row.Values[2]); src {
		case "session", "identity":
			if row.Values[1].IsNull() {
				return div, errors.New("session state probe returned malformed row")
			}
			if src == "session" {
				sessionVars[name] = string(row.Values[1])
			} else {
				identity[name] = string(row.Values[1])
			}
		case "custom":
			// NULL means the session has never seen this placeholder GUC.
			if !row.Values[1].IsNull() {
				customVars[name] = string(row.Values[1])
			}
		default:
			return div, errors.New("session state probe returned unknown source tag")
		}
	}

	// The identity rows are part of the constant probe; their absence means
	// the result cannot be trusted.
	if len(identity) != 2 {
		return div, errors.New("session state probe returned no identity rows")
	}

	// Ordinary GUCs: compare the session-source rows against the label.
	var candidates []string
	for name, realValue := range sessionVars {
		trackedValue, ok := tracked[name]
		switch {
		case !ok:
			div.Untracked = append(div.Untracked, name)
		case trackedValue != realValue:
			candidates = append(candidates, name)
		}
	}
	for name, trackedValue := range tracked {
		switch {
		case name == "role" || name == "session_authorization":
			continue // identity, handled below
		case strings.Contains(name, "."):
			// Custom GUC: judged via its probed value. A name also present
			// in sessionVars is extension-defined and already judged above.
			if _, defined := sessionVars[name]; defined {
				continue
			}
			realValue, set := customVars[name]
			switch {
			case !set:
				div.Phantom = append(div.Phantom, name)
			case realValue != trackedValue:
				// Placeholder GUCs are plain strings with no display
				// normalization, so a value difference is divergence.
				div.Mismatched = append(div.Mismatched, name)
			}
		default:
			if _, ok := sessionVars[name]; !ok {
				div.Phantom = append(div.Phantom, name)
			}
		}
	}

	c.compareIdentity(&div, tracked, identity)

	if len(candidates) > 0 {
		mismatched, err := c.confirmValueMismatches(ctx, candidates, tracked, sessionVars)
		if err != nil {
			return connpool.Divergence{}, err
		}
		div.Mismatched = append(div.Mismatched, mismatched...)
	}

	sort.Strings(div.Untracked)
	sort.Strings(div.Phantom)
	sort.Strings(div.Mismatched)
	return div, nil
}

// compareIdentity checks the two GUC_NO_SHOW_ALL identity GUCs against the
// label. Role names have no display normalization, so value differences are
// divergence outright.
func (c *Conn) compareIdentity(div *connpool.Divergence, tracked, identity map[string]string) {
	// current_setting('role') reports 'none' when no SET ROLE is in effect.
	realRole := identity["role"]
	trackedRole, hasRole := tracked["role"]
	if !hasRole {
		trackedRole = "none"
	}
	if realRole != trackedRole {
		switch {
		case !hasRole:
			div.Untracked = append(div.Untracked, "role")
		case realRole == "none":
			div.Phantom = append(div.Phantom, "role")
		default:
			div.Mismatched = append(div.Mismatched, "role")
		}
	}

	// With no tracked session_authorization, session_user must be the user
	// this connection authenticated as.
	realAuth := identity["session_authorization"]
	trackedAuth, hasAuth := tracked["session_authorization"]
	if !hasAuth {
		trackedAuth = c.conn.User()
	}
	if realAuth != trackedAuth {
		if !hasAuth {
			div.Untracked = append(div.Untracked, "session_authorization")
		} else {
			div.Mismatched = append(div.Mismatched, "session_authorization")
		}
	}
}

// confirmValueMismatches filters exact-string value mismatches on ordinary
// GUCs down to real divergence by asking PostgreSQL to normalize the tracked
// value: set_config returns the SHOW-style display form, so a tracked
// '65536' whose backend shows '64MB' compares equal after normalization. A
// failed probe fails closed: its candidates are reported as mismatched
// rather than silently cleared.
func (c *Conn) confirmValueMismatches(ctx context.Context, candidates []string, tracked, real map[string]string) ([]string, error) {
	sort.Strings(candidates)

	// Single autocommit statement: each set_config applies is_local := true,
	// so every assignment reverts when the statement's implicit transaction
	// ends. Single quotes are escaped by doubling, as in Settings.ApplyQuery.
	var b strings.Builder
	b.WriteString("SELECT n, pg_catalog.set_config(n, v, true) FROM (VALUES ")
	for i, name := range candidates {
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
		return candidates, nil
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
	var mismatched []string
	for _, name := range candidates {
		norm, ok := normalized[name]
		if !ok || norm != real[name] {
			mismatched = append(mismatched, name)
		}
	}
	return mismatched, nil
}
