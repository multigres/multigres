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

package handler

import (
	"fmt"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
)

// gmvSpec is the complete behavior a gateway-managed session variable
// must supply — every dispatch site (set_config canonicalization, SET/RESET,
// SHOW) routes through one of these. A variable is gateway-managed if and only if
// it is in the gatewayManagedVariables registry, which is the single source of
// truth consulted by the planner (routing) and the engine (execution).
//
// The whole point of gathering the behaviors here is completeness: the only way
// to register a variable is newGMVSpec, whose parameters are all the
// behaviors — so adding a gateway-managed variable that forgets one of the
// dispatch sites is a compile error, not a runtime panic discovered later.
//
// Each behavior is a thin adapter over the strongly-typed per-connection state
// (the GatewayManagedVariable[T] fields on MultigatewayConnectionState); the
// enforcement paths that need the typed value (e.g. GetStatementTimeout for the
// query context deadline) keep reading it directly.
//
// Note the deliberate split from GatewayManagedVariable[T] (gateway_managed_variable.go):
// that is the per-connection, mutable, typed *state* of one variable on one
// connection; a gmvSpec is the global, immutable, string-facing *behavior* for a
// variable, and there is exactly one in the registry regardless of connections.
type gmvSpec struct {
	// canonicalize parses value (validating it) and returns the string
	// PostgreSQL's set_config / SHOW would display for it.
	canonicalize func(value string) (string, error)
	// applySet applies `SET var = value` / set_config: session-level when isLocal
	// is false, transaction-local when true. It validates value.
	applySet func(m *MultigatewayConnectionState, value string, isLocal bool)
	// reset clears the session-level override and any transaction-local override
	// (RESET var / SET var TO DEFAULT).
	reset func(m *MultigatewayConnectionState)
	// setLocalToDefault installs a transaction-local override equal to the server
	// default, masking (not destroying) the session value (SET LOCAL var TO DEFAULT).
	setLocalToDefault func(m *MultigatewayConnectionState)
	// showEffective returns the effective value formatted for SHOW.
	showEffective func(m *MultigatewayConnectionState) string
}

// newGMVSpec builds a registry entry. Every behavior is a required
// parameter, so a registration can't compile without all of them.
func newGMVSpec(
	canonicalize func(value string) (string, error),
	applySet func(m *MultigatewayConnectionState, value string, isLocal bool),
	reset func(m *MultigatewayConnectionState),
	setLocalToDefault func(m *MultigatewayConnectionState),
	showEffective func(m *MultigatewayConnectionState) string,
) gmvSpec {
	return gmvSpec{
		canonicalize:      canonicalize,
		applySet:          applySet,
		reset:             reset,
		setLocalToDefault: setLocalToDefault,
		showEffective:     showEffective,
	}
}

// gatewayManagedVariables is the single registry of session variables the gateway
// manages itself: SET / SHOW / RESET are handled locally and the value is never
// written to SessionSettings (so it is not replayed to backends on pool
// rotation). Names compare case-insensitively (keys are lower-case).
var gatewayManagedVariables = map[string]gmvSpec{
	"statement_timeout": newGMVSpec(
		msTimeoutCanonicalize("statement_timeout"),
		msTimeoutApplySet("statement_timeout",
			(*MultigatewayConnectionState).SetStatementTimeout,
			(*MultigatewayConnectionState).SetLocalStatementTimeout),
		(*MultigatewayConnectionState).ResetStatementTimeout,
		(*MultigatewayConnectionState).SetLocalStatementTimeoutToDefault,
		(*MultigatewayConnectionState).ShowStatementTimeout,
	),
	"idle_session_timeout": newGMVSpec(
		msTimeoutCanonicalize("idle_session_timeout"),
		msTimeoutApplySet("idle_session_timeout",
			(*MultigatewayConnectionState).SetIdleSessionTimeout,
			(*MultigatewayConnectionState).SetLocalIdleSessionTimeout),
		(*MultigatewayConnectionState).ResetIdleSessionTimeout,
		(*MultigatewayConnectionState).SetLocalIdleSessionTimeoutToDefault,
		(*MultigatewayConnectionState).ShowIdleSessionTimeout,
	),
}

// msTimeoutCanonicalize builds a canonicalize func for a GUC_UNIT_MS timeout GUC:
// parse to a duration, then format the way PostgreSQL displays it.
func msTimeoutCanonicalize(name string) func(string) (string, error) {
	return func(value string) (string, error) {
		d, err := ParsePostgresInterval(name, value)
		if err != nil {
			return "", err
		}
		return formatDurationPg(d), nil
	}
}

// msTimeoutApplySet builds an applySet func for a GUC_UNIT_MS timeout GUC from the
// variable's typed session/local setters.
func msTimeoutApplySet(
	name string,
	set func(*MultigatewayConnectionState, time.Duration),
	setLocal func(*MultigatewayConnectionState, time.Duration),
) func(*MultigatewayConnectionState, string, bool) {
	return func(m *MultigatewayConnectionState, value string, isLocal bool) {
		// value is validated by canonicalize before this runs (SET/set_config both
		// validate first); ParsePostgresInterval is deterministic, so ignore the
		// error here — an invalid value never reaches applySet.
		d, _ := ParsePostgresInterval(name, value)
		if isLocal {
			setLocal(m, d)
		} else {
			set(m, d)
		}
	}
}

func gmvSpecFor(name string) (gmvSpec, bool) {
	spec, ok := gatewayManagedVariables[strings.ToLower(name)]
	return spec, ok
}

// IsGatewayManagedVariable reports whether name (case-insensitive) is a session
// variable managed entirely by the gateway and not forwarded to PostgreSQL.
func IsGatewayManagedVariable(name string) bool {
	_, ok := gmvSpecFor(name)
	return ok
}

// GatewayManagedCanonicalValue returns the canonical display form of value for a
// gateway-managed variable — the exact string PostgreSQL's set_config(name, v, _)
// would return. The gateway uses it to rewrite a gateway-managed set_config out of
// the query it routes to a backend (which would otherwise persist the real GUC and
// leak it across pooled clients), replacing the call with a constant of this
// value. It returns the same validation error set_config would raise for an
// invalid value, so a bad value still fails rather than being silently accepted.
func GatewayManagedCanonicalValue(name, value string) (string, error) {
	spec, ok := gmvSpecFor(name)
	if !ok {
		return "", gmvDispatchBug(name)
	}
	return spec.canonicalize(value)
}

// ApplyGatewayManagedVariable applies a SET / set_config(...) of a gateway-managed
// variable to gateway-local state instead of SessionSettings. Returns
// (handled=false, nil) when name is not gateway-managed so the caller falls back
// to SessionSettings; (true, err) when value is invalid, mirroring PostgreSQL's
// set-time validation.
func (m *MultigatewayConnectionState) ApplyGatewayManagedVariable(name, value string, isLocal bool) (bool, error) {
	spec, ok := gmvSpecFor(name)
	if !ok {
		return false, nil
	}
	if _, err := spec.canonicalize(value); err != nil {
		return true, err
	}
	spec.applySet(m, value, isLocal)
	return true, nil
}

// SetGatewayManaged applies `SET [LOCAL] <gmv> = value`, validating value.
func (m *MultigatewayConnectionState) SetGatewayManaged(name, value string, isLocal bool) error {
	spec, ok := gmvSpecFor(name)
	if !ok {
		return gmvDispatchBug(name)
	}
	if _, err := spec.canonicalize(value); err != nil {
		return err
	}
	spec.applySet(m, value, isLocal)
	return nil
}

// ResetGatewayManaged applies `RESET <gmv>` / `SET <gmv> TO DEFAULT`.
func (m *MultigatewayConnectionState) ResetGatewayManaged(name string) error {
	spec, ok := gmvSpecFor(name)
	if !ok {
		return gmvDispatchBug(name)
	}
	spec.reset(m)
	return nil
}

// SetGatewayManagedLocalToDefault applies `SET LOCAL <gmv> TO DEFAULT`.
func (m *MultigatewayConnectionState) SetGatewayManagedLocalToDefault(name string) error {
	spec, ok := gmvSpecFor(name)
	if !ok {
		return gmvDispatchBug(name)
	}
	spec.setLocalToDefault(m)
	return nil
}

// ShowGatewayManaged returns the effective value of a gateway-managed variable,
// formatted for SHOW.
func (m *MultigatewayConnectionState) ShowGatewayManaged(name string) (string, error) {
	spec, ok := gmvSpecFor(name)
	if !ok {
		return "", gmvDispatchBug(name)
	}
	return spec.showEffective(m), nil
}

// gmvDispatchBug is returned when a caller reaches a gateway-managed dispatch
// with a name that is not in the registry. Callers gate on IsGatewayManagedVariable
// first, so this is an internal error (XX000), not a user error.
func gmvDispatchBug(name string) error {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		"internal error dispatching gateway-managed variable (please report this as a bug)",
		fmt.Sprintf("%q is not a registered gateway-managed variable", name))
}
