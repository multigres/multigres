// Copyright 2025 Supabase, Inc.
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

package connstate

import (
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgsettings"
	"github.com/multigres/multigres/go/pb/query"
)

// ConnectionState represents the cumulative state of a connection.
// This includes all state modifiers like session settings and prepared statements.
//
// All methods are thread-safe.
type ConnectionState struct {
	// mu protects all mutable fields in this struct.
	mu sync.Mutex

	// Settings contains session variables (SET commands).
	// This is the key for connection pool bucket assignment.
	// The current role is tracked as the "role" variable inside Settings (it is
	// a session GUC), not as a separate field.
	Settings *Settings

	// PreparedStatements stores internal consolidated statements by name.
	// PreparedStatementAliases stores the client-visible aliases materialized on
	// this backend for server-side dynamic EXECUTE.
	PreparedStatements       map[string]*query.PreparedStatement
	PreparedStatementAliases map[string]*query.PreparedStatement

	// failedAliases records aliases whose Parse failed on this backend (dormant
	// aliases referencing dropped objects). Reconciliation skips an alias whose
	// definition still matches its failed entry, so one broken PREPARE costs one
	// backend round trip per backend instead of one per statement — and, inside
	// a transaction, cannot re-abort it. A re-PREPARE with a new body changes
	// the definition and clears the way for a retry.
	failedAliases map[string]*query.PreparedStatement

	// trackedVpid is the gateway virtual pid most recently recorded for this
	// backend in multigres.backend_vpid. It lets the executor skip duplicate
	// upserts within one active gateway/backend association. Cleanup resets the
	// value before recycle/release; a reconnect installs a fresh ConnectionState.
	trackedVpid uint32
}

// NewConnectionState creates a new empty ConnectionState with initialized maps.
func NewConnectionState() *ConnectionState {
	return &ConnectionState{
		PreparedStatements:       make(map[string]*query.PreparedStatement),
		PreparedStatementAliases: make(map[string]*query.PreparedStatement),
	}
}

// NewConnectionStateWithSettings creates a new ConnectionState with the given settings.
func NewConnectionStateWithSettings(settings *Settings) *ConnectionState {
	return &ConnectionState{
		Settings:                 settings,
		PreparedStatements:       make(map[string]*query.PreparedStatement),
		PreparedStatementAliases: make(map[string]*query.PreparedStatement),
	}
}

// Bucket returns the bucket number for this connection state.
// This is used by the connection pool to distribute connections across stacks.
// Returns 0 if there are no settings (clean connection).
func (s *ConnectionState) Bucket() uint32 {
	if s == nil || s.Settings == nil {
		return 0
	}
	return s.Settings.Bucket()
}

// IsClean returns true if this state has no settings modifiers applied.
// Prepared statements and portals are not considered for pool routing.
func (s *ConnectionState) IsClean() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Settings == nil || s.Settings.IsEmpty()
}

// Clone creates a deep copy of this state.
func (s *ConnectionState) Clone() *ConnectionState {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clone := &ConnectionState{
		PreparedStatements:       make(map[string]*query.PreparedStatement, len(s.PreparedStatements)),
		PreparedStatementAliases: make(map[string]*query.PreparedStatement, len(s.PreparedStatementAliases)),
	}

	if s.Settings != nil {
		clone.Settings = s.Settings.Clone()
	}

	maps.Copy(clone.PreparedStatements, s.PreparedStatements)
	maps.Copy(clone.PreparedStatementAliases, s.PreparedStatementAliases)

	return clone
}

// TrackedVpid returns the vpid most recently recorded for this backend in
// multigres.backend_vpid, or 0 if none has been recorded on this session.
func (s *ConnectionState) TrackedVpid() uint32 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trackedVpid
}

// SetTrackedVpid records the vpid whose mapping row was last written for
// this backend.
func (s *ConnectionState) SetTrackedVpid(vpid uint32) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trackedVpid = vpid
}

// Close cleans up the connection state.
func (s *ConnectionState) Close() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Settings = nil
	s.PreparedStatements = nil
	s.PreparedStatementAliases = nil
	s.failedAliases = nil
}

// GetSettings returns the current settings. Returns nil if no settings.
func (s *ConnectionState) GetSettings() *Settings {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Settings
}

// SetSettings sets the settings for this connection state.
func (s *ConnectionState) SetSettings(settings *Settings) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Settings = settings
}

// --- Prepared Statement Methods ---

// StorePreparedStatement stores a prepared statement.
func (s *ConnectionState) StorePreparedStatement(stmt *query.PreparedStatement) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.PreparedStatements[stmt.Name] = stmt
}

// GetPreparedStatement retrieves a prepared statement by name.
func (s *ConnectionState) GetPreparedStatement(name string) *query.PreparedStatement {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.PreparedStatements[name]
}

// DeletePreparedStatement removes a prepared statement by name.
func (s *ConnectionState) DeletePreparedStatement(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.PreparedStatements, name)
}

// PreparedAliases returns a snapshot of client-visible aliases materialized on
// this backend connection.
func (s *ConnectionState) PreparedAliases() map[string]*query.PreparedStatement {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.PreparedStatementAliases)
}

// StorePreparedAlias records a client-visible alias after Parse succeeds.
func (s *ConnectionState) StorePreparedAlias(stmt *query.PreparedStatement) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PreparedStatementAliases[stmt.Name] = stmt
}

// DeletePreparedAlias forgets a client-visible alias after Close succeeds.
func (s *ConnectionState) DeletePreparedAlias(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.PreparedStatementAliases, name)
}

// GetPreparedAlias returns the alias materialized under name, or nil. Unlike
// PreparedAliases it does not clone the map, so single-name reads on the query
// path stay allocation-free.
func (s *ConnectionState) GetPreparedAlias(name string) *query.PreparedStatement {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.PreparedStatementAliases[name]
}

// HasPreparedAliases reports whether any client-visible alias is materialized
// on this backend, without cloning.
func (s *ConnectionState) HasPreparedAliases() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.PreparedStatementAliases) > 0
}

// TakePreparedAliases returns the materialized aliases and clears both the
// alias map and the failed-alias cache. The pool's borrow-time purge uses it
// to remove every client-visible name before a new session sees the backend.
func (s *ConnectionState) TakePreparedAliases() map[string]*query.PreparedStatement {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	taken := s.PreparedStatementAliases
	if len(taken) > 0 {
		s.PreparedStatementAliases = make(map[string]*query.PreparedStatement)
	}
	s.failedAliases = nil
	return taken
}

// FailedAlias returns the failed-Parse record for name, or nil.
func (s *ConnectionState) FailedAlias(name string) *query.PreparedStatement {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failedAliases[name]
}

// StoreFailedAlias records that Parse for this alias definition failed on this
// backend, so reconciliation stops retrying it until the definition changes.
func (s *ConnectionState) StoreFailedAlias(stmt *query.PreparedStatement) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failedAliases == nil {
		s.failedAliases = make(map[string]*query.PreparedStatement)
	}
	s.failedAliases[stmt.Name] = stmt
}

// DeleteFailedAlias clears the failed-Parse record for name (the alias parsed
// successfully, or its definition changed).
func (s *ConnectionState) DeleteFailedAlias(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.failedAliases, name)
}

// =============================================================================
// Settings - Session variables with Vitess-style bucket management
// =============================================================================

// Settings contains session variables (SET commands) and a bucket number
// for connection pool distribution.
//
// The bucket is assigned when the Settings is created and is used by the
// connection pool to distribute connections with the same settings to the
// same stack, enabling efficient connection reuse.
//
// IMPORTANT: Settings should be created via SettingsCache.GetOrCreate() to ensure
// proper interning. When settings are interned, pointer equality can be used for
// fast comparison instead of comparing the full Vars map.
type Settings struct {
	// Vars maps variable names to their values.
	Vars map[string]string

	// bucket is used by connection pool for stack distribution.
	bucket uint32
}

// NewSettings creates a new Settings with the given variables and bucket number.
//
// NOTE: For connection pooling, prefer using SettingsCache.GetOrCreate() instead
// to ensure settings are properly interned (same settings = same pointer).
func NewSettings(vars map[string]string, bucket uint32) *Settings {
	return &Settings{
		Vars:   canonicalizeGUCVars(vars),
		bucket: bucket,
	}
}

func canonicalizeGUCVars(vars map[string]string) map[string]string {
	if len(vars) == 0 {
		return vars
	}

	// PostgreSQL GUC lookup folds only ASCII A-Z to a-z (guc_name_compare), not
	// full Unicode case. Keep the same rule here: strings.ToLower would collapse
	// distinct custom GUC names like "my.Ä" and "my.ä" that PostgreSQL keeps
	// separate.
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make(map[string]string, len(vars))
	for _, k := range keys {
		out[pgsettings.CanonicalGUCName(k)] = vars[k]
	}
	return out
}

// CanonicalGUCName returns the PostgreSQL-compatible canonical spelling used
// for settings keys in Multigres.
func CanonicalGUCName(s string) string {
	return pgsettings.CanonicalGUCName(s)
}

// Bucket returns the bucket number for these settings.
// This is used by the connection pool for stack distribution.
func (s *Settings) Bucket() uint32 {
	if s == nil {
		return 0
	}
	return s.bucket
}

// ApplyQuery returns the SQL to apply these settings to a connection.
//
// Uses pg_catalog.set_config() instead of SET SQL to correctly handle
// list-valued GUCs (e.g. search_path, DateStyle). The SET SQL command
// requires list elements to be individually quoted or unquoted:
//
//	SET search_path = 'temp_func_test, public'  -- WRONG: one schema "temp_func_test, public"
//	SET search_path = temp_func_test, public     -- RIGHT: two schemas
//
// set_config() takes a flat string and PG's GUC machinery internally splits
// it for GUC_LIST_INPUT variables. This is the same approach pg_dump uses
// (see pg_dump.c: appendStringLiteralAH for search_path serialization).
//
// Single quotes in variable names and values are escaped by doubling them
// to prevent SQL injection.
func (s *Settings) ApplyQuery() string {
	if s == nil || len(s.Vars) == 0 {
		return ""
	}

	// Sort ordinary GUC keys for deterministic output. Role/session
	// authorization are replayed separately below: they are GUC-backed, but they
	// must use the SQL commands to keep current_user/current_role/session_user in
	// lock-step with permission checks. Ordinary GUCs are intentionally applied
	// first, while the backend is still running as the authenticated user: a
	// setting may have been validated before SET SESSION AUTHORIZATION changed
	// the effective user, and PostgreSQL preserves such settings across the
	// identity change.
	keys := make([]string, 0, len(s.Vars))
	for k := range s.Vars {
		switch CanonicalGUCName(k) {
		case "role", "session_authorization":
			continue
		default:
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var b strings.Builder
	appendStmt := func(sql string) {
		if b.Len() > 0 {
			b.WriteString("; ")
		}
		b.WriteString(sql)
	}

	// Build apply query using set_config() for correct list GUC handling.
	for _, k := range keys {
		appendStmt("SELECT pg_catalog.set_config('" +
			strings.ReplaceAll(k, "'", "''") + "', '" +
			strings.ReplaceAll(s.Vars[k], "'", "''") + "', false)")
	}

	if v, ok := s.Vars["session_authorization"]; ok {
		appendStmt("SET SESSION AUTHORIZATION " + ast.QuoteStringLiteral(v))
	}
	if v, ok := s.Vars["role"]; ok {
		appendStmt("SET ROLE " + ast.QuoteStringLiteral(v))
	}
	return b.String()
}

// ResetQuery returns the SQL to reset these settings on a connection.
// Includes RESET ROLE and RESET SESSION AUTHORIZATION before RESET ALL
// because PostgreSQL marks both with GUC_NO_RESET_ALL.
func (s *Settings) ResetQuery() string {
	if s == nil || len(s.Vars) == 0 {
		return ""
	}
	return "RESET ROLE; RESET SESSION AUTHORIZATION; RESET ALL"
}

// NeedsReapplyOnReuse reports whether these settings must be re-applied to a
// pooled connection even when it already carries this exact (interned)
// Settings pointer. "role" and "session_authorization" resolve their role
// name to an OID when the SET executes; if that role was dropped and
// recreated since this backend last applied the settings, the backend is
// left referencing a dangling OID (VACUUM reports "permission denied",
// current_user raises "invalid role OID: N") even though the settings
// strings still match. Re-running ApplyQuery re-resolves the name against
// the current catalog.
func (s *Settings) NeedsReapplyOnReuse() bool {
	if s == nil {
		return false
	}
	for k := range s.Vars {
		switch CanonicalGUCName(k) {
		case "role", "session_authorization":
			return true
		}
	}
	return false
}

// IsEmpty returns true if there are no variables set.
func (s *Settings) IsEmpty() bool {
	if s == nil {
		return true
	}
	return len(s.Vars) == 0
}

// Clone creates a copy of these settings with the same bucket number.
func (s *Settings) Clone() *Settings {
	if s == nil {
		return nil
	}
	clone := &Settings{
		Vars:   make(map[string]string, len(s.Vars)),
		bucket: s.bucket, // Keep same bucket
	}
	maps.Copy(clone.Vars, s.Vars)
	return clone
}
