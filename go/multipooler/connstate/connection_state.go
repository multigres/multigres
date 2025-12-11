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

	"github.com/multigres/multigres/go/pb/query"
)

// ConnectionState represents the cumulative state of a connection.
// This includes all state modifiers like session settings and prepared statements.
//
// All methods are thread-safe.
type ConnectionState struct {
	// mu protects all mutable fields in this struct.
	mu sync.Mutex

	// User is the current role set via SET ROLE.
	// This is NOT used for pool bucket routing - only for RLS enforcement.
	// Empty string means no role has been set (using connection's default role).
	User string

	// Settings contains session variables (SET commands).
	// This is the key for connection pool bucket assignment.
	Settings *Settings

	// PreparedStatements stores prepared statements by name.
	// The unnamed statement uses the empty string "" as the key.
	PreparedStatements map[string]*query.PreparedStatement
}

// NewConnectionState creates a new empty ConnectionState with initialized maps.
func NewConnectionState() *ConnectionState {
	return &ConnectionState{
		PreparedStatements: make(map[string]*query.PreparedStatement),
	}
}

// NewConnectionStateWithSettings creates a new ConnectionState with the given settings.
func NewConnectionStateWithSettings(settings *Settings) *ConnectionState {
	return &ConnectionState{
		Settings:           settings,
		PreparedStatements: make(map[string]*query.PreparedStatement),
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
		User:               s.User,
		PreparedStatements: make(map[string]*query.PreparedStatement, len(s.PreparedStatements)),
	}

	if s.Settings != nil {
		clone.Settings = s.Settings.Clone()
	}

	maps.Copy(clone.PreparedStatements, s.PreparedStatements)

	return clone
}

// Close cleans up the connection state.
func (s *ConnectionState) Close() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.User = ""
	s.Settings = nil
	s.PreparedStatements = nil
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

// --- User/Role Methods ---

// GetUser returns the current user role set via SET ROLE.
// Returns empty string if no role has been set.
func (s *ConnectionState) GetUser() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.User
}

// SetUser sets the current user role.
// This should be called after executing SET ROLE on the connection.
func (s *ConnectionState) SetUser(user string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.User = user
}

// ClearUser clears the current user role.
// This should be called after executing RESET ROLE on the connection.
func (s *ConnectionState) ClearUser() {
	s.SetUser("")
}

// HasUser returns true if a user role has been set.
func (s *ConnectionState) HasUser() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.User != ""
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
		Vars:   vars,
		bucket: bucket,
	}
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
func (s *Settings) ApplyQuery() string {
	if s == nil || len(s.Vars) == 0 {
		return ""
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(s.Vars))
	for k := range s.Vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build apply query
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString("SET SESSION ")
		b.WriteString(k)
		b.WriteString(" = '")
		b.WriteString(s.Vars[k])
		b.WriteString("'")
	}
	return b.String()
}

// ResetQuery returns the SQL to reset these settings on a connection.
func (s *Settings) ResetQuery() string {
	if s == nil || len(s.Vars) == 0 {
		return ""
	}
	return "RESET ALL"
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
