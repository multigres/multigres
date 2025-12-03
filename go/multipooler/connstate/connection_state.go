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
	"sync/atomic"

	"github.com/multigres/multigres/go/pb/query"
)

// ConnectionState represents the cumulative state of a connection.
// This includes all state modifiers like session settings, prepared statements,
// and portals.
//
// All methods are thread-safe.
type ConnectionState struct {
	// mu protects all mutable fields in this struct.
	mu sync.Mutex

	// Settings contains session variables (SET commands).
	// This is the key for connection pool bucket assignment.
	Settings *Settings

	// PreparedStatements stores prepared statements by name.
	// The unnamed statement uses the empty string "" as the key.
	PreparedStatements map[string]*query.PreparedStatement

	// Portals stores portals (bound prepared statements) by name.
	// The unnamed portal uses the empty string "" as the key.
	Portals map[string]*query.Portal
}

// NewConnectionState creates a new empty ConnectionState with initialized maps.
func NewConnectionState() *ConnectionState {
	return &ConnectionState{
		PreparedStatements: make(map[string]*query.PreparedStatement),
		Portals:            make(map[string]*query.Portal),
	}
}

// NewConnectionStateWithSettings creates a new ConnectionState with the given settings.
func NewConnectionStateWithSettings(settings *Settings) *ConnectionState {
	return &ConnectionState{
		Settings:           settings,
		PreparedStatements: make(map[string]*query.PreparedStatement),
		Portals:            make(map[string]*query.Portal),
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

// HasMatchingSettings returns true if this state has settings that match the given settings.
func (s *ConnectionState) HasMatchingSettings(other *Settings) bool {
	if s == nil {
		return other == nil || other.IsEmpty()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Settings == nil {
		return other == nil || other.IsEmpty()
	}
	return s.Settings.Equals(other)
}

// Clone creates a deep copy of this state.
func (s *ConnectionState) Clone() *ConnectionState {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clone := &ConnectionState{
		PreparedStatements: make(map[string]*query.PreparedStatement, len(s.PreparedStatements)),
		Portals:            make(map[string]*query.Portal, len(s.Portals)),
	}

	if s.Settings != nil {
		clone.Settings = s.Settings.Clone()
	}

	maps.Copy(clone.PreparedStatements, s.PreparedStatements)
	maps.Copy(clone.Portals, s.Portals)

	return clone
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
	s.Portals = nil
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

// --- Portal Methods ---

// StorePortal stores a portal.
func (s *ConnectionState) StorePortal(portal *query.Portal) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Portals[portal.Name] = portal
}

// GetPortal retrieves a portal by name.
func (s *ConnectionState) GetPortal(name string) *query.Portal {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Portals[name]
}

// DeletePortal removes a portal by name.
func (s *ConnectionState) DeletePortal(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Portals, name)
}

// --- SQL Generation Methods ---

// GenerateResetSQL generates SQL statements to reset a connection to clean state.
func (s *ConnectionState) GenerateResetSQL() []string {
	if s == nil || s.IsClean() {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var statements []string

	// Reset settings
	if s.Settings != nil && !s.Settings.IsEmpty() {
		statements = append(statements, "RESET ALL")
	}

	// Deallocate prepared statements
	if len(s.PreparedStatements) > 0 {
		statements = append(statements, "DEALLOCATE ALL")
	}

	// Close portals
	if len(s.Portals) > 0 {
		statements = append(statements, "CLOSE ALL")
	}

	return statements
}

// =============================================================================
// Settings - Session variables with Vitess-style bucket management
// =============================================================================

// globalSettingsCounter is used to assign unique bucket numbers to Settings.
// This follows the Vitess pattern for distributing connections across pool stacks.
var globalSettingsCounter atomic.Uint32

// Settings contains session variables (SET commands) and a bucket number
// for connection pool distribution.
//
// The bucket is assigned when the Settings is created and is used by the
// connection pool to distribute connections with the same settings to the
// same stack, enabling efficient connection reuse.
type Settings struct {
	// Vars maps variable names to their values.
	Vars map[string]string

	// bucket is assigned from globalSettingsCounter when Settings is created.
	// Used by connection pool for stack distribution.
	bucket uint32
}

// NewSettings creates a new Settings with the given variables.
// A unique bucket number is assigned from the global counter.
func NewSettings(vars map[string]string) *Settings {
	return &Settings{
		Vars:   vars,
		bucket: globalSettingsCounter.Add(1),
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
// The query is constructed at runtime from the Vars map.
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

// Equals checks if two Settings are equal (same variables).
// Note: bucket numbers are NOT compared since two Settings with the same
// variables may have different buckets if created at different times.
func (s *Settings) Equals(other *Settings) bool {
	if s == other {
		return true
	}
	if s == nil || other == nil {
		return false
	}
	if len(s.Vars) != len(other.Vars) {
		return false
	}
	for k, v := range s.Vars {
		if ov, ok := other.Vars[k]; !ok || v != ov {
			return false
		}
	}
	return true
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
