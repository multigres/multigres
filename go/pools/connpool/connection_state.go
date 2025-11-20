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

package connpool

import (
	"hash/fnv"
	"maps"
	"sort"
	"sync"

	"github.com/multigres/multigres/go/parser/ast"
)

// PreparedStatement represents a prepared statement in the extended query protocol.
// Prepared statements are created by the Parse message and can be executed
// multiple times with different parameter values.
type PreparedStatement struct {
	// Name is the client-provided name for the prepared statement.
	// An empty name indicates the unnamed statement.
	Name string

	// Query is the SQL query string.
	Query ast.Stmt

	// ParamTypes contains the OIDs of the parameter types.
	// This is sent by the client in the Parse message.
	ParamTypes []uint32
}

// NewPreparedStatement creates a new PreparedStatement.
func NewPreparedStatement(name string, query ast.Stmt, paramTypes []uint32) *PreparedStatement {
	return &PreparedStatement{
		Name:       name,
		Query:      query,
		ParamTypes: paramTypes,
	}
}

// Portal represents a bound prepared statement with parameters.
// Portals are created by the Bind message and can be executed via Execute.
type Portal struct {
	// Name is the client-provided name for the portal.
	// An empty name indicates the unnamed portal.
	Name string

	// Statement is the prepared statement this portal is bound to.
	Statement *PreparedStatement

	// Params contains the parameter values sent by the client in the Bind message.
	// Each parameter is a byte slice, with nil indicating NULL.
	Params [][]byte

	// ParamFormats specifies the format code for each parameter.
	// 0 = text, 1 = binary.
	// If empty, all parameters are text format.
	// If a single element, that format applies to all parameters.
	ParamFormats []int16

	// ResultFormats specifies the format code for each result column.
	// 0 = text, 1 = binary.
	// If empty, all results are text format.
	// If a single element, that format applies to all result columns.
	ResultFormats []int16
}

// NewPortal creates a new Portal.
func NewPortal(name string, statement *PreparedStatement, params [][]byte, paramFormats, resultFormats []int16) *Portal {
	return &Portal{
		Name:          name,
		Statement:     statement,
		Params:        params,
		ParamFormats:  paramFormats,
		ResultFormats: resultFormats,
	}
}

// ConnectionState represents the cumulative state of a connection.
// This includes all state modifiers like session settings, prepared statements,
// and portals.
//
// ConnectionState instances can be interned by the Pool for fast pointer equality
// checks instead of deep equality comparisons.
//
// All methods are thread-safe.
type ConnectionState struct {
	// mu protects all mutable fields in this struct.
	mu sync.Mutex

	// Settings contains session variables (SET commands).
	// Key is the variable name, value is the variable value.
	Settings map[string]string

	// PreparedStatements stores prepared statements by name.
	// The unnamed statement uses the empty string "" as the key.
	PreparedStatements map[string]*PreparedStatement

	// Portals stores portals (bound prepared statements) by name.
	// The unnamed portal uses the empty string "" as the key.
	Portals map[string]*Portal

	// hash is the cached hash value for this state.
	// Used for distributing connections across state-specific stacks.
	// This is computed based on immutable state for pooling purposes.
	hash uint64
}

// NewConnectionState creates a new ConnectionState with the given settings.
func NewConnectionState(settings map[string]string) *ConnectionState {
	state := &ConnectionState{
		Settings:           settings,
		PreparedStatements: make(map[string]*PreparedStatement),
		Portals:            make(map[string]*Portal),
	}
	state.hash = state.computeHash()
	return state
}

// NewEmptyConnectionState creates a new empty ConnectionState with initialized maps.
func NewEmptyConnectionState() *ConnectionState {
	return &ConnectionState{
		Settings:           make(map[string]string),
		PreparedStatements: make(map[string]*PreparedStatement),
		Portals:            make(map[string]*Portal),
	}
}

// Hash returns the cached hash value for this state.
// The hash is used for connection pool state matching.
func (s *ConnectionState) Hash() uint64 {
	if s == nil {
		return 0
	}
	if s.hash == 0 {
		s.hash = s.computeHash()
	}
	return s.hash
}

// computeHash computes a hash value for this state based on all state components.
func (s *ConnectionState) computeHash() uint64 {
	h := fnv.New64a()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Hash settings in sorted order for consistency
	if len(s.Settings) > 0 {
		keys := make([]string, 0, len(s.Settings))
		for k := range s.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte{0}) // separator
			h.Write([]byte(s.Settings[k]))
			h.Write([]byte{0}) // separator
		}
	}

	// Hash prepared statements by name in sorted order
	if len(s.PreparedStatements) > 0 {
		names := make([]string, 0, len(s.PreparedStatements))
		for name := range s.PreparedStatements {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			stmt := s.PreparedStatements[name]
			h.Write([]byte(name))
			h.Write([]byte{0})
			// Hash the query string representation
			if stmt.Query != nil {
				h.Write([]byte(stmt.Query.String()))
				h.Write([]byte{0})
			}
		}
	}

	// Hash portals by name in sorted order
	if len(s.Portals) > 0 {
		names := make([]string, 0, len(s.Portals))
		for name := range s.Portals {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			h.Write([]byte(name))
			h.Write([]byte{0})
		}
	}

	return h.Sum64()
}

// Equals performs a deep equality comparison with another state.
func (s *ConnectionState) Equals(other *ConnectionState) bool {
	if s == other {
		return true
	}
	if s == nil || other == nil {
		return false
	}

	// Fast path: compare hashes first
	if s.Hash() != other.Hash() {
		return false
	}

	s.mu.Lock()
	other.mu.Lock()
	defer s.mu.Unlock()
	defer other.mu.Unlock()

	// Compare settings
	if len(s.Settings) != len(other.Settings) {
		return false
	}
	for k, v := range s.Settings {
		if otherV, ok := other.Settings[k]; !ok || v != otherV {
			return false
		}
	}

	// Compare prepared statements
	if len(s.PreparedStatements) != len(other.PreparedStatements) {
		return false
	}
	for name, stmt := range s.PreparedStatements {
		otherStmt, ok := other.PreparedStatements[name]
		if !ok {
			return false
		}
		// Compare query strings
		if stmt.Query != nil && otherStmt.Query != nil {
			if stmt.Query.String() != otherStmt.Query.String() {
				return false
			}
		} else if stmt.Query != otherStmt.Query {
			return false
		}
	}

	// Compare portals (by name)
	if len(s.Portals) != len(other.Portals) {
		return false
	}
	for name := range s.Portals {
		if _, ok := other.Portals[name]; !ok {
			return false
		}
	}

	return true
}

// IsClean returns true if this state has no modifiers applied.
func (s *ConnectionState) IsClean() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.Settings) == 0 &&
		len(s.PreparedStatements) == 0 &&
		len(s.Portals) == 0
}

// Clone creates a deep copy of this state.
// The clone is NOT automatically interned.
func (s *ConnectionState) Clone() *ConnectionState {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clone := &ConnectionState{
		Settings:           make(map[string]string, len(s.Settings)),
		PreparedStatements: make(map[string]*PreparedStatement, len(s.PreparedStatements)),
		Portals:            make(map[string]*Portal, len(s.Portals)),
	}

	maps.Copy(clone.Settings, s.Settings)
	maps.Copy(clone.PreparedStatements, s.PreparedStatements)
	maps.Copy(clone.Portals, s.Portals)

	clone.hash = clone.computeHash()
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

// ApplySetting adds or updates a setting and recomputes the hash.
func (s *ConnectionState) ApplySetting(key, value string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Settings[key] = value
	s.hash = 0 // Invalidate hash, will be recomputed on next Hash() call
}

// RemoveSetting removes a setting and recomputes the hash.
func (s *ConnectionState) RemoveSetting(key string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.Settings[key]; ok {
		delete(s.Settings, key)
		s.hash = 0 // Invalidate hash, will be recomputed on next Hash() call
	}
}

// --- Prepared Statement Methods ---

// StorePreparedStatement stores a prepared statement.
// If a statement with the same name already exists, it is replaced.
func (s *ConnectionState) StorePreparedStatement(stmt *PreparedStatement) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.PreparedStatements[stmt.Name] = stmt
	s.hash = 0 // Invalidate hash
}

// GetPreparedStatement retrieves a prepared statement by name.
// Returns nil if the statement does not exist.
func (s *ConnectionState) GetPreparedStatement(name string) *PreparedStatement {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.PreparedStatements[name]
}

// DeletePreparedStatement removes a prepared statement by name.
// Does nothing if the statement doesn't exist (PostgreSQL-compliant behavior).
func (s *ConnectionState) DeletePreparedStatement(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.PreparedStatements, name)
	s.hash = 0 // Invalidate hash
}

// --- Portal Methods ---

// StorePortal stores a portal.
// If a portal with the same name already exists, it is replaced.
func (s *ConnectionState) StorePortal(portal *Portal) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Portals[portal.Name] = portal
	s.hash = 0 // Invalidate hash
}

// GetPortal retrieves a portal by name.
// Returns nil if the portal does not exist.
func (s *ConnectionState) GetPortal(name string) *Portal {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Portals[name]
}

// DeletePortal removes a portal by name.
// Does nothing if the portal doesn't exist (PostgreSQL-compliant behavior).
func (s *ConnectionState) DeletePortal(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Portals, name)
	s.hash = 0 // Invalidate hash
}

// --- SQL Generation Methods ---

// GenerateApplySQL generates SQL statements to apply this state to a connection.
// Returns a list of SQL statements to execute in order.
func (s *ConnectionState) GenerateApplySQL() []string {
	if s == nil || s.IsClean() {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var statements []string

	// Generate SET commands for settings in sorted order for determinism
	if len(s.Settings) > 0 {
		keys := make([]string, 0, len(s.Settings))
		for k := range s.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			// Use SET SESSION to ensure the setting persists across transactions
			statements = append(statements, "SET SESSION "+k+" = '"+s.Settings[k]+"'")
		}
	}

	// TODO: Generate PREPARE commands for prepared statements

	return statements
}

// GenerateResetSQL generates SQL statements to reset a connection to clean state.
// Returns a list of SQL statements to execute in order.
func (s *ConnectionState) GenerateResetSQL() []string {
	if s == nil || s.IsClean() {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var statements []string

	// Generate RESET commands for settings
	if len(s.Settings) > 0 {
		// RESET ALL is more efficient than resetting individual settings
		statements = append(statements, "RESET ALL")
	}

	// Generate DEALLOCATE commands for prepared statements
	if len(s.PreparedStatements) > 0 {
		statements = append(statements, "DEALLOCATE ALL")
	}

	// Close all portals
	if len(s.Portals) > 0 {
		statements = append(statements, "CLOSE ALL")
	}

	return statements
}
