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

package preparedstatement

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/protoutil"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// Consolidator is used to consolidate prepared statements that
// are preparing the same statement but with different names. The intent is to be able to
// use the same connection for both of them to execute this because underlying, they are using
// the same prepared statement.
type Consolidator struct {
	// Mutex to protect the fields
	mu sync.Mutex

	// Map from (query, paramTypes) dedup key to canonical prepared statement
	stmts map[string]*PreparedStatementInfo
	// Map from connection ID and statement name to prepared statement reference
	incoming map[uint32]map[string]*PreparedStatementInfo
	// Reference count: number of connections using each prepared statement
	usageCount map[*PreparedStatementInfo]int

	// resolvedParams holds the backend-resolved parameter type OIDs recorded by
	// the Describe at SQL PREPARE time, keyed by [connID][name] — the SAME key as
	// incoming, NOT the deduplicated *PreparedStatementInfo. Resolution is
	// connection-context specific (search_path, database, DDL timing at PREPARE),
	// so two connections that dedup onto one shared PreparedStatementInfo for the
	// same (query, declared types) can still resolve their parameters to different
	// concrete types. Storing the resolution per registration keeps each PREPARE's
	// frozen types independent, mirroring PostgreSQL. See
	// docs/query_serving/prepared_statements_design.md ("Parameter type resolution").
	resolvedParams map[uint32]map[string][]uint32

	// lastUsedID is the last id of the statement name that we used.
	lastUsedID int
}

// ConsolidatorStats contains statistics about the prepared statement consolidator.
type ConsolidatorStats struct {
	// UniqueStatements is the number of unique prepared statements being tracked.
	UniqueStatements int `json:"unique_statements"`
	// TotalReferences is the total number of references across all connections.
	TotalReferences int `json:"total_references"`
	// ConnectionCount is the number of connections that have prepared statements.
	ConnectionCount int `json:"connection_count"`
	// Statements contains details about each unique prepared statement.
	Statements []StatementStats `json:"statements"`
}

// StatementStats contains statistics for a single prepared statement.
type StatementStats struct {
	// Name is the canonical name of the prepared statement.
	Name string `json:"name"`
	// Query is the SQL query of the prepared statement.
	Query string `json:"query"`
	// UsageCount is the number of connections using this prepared statement.
	UsageCount int `json:"usage_count"`
}

type PortalInfo struct {
	*querypb.Portal
	*PreparedStatementInfo
}

// PreparedStatementInfo shares a parsed statement and its immutable metadata
// across connections. It must not be copied after first use.
type PreparedStatementInfo struct {
	*querypb.PreparedStatement
	astStruct ast.Stmt

	canonicalOnce sync.Once
	canonicalSQL  string
	fingerprint   string
}

// AstStmt returns the shared parsed AST statement. Callers must not mutate it;
// semantic rewrites must clone the tree.
func (psi *PreparedStatementInfo) AstStmt() ast.Stmt {
	return psi.astStruct
}

// CanonicalSQLAndFingerprint returns the AST's canonical SQL and its fingerprint.
// Placeholders and literals are preserved; this does not normalize bind values.
// Initialization is lazy so statements that do not need a cache key pay no
// reconstruction cost. The consolidator shares this object across connections,
// so concurrent first use is synchronized; warm calls only read the cached data.
// Database/session-dependent cache keys and plans must remain with the caller.
// Empty statements return empty metadata.
func (psi *PreparedStatementInfo) CanonicalSQLAndFingerprint() (string, string) {
	psi.canonicalOnce.Do(func() {
		if psi.astStruct != nil {
			psi.canonicalSQL = psi.astStruct.SqlString()
			psi.fingerprint = ast.FingerprintSQL(psi.canonicalSQL)
		}
	})
	return psi.canonicalSQL, psi.fingerprint
}

// IsEmpty reports whether this prepared statement was created from an empty or
// comment-only query string (zero parsed statements). An empty statement has a
// nil AST; Execute answers it with EmptyQueryResponse, matching PostgreSQL.
func (psi *PreparedStatementInfo) IsEmpty() bool {
	return psi.astStruct == nil
}

// NewPreparedStatementInfo parses the query in the prepared statement and stores it along with the
// prepared statement information for future use.
func NewPreparedStatementInfo(ps *querypb.PreparedStatement) (*PreparedStatementInfo, error) {
	asts, err := parser.ParseSQL(ps.Query)
	if err != nil {
		// ParseSQL only does syntactic parsing, so any error here is a parse-stage
		// error. Surface it as the diagnostic PostgreSQL would send (the parser
		// stays mterrors-free): 42601 unless the parser named a SQLSTATE of its
		// own, carrying the cursor position for the ErrorResponse "P" field.
		var se *parser.ParseSyntaxError
		if errors.As(err, &se) {
			diag := mterrors.NewParseErrorAt(se.Message, se.CursorPosition, se.SQLState)
			diag.Hint = se.Hint
			return nil, diag
		}
		return nil, mterrors.NewParseError(err.Error())
	}
	switch {
	case len(asts) == 0:
		// Empty or comment-only query: PostgreSQL accepts this and answers a
		// subsequent Execute with EmptyQueryResponse. Represent it as a prepared
		// statement with a nil AST (the empty-statement sentinel). The gateway
		// short-circuits empty statements before they reach the planner.
		return &PreparedStatementInfo{
			PreparedStatement: ps,
			astStruct:         nil,
		}, nil
	case len(asts) > 1:
		// PostgreSQL: a Parse message "cannot insert multiple commands into a
		// prepared statement" (SQLSTATE 42601).
		return nil, mterrors.NewParseError("cannot insert multiple commands into a prepared statement")
	}
	return &PreparedStatementInfo{
		PreparedStatement: ps,
		astStruct:         asts[0],
	}, nil
}

// NewPortalInfo creates the PortalInfo.
func NewPortalInfo(psi *PreparedStatementInfo, portal *querypb.Portal) *PortalInfo {
	return &PortalInfo{
		Portal:                portal,
		PreparedStatementInfo: psi,
	}
}

// NewConsolidator gets a new prepared statement consolidator
// used to consolidate and reuse the same prepared statements.
func NewConsolidator() *Consolidator {
	return &Consolidator{
		stmts:          make(map[string]*PreparedStatementInfo),
		incoming:       make(map[uint32]map[string]*PreparedStatementInfo),
		usageCount:     make(map[*PreparedStatementInfo]int),
		resolvedParams: make(map[uint32]map[string][]uint32),
		lastUsedID:     0,
	}
}

// SetResolvedParamTypes records the backend-resolved parameter types from the
// Describe performed at SQL PREPARE time for the statement registered as
// (connId, name). It is scoped per registration, NOT per shared
// PreparedStatementInfo: parameter resolution depends on the issuing
// connection's context (search_path, database, catalog state at PREPARE), so
// two connections sharing one consolidated statement must keep their frozen
// resolutions independent — mirroring PostgreSQL, where each prepared statement
// freezes its own parameter types. A later PREPARE on the same (connId, name)
// overwrites, re-freezing against the current catalog. A nil description is
// ignored, leaving any earlier resolution intact.
func (psc *Consolidator) SetResolvedParamTypes(connId uint32, name string, desc *querypb.StatementDescription) {
	if desc == nil {
		return
	}
	oids := make([]uint32, len(desc.GetParameters()))
	for i, p := range desc.GetParameters() {
		oids[i] = p.GetDataTypeOid()
	}

	psc.mu.Lock()
	defer psc.mu.Unlock()
	if psc.resolvedParams[connId] == nil {
		psc.resolvedParams[connId] = make(map[string][]uint32)
	}
	psc.resolvedParams[connId][name] = oids
}

// ResolvedParamTypeOids returns the backend-resolved parameter type OIDs
// recorded for (connId, name) when a Describe has populated them, otherwise the
// declared ParamTypes of the registered statement (which may be empty or carry
// unspecified 0 entries for undeclared parameters). Returns nil if no statement
// is registered under (connId, name).
func (psc *Consolidator) ResolvedParamTypeOids(connId uint32, name string) []uint32 {
	psc.mu.Lock()
	defer psc.mu.Unlock()
	if byName, ok := psc.resolvedParams[connId]; ok {
		if oids, ok := byName[name]; ok {
			return oids
		}
	}
	// Fall back to the declared types of the registered statement. Guard the nil
	// *PreparedStatementInfo explicitly: the promoted GetParamTypes would
	// dereference it to reach the embedded proto and panic.
	psi := psc.incoming[connId][name]
	if psi == nil {
		return nil
	}
	return psi.GetParamTypes()
}

// clearResolvedParamsLocked drops the resolved parameter types recorded for
// (connId, name). Callers must hold psc.mu.
func (psc *Consolidator) clearResolvedParamsLocked(connId uint32, name string) {
	if byName, ok := psc.resolvedParams[connId]; ok {
		delete(byName, name)
		if len(byName) == 0 {
			delete(psc.resolvedParams, connId)
		}
	}
}

// AddPreparedStatement adds a prepared statement to the consolidator.
// Returns the PreparedStatementInfo (either existing or newly created) and any error.
func (psc *Consolidator) AddPreparedStatement(connId uint32, name, queryStr string, paramTypes []uint32) (*PreparedStatementInfo, error) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	// Initialize the map for this connection if it doesn't exist
	if psc.incoming[connId] == nil {
		psc.incoming[connId] = make(map[string]*PreparedStatementInfo)
	}

	// If the name is non-empty and a prepared statement for this name already exists
	// on the connection, replace it. This matches PostgreSQL behavior where re-parsing
	// with an existing name replaces the old statement. This is necessary to handle
	// the case where Parse succeeds (adding to consolidator) but the subsequent
	// Describe fails — the client retries Parse with the same name.
	if existing, exists := psc.incoming[connId][name]; exists && name != "" {
		slog.Debug("replacing existing prepared statement",
			"conn_id", connId,
			"name", name,
			"old_query", existing.Query,
			"new_query", queryStr,
		)
		psc.usageCount[existing]--
		if psc.usageCount[existing] == 0 {
			delete(psc.stmts, existing.Query)
			delete(psc.usageCount, existing)
		}
		delete(psc.incoming[connId], name)
		// The prior registration's resolved types belong to the statement being
		// replaced; drop them so a fresh PREPARE re-Describes into a clean slot.
		psc.clearResolvedParamsLocked(connId, name)
	}

	// Let's check if a prepared statement with this (query, paramTypes) already exists.
	key := dedupKey(queryStr, paramTypes)
	existingPs, foundExisting := psc.stmts[key]
	if foundExisting {
		// We found an existing prepared statement, we should be using that.
		psc.usageCount[existingPs] += 1
		psc.incoming[connId][name] = existingPs
		return existingPs, nil
	}

	// We didn't find any existing prepared statement with this (query, paramTypes).
	// Create a new one in our stmts list tracking unique prepared statements.
	newName := fmt.Sprintf("stmt%d", psc.lastUsedID)
	psc.lastUsedID += 1
	newPS, err := NewPreparedStatementInfo(protoutil.NewPreparedStatement(newName, queryStr, paramTypes))
	if err != nil {
		return nil, err
	}

	psc.stmts[key] = newPS
	psc.usageCount[newPS] += 1
	psc.incoming[connId][name] = newPS
	return newPS, nil
}

// GetPreparedStatementInfo gets the information for a previously added prepared statement to the consolidator.
func (psc *Consolidator) GetPreparedStatementInfo(connId uint32, name string) *PreparedStatementInfo {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	return psc.incoming[connId][name]
}

// RemovePreparedStatement removes prepared statement.
func (psc *Consolidator) RemovePreparedStatement(connId uint32, name string) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	psi, exists := psc.incoming[connId][name]
	if exists {
		psc.usageCount[psi] -= 1
		if psc.usageCount[psi] == 0 {
			delete(psc.stmts, dedupKey(psi.Query, psi.ParamTypes))
			delete(psc.usageCount, psi)
		}
		delete(psc.incoming[connId], name)
		psc.clearResolvedParamsLocked(connId, name)
	}
}

// RemoveConnection removes all prepared statements associated with a connection.
// This should be called when a client connection is closed.
func (psc *Consolidator) RemoveConnection(connId uint32) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	connStmts, exists := psc.incoming[connId]
	if !exists {
		return
	}

	for _, psi := range connStmts {
		psc.usageCount[psi]--
		if psc.usageCount[psi] == 0 {
			delete(psc.stmts, dedupKey(psi.Query, psi.ParamTypes))
			delete(psc.usageCount, psi)
		}
	}
	delete(psc.incoming, connId)
	delete(psc.resolvedParams, connId)
}

// Stats returns statistics about the consolidator's current state.
func (psc *Consolidator) Stats() ConsolidatorStats {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	stats := ConsolidatorStats{
		UniqueStatements: len(psc.stmts),
		TotalReferences:  0,
		ConnectionCount:  len(psc.incoming),
		Statements:       make([]StatementStats, 0, len(psc.stmts)),
	}

	for _, psi := range psc.stmts {
		usageCount := psc.usageCount[psi]
		stats.TotalReferences += usageCount
		stats.Statements = append(stats.Statements, StatementStats{
			Name:       psi.Name,
			Query:      psi.Query,
			UsageCount: usageCount,
		})
	}

	return stats
}
