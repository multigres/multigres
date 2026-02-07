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

package handler

import (
	"maps"
	"sync"

	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// MultiGatewayConnectionState keeps track of the information specific
// to each connection.
type MultiGatewayConnectionState struct {
	mu sync.Mutex
	// NOTE: We are not storing the map of Prepared Statements even though
	// that is also connection level information. We do not require storing
	// it because we have prepared statement consolidation in MultiGateway
	// and that has all the required information. We can choose to store the information
	// here too but it would only lead to duplicate information storage and additional
	// code burden to keep them in sync.

	// Portals stores the list of portals created on this connection.
	// Map is keyed by the name of the portal.
	Portals map[string]*preparedstatement.PortalInfo

	// ShardStates is the information per shard that needs to be maintained.
	// It keeps track of any reserved connections on each Shard currently open.
	ShardStates []*ShardState

	// SessionSettings stores session variables set via SET commands.
	// These settings are propagated to multipooler to ensure the correct
	// pooled connection (with matching settings) is reused.
	// Map keys are variable names, values are the string representation.
	SessionSettings map[string]string

	// StartupParams stores parameters from the client's startup message
	// (e.g., DateStyle, TimeZone, client_encoding, and PGOPTIONS values).
	// These are forwarded to the backend as session settings.
	StartupParams map[string]string
}

type ShardState struct {
	// Target stores the information about the shard
	Target *query.Target

	// PoolerID is the pooler ID we are going to be running the queries against.
	// This is particularly useful to ensure that we detect the case of a reparent when we are
	// holding a reserved connection and the primary pooler changes.
	PoolerID *clustermetadata.ID

	// ReservedConnectionId is the connection ID of the reserved connection being held.
	ReservedConnectionId int64
}

// NewMultiGatewayConnectionState creates a new MultiGatewayConnectionState.
func NewMultiGatewayConnectionState() *MultiGatewayConnectionState {
	return &MultiGatewayConnectionState{
		mu:      sync.Mutex{},
		Portals: make(map[string]*preparedstatement.PortalInfo),
	}
}

// StorePortalInfo stores the portal information.
func (m *MultiGatewayConnectionState) StorePortalInfo(portal *query.Portal, psi *preparedstatement.PreparedStatementInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Portals[portal.Name] = preparedstatement.NewPortalInfo(psi, portal)
}

// GetPortalInfo gets the portal information for a previously stored portal.
func (m *MultiGatewayConnectionState) GetPortalInfo(portalName string) *preparedstatement.PortalInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Portals[portalName]
}

// DeletePortalInfo deletes the portal information
func (m *MultiGatewayConnectionState) DeletePortalInfo(portalName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Portals, portalName)
}

// NewShardState creates a new shard state.
func NewShardState(target *query.Target) *ShardState {
	return &ShardState{
		Target: target,
	}
}

// GetMatchingShardState gets the shardState (if any) that matches the target specified.
func (m *MultiGatewayConnectionState) GetMatchingShardState(target *query.Target) *ShardState {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			return ss
		}
	}
	return nil
}

// StoreReservedConnection stores a new reserved connection that has been created.
func (m *MultiGatewayConnectionState) StoreReservedConnection(target *query.Target, rs queryservice.ReservedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			ss.PoolerID = rs.PoolerID
			ss.ReservedConnectionId = int64(rs.ReservedConnectionId)
			return
		}
	}
	ss := NewShardState(target)
	ss.PoolerID = rs.PoolerID
	ss.ReservedConnectionId = int64(rs.ReservedConnectionId)
	m.ShardStates = append(m.ShardStates, ss)
}

// ClearReservedConnection removes a reserved connection for a given target.
// This should be called when a reserved connection is released (e.g., after COPY completes).
func (m *MultiGatewayConnectionState) ClearReservedConnection(target *query.Target) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			// Remove by swapping with last element and truncating
			lastIdx := len(m.ShardStates) - 1
			if i != lastIdx {
				m.ShardStates[i] = m.ShardStates[lastIdx]
			}
			m.ShardStates = m.ShardStates[:lastIdx]
			return
		}
	}
}

// SetSessionVariable sets a session variable (from SET command).
// The variable name and value are stored to be propagated to multipooler.
func (m *MultiGatewayConnectionState) SetSessionVariable(name, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SessionSettings == nil {
		m.SessionSettings = make(map[string]string)
	}
	m.SessionSettings[name] = value
}

// ResetSessionVariable removes a session variable (from RESET command).
func (m *MultiGatewayConnectionState) ResetSessionVariable(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SessionSettings != nil {
		delete(m.SessionSettings, name)
	}
}

// ResetAllSessionVariables clears all session variables (from RESET ALL command).
func (m *MultiGatewayConnectionState) ResetAllSessionVariables() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SessionSettings = make(map[string]string)
}

// GetSessionSettings returns a copy of the current session settings.
// Returns nil if no settings have been set.
// The copy prevents external mutation of the internal state.
func (m *MultiGatewayConnectionState) GetSessionSettings() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.SessionSettings) == 0 {
		return nil
	}
	// Return copy to prevent external mutation
	settings := make(map[string]string, len(m.SessionSettings))
	maps.Copy(settings, m.SessionSettings)
	return settings
}

// GetSessionVariable returns the value of a specific session variable.
// Returns (value, true) if exists, ("", false) if not.
func (m *MultiGatewayConnectionState) GetSessionVariable(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SessionSettings == nil {
		return "", false
	}
	value, exists := m.SessionSettings[name]
	return value, exists
}

// GetStartupParams returns a copy of the startup parameters.
// Returns nil if no startup parameters were set.
func (m *MultiGatewayConnectionState) GetStartupParams() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.StartupParams) == 0 {
		return nil
	}
	result := make(map[string]string, len(m.StartupParams))
	maps.Copy(result, m.StartupParams)
	return result
}

// RestoreSessionSettings replaces the current session settings with a new map.
// Used for rolling back RESET ALL failures.
func (m *MultiGatewayConnectionState) RestoreSessionSettings(settings map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if settings == nil {
		m.SessionSettings = nil
	} else {
		// Make a copy to prevent external mutation
		m.SessionSettings = make(map[string]string, len(settings))
		maps.Copy(m.SessionSettings, settings)
	}
}
