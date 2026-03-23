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
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
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

	// StartupParams stores startup parameters from the client's StartupMessage.
	// These are forwarded to the backend for every query.
	StartupParams map[string]string

	// SessionSettings stores session variables set via SET commands.
	// These settings are propagated to multipooler to ensure the correct
	// pooled connection (with matching settings) is reused.
	// Map keys are variable names, values are the string representation.
	SessionSettings map[string]string

	// PendingBeginQuery stores the original BEGIN/START TRANSACTION statement text
	// (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE") when a transaction is started
	// with deferred execution. This is consumed when creating the first reserved
	// connection so the multipooler can use the exact statement instead of plain "BEGIN".
	PendingBeginQuery string

	// TxnStartTime records when the current transaction began (set at BEGIN,
	// read at COMMIT/ROLLBACK to compute transaction duration). Zero value
	// means no active transaction is being timed.
	TxnStartTime time.Time

	// ListenChannels tracks active LISTEN channels for this connection.
	ListenChannels map[string]bool

	// PendingListens/PendingUnlistens track LISTEN/UNLISTEN inside transactions.
	PendingListens     []string
	PendingUnlistens   []string
	PendingUnlistenAll bool

	// NotifCh receives notifications from the PubSubListener.
	NotifCh chan *Notification

	// AsyncNotifCh is the channel for the server.Conn async notification pusher.
	AsyncNotifCh chan<- *server.NotificationPayload

	// statementTimeout is the session-level statement timeout set via SET statement_timeout.
	// This is managed entirely by the gateway and is NOT forwarded to PostgreSQL.
	// The default is initialized from startup params (if present) or the --statement-timeout flag.
	// Parsed at SET time to avoid repeated parsing on every query.
	statementTimeout GatewayManagedVariable[time.Duration]
}

type ShardState struct {
	// Target stores the information about the shard
	Target *query.Target

	// ReservedState holds the authoritative reservation state from the multipooler,
	// including the pooler ID, reserved connection ID, and reservation reasons bitmask.
	ReservedState *query.ReservedState
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

// SetReservedConnection stores the authoritative reservation state from the multipooler.
// The reasons in rs.ReservationReasons are set exactly as provided (not OR'd).
// Creates a new entry if none exists for the target.
func (m *MultiGatewayConnectionState) SetReservedConnection(target *query.Target, rs *query.ReservedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ss := range m.ShardStates {
		if protoutil.TargetEquals(ss.Target, target) {
			ss.ReservedState = rs
			return
		}
	}
	ss := NewShardState(target)
	ss.ReservedState = rs
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

// ClearAllReservedConnections removes all reserved connection entries.
// Called after COMMIT or ROLLBACK to clean up stale shard state.
func (m *MultiGatewayConnectionState) ClearAllReservedConnections() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ShardStates = nil
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
	delete(m.SessionSettings, name)
}

// ResetAllSessionVariables clears all session variables (from RESET ALL command).
func (m *MultiGatewayConnectionState) ResetAllSessionVariables() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SessionSettings = nil
}

// SetStatementTimeout sets the session-level statement timeout override.
func (m *MultiGatewayConnectionState) SetStatementTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.Set(d)
}

// ResetStatementTimeout clears the session-level statement timeout,
// reverting to the default (from startup params or flag).
func (m *MultiGatewayConnectionState) ResetStatementTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout.Reset()
}

// GetStatementTimeout returns the effective statement timeout:
// the session override if set, otherwise the default.
func (m *MultiGatewayConnectionState) GetStatementTimeout() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statementTimeout.GetEffective()
}

// ShowStatementTimeout returns the effective statement timeout formatted
// using PostgreSQL's GUC_UNIT_MS display convention for SHOW output.
func (m *MultiGatewayConnectionState) ShowStatementTimeout() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return formatDurationPg(m.statementTimeout.GetEffective())
}

// InitStatementTimeout sets the default for the statement timeout variable.
// Called once during connection initialization with the value from startup params
// (if present) or the --statement-timeout flag.
func (m *MultiGatewayConnectionState) InitStatementTimeout(defaultValue time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statementTimeout = NewGatewayManagedVariable(defaultValue)
}

// GetSessionSettings returns a merged view of startup parameters and session settings.
// Session settings (from SET commands) take precedence over startup params for the same key.
// When a variable is RESET (deleted from SessionSettings), the startup param value becomes visible again.
// Returns nil if neither startup params nor session settings exist.
// The copy prevents external mutation of the internal state.
func (m *MultiGatewayConnectionState) GetSessionSettings() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.StartupParams) == 0 && len(m.SessionSettings) == 0 {
		return nil
	}
	// Start with startup params, then overlay session settings (which take precedence)
	merged := make(map[string]string, len(m.StartupParams)+len(m.SessionSettings))
	maps.Copy(merged, m.StartupParams)
	maps.Copy(merged, m.SessionSettings)
	return merged
}

// GetSessionVariable returns the value of a specific session variable.
// Returns (value, true) if exists, ("", false) if not.
func (m *MultiGatewayConnectionState) GetSessionVariable(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.SessionSettings[name]
	return value, exists
}

// GetStartupParams returns a copy of the startup parameters.
// Returns nil if no startup params were set.
func (m *MultiGatewayConnectionState) GetStartupParams() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.StartupParams) == 0 {
		return nil
	}
	params := make(map[string]string, len(m.StartupParams))
	maps.Copy(params, m.StartupParams)
	return params
}

// --- LISTEN/NOTIFY state tracking ---

// AddListenChannel registers a channel as actively listened.
func (m *MultiGatewayConnectionState) AddListenChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ListenChannels == nil {
		m.ListenChannels = make(map[string]bool)
	}
	m.ListenChannels[channel] = true
}

// RemoveListenChannel removes a channel from active listeners.
func (m *MultiGatewayConnectionState) RemoveListenChannel(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ListenChannels, channel)
}

// ClearListenChannels removes all listen channels (UNLISTEN *).
func (m *MultiGatewayConnectionState) ClearListenChannels() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ListenChannels = nil
}

// GetListenChannels returns a copy of the active listen channels.
func (m *MultiGatewayConnectionState) GetListenChannels() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ListenChannels) == 0 {
		return nil
	}
	channels := make(map[string]bool, len(m.ListenChannels))
	maps.Copy(channels, m.ListenChannels)
	return channels
}

// AddPendingListen adds a channel to the pending listen list (for commit).
func (m *MultiGatewayConnectionState) AddPendingListen(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PendingListens = append(m.PendingListens, channel)
}

// AddPendingUnlisten adds a channel to the pending unlisten list.
func (m *MultiGatewayConnectionState) AddPendingUnlisten(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PendingUnlistens = append(m.PendingUnlistens, channel)
}

// AddPendingUnlistenAll marks that UNLISTEN * should happen on commit.
func (m *MultiGatewayConnectionState) AddPendingUnlistenAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PendingUnlistenAll = true
}

// CommitPendingListens applies pending listen/unlisten state on transaction commit.
// Returns lists of channels to subscribe and unsubscribe from.
func (m *MultiGatewayConnectionState) CommitPendingListens() (subscribes []string, unsubscribes []string, unsubscribeAll bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.PendingUnlistenAll {
		unsubscribeAll = true
		// UnsubscribeAll handles all channels — no need to populate individual unsubscribes.
		m.ListenChannels = nil
	}

	// Process individual unlistens only when UNLISTEN * wasn't issued.
	for _, ch := range m.PendingUnlistens {
		if !unsubscribeAll {
			if m.ListenChannels != nil && m.ListenChannels[ch] {
				unsubscribes = append(unsubscribes, ch)
			}
		}
		delete(m.ListenChannels, ch)
	}

	for _, ch := range m.PendingListens {
		if m.ListenChannels == nil {
			m.ListenChannels = make(map[string]bool)
		}
		if !m.ListenChannels[ch] {
			subscribes = append(subscribes, ch)
			m.ListenChannels[ch] = true
		}
	}

	m.PendingListens = nil
	m.PendingUnlistens = nil
	m.PendingUnlistenAll = false
	return subscribes, unsubscribes, unsubscribeAll
}

// DiscardPendingListens discards pending listen/unlisten state on rollback.
func (m *MultiGatewayConnectionState) DiscardPendingListens() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PendingListens = nil
	m.PendingUnlistens = nil
	m.PendingUnlistenAll = false
}

// HasPendingListens returns true if there are pending listen/unlisten changes.
func (m *MultiGatewayConnectionState) HasPendingListens() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.PendingListens) > 0 || len(m.PendingUnlistens) > 0 || m.PendingUnlistenAll
}
