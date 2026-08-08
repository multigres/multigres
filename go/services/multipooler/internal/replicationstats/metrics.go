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

// Package replicationstats polls Postgres system views
// (pg_stat_replication, pg_replication_slots) for logical-replication
// connections opened via reserved.Pool.NewLogicalReplicationConn, and
// exports them as OTel metrics.
//
// This is a separate concern from internal/replication, which instruments
// the protocol-blind multigateway<->multipooler tunnel itself from bytes
// observed on the wire; this package's data comes from Postgres's own
// bookkeeping, polled out-of-band on a timer, not from anything seen in the
// tunnel.
package replicationstats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/replicationstats"

// Attribute keys for replicationstats metrics.
const (
	attrUser     = "user"
	attrConnID   = "conn_id"
	attrSlotName = "slot_name"
)

// Metrics holds the process-global OpenTelemetry instruments for polled
// logical-replication connection stats, and the latest polled snapshot they
// report from.
//
// The per-connection gauges (replayLag, lastAckAge, lastMsgAge, slotRetained)
// are deliberately observable/async, not synchronous: conn_id is minted
// fresh per connection (reserved.Pool seeds it from a boot-time nanosecond
// counter specifically so it's never reused) and every Realtime-style
// reconnect creates a brand-new one. A synchronous gauge's cumulative
// aggregation retains every attribute set it has ever seen for the life of
// the process — see go.opentelemetry.io/otel/sdk/metric's own
// internal/aggregate/lastvalue.go TODO(#3006) on this — so per-connection
// labels on a synchronous gauge leak one permanent "zombie" series per
// historical connection. An observable gauge's callback starts from an
// empty set on every collection; only attribute sets the callback actively
// calls Observe on that cycle are reported, so a retired conn_id simply
// stops appearing after one collection cycle instead of persisting forever.
type Metrics struct {
	meter metric.Meter

	replayLag         metric.Float64ObservableGauge
	lastAckAge        metric.Float64ObservableGauge
	lastMsgAge        metric.Float64ObservableGauge
	slotRetained      metric.Int64ObservableGauge
	activeConnections metric.Int64ObservableGauge

	// registration is the handle returned by RegisterCallback. Stored so the
	// callback can be unregistered when this Metrics is retired for good
	// (see Close) — mirroring connpoolmanager.Metrics.
	registration metric.Registration

	mu       sync.Mutex
	snapshot []ConnStats
}

// NewMetrics initialises the replicationstats instruments and registers the
// callback that reports the latest polled snapshot (see setSnapshot). An
// instrument that fails to initialise is reported in the returned (joined)
// error and simply never observed by the callback; the returned *Metrics is
// always usable.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter(meterName)
	m := &Metrics{meter: meter}
	var errs []error

	var err error
	if m.replayLag, err = meter.Float64ObservableGauge(
		"mg.pooler.replication.replay_lag",
		metric.WithDescription("pg_stat_replication.replay_lag for a logical-replication connection, in seconds"),
		metric.WithUnit("s"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.replay_lag: %w", err))
	}

	if m.lastAckAge, err = meter.Float64ObservableGauge(
		"mg.pooler.replication.last_ack_age",
		metric.WithDescription("Seconds since pg_stat_replication.reply_time was last observed. "+
			"Growing without bound indicates a disconnected or stuck consumer; alert if this exceeds 60s."),
		metric.WithUnit("s"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.last_ack_age: %w", err))
	}

	if m.lastMsgAge, err = meter.Float64ObservableGauge(
		"mg.pooler.replication.last_message_age",
		metric.WithDescription("Seconds since pg_stat_replication.sent_lsn was last observed to advance. "+
			"High while last_ack_age is low points at the server (walsender) side rather than the consumer."),
		metric.WithUnit("s"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.last_message_age: %w", err))
	}

	if m.slotRetained, err = meter.Int64ObservableGauge(
		"mg.pooler.replication.slot_retained_wal",
		metric.WithDescription("Bytes of WAL retained by a logical replication slot (pg_current_wal_lsn() - restart_lsn). "+
			"Unbounded growth risks disk exhaustion and a Postgres outage; alert on sustained growth or an absolute size threshold."),
		metric.WithUnit("By"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.slot_retained_wal: %w", err))
	}

	if m.activeConnections, err = meter.Int64ObservableGauge(
		"mg.pooler.replication.active_connections",
		metric.WithDescription("Number of active logical-replication connections, by user. "+
			"An unexpected drop indicates a consumer (e.g. Realtime) is no longer connected."),
		metric.WithUnit("{connection}"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.active_connections: %w", err))
	}

	var instruments []metric.Observable
	for _, inst := range []metric.Observable{m.replayLag, m.lastAckAge, m.lastMsgAge, m.slotRetained, m.activeConnections} {
		if inst != nil {
			instruments = append(instruments, inst)
		}
	}
	if len(instruments) > 0 {
		registration, err := meter.RegisterCallback(m.observe, instruments...)
		if err != nil {
			errs = append(errs, fmt.Errorf("replicationstats callback registration: %w", err))
		} else {
			m.registration = registration
		}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// observe is the OTel callback: it reports the latest polled snapshot (set
// by setSnapshot once per poll tick) for every currently-registered
// instrument. Attribute sets not observed in a given call simply don't
// appear in that collection — see the Metrics doc comment for why this
// (rather than a synchronous gauge) is required here.
func (m *Metrics) observe(ctx context.Context, o metric.Observer) error {
	m.mu.Lock()
	snapshot := m.snapshot
	m.mu.Unlock()

	activeByUser := make(map[string]int64, len(snapshot))
	for _, s := range snapshot {
		activeByUser[s.User]++

		connAttrs := metric.WithAttributes(
			attribute.String(attrUser, s.User),
			attribute.String(attrConnID, s.ConnID),
		)
		if m.replayLag != nil && s.HaveReplayLag {
			o.ObserveFloat64(m.replayLag, s.ReplayLag, connAttrs)
		}
		if m.lastAckAge != nil && s.HaveAck {
			o.ObserveFloat64(m.lastAckAge, s.LastAckAge, connAttrs)
		}
		if m.lastMsgAge != nil && s.HaveMsgAge {
			o.ObserveFloat64(m.lastMsgAge, s.LastMsgAge, connAttrs)
		}
		if m.slotRetained != nil && s.HaveSlot {
			o.ObserveInt64(m.slotRetained, s.RetainedWAL, metric.WithAttributes(
				attribute.String(attrUser, s.User),
				attribute.String(attrConnID, s.ConnID),
				attribute.String(attrSlotName, s.SlotName),
			))
		}
	}

	if m.activeConnections != nil {
		for user, count := range activeByUser {
			o.ObserveInt64(m.activeConnections, count, metric.WithAttributes(attribute.String(attrUser, user)))
		}
	}
	return nil
}

// setSnapshot replaces the snapshot reported by the next collection. Called
// once per poll tick with every currently-active connection's stats, and
// with nil to stop reporting entirely (see Poller.Close). Safe on a nil
// receiver (no-op), matching this repo's nil-safe metrics convention (see
// internal/replication.Stream).
func (m *Metrics) setSnapshot(snapshot []ConnStats) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.snapshot = snapshot
	m.mu.Unlock()
}

// currentSnapshot returns the most recently polled connection stats, for
// status-page use (see Poller.Status). Safe on a nil receiver.
func (m *Metrics) currentSnapshot() []ConnStats {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot
}

// Close unregisters the observable callback so the OTel SDK stops invoking
// it. Safe to call multiple times, when no callback was registered, and on
// a nil receiver.
func (m *Metrics) Close() error {
	if m == nil || m.registration == nil {
		return nil
	}
	err := m.registration.Unregister()
	m.registration = nil
	return err
}

// ConnStats is one polled connection's derived values. The Have* flags
// distinguish "value is genuinely zero" from "not available this tick"
// (e.g. no ack yet, no matching slot) so a gauge doesn't emit a spurious
// zero data point for an unset field. Exported for the multipooler status
// page (see Poller.Status); constructed only within this package.
type ConnStats struct {
	User   string
	ConnID string

	ReplayLag     float64
	HaveReplayLag bool

	LastAckAge float64
	HaveAck    bool

	LastMsgAge float64
	HaveMsgAge bool

	SlotName    string
	HaveSlot    bool
	RetainedWAL int64
}
