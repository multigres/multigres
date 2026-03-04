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

package command

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const pgctldMeterName = "github.com/multigres/multigres/go/cmd/pgctld/command"

// Metrics holds OTel gauge instruments for pgBackRest server health.
// Atomic values are used because the OTel callback and setPgBackRestStatus run on different goroutines.
type Metrics struct {
	serverUp     metric.Int64ObservableGauge
	restartCount metric.Int64ObservableGauge
	serverUptime metric.Int64ObservableGauge

	serverUpVal     atomic.Int64
	restartCountVal atomic.Int64
	startedAtNanos  atomic.Int64
}

// NewMetrics creates and registers the pgBackRest health gauges.
// It always returns a non-nil *Metrics; any registration errors are collected and returned.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{}
	meter := otel.Meter(pgctldMeterName)

	var errs []error

	var err error
	m.serverUp, err = meter.Int64ObservableGauge(
		"pgbackrest_server_up",
		metric.WithDescription("1 if the pgBackRest TLS server is running, 0 if not"),
	)
	errs = append(errs, err)

	m.restartCount, err = meter.Int64ObservableGauge(
		"pgbackrest_restart_count",
		metric.WithDescription("Cumulative number of times the pgBackRest TLS server has been restarted"),
	)
	errs = append(errs, err)

	m.serverUptime, err = meter.Int64ObservableGauge(
		"pgbackrest_server_uptime_seconds",
		metric.WithDescription("Seconds since the pgBackRest TLS server last started, 0 if down"),
	)
	errs = append(errs, err)

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(m.serverUp, m.serverUpVal.Load())
			o.ObserveInt64(m.restartCount, m.restartCountVal.Load())
			var uptime int64
			if started := m.startedAtNanos.Load(); started > 0 {
				uptime = (time.Now().UnixNano() - started) / int64(time.Second)
			}
			o.ObserveInt64(m.serverUptime, uptime)
			return nil
		},
		m.serverUp, m.restartCount, m.serverUptime,
	)
	errs = append(errs, err)

	return m, errors.Join(errs...)
}

// SetServerUp atomically stores 1 if running is true, 0 if false.
// It also records the start timestamp for the uptime gauge.
func (m *Metrics) SetServerUp(running bool) {
	if running {
		m.serverUpVal.Store(1)
		m.startedAtNanos.Store(time.Now().UnixNano())
	} else {
		m.serverUpVal.Store(0)
		m.startedAtNanos.Store(0)
	}
}

// SetRestartCount atomically stores the restart count as int64.
func (m *Metrics) SetRestartCount(count int32) {
	m.restartCountVal.Store(int64(count))
}
