// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgbackrestd

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const pgServerMeterName = "github.com/multigres/multigres/go/services/pgbackrestd"

// registerPgServerMetrics exports the gauges under the names pgctld used for
// the same child, so existing dashboards keep working.
func (s *Server) registerPgServerMetrics() {
	p := &s.pg
	meter := otel.Meter(pgServerMeterName)
	up, err := meter.Int64ObservableGauge("pgbackrest.server.up",
		metric.WithDescription("1 if the pgBackRest TLS server is running, 0 if not"))
	if err != nil {
		s.logger.Warn("register pgbackrest.server.up", "error", err)
		return
	}
	restarts, err := meter.Int64ObservableGauge("pgbackrest.server.restart_count",
		metric.WithDescription("Cumulative number of times the pgBackRest TLS server has been restarted"))
	if err != nil {
		s.logger.Warn("register pgbackrest.server.restart_count", "error", err)
		return
	}
	uptime, err := meter.Int64ObservableGauge("pgbackrest.server.uptime",
		metric.WithDescription("Seconds since the pgBackRest TLS server last started, 0 if down"))
	if err != nil {
		s.logger.Warn("register pgbackrest.server.uptime", "error", err)
		return
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var upVal int64
		if p.up.Load() {
			upVal = 1
		}
		o.ObserveInt64(up, upVal)
		o.ObserveInt64(restarts, p.restarts.Load())
		var secs int64
		if started := p.startedAt.Load(); started > 0 {
			secs = (time.Now().UnixNano() - started) / int64(time.Second)
		}
		o.ObserveInt64(uptime, secs)
		return nil
	}, up, restarts, uptime)
	if err != nil {
		s.logger.Warn("register pgbackrest server gauges", "error", err)
	}
}
