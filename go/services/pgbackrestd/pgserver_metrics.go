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

// registerTLSVerifyMetric exports whether this sidecar verifies the S3
// endpoint's certificate, so a fleet can alert on one that does not: the
// warning in the log and Status say the same, but only to whoever reads them.
func (s *Server) registerTLSVerifyMetric() {
	meter := otel.Meter(pgServerMeterName)
	disabled, err := meter.Int64ObservableGauge("pgbackrest.s3.tls_verify_disabled",
		metric.WithDescription("1 if the sidecar was started with --s3-skip-tls-verify and does not verify the S3 endpoint's TLS certificate, 0 if it verifies"))
	if err != nil {
		s.logger.Warn("register pgbackrest.s3.tls_verify_disabled", "error", err)
		return
	}
	var val int64
	if s.cfg.LocationType == LocationS3 && s.cfg.S3SkipTLSVerify {
		val = 1
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(disabled, val)
		return nil
	}, disabled)
	if err != nil {
		s.logger.Warn("register pgbackrest.s3.tls_verify_disabled", "error", err)
	}
}
