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
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type failingServerMeter struct {
	metric.Meter
	failAt string
	calls  []string
}

func (m *failingServerMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	m.calls = append(m.calls, name)
	if m.failAt == name {
		return nil, errors.New("fixture metric registration failure")
	}
	return m.Meter.Int64ObservableGauge(name, opts...)
}

func (m *failingServerMeter) RegisterCallback(cb metric.Callback, instruments ...metric.Observable) (metric.Registration, error) {
	m.calls = append(m.calls, "callback")
	if m.failAt == "callback" {
		return nil, errors.New("fixture callback registration failure")
	}
	return m.Meter.RegisterCallback(cb, instruments...)
}

type serverMeterProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (p serverMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter { return p.meter }

func TestPgServerMetricsRegistrationFailureIsNonfatal(t *testing.T) {
	for _, failure := range []string{"pgbackrest.server.up", "pgbackrest.server.restart_count", "pgbackrest.server.uptime", "callback"} {
		t.Run(failure, func(t *testing.T) {
			provider := noop.NewMeterProvider()
			meter := &failingServerMeter{Meter: provider.Meter("fixture"), failAt: failure}
			original := otel.GetMeterProvider()
			otel.SetMeterProvider(serverMeterProvider{MeterProvider: provider, meter: meter})
			t.Cleanup(func() { otel.SetMeterProvider(original) })
			var logs bytes.Buffer
			s := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil))}
			s.pg.up.Store(true)
			s.pg.restarts.Store(7)
			require.NotPanics(t, s.registerPgServerMetrics)
			require.Contains(t, logs.String(), "level=WARN")
			require.Contains(t, logs.String(), "registration failure")
			require.Equal(t, failure, meter.calls[len(meter.calls)-1], "stop registering after the first failed instrument")
			require.True(t, s.pg.up.Load(), "telemetry failure cannot change child state")
			require.Equal(t, int64(7), s.pg.restarts.Load())
		})
	}
}
