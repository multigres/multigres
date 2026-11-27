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
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const pgctldMeterName = "github.com/multigres/multigres/go/cmd/pgctld/command"

// sidecarMetrics holds the exposure gauge. pgctld publishes no
// pgbackrest.server.* gauges: the sidecar's own gauge reports the 8432 server.
type sidecarMetrics struct {
	credentialExposed    metric.Int64ObservableGauge
	credentialExposedVal atomic.Int64
}

func newSidecarMetrics() (*sidecarMetrics, error) {
	m := &sidecarMetrics{}
	meter := otel.Meter(pgctldMeterName)

	var err error
	m.credentialExposed, err = meter.Int64ObservableGauge(
		"credential_exposed",
		metric.WithDescription("1 if a cloud credential source is present in pgctld's environment, else 0"),
	)
	if err != nil {
		return m, err
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(m.credentialExposed, m.credentialExposedVal.Load())
			return nil
		},
		m.credentialExposed,
	)
	return m, err
}
