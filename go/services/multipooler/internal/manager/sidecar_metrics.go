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

package manager

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// registerTableBehindGauge exposes pm.tableBehind: 1 while the generation table
// trails the sidecar or the PVC copy and this pooler cannot fix it.
func (pm *MultipoolerManager) registerTableBehindGauge() {
	meter := otel.Meter(meterName)
	gauge, err := meter.Int64ObservableGauge("pgbackrest.generation_table_behind",
		metric.WithDescription("1 while the pgbackrest generation table trails the sidecar's revision or the PVC copy"))
	if err != nil {
		pm.logger.Warn("generation_table_behind gauge registration failed", "error", err)
		return
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, pm.tableBehind.Load())
		return nil
	}, gauge); err != nil {
		pm.logger.Warn("generation_table_behind gauge callback registration failed", "error", err)
	}
}

// registerRestoreMarkerAgeGauge exposes how long the incomplete-restore marker
// has existed, in seconds (0 when there is none), so an alert can fire on a
// restore that waits far longer than a restore can take.
func (pm *MultipoolerManager) registerRestoreMarkerAgeGauge() {
	meter := otel.Meter(meterName)
	gauge, err := meter.Int64ObservableGauge("pgbackrest.restore_marker_age",
		metric.WithDescription("Seconds the incomplete-restore marker has existed; 0 when there is none"),
		metric.WithUnit("s"))
	if err != nil {
		pm.logger.Warn("restore_marker_age gauge registration failed", "error", err)
		return
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, pm.markerAgeSec.Load())
		return nil
	}, gauge); err != nil {
		pm.logger.Warn("restore_marker_age gauge callback registration failed", "error", err)
	}
}

// registerCredentialExposedGauge exposes pm.credentialExposed. Registration
// failure is logged only: it must not stop a pooler from serving.
func (pm *MultipoolerManager) registerCredentialExposedGauge() {
	meter := otel.Meter(meterName)
	gauge, err := meter.Int64ObservableGauge("pgbackrest.credential_exposed",
		metric.WithDescription("1 when multipooler's environment holds an AWS credential source meant only for the pgbackrestd sidecar"))
	if err != nil {
		pm.logger.Warn("credential_exposed gauge registration failed", "error", err)
		return
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, pm.credentialExposed.Load())
		return nil
	}, gauge); err != nil {
		pm.logger.Warn("credential_exposed gauge callback registration failed", "error", err)
	}
}
