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

package servenv

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

const buildInfoMeterName = "github.com/multigres/multigres/go/common/servenv"

var (
	buildVersionKey    = attribute.Key("version")
	buildRevisionKey   = attribute.Key("revision")
	buildModifiedKey   = attribute.Key("modified")
	buildCommitTimeKey = attribute.Key("commit.time")
	buildGoVersionKey  = attribute.Key("go.version")
	buildMainPathKey   = attribute.Key("go.main.path")
)

// registerBuildInfoMetric exposes the running component's immutable build
// identity as a single Prometheus-style info gauge (value fixed at 1,
// metadata carried in labels).
func registerBuildInfoMetric(serviceName string) error {
	meter := otel.Meter(buildInfoMeterName)
	info, err := meter.Int64ObservableGauge(
		"multigres.build.info",
		metric.WithDescription("Build and version information for the running Multigres component."),
	)
	if err != nil {
		return err
	}

	attrs := buildInfoMetricAttributes(serviceName, Version(), readBuildSnapshot())
	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(info, 1, metric.WithAttributeSet(attrs))
			return nil
		},
		info,
	)
	return err
}

func buildInfoMetricAttributes(serviceName, version string, snap buildSnapshot) attribute.Set {
	commitTime := ""
	if !snap.commitTime.IsZero() {
		commitTime = snap.commitTime.UTC().Format(time.RFC3339)
	}

	return attribute.NewSet(
		semconv.ServiceName(serviceName),
		buildVersionKey.String(version),
		buildRevisionKey.String(snap.revision),
		buildModifiedKey.Bool(snap.modified),
		buildCommitTimeKey.String(commitTime),
		buildGoVersionKey.String(snap.goVersion),
		buildMainPathKey.String(snap.mainPath),
	)
}
