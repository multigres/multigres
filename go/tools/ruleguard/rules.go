// Copyright 2025 Supabase, Inc.
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

//go:build ruleguard

package gorules

import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

func disallowUnderscoreInFlags(m dsl.Matcher) {
	m.Import("github.com/spf13/pflag")

	m.Match(
		`viperutil.Configure($_, $name, $*_)`,
		`viperutil.Options[$_]{$*_, FlagName: $name, $*_}`).
		Where(m["name"].Text.Matches("_")).
		Report("viper flag name contains an underscore; use dashes instead")

	m.Match(`$fs.$_($name, $*_)`).
		Where(
			m["fs"].Type.Is("*pflag.FlagSet") && m["name"].Text.Matches("_")).
		Report("FlagSet flag name contains an underscore; use dashes instead")

	m.Match(`$fs.$_($ptr, $name, $*_)`).
		Where(
			m["fs"].Type.Is("*pflag.FlagSet") && m["ptr"].Type.HasPointers() && m["name"].Text.Matches("_")).
		Report("FlagSet flag name contains an underscore; use dashes instead")
}

func disallowOtelMeterOutsideMetricsFiles(m dsl.Matcher) {
	m.Import("go.opentelemetry.io/otel")

	m.Match(`otel.Meter($*_)`).
		Where(!m.File().Name.Matches(`(_test|metrics)\.go$`)).
		Report("otel.Meter() should only be called in *_test.go or *metrics.go files")
}

func disallowMetricsConstructorArgs(m dsl.Matcher) {
	m.Match(`func NewMetrics($*params) $*_ { $*_ }`).
		Where(
			m.File().Name.Matches(`metrics\.go$`) &&
				m["params"].Text != "()").
		Report("NewMetrics() in metrics.go should take no arguments to maintain isolation from service code. Return (*Metrics, error) and let caller handle logging.")
}

func requireContextBackgroundJustification(m dsl.Matcher) {
	m.Match(`context.Background()`).
		Where(
			!m.File().Name.Matches(`_test\.go$`) &&
				!m.File().PkgPath.Matches(`/test/|/testutil/|testutil$`)).
		Report("context.Background() requires justification. Use context.TODO() if no context is available, ctxutil.Detach(ctx) for long-lived background tasks, or add //nolint:gocritic // <reason> for legitimate entry points")
}

func requireGrpcCommonNewClient(m dsl.Matcher) {
	m.Import("google.golang.org/grpc")

	m.Match(`grpc.NewClient($*_)`).
		Where(
			!m.File().Name.Matches(`_test\.go$`) &&
				!m.File().PkgPath.Matches(`/grpccommon$|/test/|/testutil/|testutil$`)).
		Report("use grpccommon.NewClient() instead of grpc.NewClient() to ensure telemetry instrumentation")
}
