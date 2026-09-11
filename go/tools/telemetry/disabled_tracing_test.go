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

package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

func TestDisabledExporterSamplingAndPropagation(t *testing.T) {
	cases := []struct {
		name, exporter, sampler string
		localRecords            bool
	}{
		{name: "explicit none, no sampler", exporter: "none", localRecords: false},
		{name: "explicit none, explicit sampler", exporter: "none", sampler: "always_on", localRecords: true},
		{name: "unset exporter keeps default sampler", exporter: "", localRecords: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", tc.exporter)
			t.Setenv("OTEL_TRACES_SAMPLER", tc.sampler)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
			old := otel.GetTracerProvider()
			t.Cleanup(func() { otel.SetTracerProvider(old) })
			telemetry := NewTelemetry()
			require.NoError(t, telemetry.initTracing(t.Context(), resource.Empty()))
			t.Cleanup(func() { require.NoError(t, telemetry.tracerProvider.Shutdown(context.Background())) })
			tracer := telemetry.tracerProvider.Tracer("test")

			ctx, parent := tracer.Start(t.Context(), "parent")
			defer parent.End()
			require.Equal(t, tc.localRecords, parent.IsRecording())
			require.True(t, parent.SpanContext().IsValid())
			require.Equal(t, tc.localRecords, parent.SpanContext().IsSampled())
			_, child := tracer.Start(ctx, "child")
			defer child.End()
			require.Equal(t, parent.SpanContext().TraceID(), child.SpanContext().TraceID())
			require.NotEqual(t, parent.SpanContext().SpanID(), child.SpanContext().SpanID())

			// A sampled remote parent is always honoured, so a process with export
			// disabled never strips the sampled flag from a trace passing through it.
			remote := trace.NewSpanContext(trace.SpanContextConfig{TraceID: parent.SpanContext().TraceID(), SpanID: parent.SpanContext().SpanID(), TraceFlags: trace.FlagsSampled, Remote: true})
			_, continued := tracer.Start(trace.ContextWithRemoteSpanContext(t.Context(), remote), "remote-child")
			defer continued.End()
			require.Equal(t, remote.TraceID(), continued.SpanContext().TraceID())
			require.True(t, continued.IsRecording())
			require.True(t, continued.SpanContext().IsSampled())
		})
	}
}
