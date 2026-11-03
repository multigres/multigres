// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package queryrpc

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/multigres/multigres/go/tools/latencydiag"
)

type fullCarrier struct{}

func (fullCarrier) Inject(_ context.Context, c propagation.TextMapCarrier) {
	c.Set("tracestate", strings.Repeat("x", 16*1024-len("tracestate")))
}

func (fullCarrier) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}
func (fullCarrier) Fields() []string { return []string{"tracestate"} }

// Recording must not force legacy-RPC fallback when the existing carrier is
// already at the operation envelope's limit. Report the coverage gap instead.
func TestDiagnosticsPreservePropagationLimit(t *testing.T) {
	original := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(fullCarrier{})
	defer otel.SetTextMapPropagator(original)
	base, valid := Propagation(t.Context())
	require.True(t, valid)
	c, err := latencydiag.Begin(latencydiag.Options{ID: "bounds", SampleEvery: 1, MaxEvents: 10})
	require.NoError(t, err)
	defer c.Stop()
	root := latencydiag.Message(t.Context(), 1, 1)
	carrier, valid := Propagation(root.Context())
	require.True(t, valid)
	require.Equal(t, base, carrier)
	root.End(nil, 0)
	report := c.Stop()
	require.EqualValues(t, 1, report.PropagationDropped)
}
