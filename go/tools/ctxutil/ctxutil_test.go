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

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestDetach_NotCancelledWhenParentCancelled(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())

	detached := Detach(parent)

	// Cancel the parent
	cancel()

	// Parent should be cancelled
	select {
	case <-parent.Done():
		// Expected
	default:
		t.Fatal("parent context should be cancelled")
	}

	// Detached should NOT be cancelled
	select {
	case <-detached.Done():
		t.Fatal("detached context should not be cancelled when parent is cancelled")
	default:
		// Expected - detached is still active
	}

	// Verify detached has no deadline
	_, hasDeadline := detached.Deadline()
	assert.False(t, hasDeadline, "detached context should have no deadline")

	// Verify detached has no error
	assert.NoError(t, detached.Err(), "detached context should have no error")
}

func TestDetach_PreservesBaggage(t *testing.T) {
	// Create baggage with test values
	member1, err := baggage.NewMember("service", "multiorch")
	require.NoError(t, err)
	member2, err := baggage.NewMember("request-id", "abc123")
	require.NoError(t, err)
	bag, err := baggage.New(member1, member2)
	require.NoError(t, err)

	parent := baggage.ContextWithBaggage(context.Background(), bag)

	detached := Detach(parent)

	// Verify baggage is preserved
	preservedBag := baggage.FromContext(detached)
	assert.Equal(t, "multiorch", preservedBag.Member("service").Value())
	assert.Equal(t, "abc123", preservedBag.Member("request-id").Value())
}

func TestDetach_WorksWithNoBaggage(t *testing.T) {
	parent := context.Background()

	detached := Detach(parent)

	// Should work without panicking
	assert.NotNil(t, detached)

	// Baggage should be empty
	bag := baggage.FromContext(detached)
	assert.Equal(t, 0, bag.Len())
}

func TestDetach_StoresParentSpanContext(t *testing.T) {
	// Create a mock span context
	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	// Create a context with this span context
	parent := trace.ContextWithSpanContext(context.Background(), sc)

	detached := Detach(parent)

	// Verify parent span context is retrievable
	psc, ok := ParentSpanContext(detached)
	require.True(t, ok, "should have parent span context")
	assert.Equal(t, traceID, psc.TraceID())
	assert.Equal(t, spanID, psc.SpanID())
	assert.True(t, psc.IsSampled())
}

func TestDetach_WorksWithNoSpan(t *testing.T) {
	parent := context.Background()

	detached := Detach(parent)

	// Should work without panicking
	assert.NotNil(t, detached)

	// ParentSpanContext should return false
	_, ok := ParentSpanContext(detached)
	assert.False(t, ok, "should not have parent span context when parent had no span")
}

func TestDetach_DoesNotInheritParentSpan(t *testing.T) {
	// Create a mock span context
	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	parent := trace.ContextWithSpanContext(context.Background(), sc)

	detached := Detach(parent)

	// The detached context should NOT have a span (trace.SpanFromContext returns noop)
	span := trace.SpanFromContext(detached)
	assert.False(t, span.SpanContext().IsValid(), "detached context should not have an active span")
}

func TestDetach_CanAddOwnCancellation(t *testing.T) {
	parent := context.Background()
	detached := Detach(parent)

	// Add cancellation to the detached context
	ctx, cancel := context.WithCancel(detached)
	defer cancel()

	// Add timeout to the detached context
	ctxWithTimeout, cancelTimeout := context.WithTimeout(detached, time.Hour)
	defer cancelTimeout()

	// Both should work
	assert.NotNil(t, ctx)
	assert.NotNil(t, ctxWithTimeout)

	// Cancel should work
	cancel()
	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Fatal("derived context should be cancellable")
	}

	// Original detached should still be active
	select {
	case <-detached.Done():
		t.Fatal("original detached context should not be affected")
	default:
		// Expected
	}
}

func TestParentSpanContext_ReturnsEmptyWhenNotDetached(t *testing.T) {
	// Regular context (not from Detach)
	ctx := context.Background()

	psc, ok := ParentSpanContext(ctx)
	assert.False(t, ok)
	assert.False(t, psc.IsValid())
}

func TestDetach_PreservesBothBaggageAndSpan(t *testing.T) {
	// Create baggage
	member, err := baggage.NewMember("key", "value")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)

	// Create span context
	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	})

	// Create parent with both
	parent := context.Background()
	parent = baggage.ContextWithBaggage(parent, bag)
	parent = trace.ContextWithSpanContext(parent, sc)

	detached := Detach(parent)

	// Verify both are preserved
	preservedBag := baggage.FromContext(detached)
	assert.Equal(t, "value", preservedBag.Member("key").Value())

	psc, ok := ParentSpanContext(detached)
	require.True(t, ok)
	assert.Equal(t, traceID, psc.TraceID())
}

func TestStartLinkedSpan_LinksToParentSpan(t *testing.T) {
	// Create a parent span context
	parentTraceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	parentSpanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	parentSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    parentTraceID,
		SpanID:     parentSpanID,
		TraceFlags: trace.FlagsSampled,
	})

	parent := trace.ContextWithSpanContext(context.Background(), parentSC)
	detached := Detach(parent)

	// Set up an in-memory exporter to capture spans
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test")

	// Create a linked span
	ctx, span := StartLinkedSpan(detached, tracer, "test-span")
	span.End()

	// Verify the span was created
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)

	// Verify it's a new root (different trace ID from parent)
	createdSpan := spans[0]
	assert.NotEqual(t, parentTraceID, createdSpan.SpanContext.TraceID(),
		"linked span should have a new trace ID (new root)")

	// Verify it has a link to the parent span
	require.Len(t, createdSpan.Links, 1, "should have exactly one link")
	link := createdSpan.Links[0]
	assert.Equal(t, parentTraceID, link.SpanContext.TraceID(),
		"link should reference parent trace ID")
	assert.Equal(t, parentSpanID, link.SpanContext.SpanID(),
		"link should reference parent span ID")

	// Verify the context has the new span
	spanFromCtx := trace.SpanFromContext(ctx)
	assert.Equal(t, createdSpan.SpanContext.SpanID(), spanFromCtx.SpanContext().SpanID())
}

func TestStartLinkedSpan_WorksWithoutParentSpan(t *testing.T) {
	// Detach from a context with no span
	detached := Detach(context.Background())

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test")

	// Should not panic and should create a span without links
	ctx, span := StartLinkedSpan(detached, tracer, "test-span")
	span.End()

	assert.NotNil(t, ctx)

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Empty(t, spans[0].Links, "should have no links when parent had no span")
}

func TestStartLinkedSpan_PassesAdditionalOptions(t *testing.T) {
	detached := Detach(context.Background())

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test")

	// Pass additional span options
	_, span := StartLinkedSpan(detached, tracer, "test-span",
		trace.WithSpanKind(trace.SpanKindServer))
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, trace.SpanKindServer, spans[0].SpanKind)
}
