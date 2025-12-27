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

// Package ctxutil provides utilities for context manipulation, particularly
// for creating detached contexts that preserve telemetry metadata.
package ctxutil

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

type parentSpanContextKey struct{}

// Detach creates a new context that is NOT cancelled when the parent is cancelled,
// but preserves telemetry metadata (baggage, parent span context for linking).
//
// Use this for long-lived background tasks that should outlive the initiating request:
//   - Health checks for newly-pooled connections
//   - Graceful shutdown sequences
//   - Periodic maintenance tasks
//
// The returned context preserves:
//   - All baggage from the parent context
//   - Parent span context (retrievable via ParentSpanContext for optional linking)
//
// Unlike context.WithoutCancel() which preserves the parent span (making background
// work appear as children), Detach() stores the span context separately so callers
// can optionally link new spans without making them child spans.
//
// Example - starting a linked span:
//
//	bgCtx := ctxutil.Detach(requestCtx)
//	bgCtx, span := ctxutil.StartLinkedSpan(bgCtx, tracer, "background-task")
//	defer span.End()
func Detach(parent context.Context) context.Context {
	// Start fresh - no cancellation inheritance
	//nolint:gocritic // This is the legitimate entry point for detached contexts
	ctx := context.Background()

	// Preserve baggage (service metadata, etc.)
	if bag := baggage.FromContext(parent); bag.Len() > 0 {
		ctx = baggage.ContextWithBaggage(ctx, bag)
	}

	// Store parent span context for optional linking (but not as parent span)
	if span := trace.SpanFromContext(parent); span.SpanContext().IsValid() {
		ctx = context.WithValue(ctx, parentSpanContextKey{}, span.SpanContext())
	}

	return ctx
}

// ParentSpanContext retrieves the span context from the original parent context,
// if it was present when Detach was called. This allows callers to link new spans
// to the original context without making them child spans.
//
// Returns the parent span context and true if present, or an empty span context
// and false if no parent span was available when Detach was called.
func ParentSpanContext(ctx context.Context) (trace.SpanContext, bool) {
	psc, ok := ctx.Value(parentSpanContextKey{}).(trace.SpanContext)
	return psc, ok
}

// StartLinkedSpan creates a new root span that is linked to the parent span context
// stored in the context (if any). This is useful for background tasks that should
// have their own trace but maintain correlation with the originating request.
//
// The span is created with trace.WithNewRoot() so it starts a new trace, and if
// a parent span context is available (from a prior Detach call), it's linked via
// trace.WithLinks().
//
// Example:
//
//	bgCtx := ctxutil.Detach(requestCtx)
//	bgCtx, span := ctxutil.StartLinkedSpan(bgCtx, tracer, "background-task")
//	defer span.End()
func StartLinkedSpan(ctx context.Context, tracer trace.Tracer, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	spanOpts := []trace.SpanStartOption{trace.WithNewRoot()}
	if psc, ok := ParentSpanContext(ctx); ok {
		spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: psc}))
	}
	spanOpts = append(spanOpts, opts...)
	return tracer.Start(ctx, name, spanOpts...)
}
