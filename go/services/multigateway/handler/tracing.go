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

package handler

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// startQuerySpan creates a server-side span for a gateway query operation.
// The span follows OTel database semantic conventions and does NOT include
// db.query.text for security reasons (user queries may contain PII).
func startQuerySpan(
	ctx context.Context,
	operationName string,
	protocol string,
	dbNamespace string,
	user string,
) (context.Context, trace.Span) {
	return telemetry.Tracer().Start(ctx, "gateway.query",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			semconv.DBSystemNamePostgreSQL,
			semconv.DBOperationName(operationName),
			semconv.DBNamespace(dbNamespace),
			attribute.String("db.query.protocol", protocol),
			attribute.String("db.user", user),
		),
	)
}

// setSpanPlanAttributes enriches the active span with plan-level metadata.
// Called from recordQueryCompletion after the executor returns.
func setSpanPlanAttributes(ctx context.Context, planType string, tablesUsed []string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	if planType != "" {
		span.SetAttributes(attribute.String("db.plan.type", planType))
	}
	if len(tablesUsed) > 0 {
		span.SetAttributes(attribute.StringSlice("db.tables_used", tablesUsed))
	}
}

// recordSpanError records an error on a span with its SQLSTATE code.
func recordSpanError(span trace.Span, err error, sqlstate string) {
	span.RecordError(err)
	attrs := []attribute.KeyValue{}
	if sqlstate != "" {
		attrs = append(attrs, attribute.String("db.response.status_code", sqlstate))
	}
	span.SetStatus(codes.Error, err.Error())
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}
