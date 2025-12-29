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

package telemetry

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func getTraceparent(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {

		// Extract trace context to W3C Trace Context format
		carrier := propagation.MapCarrier{}
		propagator := otel.GetTextMapPropagator()
		propagator.Inject(ctx, carrier)

		// Get traceparent value (format: version-trace_id-span_id-flags)
		if traceparent, ok := carrier["traceparent"]; ok {
			return traceparent
		}
	}
	return ""
}

func addTraceparent(ctx context.Context, cmd *exec.Cmd) {
	var traceparent string
	if cmd.Process == nil {
		traceparent = getTraceparent(ctx)
	}

	if traceparent != "" {
		// Initialize Env with current environment if not set
		if cmd.Env == nil {
			cmd.Env = os.Environ()
		}
		// Add TRACEPARENT environment variable
		cmd.Env = append(cmd.Env, fmt.Sprintf("TRACEPARENT=%s", traceparent))
	}
}

func StartCmd(ctx context.Context, cmd *exec.Cmd) error {
	addTraceparent(ctx, cmd)
	return cmd.Start()
}

// prepareCmd sets up tracing context for a command and returns a cleanup function.
// If clientSpan is true, creates a span that must be ended by calling the returned function.
func prepareCmd(ctx context.Context, cmd *exec.Cmd, clientSpan bool) func() {
	var span trace.Span
	if clientSpan {
		ctx, span = tracer.Start(ctx, cmd.Path)
	}
	addTraceparent(ctx, cmd)
	return func() {
		if span != nil {
			span.End()
		}
	}
}

// RunCmd runs a command with optional tracing.
func RunCmd(ctx context.Context, cmd *exec.Cmd, clientSpan bool) error {
	defer prepareCmd(ctx, cmd, clientSpan)()
	return cmd.Run()
}

// RunCmdOutput runs a command and captures its stdout, with optional tracing.
func RunCmdOutput(ctx context.Context, cmd *exec.Cmd, clientSpan bool) ([]byte, error) {
	defer prepareCmd(ctx, cmd, clientSpan)()
	return cmd.Output()
}
