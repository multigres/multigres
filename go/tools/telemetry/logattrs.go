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

package telemetry

import (
	"context"
	"log/slog"
	"net"
	"slices"

	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Keys of the context-carried log attributes, emitted on every *Context log
// call. Each key holds at most one value; setting it again replaces the old
// one. Set them with WithTask, WithStep and WithClientAddress.
const (
	LogKeyTask          = "task"
	LogKeyStep          = "step"
	LogKeyClientAddress = string(semconv.ClientAddressKey)
)

type logAttrsKey struct{}

// WithTask names the work ctx belongs to: an RPC, a background loop or
// one-shot task, or a lifecycle phase. It clears any inherited step.
func WithTask(ctx context.Context, name string) context.Context {
	return withLogAttr(ctx, slog.String(LogKeyTask, name), LogKeyStep)
}

// WithStep names the current step within the task.
func WithStep(ctx context.Context, name string) context.Context {
	return withLogAttr(ctx, slog.String(LogKeyStep, name))
}

// WithClientAddress records the caller's address, without the port.
func WithClientAddress(ctx context.Context, addr net.Addr) context.Context {
	return withLogAttr(ctx, slog.Any(LogKeyClientAddress, hostOnly{addr}))
}

// withLogAttr sets attr on ctx, replacing any attr with the same key and
// dropping the attrs named in clear.
func withLogAttr(ctx context.Context, attr slog.Attr, clear ...string) context.Context {
	existing := logAttrsFromContext(ctx)
	attrs := make([]slog.Attr, 0, len(existing)+1)
	for _, a := range existing {
		if a.Key != attr.Key && !slices.Contains(clear, a.Key) {
			attrs = append(attrs, a)
		}
	}
	attrs = append(attrs, attr)
	return context.WithValue(ctx, logAttrsKey{}, attrs)
}

func logAttrsFromContext(ctx context.Context) []slog.Attr {
	attrs, _ := ctx.Value(logAttrsKey{}).([]slog.Attr)
	return attrs
}

// hostOnly logs an address without its port. It is resolved lazily, so
// contexts that never log don't pay for formatting.
type hostOnly struct{ addr net.Addr }

func (h hostOnly) LogValue() slog.Value {
	s := h.addr.String()
	if host, _, err := net.SplitHostPort(s); err == nil {
		return slog.StringValue(host)
	}
	return slog.StringValue(s)
}
