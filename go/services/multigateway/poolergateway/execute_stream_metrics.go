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

package poolergateway

import (
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Standard rpc.* instruments describe ExecuteStream transports, whose lifetime
// spans many SQL operations. These counters describe the operations and the
// pool itself. Attribute values come from the fixed vocabularies below; never
// SQL text, reservation, caller or stream identifiers.
type streamPoolMetrics struct {
	operations metric.Int64Counter       // transport=new|reused|legacy_*
	discards   metric.Int64Counter       // reason=...
	active     metric.Int64UpDownCounter // leased streams
	idle       metric.Int64UpDownCounter // retained idle streams
}

// Discard reasons.
const (
	discardCancelled   = "cancelled"    // caller context ended during the operation
	discardIncomplete  = "incomplete"   // consumer stopped before the completion frame
	discardTransport   = "transport"    // stream context already failed
	discardRetired     = "retired"      // operation cap reached
	discardIdleFull    = "idle_full"    // idle cache at capacity
	discardIdleExpired = "idle_expired" // idle timeout elapsed
	discardClosed      = "closed"       // pool closed
)

func transportAttr(v string) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(attribute.String("transport", v)))
}

var (
	attrNew               = transportAttr("new")
	attrReused            = transportAttr("reused")
	attrLegacyUnsupported = transportAttr("legacy_unsupported") // peer lacks ExecuteStream
	attrLegacyMetadata    = transportAttr("legacy_metadata")    // call carries custom RPC metadata
	attrLegacyPropagation = transportAttr("legacy_propagation") // nonstandard propagation fields
	discardAttr           = map[string]metric.MeasurementOption{}
)

func init() {
	for _, r := range []string{discardCancelled, discardIncomplete, discardTransport, discardRetired, discardIdleFull, discardIdleExpired, discardClosed} {
		discardAttr[r] = metric.WithAttributeSet(attribute.NewSet(attribute.String("reason", r)))
	}
}

var streamMetrics = sync.OnceValue(func() *streamPoolMetrics {
	return newStreamPoolMetrics(otel.Meter("github.com/multigres/multigres/go/services/multigateway/poolergateway"))
})

func newStreamPoolMetrics(meter metric.Meter) *streamPoolMetrics {
	m := &streamPoolMetrics{operations: noop.Int64Counter{}, discards: noop.Int64Counter{}, active: noop.Int64UpDownCounter{}, idle: noop.Int64UpDownCounter{}}
	warn := func(name string, err error) {
		slog.Warn("query stream metric unavailable", "metric", name, "error", err)
	}
	if c, err := meter.Int64Counter("mg.gateway.query_stream.operations",
		metric.WithDescription("Simple-query operations by transport: a new reusable stream, a reused one, or the legacy per-call StreamExecute RPC (legacy_* says why)"),
		metric.WithUnit("{operation}")); err != nil {
		warn("mg.gateway.query_stream.operations", err)
	} else {
		m.operations = c
	}
	if c, err := meter.Int64Counter("mg.gateway.query_stream.discards",
		metric.WithDescription("Reusable query streams closed, by reason; normal retirement appears here, not as a failure"),
		metric.WithUnit("{stream}")); err != nil {
		warn("mg.gateway.query_stream.discards", err)
	} else {
		m.discards = c
	}
	if c, err := meter.Int64UpDownCounter("mg.gateway.query_stream.active",
		metric.WithDescription("Reusable query streams currently leased to an operation"),
		metric.WithUnit("{stream}")); err != nil {
		warn("mg.gateway.query_stream.active", err)
	} else {
		m.active = c
	}
	if c, err := meter.Int64UpDownCounter("mg.gateway.query_stream.idle",
		metric.WithDescription("Reusable query streams retained idle for reuse"),
		metric.WithUnit("{stream}")); err != nil {
		warn("mg.gateway.query_stream.idle", err)
	} else {
		m.idle = c
	}
	return m
}
