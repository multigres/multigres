// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package latencydiag

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/baggage"
)

const baggageKey = "multigres.latency"

// ExportReference encodes only capture and span identity for transport adapters.
// It returns an empty string when this context is not selected.
func ExportReference(ctx context.Context) string {
	if !Selected(ctx) {
		return ""
	}
	s := ctx.Value(contextKey{}).(*Span)
	return fmt.Sprintf("%s|%s|%s:%d", s.capture.opts.ID, s.event.Root, instance, s.event.ID)
}

// ImportReference accepts identity only for the active local capture.
// Remote metadata cannot enable capture or inject arbitrary diagnostic values.
func ImportReference(ctx context.Context, value, phase string) (context.Context, *Span) {
	c := active.Load()
	if c == nil || len(value) > 256 {
		return ctx, nil
	}
	parts := strings.Split(value, "|")
	if len(parts) != 3 || parts[0] != c.opts.ID || !safeReference(parts[1]) || !safeReference(parts[2]) {
		return ctx, nil
	}
	s := c.startSpan(ctx, Event{Root: parts[1], RemoteParent: parts[2], Phase: phase})
	if s == nil {
		return ctx, nil
	}
	return s.ctx, s
}

// InjectCarrier carries per-operation identity in the existing W3C baggage
// envelope, including when the process's OTel exporter is disabled. Old peers
// accept and ignore the member; no new wire fields or transport changes.
func InjectCarrier(ctx context.Context, carrier map[string]string) {
	value := ExportReference(ctx)
	if value == "" {
		return
	}
	bag, err := baggage.Parse(carrier["baggage"])
	if err != nil {
		PropagationDropped(ctx)
		return
	}
	member, err := baggage.NewMember(baggageKey, value)
	if err != nil {
		PropagationDropped(ctx)
		return
	}
	bag, err = bag.SetMember(member)
	if err == nil {
		carrier["baggage"] = bag.String()
	} else {
		PropagationDropped(ctx)
	}
}

func ExtractCarrier(ctx context.Context, carrier map[string]string) (context.Context, *Span) {
	if !Active() || carrier["baggage"] == "" {
		return ctx, nil
	}
	bag, err := baggage.Parse(carrier["baggage"])
	if err != nil {
		return ctx, nil
	}
	return ImportReference(ctx, bag.Member(baggageKey).Value(), "rpc.operation.server")
}

func safeReference(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, b := range s {
		if !(b >= 'a' && b <= 'f' || b >= '0' && b <= '9' || b == '/' || b == ':') {
			return false
		}
	}
	return true
}
