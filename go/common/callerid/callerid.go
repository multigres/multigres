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

// Package callerid carries a client's identity from the multigateway edge down
// to the multipooler. It serves two layers:
//
//   - Layer 2 (observability): principal/component are mirrored into
//     OpenTelemetry baggage, which propagates automatically over gRPC to the
//     pooler (and future shards), so spans and logs can attribute a query to
//     the app that issued it, not just the shared database user.
//   - Layer 3 (typed identity): a mtrpc.CallerID is stashed in the context so
//     the gateway's queryservice client can set it as a first-class request
//     field the pooler can read.
//
// principal is the authenticated database user and is trustworthy; component is
// the client-supplied application_name and is only an assertion. Code that
// makes decisions (authorization, quotas) must key on principal, never
// component.
package callerid

import (
	"context"

	"go.opentelemetry.io/otel/baggage"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Baggage keys for the observability layer.
const (
	BaggagePrincipal = "mg.principal"
	BaggageComponent = "mg.component"
)

type callerIDKey struct{}

// New builds a CallerID from a client's identity. principal is the
// authenticated database user; component is the client's application_name.
func New(principal, component string) *mtrpcpb.CallerID {
	return &mtrpcpb.CallerID{Principal: principal, Component: component}
}

// NewContext returns a context carrying cid for the typed request field
// (Layer 3) and mirrors its principal/component into OpenTelemetry baggage so
// they propagate downstream for observability (Layer 2). A nil cid is a no-op.
func NewContext(ctx context.Context, cid *mtrpcpb.CallerID) context.Context {
	if cid == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, callerIDKey{}, cid)

	var members []baggage.Member
	// NewMemberRaw keeps arbitrary values (e.g. an application_name with spaces)
	// intact; the SDK encodes them on the wire.
	if p := cid.GetPrincipal(); p != "" {
		if m, err := baggage.NewMemberRaw(BaggagePrincipal, p); err == nil {
			members = append(members, m)
		}
	}
	if c := cid.GetComponent(); c != "" {
		if m, err := baggage.NewMemberRaw(BaggageComponent, c); err == nil {
			members = append(members, m)
		}
	}
	if len(members) > 0 {
		if bag, err := baggage.New(members...); err == nil {
			ctx = baggage.ContextWithBaggage(ctx, bag)
		}
	}
	return ctx
}

// FromContext returns the CallerID stored in ctx by NewContext, or nil.
func FromContext(ctx context.Context) *mtrpcpb.CallerID {
	cid, _ := ctx.Value(callerIDKey{}).(*mtrpcpb.CallerID)
	return cid
}
