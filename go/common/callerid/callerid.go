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
// to the multipooler, in two forms:
//
//   - Observability: the identity is mirrored into OpenTelemetry baggage, which
//     propagates automatically over gRPC to the pooler (and future shards), so
//     spans and logs can attribute a query to the app that issued it, not just
//     the shared database user.
//   - Typed identity: a mtrpc.CallerID is stashed in the context so the
//     gateway's queryservice client can set it as a first-class request field
//     the pooler can read.
//
// This is attribution (who issued the query), not correlation (which request
// this is). Correlation is already handled by the OpenTelemetry trace id, which
// propagates on the same calls; this package does not touch it.
//
// The identity has two parts, with different trust levels: the authenticated
// database user (proven at login, trustworthy) and the client-supplied
// application_name (an assertion). Code that makes decisions (authorization,
// quotas) must key on the authenticated user, never the application name. They
// map to the CallerID proto's Principal and Component fields respectively.
package callerid

import (
	"context"

	"go.opentelemetry.io/otel/baggage"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Telemetry keys for the caller identity, used both as OpenTelemetry baggage
// members (propagated over gRPC) and as span attributes on the pooler.
const (
	KeyAuthenticatedUser = "mg.caller.authenticated_user"
	KeyApplicationName   = "mg.caller.application_name"
)

type callerIDKey struct{}

// New builds a CallerID from a client's identity. authenticatedUser is the
// database user proven at login (trustworthy); clientApplicationName is the
// client-supplied application_name (an assertion). They map to the CallerID
// proto's Principal and Component fields.
func New(authenticatedUser, clientApplicationName string) *mtrpcpb.CallerID {
	return &mtrpcpb.CallerID{Principal: authenticatedUser, Component: clientApplicationName}
}

// NewContext returns a context carrying cid for the typed request field and
// mirrors the caller identity into OpenTelemetry baggage so it propagates
// downstream for observability. A nil cid is a no-op.
func NewContext(ctx context.Context, cid *mtrpcpb.CallerID) context.Context {
	if cid == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, callerIDKey{}, cid)

	var members []baggage.Member
	// NewMemberRaw keeps arbitrary values (e.g. an application_name with spaces)
	// intact; the SDK encodes them on the wire.
	if u := cid.GetPrincipal(); u != "" {
		if m, err := baggage.NewMemberRaw(KeyAuthenticatedUser, u); err == nil {
			members = append(members, m)
		}
	}
	if a := cid.GetComponent(); a != "" {
		if m, err := baggage.NewMemberRaw(KeyApplicationName, a); err == nil {
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
