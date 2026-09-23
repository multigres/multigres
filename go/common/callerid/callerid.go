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
// to the multipooler as a typed mtrpc.CallerID: the gateway stashes it in the
// context, its queryservice client sets it as a first-class request field, and
// the pooler reads it from the request to attribute spans and logs to the app
// that issued the query rather than only the shared database user.
//
// The identity deliberately travels only in the request field, not in
// OpenTelemetry baggage. It used to be mirrored there too, but nothing read the
// baggage members (the pooler attributes from the typed field) while every RPC
// on the gateway to pooler hop, one per SQL statement, paid to encode the header
// on the gateway and parse it on the pooler. Anything that needs the identity
// in telemetry should take it from the request, as the pooler does.
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

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Telemetry keys for the caller identity, used as span attributes on the pooler.
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

// NewContext returns a context carrying cid for the typed request field. A nil
// cid is a no-op.
func NewContext(ctx context.Context, cid *mtrpcpb.CallerID) context.Context {
	if cid == nil {
		return ctx
	}
	return context.WithValue(ctx, callerIDKey{}, cid)
}

// FromContext returns the CallerID stored in ctx by NewContext, or nil.
func FromContext(ctx context.Context) *mtrpcpb.CallerID {
	cid, _ := ctx.Value(callerIDKey{}).(*mtrpcpb.CallerID)
	return cid
}
