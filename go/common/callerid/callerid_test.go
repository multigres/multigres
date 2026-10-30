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

package callerid

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/baggage"
)

func TestNewContext_RoundTrip(t *testing.T) {
	ctx := NewContext(context.Background(), New("alice", "checkout-service"))

	cid := FromContext(ctx)
	if assert.NotNil(t, cid) {
		assert.Equal(t, "alice", cid.GetPrincipal())
		assert.Equal(t, "checkout-service", cid.GetComponent())
	}
}

func TestNewContext_ArbitraryApplicationNameSurvives(t *testing.T) {
	// application_name can contain spaces/punctuation and is carried verbatim.
	ctx := NewContext(context.Background(), New("alice", "My App v1.2 (beta)"))

	assert.Equal(t, "My App v1.2 (beta)", FromContext(ctx).GetComponent())
}

func TestNewContext_DoesNotTouchBaggage(t *testing.T) {
	// The identity travels only in the typed request field. Putting it in
	// baggage as well would add a header to every gateway to pooler RPC (one
	// per SQL statement) that nothing reads; see the package comment.
	ctx := NewContext(context.Background(), New("alice", "checkout-service"))

	assert.Equal(t, 0, baggage.FromContext(ctx).Len())
}

func TestNewContext_PreservesExistingBaggage(t *testing.T) {
	member, err := baggage.NewMemberRaw("tenant", "acme")
	assert.NoError(t, err)
	bag, err := baggage.New(member)
	assert.NoError(t, err)
	parent := baggage.ContextWithBaggage(context.Background(), bag)

	ctx := NewContext(parent, New("alice", "checkout-service"))

	assert.Equal(t, "acme", baggage.FromContext(ctx).Member("tenant").Value(), "unrelated baggage set by callers is left alone")
	assert.Equal(t, "alice", FromContext(ctx).GetPrincipal())
}

func TestNewContext_NilIsNoop(t *testing.T) {
	ctx := NewContext(context.Background(), nil)

	assert.Nil(t, FromContext(ctx))
}

func TestFromContext_Absent(t *testing.T) {
	assert.Nil(t, FromContext(context.Background()))
}
