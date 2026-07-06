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

func TestNewContext_MirrorsBaggage(t *testing.T) {
	ctx := NewContext(context.Background(), New("alice", "checkout-service"))

	bag := baggage.FromContext(ctx)
	assert.Equal(t, "alice", bag.Member(BaggagePrincipal).Value())
	assert.Equal(t, "checkout-service", bag.Member(BaggageComponent).Value())
}

func TestNewContext_ArbitraryComponentSurvives(t *testing.T) {
	// application_name can contain spaces/punctuation; NewMemberRaw must keep it.
	ctx := NewContext(context.Background(), New("alice", "My App v1.2 (beta)"))

	assert.Equal(t, "My App v1.2 (beta)", FromContext(ctx).GetComponent())
	assert.Equal(t, "My App v1.2 (beta)", baggage.FromContext(ctx).Member(BaggageComponent).Value())
}

func TestNewContext_NilIsNoop(t *testing.T) {
	ctx := NewContext(context.Background(), nil)

	assert.Nil(t, FromContext(ctx))
	assert.Equal(t, 0, baggage.FromContext(ctx).Len())
}

func TestFromContext_Absent(t *testing.T) {
	assert.Nil(t, FromContext(context.Background()))
}

func TestNewContext_EmptyComponentOmittedFromBaggage(t *testing.T) {
	ctx := NewContext(context.Background(), New("alice", ""))

	assert.Equal(t, "alice", baggage.FromContext(ctx).Member(BaggagePrincipal).Value())
	// No component member when application_name is empty.
	assert.Empty(t, baggage.FromContext(ctx).Member(BaggageComponent).Value())
}
