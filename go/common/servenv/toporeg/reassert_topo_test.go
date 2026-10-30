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

package toporeg

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// TestReassert_RestoresRegistrationThroughStore drives the real registration
// path — toporeg over a topoclient.Store — and removes the record behind the
// component's back, the way a lease expiry, a replaced connection, or a cell
// reconfiguration does. The record must come back on its own.
//
// This is the end-to-end form of the guarantee the ephemeral registrations
// depend on: the topology layer is free to drop an entry whenever its
// liveness binding ends, because the owner puts it back.
func TestReassert_RestoresRegistrationThroughStore(t *testing.T) {
	shortenReassert(t)

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const cell = "zone-1"
		ts := memorytopo.NewServer(ctx, cell)
		defer ts.Close()

		gateway := topoclient.NewMultigateway("gw-1", cell, "gw-1.example.com")

		tr := Register(
			func(ctx context.Context) error { return ts.RegisterMultigateway(ctx, gateway, true) },
			func(ctx context.Context) error { return ts.UnregisterMultigateway(ctx, gateway.Id) },
			func(string) {},
			WithReassert(),
		)
		require.NotNil(t, tr)

		_, err := ts.GetMultigateway(ctx, gateway.Id)
		require.NoError(t, err, "component should be registered after Register returns")

		// Something removed the entry without telling the component.
		require.NoError(t, ts.UnregisterMultigateway(ctx, gateway.Id))
		_, err = ts.GetMultigateway(ctx, gateway.Id)
		require.Error(t, err, "precondition: the entry is gone")

		assert.Eventually(t, func() bool {
			_, err := ts.GetMultigateway(ctx, gateway.Id)
			return err == nil
		}, 2*time.Second, 10*time.Millisecond,
			"re-assertion should restore a registration removed behind the component's back")

		// And a real deregistration still wins: nothing puts it back afterwards.
		tr.Unregister()
		testpoll.Never(t, func() bool {
			_, err := ts.GetMultigateway(ctx, gateway.Id)
			return err == nil
		}, 300*time.Millisecond, 10*time.Millisecond,
			"deregistration must be final; re-assertion must not resurrect the entry")
	})
}
