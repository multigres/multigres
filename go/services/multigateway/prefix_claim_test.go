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

package multigateway

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func gatewayID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIGATEWAY,
		Cell:      cell,
		Name:      name,
	}
}

// TestClaimUnusedPrefix_DistinctClaims verifies that gateways claiming
// concurrently from the same pool end up with distinct prefixes: ownership is
// decided by the atomic claim, not by reading each other's records.
func TestClaimUnusedPrefix_DistinctClaims(t *testing.T) {
	ctx := context.Background()
	const cell = "zone-1"
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	mg := &Multigateway{ts: ts}

	seen := make(map[uint32]bool)
	for i := range 5 {
		id := gatewayID(cell, string(rune('a'+i)))
		prefix, err := mg.claimUnusedPrefix(ctx, id)
		require.NoError(t, err)
		require.NotZero(t, prefix)
		assert.False(t, seen[prefix], "prefix %d claimed twice", prefix)
		seen[prefix] = true
	}
}

// TestClaimUnusedPrefix_AvoidsRecordAdvertisedPrefixes covers the
// mixed-version case: a gateway from before prefix claims advertises its
// prefix only in its record, with no claim file. New gateways must avoid it.
// Every prefix except the record-advertised one is claimed, so the claim loop
// must exhaust rather than take the advertised prefix.
func TestClaimUnusedPrefix_AvoidsRecordAdvertisedPrefixes(t *testing.T) {
	ctx := context.Background()
	const cell = "zone-1"
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	// An old-version gateway advertising prefix 7 in its record only.
	oldGW := topoclient.NewMultigateway("old", cell, "old.example.com")
	oldGW.PidPrefix = 7
	require.NoError(t, ts.RegisterMultigateway(ctx, oldGW, true))

	// Claim every other prefix on behalf of a third party.
	blocker := gatewayID(cell, "blocker")
	for p := uint32(1); p <= pid.MaxPrefix; p++ {
		if p == 7 {
			continue
		}
		require.NoError(t, ts.ClaimGatewayPrefix(ctx, p, blocker))
	}

	mg := &Multigateway{ts: ts}
	_, err := mg.claimUnusedPrefix(ctx, gatewayID(cell, "new"))
	require.Error(t, err, "prefix 7 is advertised by an unclaimed old-version record and must not be taken")
	assert.Contains(t, err.Error(), "no available PID prefix")
}

// TestClaimGatewayPrefix_TheftDetection verifies the store-level claim
// semantics the re-assertion path relies on: a holder refreshes its own claim
// freely, a competitor is rejected without overwriting it, and once a claim
// expires (claims are never explicitly released — lease expiry is the only
// release path, simulated here with a direct delete) the prefix passes to
// the next claimant and the previous holder's refresh detects the loss.
func TestClaimGatewayPrefix_TheftDetection(t *testing.T) {
	ctx := context.Background()
	const cell = "zone-1"
	ts := memorytopo.NewServer(ctx, cell)
	defer ts.Close()

	idA := gatewayID(cell, "gw-a")
	idB := gatewayID(cell, "gw-b")

	require.NoError(t, ts.ClaimGatewayPrefix(ctx, 42, idA))
	// Refresh by the holder succeeds.
	require.NoError(t, ts.ClaimGatewayPrefix(ctx, 42, idA))
	// A competitor is rejected.
	err := ts.ClaimGatewayPrefix(ctx, 42, idB)
	assert.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}),
		"competitor claim should return NodeExists, got: %v", err)
	// Simulate lease expiry of A's claim; the competitor can now claim.
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err)
	require.NoError(t, conn.Delete(ctx, topoclient.GatewayPrefixesPath+"/42", nil))
	require.NoError(t, ts.ClaimGatewayPrefix(ctx, 42, idB))
	// And the previous holder's refresh now fails — this is how a gateway
	// discovers its claim was lost after an expiry.
	err = ts.ClaimGatewayPrefix(ctx, 42, idA)
	assert.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}),
		"previous holder's refresh should return NodeExists, got: %v", err)
}

// TestRefreshPrefixClaim_LostClaimIsFatal verifies the gateway's response to
// losing its prefix claim: a healthy refresh triggers nothing, a transient
// topology error triggers nothing (the next re-assertion retries), and a
// claim held by another gateway sets prefixLost and initiates graceful
// shutdown exactly once — a live gateway cannot renumber, so a fresh process
// claiming a fresh prefix is the only repair.
func TestRefreshPrefixClaim_LostClaimIsFatal(t *testing.T) {
	ctx := context.Background()
	const cell = "zone-1"
	ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
	defer ts.Close()

	idA := gatewayID(cell, "gw-a")
	idB := gatewayID(cell, "gw-b")

	var shutdowns atomic.Int32
	mg := &Multigateway{ts: ts}
	mg.shutdownOnPrefixLoss = func() { shutdowns.Add(1) }

	require.NoError(t, ts.ClaimGatewayPrefix(ctx, 42, idA))

	// Healthy refresh: no flag, no shutdown.
	require.NoError(t, mg.refreshPrefixClaim(ctx, 42, idA))
	assert.False(t, mg.prefixLost.Load())
	assert.Zero(t, shutdowns.Load())

	// Transient topology error: surfaced to the caller, but not fatal.
	factory.SetError(errors.New("topology unavailable"))
	err := mg.refreshPrefixClaim(ctx, 42, idA)
	require.Error(t, err)
	assert.False(t, mg.prefixLost.Load(), "a transient error must not be treated as a lost claim")
	assert.Zero(t, shutdowns.Load(), "a transient error must not shut the gateway down")
	factory.SetError(nil)

	// The claim expires (simulated) and another gateway takes it.
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err)
	require.NoError(t, conn.Delete(ctx, topoclient.GatewayPrefixesPath+"/42", nil))
	require.NoError(t, ts.ClaimGatewayPrefix(ctx, 42, idB))

	// Loss detected: flag set, shutdown initiated.
	err = mg.refreshPrefixClaim(ctx, 42, idA)
	require.Error(t, err)
	assert.True(t, mg.prefixLost.Load())
	assert.Equal(t, int32(1), shutdowns.Load())

	// A second detection (re-assert loop may tick again before shutdown
	// completes) must not initiate another shutdown.
	err = mg.refreshPrefixClaim(ctx, 42, idA)
	require.Error(t, err)
	assert.Equal(t, int32(1), shutdowns.Load(), "shutdown must be initiated exactly once")
}
