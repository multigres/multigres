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

package store

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// refreshNow issues a live Status RPC against p right now, not cached state,
// and writes the confirmed fields into p's cache via applyStatusResponse, the
// same field-writing logic a streamed snapshot uses, so the two can't drift on
// which fields a fresh observation updates. Callers re-derive whatever
// shard-wide view they need (a fresh ShardAnalysis) after this returns.
//
// Returns an error, leaving the cache untouched, if the pooler is unreachable
// or the RPC came back with no Status payload.
func (p *Pooler) refreshNow(ctx context.Context, rpcClient rpcclient.MultipoolerClient) error {
	requestedAt := time.Now()
	statusResp, err := rpcClient.Status(ctx, p.Health().GetMultipooler(), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return mterrors.Wrap(err, "pooler unreachable during health refresh")
	}
	if !p.applyStatusResponse(statusResp, timestamppb.Now(), requestedAt) {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "pooler %s returned an empty status response",
			topoclient.ComponentIDString(p.Health().GetMultipooler().GetId()))
	}
	return nil
}

// RefreshResult counts what RefreshRecentInParallel did.
type RefreshResult struct {
	Refreshed int // answered, cache updated
	Failed    int // contacted, but unreachable or answered with nothing usable
	Skipped   int // not contacted: not seen recently
}

// RefreshRecentInParallel refreshes, concurrently and within timeout overall,
// every pooler that already looks reachable (a fresh cached observation). Ones
// that look stale are skipped: another attempt is unlikely to succeed in time,
// and an unreachable pooler is itself evidence for whatever judgment follows.
// Best-effort: failures are logged and counted, never returned.
//
// TODO: "looks reachable" is DefaultObservationFreshness here, but the analyzer
// decides whether the leader is observed live with the policy's own
// LeaderLivenessFreshness. They are both 15s today without being linked; take
// the threshold from the same source once the policy moves to a shared package.
func RefreshRecentInParallel(ctx context.Context, rpcClient rpcclient.MultipoolerClient, poolers []*Pooler, timeout time.Duration, logger *slog.Logger) RefreshResult {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	now := time.Now()
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		result RefreshResult
	)
	for _, pooler := range poolers {
		age, ok := pooler.ObservationAge(now)
		if !ok || age > DefaultObservationFreshness {
			result.Skipped++
			continue
		}
		wg.Add(1)
		go func(pooler *Pooler) {
			defer wg.Done()
			err := pooler.refreshNow(ctx, rpcClient)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				result.Failed++
				logger.DebugContext(ctx, "best-effort health refresh failed, continuing with cached state",
					"pooler", topoclient.ComponentIDString(pooler.Health().GetMultipooler().GetId()), "error", err)
				return
			}
			result.Refreshed++
		}(pooler)
	}
	wg.Wait()
	return result
}
