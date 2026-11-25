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

package schematracker

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// fakeQueryService answers the token transaction with whatever token or error
// the test has set, and records the statements each transaction ran.
type fakeQueryService struct {
	*mock.QueryService

	mu         sync.Mutex
	token      string
	err        error
	statements [][]string
}

func newFakeQueryService(token string) *fakeQueryService {
	return &fakeQueryService{QueryService: mock.NewQueryService(), token: token}
}

func (f *fakeQueryService) set(token string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.token, f.err = token, err
}

func (f *fakeQueryService) transactions() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.statements)
}

func (f *fakeQueryService) BeginAdmin(context.Context) (executor.InternalTx, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statements = append(f.statements, nil)
	return &fakeTx{f: f, index: len(f.statements) - 1}, nil
}

type fakeTx struct {
	f     *fakeQueryService
	index int
}

func (tx *fakeTx) Query(_ context.Context, query string) (*sqltypes.Result, error) {
	tx.f.mu.Lock()
	defer tx.f.mu.Unlock()
	tx.f.statements[tx.index] = append(tx.f.statements[tx.index], query)
	if strings.Contains(query, "set_config") {
		return mock.MakeQueryResult(nil, [][]any{{}}), nil
	}
	if tx.f.err != nil {
		return nil, tx.f.err
	}
	return mock.MakeQueryResult([]string{"token"}, [][]any{{tx.f.token}}), nil
}

func (tx *fakeTx) QueryArgs(ctx context.Context, query string, _ ...any) (*sqltypes.Result, error) {
	return tx.Query(ctx, query)
}

func (tx *fakeTx) Commit(context.Context) error { return nil }

func (tx *fakeTx) Rollback(context.Context) error { return nil }

// published records every token the tracker publishes, in order.
type published struct {
	mu     sync.Mutex
	tokens []string
}

func (p *published) record(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tokens = append(p.tokens, token)
}

func (p *published) get() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.tokens)
}

func writableState() servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}}
}

func notWritableState() servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}}
}

const testInterval = 10 * time.Millisecond

func newTestTracker(t *testing.T, qs executor.InternalQueryService) (*Tracker, *published) {
	t.Helper()
	pub := &published{}
	tr := NewTracker(qs, slog.Default(), testInterval, pub.record)
	t.Cleanup(tr.Close)
	return tr, pub
}

func requirePublished(t *testing.T, pub *published, want ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, want, pub.get())
	}, 2*time.Second, testInterval)
}

func TestTracker_PublishesOnlyChanges(t *testing.T) {
	qs := newFakeQueryService("a")
	tr, pub := newTestTracker(t, qs)

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	requirePublished(t, pub, "a")
	assert.Equal(t, "a", tr.Token())

	// Polls that see the same schema publish nothing.
	require.Eventually(t, func() bool { return len(qs.transactions()) >= 5 }, 2*time.Second, testInterval)
	assert.Equal(t, []string{"a"}, pub.get())

	qs.set("b", nil)
	requirePublished(t, pub, "a", "b")
	assert.Equal(t, "b", tr.Token())
}

func TestTracker_PinsSettingsBeforeHashing(t *testing.T) {
	qs := newFakeQueryService("a")
	tr, pub := newTestTracker(t, qs)

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	requirePublished(t, pub, "a")

	txs := qs.transactions()
	require.NotEmpty(t, txs)
	assert.Equal(t, []string{pinSettingsQuery, schemaHashQuery}, txs[0])
}

func TestTracker_FailureClearsToken(t *testing.T) {
	qs := newFakeQueryService("a")
	tr, pub := newTestTracker(t, qs)

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	requirePublished(t, pub, "a")

	// A failed computation publishes the empty token, so a consumer sees the
	// recovered token as a change even though the schema is the same.
	qs.set("", errors.New("connection refused"))
	requirePublished(t, pub, "a", "")
	assert.Empty(t, tr.Token())

	qs.set("a", nil)
	requirePublished(t, pub, "a", "", "a")
}

func TestTracker_TracksOnlyWhileWritable(t *testing.T) {
	qs := newFakeQueryService("a")
	tr, pub := newTestTracker(t, qs)

	require.NoError(t, tr.OnStateChange(t.Context(), notWritableState()))
	time.Sleep(5 * testInterval)
	assert.Empty(t, qs.transactions(), "a follower must not compute the token")
	assert.Empty(t, pub.get())

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	requirePublished(t, pub, "a")

	require.NoError(t, tr.OnStateChange(t.Context(), notWritableState()))
	requirePublished(t, pub, "a", "")
	stopped := len(qs.transactions())
	time.Sleep(5 * testInterval)
	assert.Len(t, qs.transactions(), stopped, "a demoted pooler must stop computing the token")

	// Becoming the leader again starts from the empty token, so the token is
	// republished even if the schema did not change while it was a follower.
	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	requirePublished(t, pub, "a", "", "a")
}

func TestTracker_CloseIsTerminal(t *testing.T) {
	qs := newFakeQueryService("a")
	tr, pub := newTestTracker(t, qs)

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	requirePublished(t, pub, "a")

	tr.Close()
	assert.Equal(t, []string{"a", ""}, pub.get())

	require.NoError(t, tr.OnStateChange(t.Context(), writableState()))
	time.Sleep(5 * testInterval)
	assert.Equal(t, []string{"a", ""}, pub.get(), "a closed tracker must not restart")
}
