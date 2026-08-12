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

// Package switcher provides RoleSwitcher, a small state machine that runs
// one of two Toggleable components based on servingstate.State.Writable(),
// and guarantees both are stopped on Close regardless of which was last
// active.
//
// heartbeat.ReplTracker (writer while writable, reader otherwise) and the
// replicationstats poller (poller while writable, nothing otherwise) both
// need exactly this shape; extracting it here means both get Close's
// stop-both guarantee for free instead of each hand-rolling it — which is
// how heartbeat.ReplTracker's role-swap logic came to leave a ticker running
// past a real shutdown in the first place (see the design doc's shutdown
// section).
package switcher

import (
	"context"
	"sync"

	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// Toggleable is a periodic task that can be started and stopped, and reports
// whether it is currently running. heartbeat.Reader/Writer and
// replicationstats.Poller all satisfy this by their existing Open/Close/
// IsOpen methods.
type Toggleable interface {
	Open()
	Close()
	IsOpen() bool
}

// RoleSwitcher runs primary while the pooler is the writable leader
// (servingstate.State.Writable()) and secondary otherwise.
type RoleSwitcher struct {
	mu        sync.Mutex
	primary   Toggleable
	secondary Toggleable
}

// NewRoleSwitcher returns a RoleSwitcher that runs primary while Writable()
// and secondary otherwise. Neither is started; the caller drives the first
// transition via OnStateChange (typically through
// StateManager.RegisterAndSync).
func NewRoleSwitcher(primary, secondary Toggleable) *RoleSwitcher {
	return &RoleSwitcher{primary: primary, secondary: secondary}
}

// OnStateChange switches which of primary/secondary is running based on
// state.Writable(). No-ops if the target role is already running, so
// redundant notifications (e.g. a rule-only bump that doesn't change
// writability) don't needlessly close and reopen an already-running
// component. Satisfies manager.StateAware structurally.
func (p *RoleSwitcher) OnStateChange(_ context.Context, state servingstate.State) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if state.Writable() {
		if p.primary.IsOpen() {
			return nil
		}
		p.secondary.Close()
		p.primary.Open()
		return nil
	}
	if p.secondary.IsOpen() {
		return nil
	}
	p.primary.Close()
	p.secondary.Open()
	return nil
}

// Close stops both primary and secondary, regardless of which is currently
// active. Callers on a real shutdown path must call this explicitly —
// OnStateChange(notWritable) alone only switches roles, it does not tear
// anything down. Toggleable.Close implementations must be idempotent
// (heartbeat.Reader/Writer already are, via timer.PeriodicRunner.Stop), so
// closing an already-closed role here is safe.
func (p *RoleSwitcher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.primary.Close()
	p.secondary.Close()
}

// NoOp is a Toggleable that does nothing and reports never-open. Use it as
// RoleSwitcher's secondary for a component that should run only while
// writable, with nothing meaningful to do otherwise (e.g.
// replicationstats.Poller).
//
// IsOpen must return false unconditionally: RoleSwitcher's "already in
// target role, skip" fast path uses it, and a NoOp secondary that claimed
// to be "open" would make RoleSwitcher skip closing an active primary on
// every not-writable transition.
type NoOp struct{}

func (NoOp) Open()        {}
func (NoOp) Close()       {}
func (NoOp) IsOpen() bool { return false }

// Compile-time check that NoOp implements Toggleable.
var _ Toggleable = NoOp{}
