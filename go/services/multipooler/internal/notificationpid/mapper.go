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

// Package notificationpid maps real PostgreSQL backend PIDs to multigateway
// virtual client PIDs for async NotificationResponse delivery.
package notificationpid

import (
	"sync"
	"time"
)

const defaultTTL = 10 * time.Minute

type entry struct {
	virtualPID int32
	expiresAt  time.Time
}

// Mapper records the virtual PID currently executing on each PostgreSQL
// backend. It is intentionally best-effort and bounded: PostgreSQL emits
// NOTIFY messages during the notifying statement/COMMIT, so a TTL that covers
// long-running statements is enough to rewrite gateway-originated notifications
// while letting direct PostgreSQL/background-worker notifications retain their
// real backend PID.
type Mapper struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[int32]entry
}

// New creates a Mapper with the default entry TTL.
func New() *Mapper {
	return NewWithTTL(defaultTTL)
}

// NewWithTTL creates a Mapper with a custom TTL. Primarily useful for tests.
func NewWithTTL(ttl time.Duration) *Mapper {
	return &Mapper{
		ttl:     ttl,
		entries: make(map[int32]entry),
	}
}

// Record associates a real PostgreSQL backend PID with a multigateway virtual
// PID for the mapper TTL. Zero values are ignored.
func (m *Mapper) Record(realPID uint32, virtualPID uint32) {
	if m == nil || realPID == 0 || virtualPID == 0 {
		return
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[int32(realPID)] = entry{virtualPID: int32(virtualPID), expiresAt: now.Add(m.ttl)}
	m.purgeExpiredLocked(now)
}

// Rewrite returns the virtual PID for realPID when a fresh mapping is known;
// otherwise it returns realPID unchanged.
func (m *Mapper) Rewrite(realPID int32) int32 {
	if m == nil || realPID == 0 {
		return realPID
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	ent, ok := m.entries[realPID]
	if !ok {
		return realPID
	}
	if now.After(ent.expiresAt) {
		delete(m.entries, realPID)
		return realPID
	}
	return ent.virtualPID
}

func (m *Mapper) purgeExpiredLocked(now time.Time) {
	for realPID, ent := range m.entries {
		if now.After(ent.expiresAt) {
			delete(m.entries, realPID)
		}
	}
}
