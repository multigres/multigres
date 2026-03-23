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

package preparedstatement

import (
	"fmt"
	"strings"
	"sync"
)

// PoolerConsolidator deduplicates prepared statements at the multipooler level.
//
// Unlike the gateway Consolidator which tracks per-connection name mappings,
// the PoolerConsolidator has no connection context — it receives requests from
// multiple stateless gateway replicas, each of which may assign the same
// canonical name to different queries. The PoolerConsolidator therefore
// deduplicates purely by (query text, param types), ignoring incoming names.
//
// Entries are never removed: in practice the set of unique (query, paramTypes)
// pairs is bounded by the application's query surface, so the map does not
// grow without bound. Per-postgres-connection state (which statements are
// prepared on which connection) is tracked separately by
// connstate.ConnectionState.
type PoolerConsolidator struct {
	mu     sync.Mutex
	stmts  map[string]string // dedup key → canonical name
	lastID int
}

// NewPoolerConsolidator creates a new PoolerConsolidator.
func NewPoolerConsolidator() *PoolerConsolidator {
	return &PoolerConsolidator{
		stmts: make(map[string]string),
	}
}

// CanonicalName returns a stable canonical name for the given query and param
// types. If this (query, paramTypes) combination has been seen before, the
// same name is returned. Otherwise a new unique name is generated.
func (pc *PoolerConsolidator) CanonicalName(query string, paramTypes []uint32) string {
	key := dedupKey(query, paramTypes)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if name, ok := pc.stmts[key]; ok {
		return name
	}

	name := fmt.Sprintf("ppstmt%d", pc.lastID)
	pc.lastID++
	pc.stmts[key] = name
	return name
}

// dedupKey builds a deduplication key from query text and param type OIDs.
// The key is length-prefixed so that no query text can collide with the
// separator/paramTypes suffix.
func dedupKey(query string, paramTypes []uint32) string {
	if len(paramTypes) == 0 {
		return query
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", len(query))
	b.WriteString(query)
	for i, oid := range paramTypes {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", oid)
	}
	return b.String()
}
