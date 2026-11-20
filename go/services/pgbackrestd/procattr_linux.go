//go:build linux

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

package pgbackrestd

import (
	"bytes"
	"os"
	"strconv"
	"strings"
)

// descendantsAlive reports whether any live process descends from root.
func descendantsAlive(root int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	parent := map[int]int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		rest := strings.Fields(string(b[bytes.LastIndexByte(b, ')')+1:]))
		if len(rest) < 2 || rest[0] == "Z" || rest[0] == "X" {
			continue
		}
		ppid, _ := strconv.Atoi(rest[1])
		parent[pid] = ppid
	}
	for pid := range parent {
		for p, hops := pid, 0; hops < 64; hops++ {
			pp, ok := parent[p]
			if !ok {
				break
			}
			if pp == root {
				return true
			}
			p = pp
		}
	}
	return false
}
