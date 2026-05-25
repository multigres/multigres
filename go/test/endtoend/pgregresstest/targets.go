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

package pgregresstest

import (
	"errors"
	"fmt"
	"strings"
)

// Target identifies a frontend the regression and isolation suites can run
// against. Each target stands up its own backend PostgreSQL instance so
// failures observed on one target do not contaminate another. See README for
// the comparison-mode rationale.
type Target string

const (
	// TargetMultigateway routes through multigres: multigateway -> multipooler
	// -> PostgreSQL. Patch-based verification is enabled.
	TargetMultigateway Target = "multigateway"
	// TargetPgbouncerSession routes through pgbouncer in session pooling mode
	// against a standalone PostgreSQL. Used as a "near-vanilla" pooler
	// baseline. No patch handling.
	TargetPgbouncerSession Target = "pgbouncer-session"
	// TargetPgbouncerTx routes through pgbouncer in transaction pooling mode
	// against a standalone PostgreSQL. Used as the strictest pooler baseline.
	// No patch handling.
	TargetPgbouncerTx Target = "pgbouncer-tx"
)

// allTargets lists every recognised target value in canonical order.
var allTargets = []Target{
	TargetMultigateway,
	TargetPgbouncerSession,
	TargetPgbouncerTx,
}

// String returns the canonical name of the target. Implements fmt.Stringer.
func (t Target) String() string { return string(t) }

// ParseTargets reads a comma-separated PGREGRESS_TARGETS value and returns the
// list of targets to run. Whitespace around each entry is ignored. Duplicates
// are deduplicated while preserving first-seen order. An empty input returns
// the default single-target list of TargetMultigateway. Unknown target names
// return an error listing the valid choices.
func ParseTargets(envVal string) ([]Target, error) {
	envVal = strings.TrimSpace(envVal)
	if envVal == "" {
		return []Target{TargetMultigateway}, nil
	}

	seen := make(map[Target]struct{}, 3)
	var out []Target
	for raw := range strings.SplitSeq(envVal, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		t, ok := lookupTarget(name)
		if !ok {
			return nil, fmt.Errorf("unknown target %q (valid: %s)", name, joinTargets(allTargets))
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, errors.New("PGREGRESS_TARGETS contained no usable target names")
	}
	return out, nil
}

func lookupTarget(name string) (Target, bool) {
	for _, t := range allTargets {
		if string(t) == name {
			return t, true
		}
	}
	return "", false
}

func joinTargets(ts []Target) string {
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = string(t)
	}
	return strings.Join(names, ", ")
}
