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

package backup

import "runtime"

// ProcessLimits holds computed pgBackRest process-max values for each command
// class. Values scale with the host vCPU count so pgBackRest parallelism is
// appropriate across the supported compute range.
//
// Formulas mirror supabase-admin-agent's pkg/pgbackrest/tune.go so multigres
// tunes pgBackRest the same way Supabase platform does.
type ProcessLimits struct {
	Global int // [global]              — fallback for all commands not listed below
	Backup int // [<stanza>:backup]     — backup parallelism
	Get    int // [global:archive-get]  — restore; PG offline, maximise throughput
	Push   int // [global:archive-push] — live archive-push, conservative
}

// ComputeProcessLimits derives per-class process-max values from the given
// CPU count. Each value is floored at 1 and capped to prevent saturation.
func ComputeProcessLimits(cpus int) ProcessLimits {
	return ProcessLimits{
		Global: clamp(1, 4, cpus/4),
		Backup: clamp(1, 8, cpus/2),
		Get:    clamp(1, 8, (cpus*3)/4),
		Push:   clamp(1, 4, cpus/3),
	}
}

// CurrentProcessLimits returns ComputeProcessLimits for the current host.
func CurrentProcessLimits() ProcessLimits {
	return ComputeProcessLimits(runtime.NumCPU())
}

func clamp(lo, hi, val int) int {
	if val < lo {
		return lo
	}
	if val > hi {
		return hi
	}
	return val
}
