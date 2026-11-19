//go:build !linux && !darwin

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

package local

import (
	"github.com/shirou/gopsutil/v4/process"
)

// processStartTime uses gopsutil where there is no reader of our own. On
// FreeBSD that is kinfo_proc's ki_start, which the kernel stores at fork; on
// platforms gopsutil does not implement it is an error, and no sidecar is
// adopted or signalled there.
func processStartTime(pid int) (int64, error) {
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0, err
	}
	return proc.CreateTime()
}
