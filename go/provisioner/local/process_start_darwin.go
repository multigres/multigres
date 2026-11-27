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
	"golang.org/x/sys/unix"
)

// processStartTime returns kinfo_proc's p_starttime in microseconds, the
// timeval the kernel recorded at fork. The kernel stores it once and never
// recomputes it, so a later clock change does not move it.
func processStartTime(pid int) (int64, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	return kp.Proc.P_starttime.Sec*1_000_000 + int64(kp.Proc.P_starttime.Usec), nil
}
