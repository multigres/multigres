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
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
)

// processStartTime returns field 22 (starttime) of /proc/<pid>/stat: the
// process's start in clock ticks since boot, exactly as the kernel stored it at
// fork. It is deliberately not converted to a wall-clock time. That conversion
// needs the boot time, which the kernel does not store: it is estimated from
// the wall clock, so it moves when the clock is stepped or slewed, and the
// same process would then appear to have two different birth times.
func processStartTime(pid int) (int64, error) {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	return parseStatStartTime(stat)
}

// parseStatStartTime reads starttime out of a /proc/<pid>/stat line. Field 2
// (comm) is the executable name in parentheses and may itself contain spaces
// and parentheses, so fields are counted from the last ")".
func parseStatStartTime(stat []byte) (int64, error) {
	end := bytes.LastIndexByte(stat, ')')
	if end < 0 {
		return 0, errors.New("malformed /proc stat line: no comm field")
	}
	// fields[0] is field 3 (state), so starttime, field 22, is fields[19].
	fields := bytes.Fields(stat[end+1:])
	if len(fields) < 20 {
		return 0, fmt.Errorf("malformed /proc stat line: %d fields after comm", len(fields))
	}
	start, err := strconv.ParseInt(string(fields[19]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed /proc stat starttime: %w", err)
	}
	return start, nil
}
