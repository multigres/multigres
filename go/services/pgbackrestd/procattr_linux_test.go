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
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseProcStat(t *testing.T) {
	for name, tc := range map[string]struct {
		line string
		want procEntry
		live bool
	}{
		"plain":               {"77 (sleep) S 12 34 5 6", procEntry{parent: 12, group: 34}, true},
		"awkward name":        {"77 (a b) R 9 9 (c) S 12 34 5 6", procEntry{parent: 12, group: 34}, true},
		"zombie":              {"77 (sleep) Z 12 34 5 6", procEntry{}, false},
		"dead":                {"77 (sleep) X 12 34 5 6", procEntry{}, false},
		"just enough columns": {"77 (sleep) S 12 34", procEntry{parent: 12, group: 34}, true},
	} {
		t.Run(name, func(t *testing.T) {
			got, live, err := parseProcStat([]byte(tc.line))
			require.NoError(t, err)
			assert.Equal(t, tc.live, live)
			assert.Equal(t, tc.want, got)
		})
	}
	for name, line := range map[string]string{
		"empty":          "",
		"no name":        "77 sleep S 12 34",
		"truncated":      "77 (sleep) S 12",
		"parent not int": "77 (sleep) S twelve 34",
		"group not int":  "77 (sleep) S 12 thirty",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := parseProcStat([]byte(line))
			require.Error(t, err)
		})
	}
}

// One row that cannot be read (for a reason other than the process having
// exited) or parsed may be the live worker, so the table must say it is
// incomplete, not answer as if the row were absent.
func TestReadProcessTable_UnreadableRowMarksTheTableIncomplete(t *testing.T) {
	self := "/proc/" + strconv.Itoa(os.Getpid()) + "/stat"
	old := readProcStat
	t.Cleanup(func() { readProcStat = old })

	for name, tc := range map[string]struct {
		read       func(string) ([]byte, error)
		incomplete bool
	}{
		"permission denied":  {func(string) ([]byte, error) { return nil, syscall.EACCES }, true},
		"input/output":       {func(string) ([]byte, error) { return nil, syscall.EIO }, true},
		"malformed row":      {func(string) ([]byte, error) { return []byte("garbage"), nil }, true},
		"exited: no entry":   {func(string) ([]byte, error) { return nil, os.ErrNotExist }, false},
		"exited: no process": {func(string) ([]byte, error) { return nil, syscall.ESRCH }, false},
	} {
		t.Run(name, func(t *testing.T) {
			readProcStat = func(path string) ([]byte, error) {
				if path == self {
					return tc.read(path)
				}
				return old(path)
			}
			table, err := readProcessTable()
			require.NoError(t, err)
			_, present := table.procs[os.Getpid()]
			assert.False(t, present)
			if !tc.incomplete {
				require.NoError(t, table.unread)
				return
			}
			require.Error(t, table.unread)
			assert.True(t, strings.Contains(table.unread.Error(), "pid "+strconv.Itoa(os.Getpid())), table.unread.Error())

			// With that row missing, "no worker" is not a conclusion.
			alive, err := serverWorkersAlive(1<<30, 0)
			assert.False(t, alive)
			require.Error(t, err)
			assert.True(t, errors.Is(err, table.unread) || strings.Contains(err.Error(), "incomplete process table"))
		})
	}
}
