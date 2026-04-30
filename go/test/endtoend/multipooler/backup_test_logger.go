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

package multipooler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// backupTestLogger collects timestamped events during a backup-fault test
// and dumps them to test logs on cleanup if the test failed. The buffered
// timeline is the fastest path to reconstructing what happened in flaky
// multi-actor tests where t.Logf alone interleaves badly.
//
// All methods are safe for concurrent use.
type backupTestLogger struct {
	t      *testing.T
	mu     sync.Mutex
	start  time.Time
	events []string
}

func newBackupTestLogger(t *testing.T) *backupTestLogger {
	t.Helper()
	s := &backupTestLogger{t: t, start: time.Now()}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		t.Log("--- backup test log ---")
		for _, e := range s.events {
			t.Log(e)
		}
		t.Log("--- end backup test log ---")
	})
	return s
}

func (s *backupTestLogger) Log(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := time.Since(s.start).Round(time.Millisecond)
	s.events = append(s.events, fmt.Sprintf("[+%s] %s", elapsed, fmt.Sprintf(format, args...)))
}
