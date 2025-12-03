// Copyright 2023 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package servenv

import (
	"fmt"
	"log/slog"
	"os"
)

func (sv *ServEnv) registerPidFile() {
	pidFileCreated := false

	// Create pid file after flags are parsed.
	sv.OnInit(func() {
		pidFile := sv.pidFile.Get()
		if pidFile == "" {
			return
		}

		file, err := os.OpenFile(pidFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to create pid file '%s': %v", pidFile, err))
			return
		}
		pidFileCreated = true
		fmt.Fprintln(file, os.Getpid())
		_ = file.Close()
	})

	// Remove pid file on graceful shutdown.
	sv.OnClose(func() {
		pidFile := sv.pidFile.Get()
		if pidFile == "" {
			return
		}
		if !pidFileCreated {
			return
		}

		if err := os.Remove(pidFile); err != nil {
			slog.Error(fmt.Sprintf("Unable to remove pid file '%s': %v", pidFile, err))
		}
	})
}
