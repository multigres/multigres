// Copyright 2025 Supabase, Inc.
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

// pgctld manages PostgreSQL server instances within the Multigres cluster,
// providing lifecycle management and configuration control.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/cmd/pgctld/command"
	"github.com/multigres/multigres/go/tools/initproc"
)

func main() {
	// As a container's PID 1, pgctld inherits the postmaster after `pg_ctl
	// start -W` exits, plus any other orphan in the container. Reaping those from
	// inside pgctld with Wait4(-1) races its own os/exec children for their exit
	// status, so PID 1 becomes a minimal init and pgctld proper runs as its child.
	if initproc.IsInit() {
		os.Exit(initproc.Run(slog.New(slog.NewJSONHandler(os.Stderr, nil)))) //nolint:forbidigo // main() is allowed to call os.Exit
	}

	root, pgctlCmd := command.GetRootCommand()

	if err := root.Execute(); err != nil {
		logger := pgctlCmd.GetLogger()
		logger.Error("command execution failed", "error", err)
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}
