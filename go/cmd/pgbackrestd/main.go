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

// pgbackrestd is the pgBackRest sidecar: PID 1 of the pgBackRest container.
// It runs every pgBackRest invocation as a sandboxed child and serves the
// WAL and control APIs on unix sockets.
package main

import (
	"fmt"
	"os"

	"github.com/multigres/multigres/go/cmd/pgbackrestd/command"
	"github.com/multigres/multigres/go/common/sandbox"
)

func main() {
	// sandbox-exec is the re-executed side of every child. It must confine
	// the process before anything else runs, so it is routed ahead of flag
	// parsing and logging setup.
	if len(os.Args) > 1 && os.Args[1] == sandbox.SubcommandName {
		if err := sandbox.ExecMain(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "pgbackrestd sandbox-exec:", err)
		}
		// ExecMain returns only on failure; never fall through to run unconfined.
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}

	root, lg := command.GetRootCommand()
	if err := root.Execute(); err != nil {
		lg().Error("command execution failed", "error", err)
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}
