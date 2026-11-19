// Copyright 2026 Supabase, Inc.
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

package command

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/multigres/multigres/go/tools/repocreds"
)

// errSidecarNeedsCurl is returned when pgctld starts without curl,
// because archive_command and restore_command are curl invocations against the
// sidecar's WAL socket.
var errSidecarNeedsCurl = errors.New("the pgbackrest sidecar needs curl on PATH for archive_command")

// startSidecarChecks runs the startup checks for the sidecar. A missing curl is
// fatal, since every WAL command would fail. An exposed credential is not: a
// config slip should page someone through the gauge, not take postgres down.
func startSidecarChecks(logger *slog.Logger) (*sidecarMetrics, error) {
	if _, err := exec.LookPath("curl"); err != nil {
		return nil, errSidecarNeedsCurl
	}

	m, err := newSidecarMetrics()
	if err != nil {
		logger.Warn("failed to register pgctld sidecar metrics", "error", err)
	}

	// postgres inherits this environment, and so does everything it forks:
	// only the sidecar's own pgBackRest commands may hold a repository credential.
	if exposed := repocreds.EnvVars(os.Environ()); len(exposed) > 0 {
		logger.Error("backup repository credential present in pgctld environment; postgres and its children could use it",
			"variables", strings.Join(exposed, ","))
		m.credentialExposedVal.Store(1)
	} else {
		m.credentialExposedVal.Store(0)
	}
	return m, nil
}
