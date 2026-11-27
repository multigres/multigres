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
	"os"
	"os/exec"

	"github.com/multigres/multigres/go/tools/repocreds"
)

// serviceEnv is the environment for every process this provisioner starts
// other than pgbackrestd: this process's own environment without the backup
// repository credentials, plus extra. With use-env-credentials the launcher's
// environment holds the S3 keys, and postgres (through pgctld), multipooler
// and the rest must not inherit them: only the sidecar may reach the
// repository, and it gets the keys from its key directory.
func serviceEnv(extra ...string) []string {
	return append(repocreds.WithoutEnv(os.Environ()), extra...)
}

// binaryVersionOutput runs `binary --version` with the same environment as a
// service and returns what it printed.
func binaryVersionOutput(binary string) ([]byte, error) {
	cmd := exec.Command(binary, "--version")
	cmd.Env = serviceEnv()
	return cmd.Output()
}
