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

package constants

import "fmt"

// PgBackRestSandboxEnvVar names the environment variable that supplies the
// default of pgbackrestd's --sandbox flag, and that the local provisioner and
// the end-to-end harness read to choose it for every sidecar they start.
const PgBackRestSandboxEnvVar = "MULTIGRES_PGBACKREST_SANDBOX"

// The values of --sandbox. Whether pgBackRest children are confined with
// Landlock is a property of the deployment, because Landlock needs Linux 5.13
// or later with the LSM enabled and a seccomp profile that allows it.
const (
	// SandboxRequired refuses to start unless Landlock works. The default.
	SandboxRequired = "required"
	// SandboxAuto confines like SandboxRequired when the host supports
	// Landlock and runs children unconfined when it does not.
	SandboxAuto = "auto"
	// SandboxOff never uses Landlock.
	SandboxOff = "off"
)

// ValidateSandboxMode rejects anything but the three values above.
func ValidateSandboxMode(mode string) error {
	switch mode {
	case SandboxRequired, SandboxAuto, SandboxOff:
		return nil
	}
	return fmt.Errorf("sandbox mode must be %q, %q or %q, got %q", SandboxRequired, SandboxAuto, SandboxOff, mode)
}
