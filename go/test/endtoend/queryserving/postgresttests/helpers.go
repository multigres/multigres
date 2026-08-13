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

package postgresttests

import (
	"os"
	"os/exec"
	"strings"
)

// brewPrefix returns the homebrew prefix for keg if it is actually installed,
// else "". `brew --prefix X` prints a path even for uninstalled formulae, so the
// path is stat'd to confirm the keg exists on disk.
func brewPrefix(keg string) string {
	brew, err := exec.LookPath("brew")
	if err != nil {
		return ""
	}
	out, err := exec.Command(brew, "--prefix", keg).Output()
	if err != nil {
		return ""
	}
	p := strings.TrimSpace(string(out))
	if p == "" {
		return ""
	}
	if info, err := os.Stat(p); err != nil || !info.IsDir() {
		return ""
	}
	return p
}

// lookPathOrEmpty returns the absolute path to a binary on PATH (e.g.
// geos-config), or "" if not found.
func lookPathOrEmpty(name string) string {
	p, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	return p
}
