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

//go:build !linux

package sandbox

import (
	"fmt"
	"os"
	"runtime"
)

// ABI returns 0: Landlock exists only on Linux.
func ABI() int { return 0 }

// Probe reports Landlock as unsupported: it exists only on Linux.
func Probe() (Capability, error) {
	return Capability{Reason: fmt.Sprintf("Landlock exists only on Linux (this host is %s)", runtime.GOOS)}, nil
}

// CheckAvailable always fails: Landlock exists only on Linux.
func CheckAvailable() error {
	c, _ := Probe()
	return fmt.Errorf("%w: %s: %s", ErrUnsupported, RequirementMessage, c.Reason)
}

// OpenAnchor is unsupported outside Linux.
func OpenAnchor(string, AnchorMode) (*os.File, error) { return nil, ErrUnsupported }

// RestrictSelf always fails: there is nothing to confine with.
func RestrictSelf(Policy) error {
	return CheckAvailable()
}
