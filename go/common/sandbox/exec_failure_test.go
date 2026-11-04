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

package sandbox

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An exec the kernel refuses after confinement is an error return, never a
// silent fall through to the caller.
func TestExecMain_ReportsAnExecFailure(t *testing.T) {
	old := restrictSelf
	restrictSelf = func(Policy) error { return nil }
	t.Cleanup(func() { restrictSelf = old })

	// Executable by mode but not a program the kernel can load.
	bin := filepath.Join(t.TempDir(), "not-a-program")
	require.NoError(t, os.WriteFile(bin, []byte("\x00\x01 not an executable format"), 0o755))

	err := ExecMain([]string{"--policy", Policy{}.encode(), "--", bin})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exec")
	assert.Contains(t, err.Error(), bin)
}

func TestExecMain_ReportsAConfinementFailureBeforeExec(t *testing.T) {
	old := restrictSelf
	restrictSelf = func(Policy) error { return ErrUnsupported }
	t.Cleanup(func() { restrictSelf = old })

	bin := filepath.Join(t.TempDir(), "marker")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\ntrue\n"), 0o755))
	err := ExecMain([]string{"--policy", Policy{}.encode(), "--", bin})
	require.ErrorIs(t, err, ErrUnsupported)
}
