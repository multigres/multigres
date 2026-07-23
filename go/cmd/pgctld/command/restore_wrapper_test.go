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

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestoreWrapperSkipsTelemetry guards the coupling between
// restore_wrapper.go (which sets skipTelemetryAnnotation) and root.go's
// PersistentPreRunE/PersistentPostRunE (which check it) — a typo or rename
// on either side would silently re-enable telemetry init for every
// restore_command invocation instead of failing loudly.
func TestRestoreWrapperSkipsTelemetry(t *testing.T) {
	root, _ := GetRootCommand()

	cmd, _, err := root.Find([]string{"restore-wrapper"})
	require.NoError(t, err)
	assert.Equal(t, "true", cmd.Annotations[skipTelemetryAnnotation],
		"restore-wrapper must carry the annotation root's hooks check")

	// Running it with no args fails in RunE (missing pidfile/command), but
	// PersistentPreRunE must still have run first — proving the shared
	// logging/SilenceUsage setup isn't skipped, only telemetry init is.
	root.SetArgs([]string{"restore-wrapper"})
	err = root.Execute()
	require.Error(t, err)
	assert.True(t, root.SilenceUsage, "PersistentPreRunE should have run despite skipping telemetry")
}
