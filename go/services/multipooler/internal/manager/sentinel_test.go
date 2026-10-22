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

package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// newSentinelTestManager builds a manager whose pooler directory is a writable
// temp dir, so the on-disk sentinels can be exercised.
func newSentinelTestManager(t *testing.T) *MultipoolerManager {
	t.Helper()
	return newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
		PoolerDir:     t.TempDir(),
	})))
}

// testSentinels lists every sentinel kind with the file it must live at, so the
// shared behaviour is checked for each of them.
var testSentinels = []struct {
	name string
	file string
	get  func(*MultipoolerManager) sentinel
}{
	{"bootstrap", constants.BootstrapSentinelFile, (*MultipoolerManager).bootstrapSentinel},
	{"rewind", constants.RewindSentinelFile, (*MultipoolerManager).rewindSentinel},
	{"restore", constants.RestoreSentinelFile, (*MultipoolerManager).restoreSentinel},
}

func TestSentinel_RoundTrip(t *testing.T) {
	for _, tc := range testSentinels {
		t.Run(tc.name, func(t *testing.T) {
			pm := newSentinelTestManager(t)
			s := tc.get(pm)
			assert.Equal(t, filepath.Join(pm.record.PoolerDir(), tc.file), s.path,
				"the sentinel must live in pooler_dir, never under PGDATA")

			present, err := s.present()
			require.NoError(t, err)
			assert.False(t, present, "no sentinel initially")

			require.NoError(t, s.write())
			present, err = s.present()
			require.NoError(t, err)
			assert.True(t, present, "sentinel present after write")

			// Writing again is idempotent.
			require.NoError(t, s.write())

			require.NoError(t, s.remove())
			present, err = s.present()
			require.NoError(t, err)
			assert.False(t, present, "sentinel gone after remove")

			// Removing a missing sentinel is not an error.
			require.NoError(t, s.remove())
		})
	}
}

// TestSentinel_ErrorPaths covers the sentinel error branches so a failure to
// check, record or clear the marker is surfaced rather than silently ignored.
func TestSentinel_ErrorPaths(t *testing.T) {
	// fsyncPath surfaces an open error for a path that does not exist.
	require.Error(t, fsyncPath(filepath.Join(t.TempDir(), "does-not-exist")))

	for _, tc := range testSentinels {
		t.Run(tc.name, func(t *testing.T) {
			// present surfaces a non-NotExist stat error: a pooler dir that is a
			// regular file makes the stat fail with ENOTDIR.
			notADir := filepath.Join(t.TempDir(), "file")
			require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o644))
			fileDir := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
				Type:      clustermetadatapb.PoolerType_REPLICA,
				PoolerDir: notADir,
			})))
			_, err := tc.get(fileDir).present()
			require.Error(t, err, "a stat failure other than not-exist should error")

			// write surfaces a write error when the pooler dir is missing.
			missingDir := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
				Type:      clustermetadatapb.PoolerType_REPLICA,
				PoolerDir: filepath.Join(t.TempDir(), "no", "such", "dir"),
			})))
			require.Error(t, tc.get(missingDir).write(), "write to a nonexistent pooler dir should error")

			// remove fsyncs the pooler dir even when the sentinel is already gone,
			// so with no pooler dir to fsync it errors.
			require.Error(t, tc.get(missingDir).remove(), "remove should fsync the pooler dir even for a missing sentinel")

			// remove surfaces a non-NotExist error: a non-empty directory at the
			// sentinel path cannot be removed by os.Remove.
			pm := newSentinelTestManager(t)
			s := tc.get(pm)
			require.NoError(t, os.Mkdir(s.path, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(s.path, "child"), []byte("x"), 0o644))
			require.Error(t, s.remove(), "removing a non-empty directory at the sentinel path should error")
		})
	}
}
