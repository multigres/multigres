// Copyright 2025 Supabase, Inc.
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

package pgctld

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeriveWalSettings(t *testing.T) {
	tests := []struct {
		name            string
		volumeBytes     uint64
		walSegmentBytes uint64
		wantWalSettings walSettings
	}{
		{
			name:            "tiny volume uses postgres-compatible floors",
			volumeBytes:     1,
			walSegmentBytes: defaultWalSegmentSizeBytes,
			wantWalSettings: walSettings{
				minWalSizeMB:         32,
				maxWalSizeMB:         64,
				walKeepSizeMB:        32,
				maxSlotWalKeepSizeMB: 64,
			},
		},
		{
			name:            "one GiB volume scales down",
			volumeBytes:     1 << 30,
			walSegmentBytes: defaultWalSegmentSizeBytes,
			wantWalSettings: walSettings{
				minWalSizeMB:         64,
				maxWalSizeMB:         256,
				walKeepSizeMB:        128,
				maxSlotWalKeepSizeMB: 256,
			},
		},
		{
			name:            "972 MiB filesystem scales down",
			volumeBytes:     972 << 20,
			walSegmentBytes: defaultWalSegmentSizeBytes,
			wantWalSettings: walSettings{
				minWalSizeMB:         60,
				maxWalSizeMB:         243,
				walKeepSizeMB:        121,
				maxSlotWalKeepSizeMB: 243,
			},
		},
		{
			name:            "sixteen GiB volume preserves previous limits",
			volumeBytes:     16 << 30,
			walSegmentBytes: defaultWalSegmentSizeBytes,
			wantWalSettings: walSettings{
				minWalSizeMB:         1024,
				maxWalSizeMB:         4096,
				walKeepSizeMB:        1000,
				maxSlotWalKeepSizeMB: 4096,
			},
		},
		{
			name:            "large volume remains capped",
			volumeBytes:     1 << 40,
			walSegmentBytes: defaultWalSegmentSizeBytes,
			wantWalSettings: walSettings{
				minWalSizeMB:         1024,
				maxWalSizeMB:         4096,
				walKeepSizeMB:        1000,
				maxSlotWalKeepSizeMB: 4096,
			},
		},
		{
			name:            "custom WAL segments raise postgres-compatible floors",
			volumeBytes:     1 << 30,
			walSegmentBytes: 64 << 20,
			wantWalSettings: walSettings{
				minWalSizeMB:         128,
				maxWalSizeMB:         256,
				walKeepSizeMB:        128,
				maxSlotWalKeepSizeMB: 256,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := deriveWalSettings(tt.volumeBytes, tt.walSegmentBytes)
			require.NoError(t, err)
			require.Equal(t, tt.wantWalSettings, got)
		})
	}
}

func TestDeriveWalSettingsRejectsUnsupportedSegmentSizes(t *testing.T) {
	for _, walSegmentBytes := range []uint64{0, megabyte + 1, 2 << 30} {
		_, err := deriveWalSettings(16<<30, walSegmentBytes)
		require.Error(t, err)
	}
}

func TestParseWalSegmentSizeBytes(t *testing.T) {
	output := `pg_control version number:            1300
Bytes per WAL segment:                67108864
Data page checksum version:           1
`

	got, err := parseWalSegmentSizeBytes(output)
	require.NoError(t, err)
	require.Equal(t, uint64(64<<20), got)

	_, err = parseWalSegmentSizeBytes("Database cluster state: shut down\n")
	require.Error(t, err)
}

func TestWalSegmentSizeBytesDefaultsBeforeInitdb(t *testing.T) {
	got, err := walSegmentSizeBytes(filepath.Join(t.TempDir(), "pg_data"))
	require.NoError(t, err)
	require.Equal(t, defaultWalSegmentSizeBytes, got)
}

func TestVolumeTotalBytesUsesNearestExistingAncestor(t *testing.T) {
	tempDir := t.TempDir()
	want, err := volumeTotalBytes(tempDir)
	require.NoError(t, err)

	got, err := volumeTotalBytes(filepath.Join(tempDir, "pg_data", "not-created-yet"))
	require.NoError(t, err)
	require.Equal(t, want, got)
}
