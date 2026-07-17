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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	megabyte                   = uint64(1 << 20)
	defaultWalSegmentSizeBytes = 16 * megabyte
	maxWalSizeCapMB            = uint64(4096)
)

// walSettings holds the postgres WAL disk-usage settings derived from the
// size of the volume backing the data directory, in megabytes.
type walSettings struct {
	minWalSizeMB         uint64
	maxWalSizeMB         uint64
	walKeepSizeMB        uint64
	maxSlotWalKeepSizeMB uint64
}

// deriveWalSettings scales postgres' WAL disk-usage knobs to the volume so
// routine WAL retention is budgeted below the disk size. Fixed values
// (previously max_wal_size=4GB, wal_keep_size=1000MB) could fill a small
// volume. The upper clamps preserve the previous values on large volumes;
// smaller volumes scale down proportionally. PostgreSQL requires min_wal_size
// and max_wal_size to be at least two WAL segments, so the lower clamps also
// account for the segment size selected at initdb time.
func deriveWalSettings(volumeBytes, walSegmentSizeBytes uint64) (walSettings, error) {
	if walSegmentSizeBytes == 0 || walSegmentSizeBytes%megabyte != 0 {
		return walSettings{}, fmt.Errorf("WAL segment size must be a non-zero whole number of megabytes, got %d bytes", walSegmentSizeBytes)
	}

	volMB := volumeBytes / megabyte
	walSegmentSizeMB := walSegmentSizeBytes / megabyte
	// Keep enough distance between the two PostgreSQL minimums that
	// min_wal_size does not consume the entire max_wal_size allowance.
	maxWalFloor := max(uint64(64), 4*walSegmentSizeMB)
	if maxWalFloor > maxWalSizeCapMB {
		return walSettings{}, fmt.Errorf("WAL segment size %dMB requires max_wal_size above the supported %dMB cap", walSegmentSizeMB, maxWalSizeCapMB)
	}
	minWalFloor := max(uint64(32), 2*walSegmentSizeMB)
	maxWal := clampMB(volMB/4, maxWalFloor, maxWalSizeCapMB)

	return walSettings{
		// min_wal_size is WAL that is recycled rather than removed at
		// checkpoint, so it must stay well under max_wal_size; a quarter
		// preserves the previous 1GB:4GB ratio.
		minWalSizeMB: clampMB(maxWal/4, minWalFloor, max(uint64(1024), minWalFloor)),
		maxWalSizeMB: maxWal,
		// wal_keep_size is what lets a lagging replica reconnect without a
		// reseed. Physical replicas currently connect without slots, so
		// shrinking this on small volumes trades reconnect headroom for not
		// filling the primary's disk.
		walKeepSizeMB: clampMB(volMB/8, max(uint64(32), walSegmentSizeMB), max(uint64(1000), walSegmentSizeMB)),
		// Logical replication does use slots. A bounded cap protects the
		// primary's disk, at the explicit cost that a consumer lagging beyond
		// the cap can lose its slot at checkpoint and require resynchronization.
		maxSlotWalKeepSizeMB: maxWal,
	}, nil
}

func clampMB(v, lo, hi uint64) uint64 {
	return min(max(v, lo), hi)
}

// walSegmentSizeBytes returns the segment size recorded in pg_control. Config
// generation normally runs after initdb, including when --wal-segsize was
// supplied. Some unit-test and utility callers generate config before initdb;
// for an absent pg_control, PostgreSQL's default segment size is used.
func walSegmentSizeBytes(dataDir string) (uint64, error) {
	pgControlPath := filepath.Join(dataDir, "global", "pg_control")
	if _, err := os.Stat(pgControlPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return defaultWalSegmentSizeBytes, nil
		}
		return 0, fmt.Errorf("stat %s: %w", pgControlPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pg_controldata", dataDir)
	// pg_controldata localizes its labels. Force stable output for parsing.
	cmd.Env = append(os.Environ(), "LC_ALL=C", "LANG=C")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("pg_controldata %s: %w (output: %s)", dataDir, err, strings.TrimSpace(string(output)))
	}

	segmentBytes, err := parseWalSegmentSizeBytes(string(output))
	if err != nil {
		return 0, fmt.Errorf("pg_controldata %s: %w", dataDir, err)
	}
	return segmentBytes, nil
}

func parseWalSegmentSizeBytes(output string) (uint64, error) {
	const field = "Bytes per WAL segment:"
	for line := range strings.SplitSeq(output, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), field) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), field))
		segmentBytes, err := strconv.ParseUint(value, 10, 64)
		if err != nil || segmentBytes == 0 {
			return 0, fmt.Errorf("invalid %s value %q", field, value)
		}
		return segmentBytes, nil
	}
	return 0, fmt.Errorf("%s not found", field)
}

// volumeTotalBytes returns the total size of the filesystem backing dir. If
// dir doesn't exist yet, the nearest existing ancestor is used — that is the
// volume the directory would be created on.
func volumeTotalBytes(dir string) (uint64, error) {
	for {
		var st unix.Statfs_t
		err := unix.Statfs(dir, &st)
		if err == nil {
			return uint64(st.Blocks) * uint64(st.Bsize), nil
		}
		parent := filepath.Dir(dir)
		if !errors.Is(err, fs.ErrNotExist) || parent == dir {
			return 0, fmt.Errorf("statfs %s: %w", dir, err)
		}
		dir = parent
	}
}
