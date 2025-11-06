// Copyright 2025 Supabase, Inc.
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

// Package backup provides backup and restore functionality for PostgreSQL shards
package backup

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// BackupOptions contains options for performing a backup
type BackupOptions struct {
	ForcePrimary bool
	Type         string // "full", "differential", "incremental"
}

// BackupResult contains the result of a backup operation
type BackupResult struct {
	BackupID string
}

// BackupShard performs a backup on a specific shard
func BackupShard(ctx context.Context, configPath, stanzaName string, opts BackupOptions) (*BackupResult, error) {
	// Validation
	if opts.Type == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "type is required")
	}

	// Type validation and mapping to pgbackrest types
	// pgbackrest uses abbreviated types: full, diff, incr
	pgBackRestType := ""
	switch opts.Type {
	case "full":
		pgBackRestType = "full"
	case "differential":
		pgBackRestType = "diff"
	case "incremental":
		pgBackRestType = "incr"
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid backup type '%s': must be one of: full, differential, incremental", opts.Type))
	}

	// Validate required backup configuration
	if configPath == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	// Execute pgbackrest backup command
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute) // Backups can take a long time
	defer cancel()

	cmd := exec.CommandContext(ctx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--type="+pgBackRestType,
		"--log-level-console=info",
		"backup")

	// Capture output for logging and to extract backup ID
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest backup failed: %v\nOutput: %s", err, string(output)))
	}

	// Parse the backup ID from the output
	backupID, err := extractBackupID(string(output))
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to extract backup ID from output: %v\nOutput: %s", err, string(output)))
	}

	return &BackupResult{
		BackupID: backupID,
	}, nil
}

// extractBackupID extracts the backup label from pgbackrest output
//
// TODO: find a way of of doing this that does that does not rely on text matching
func extractBackupID(output string) (string, error) {
	// First, try to find "new backup label" in the output (most reliable)
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "new backup label") {
			// Extract the label after the "=" sign
			parts := strings.Split(line, "=")
			if len(parts) >= 2 {
				label := strings.TrimSpace(parts[len(parts)-1])
				if label != "" {
					return label, nil
				}
			}
		}
	}

	// Fallback: Look for backup label pattern like "20250104-100000F" or "20250104-100000F_20250104-120000I"
	// The pattern is: YYYYMMDD-HHMMSS followed by F (full), D (differential), or I (incremental)
	// Find all matches and take the last one (newest backup)
	re := regexp.MustCompile(`(\d{8}-\d{6}[FDI](?:_\d{8}-\d{6}[FDI])?)`)
	matches := re.FindAllStringSubmatch(output, -1)
	if len(matches) > 0 {
		// Return the last match (most recent backup ID)
		lastMatch := matches[len(matches)-1]
		if len(lastMatch) >= 2 {
			return lastMatch[1], nil
		}
	}

	return "", fmt.Errorf("backup ID not found in output")
}
