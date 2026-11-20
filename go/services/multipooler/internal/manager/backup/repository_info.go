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

package backup

import (
	"context"
	"fmt"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// RepositoryInfo is what one `pgbackrest info` reports about the stanza.
type RepositoryInfo struct {
	// Backups are this shard's backups; those annotated for another shard are left out.
	Backups []*multipoolermanagerdata.BackupMetadata
	// TotalBackups counts every backup under the stanza, whichever shard made
	// it. Repositories from before shard-scoped paths can hold several shards' backups.
	TotalBackups int
	// StanzaExists is false only when pgbackrest reports the stanza never existed.
	StanzaExists bool
}

// RepositoryInfo runs `pgbackrest info` once, uncached. Callers reset the
// stanza on an empty answer, so unlike ListBackups any failed run is an error.
func (e *Engine) RepositoryInfo(ctx context.Context) (RepositoryInfo, error) {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return RepositoryInfo{}, mterrors.Wrap(err, "pgbackrest config not found")
	}

	if _, err := e.requireBackupConfig(); err != nil {
		return RepositoryInfo{}, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup config not loaded from topology")
	}

	output, err := e.runInfoCmd(ctx, configPath)
	if err != nil {
		return RepositoryInfo{}, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest info failed: %v\nOutput: %s", err, output))
	}

	infoData, err := commonbackup.DecodeInfo(output)
	if err != nil {
		return RepositoryInfo{}, err
	}
	if err := commonbackup.RepoStatusError(infoData); err != nil {
		return RepositoryInfo{}, err
	}

	info := RepositoryInfo{
		Backups:      e.parseBackups(ctx, infoData),
		StanzaExists: commonbackup.StanzaExists(infoData),
	}
	if len(infoData) > 0 {
		info.TotalBackups = len(infoData[0].Backup)
	}
	return info, nil
}
