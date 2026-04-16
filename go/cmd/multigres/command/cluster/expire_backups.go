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

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/constants"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type expireBackupsCmd struct {
	database viperutil.Value[string]
	timeout  viperutil.Value[time.Duration]
}

// AddExpireBackupsCommand adds the expire-backups subcommand to the cluster command
func AddExpireBackupsCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()

	ecmd := &expireBackupsCmd{
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default:  "postgres",
			FlagName: "database",
			Dynamic:  false,
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default:  10 * time.Minute,
			FlagName: "timeout",
			Dynamic:  false,
		}),
	}

	cmd := &cobra.Command{
		Use:   "expire-backups",
		Short: "Remove old backups that exceed retention policy",
		Long:  "Runs pgbackrest expire to remove backups that exceed the configured retention policy.",
		RunE:  ecmd.runExpireBackups,
	}

	cmd.Flags().String("database", ecmd.database.Default(), "Database name")
	cmd.Flags().Duration("timeout", ecmd.timeout.Default(), "Timeout for the expire operation")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(), ecmd.database, ecmd.timeout)

	clusterCmd.AddCommand(cmd)
}

func (ecmd *expireBackupsCmd) runExpireBackups(cmd *cobra.Command, args []string) error {
	database := ecmd.database.Get()
	timeout := ecmd.timeout.Get()

	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	cmd.Printf("Expiring old backups for database=%s...\n", database)

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	resp, err := client.ExpireBackups(ctx, &multiadminpb.ExpireBackupsRequest{
		Database:   database,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	})
	if err != nil {
		return fmt.Errorf("failed to expire backups: %w", err)
	}

	if len(resp.ExpiredBackupIds) > 0 {
		cmd.Printf("Expired %d backup(s):\n", len(resp.ExpiredBackupIds))
		for _, id := range resp.ExpiredBackupIds {
			cmd.Printf("  - %s\n", id)
		}
	} else {
		cmd.Println("No backups were expired.")
	}
	return nil
}
