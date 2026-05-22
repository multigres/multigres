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

type verifyBackupsCmd struct {
	timeout viperutil.Value[time.Duration]
}

// AddVerifyBackupsCommand adds the verify-backups subcommand to the cluster command.
func AddVerifyBackupsCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()

	vcmd := &verifyBackupsCmd{
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default:  30 * time.Minute,
			FlagName: "timeout",
			Dynamic:  false,
		}),
	}

	cmd := &cobra.Command{
		Use:   "verify-backups",
		Short: "Verify the integrity of all backups and the WAL archive",
		Long:  "Runs `pgbackrest verify` against the full stanza to validate every backup file and WAL segment in the repository.",
		RunE:  vcmd.runVerifyBackups,
	}

	cmd.Flags().Duration("timeout", vcmd.timeout.Default(), "Timeout for the verify operation")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(), vcmd.timeout)

	clusterCmd.AddCommand(cmd)
}

func (vcmd *verifyBackupsCmd) runVerifyBackups(cmd *cobra.Command, args []string) error {
	timeout := vcmd.timeout.Get()

	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	cmd.Println("Verifying backups...")

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	resp, err := client.VerifyBackups(ctx, &multiadminpb.VerifyBackupsRequest{
		Database:   "postgres",
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	})
	if err != nil {
		return fmt.Errorf("verify failed: %w", err)
	}

	cmd.Printf("Backup verification completed in %s.\n", resp.Duration.AsDuration())
	if resp.RawOutput != "" {
		cmd.Println(resp.RawOutput)
	}
	return nil
}
