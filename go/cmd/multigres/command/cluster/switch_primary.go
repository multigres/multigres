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

package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type switchPrimaryCmd struct {
	database   viperutil.Value[string]
	tableGroup viperutil.Value[string]
	shard      viperutil.Value[string]
	reason     viperutil.Value[string]
	yes        viperutil.Value[bool]
	timeout    viperutil.Value[time.Duration]
}

// AddSwitchPrimaryCommand registers the switch-primary subcommand.
func AddSwitchPrimaryCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()
	pf := &switchPrimaryCmd{
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default: "postgres", FlagName: "database",
		}),
		tableGroup: viperutil.Configure(reg, "table-group", viperutil.Options[string]{
			Default: constants.DefaultTableGroup, FlagName: "table-group",
		}),
		shard: viperutil.Configure(reg, "shard", viperutil.Options[string]{
			Default: constants.DefaultShard, FlagName: "shard",
		}),
		reason: viperutil.Configure(reg, "reason", viperutil.Options[string]{
			Default: "", FlagName: "reason",
		}),
		yes: viperutil.Configure(reg, "yes", viperutil.Options[bool]{
			Default: false, FlagName: "yes",
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default: 120 * time.Second, FlagName: "timeout",
		}),
	}

	cmd := &cobra.Command{
		Use:   "switch-primary",
		Short: "Perform a graceful switchover to a standby pooler",
		Long: `Perform a graceful switchover for a shard.

New queries for the current primary are rejected with MTF01 so the
gateway can buffer and retry. The current primary is then restarted as
a standby and publishes REQUESTING_DEMOTION. Multiorch's recovery engine
picks the most-advanced standby and promotes it via the normal
Recruit/Promote consensus flow.

No pg_rewind is required because the old primary and the new primary
diverge at the same WAL position.

The command returns as soon as the old primary has been quiesced and
restarted as a standby. Use "multigres cluster status" to watch for the
new primary to appear.

Examples:

  # Graceful switchover
  multigres cluster switch-primary \
    --database=postgres --reason="maintenance"`,
		RunE: pf.run,
	}

	cmd.Flags().String("database", pf.database.Default(), "Database name")
	cmd.Flags().String("table-group", pf.tableGroup.Default(), "Table group name")
	cmd.Flags().String("shard", pf.shard.Default(), "Shard name")
	cmd.Flags().String("reason", pf.reason.Default(), "Free-text reason for the failover (recorded for audit)")
	cmd.Flags().Bool("yes", pf.yes.Default(), "Skip the interactive confirmation prompt")
	cmd.Flags().Duration("timeout", pf.timeout.Default(), "Overall RPC timeout")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(),
		pf.database, pf.tableGroup, pf.shard,
		pf.reason, pf.yes, pf.timeout)

	clusterCmd.AddCommand(cmd)
}

func (pf *switchPrimaryCmd) run(cmd *cobra.Command, _ []string) error {
	req, err := pf.buildRequest()
	if err != nil {
		return err
	}

	if !pf.yes.Get() {
		if err := confirmSwitchPrimary(cmd, req); err != nil {
			return err
		}
	}

	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), pf.timeout.Get())
	defer cancel()

	resp, err := client.SwitchPrimary(ctx, req)
	if err != nil {
		return fmt.Errorf("switch-primary failed: %w", err)
	}

	cmd.Printf("Switch-primary complete.\n")
	cmd.Printf("Old leader: %s/%s\n",
		resp.GetOldLeaderId().GetCell(),
		resp.GetOldLeaderId().GetName())
	cmd.Printf("Multiorch will elect a new leader. Run \"multigres cluster status\" to monitor.\n")
	return nil
}

func (pf *switchPrimaryCmd) buildRequest() (*multiadminpb.SwitchPrimaryRequest, error) {
	req := &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   pf.database.Get(),
			TableGroup: pf.tableGroup.Get(),
			Shard:      pf.shard.Get(),
		},
		Reason: pf.reason.Get(),
	}

	return req, nil
}

// confirmSwitchPrimary prints a summary and prompts the operator to type the
// shard name before proceeding.
func confirmSwitchPrimary(cmd *cobra.Command, req *multiadminpb.SwitchPrimaryRequest) error {
	sk := req.GetShardKey()
	cmd.Printf("\nShard:   %s/%s/%s\n", sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())
	cmd.Printf("Reason:  %s\n", req.GetReason())

	cmd.Print("\nThis will quiesce writes on the current primary. The gateway will\n" +
		"buffer client queries and retry them on the new primary.\n\n")
	cmd.Printf("Type the shard name (%s) to confirm: ", sk.GetShard())

	scanner := bufio.NewScanner(cmd.InOrStdin())
	if !scanner.Scan() {
		return errors.New("aborted")
	}
	if strings.TrimSpace(scanner.Text()) != sk.GetShard() {
		return errors.New("aborted: confirmation did not match shard name")
	}
	return nil
}
