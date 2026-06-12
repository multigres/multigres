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
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type switchPrimaryCmd struct {
	database       viperutil.Value[string]
	tableGroup     viperutil.Value[string]
	shard          viperutil.Value[string]
	catchupTimeout viperutil.Value[time.Duration]
	reason         viperutil.Value[string]
	yes            viperutil.Value[bool]
	timeout        viperutil.Value[time.Duration]
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
		catchupTimeout: viperutil.Configure(reg, "catchup-timeout", viperutil.Options[time.Duration]{
			Default: 30 * time.Second, FlagName: "catchup-timeout",
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

--catchup-timeout controls how long to wait for multiorch to elect a new
primary. If the timeout expires, the REQUESTING_DEMOTION signal persists
and multiorch will complete the election on its own.

Examples:

  # Switchover with default 30s timeout
  multigres cluster switch-primary \
    --database=postgres --reason="maintenance"

  # Switchover with extended election wait
  multigres cluster switch-primary \
    --database=postgres --catchup-timeout=60s --reason="zone1 maintenance"`,
		RunE: pf.run,
	}

	cmd.Flags().String("database", pf.database.Default(), "Database name")
	cmd.Flags().String("table-group", pf.tableGroup.Default(), "Table group name")
	cmd.Flags().String("shard", pf.shard.Default(), "Shard name")
	cmd.Flags().Duration("catchup-timeout", pf.catchupTimeout.Default(), "How long to wait for multiorch to elect a new primary")
	cmd.Flags().String("reason", pf.reason.Default(), "Free-text reason for the failover (recorded for audit)")
	cmd.Flags().Bool("yes", pf.yes.Default(), "Skip the interactive confirmation prompt")
	cmd.Flags().Duration("timeout", pf.timeout.Default(), "Overall RPC timeout (must exceed --catchup-timeout)")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(),
		pf.database, pf.tableGroup, pf.shard,
		pf.catchupTimeout, pf.reason, pf.yes, pf.timeout)

	clusterCmd.AddCommand(cmd)
}

func (pf *switchPrimaryCmd) run(cmd *cobra.Command, _ []string) error {
	if pf.timeout.Get() <= pf.catchupTimeout.Get() {
		return fmt.Errorf("--timeout (%s) must be greater than --catchup-timeout (%s)",
			pf.timeout.Get(), pf.catchupTimeout.Get())
	}

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
	cmd.Printf("New leader: %s/%s\n",
		resp.GetNewLeaderId().GetCell(),
		resp.GetNewLeaderId().GetName())
	return nil
}

func (pf *switchPrimaryCmd) buildRequest() (*multiadminpb.SwitchPrimaryRequest, error) {
	req := &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   pf.database.Get(),
			TableGroup: pf.tableGroup.Get(),
			Shard:      pf.shard.Get(),
		},
		Reason:         pf.reason.Get(),
		CatchupTimeout: durationpb.New(pf.catchupTimeout.Get()),
	}

	return req, nil
}

// confirmSwitchPrimary prints a summary and prompts the operator to type the
// shard name before proceeding.
func confirmSwitchPrimary(cmd *cobra.Command, req *multiadminpb.SwitchPrimaryRequest) error {
	sk := req.GetShardKey()
	cmd.Printf("\nShard:           %s/%s/%s\n", sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())
	cmd.Printf("Target standby:  (multiorch will pick the most-advanced standby)\n")
	cmd.Printf("Catchup timeout: %s\n", req.GetCatchupTimeout().AsDuration())
	cmd.Printf("Reason:          %s\n", req.GetReason())

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
