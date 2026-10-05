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
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type readOnlyCmd struct {
	database viperutil.Value[string]
	disable  viperutil.Value[bool]
	force    viperutil.Value[bool]
	timeout  viperutil.Value[time.Duration]
}

// AddReadOnlyCommand registers the read-only subcommand.
func AddReadOnlyCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()
	ro := &readOnlyCmd{
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default: "postgres", FlagName: "database",
		}),
		disable: viperutil.Configure(reg, "disable", viperutil.Options[bool]{
			Default: false, FlagName: "disable",
		}),
		force: viperutil.Configure(reg, "force", viperutil.Options[bool]{
			Default: false, FlagName: "force",
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default: 30 * time.Second, FlagName: "timeout",
		}),
	}

	cmd := &cobra.Command{
		Use:   "read-only",
		Short: "Put a database into read-only mode, or lift it",
		Long: `Put a database into read-only mode, or lift it with --disable.

While read-only, every multigateway rejects new write transactions for the
database with SQLSTATE 25006 (read_only_sql_transaction); reads keep
working. Sessions already inside a transaction, or holding a pinned backend
(temp tables, advisory locks, held cursors), keep their read-write default
until they end. Pass --force to terminate those sessions as well.

The flag lives on the database's topology record, so it survives gateway
restarts and applies to gateways started later.

Examples:

  # Stop writes before the disk fills up
  multigres cluster read-only --database=postgres

  # Same, and kill sessions that could still write
  multigres cluster read-only --database=postgres --force

  # Back to read-write
  multigres cluster read-only --database=postgres --disable`,
		RunE: ro.run,
	}

	cmd.Flags().String("database", ro.database.Default(), "Database name")
	cmd.Flags().Bool("disable", ro.disable.Default(), "Lift read-only mode")
	cmd.Flags().Bool("force", ro.force.Default(), "Also terminate sessions that are mid-transaction or hold a pinned backend")
	cmd.Flags().Duration("timeout", ro.timeout.Default(), "RPC timeout")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(), ro.database, ro.disable, ro.force, ro.timeout)

	clusterCmd.AddCommand(cmd)
}

func (ro *readOnlyCmd) run(cmd *cobra.Command, _ []string) error {
	if ro.disable.Get() && ro.force.Get() {
		return errors.New("--force only applies when enabling read-only mode")
	}

	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), ro.timeout.Get())
	defer cancel()

	_, err = client.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{
		Database: ro.database.Get(),
		ReadOnly: !ro.disable.Get(),
		Force:    ro.force.Get(),
	})
	if err != nil {
		return fmt.Errorf("read-only failed: %w", err)
	}

	switch {
	case ro.disable.Get():
		cmd.Printf("Database %q is read-write again.\n", ro.database.Get())
	case ro.force.Get():
		cmd.Printf("Database %q is read-only; gateways are terminating sessions that could still write.\n", ro.database.Get())
	default:
		cmd.Printf("Database %q is read-only; new write transactions are rejected.\n", ro.database.Get())
	}
	return nil
}
