// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package migration holds the multigres CLI subcommands for table migrations
// (Multigres Migrator, temporary name). They call multiadmin, which forwards to Multigres Migrator.
package migration

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/tools/units"
)

// markersToSelection converts the CLI's flat --tables markers into the
// request's single SelectionObject: "*" selects all tables, "schema.*"
// selects a whole schema, and a plain "schema.table" selects that table. The
// markers must agree on one form (mirroring Postgres's own restriction that a
// migration's FOR clause is a table list, a schema list, or all — never a
// mix); mixing forms is rejected.
func markersToSelection(markers []string) (*migratorpb.SelectionObject, error) {
	var all bool
	var tables, schemas []string
	for _, m := range markers {
		switch {
		case m == "*":
			all = true
		case strings.HasSuffix(m, ".*"):
			schemas = append(schemas, strings.TrimSuffix(m, ".*"))
		default:
			tables = append(tables, m)
		}
	}
	kinds := 0
	for _, present := range []bool{all, len(tables) > 0, len(schemas) > 0} {
		if present {
			kinds++
		}
	}
	if kinds > 1 {
		return nil, errors.New("--tables cannot mix '*', 'schema.*', and 'schema.table' markers")
	}
	switch {
	case all:
		return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_All{All: true}}, nil
	case len(tables) > 0:
		return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Table{
			Table: &migratorpb.TableSpec{QualifiedNames: tables},
		}}, nil
	case len(schemas) > 0:
		return &migratorpb.SelectionObject{Object: &migratorpb.SelectionObject_Schema{
			Schema: &migratorpb.SchemaSpec{Schemata: schemas},
		}}, nil
	default:
		return nil, nil
	}
}

// splitRef interprets the --id flag, which accepts either a numeric migration id
// or a name: a value that parses as an integer is the id, otherwise it is a name.
func splitRef(ref string) (id int64, name string) {
	if n, err := strconv.ParseInt(ref, 10, 64); err == nil {
		return n, ""
	}
	return 0, ref
}

// toRef builds a MigrationRef from splitRef's result.
func toRef(id int64, name string) *migratorpb.MigrationRef {
	if name != "" {
		return &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Name{Name: name}}
	}
	return &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: id}}
}

func printJSON(cmd *cobra.Command, msg proto.Message) error {
	marshaler := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}
	data, err := marshaler.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal response to JSON: %w", err)
	}
	cmd.Println(string(data))
	return nil
}

// AddCreateMigrationCommand records a migration's configuration.
func AddCreateMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create-migration",
		Short: "Create a table migration from a Postgres source into a Multigres target",
		RunE: func(cmd *cobra.Command, args []string) error {
			f := cmd.Flags()
			connection, _ := f.GetString("connection")
			targetDB, _ := f.GetString("target-database")
			targetShard, _ := f.GetString("target-shard")
			targetTableGroup, _ := f.GetString("target-table-group")
			name, _ := f.GetString("name")
			tables, _ := f.GetStringSlice("tables")
			skipSchemaCopy, _ := f.GetBool("skip-schema-copy")
			quiesceRoles, _ := f.GetStringSlice("quiesce-roles")

			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			objects, err := markersToSelection(tables)
			if err != nil {
				return err
			}
			req := &migratorpb.CreateMigrationRequest{
				Migration: &migratorpb.MigrationRecord{
					Target: &clustermetadatapb.ShardKey{
						Database:   targetDB,
						TableGroup: targetTableGroup,
						Shard:      targetShard,
					},
					Name:           name,
					ConnectionName: connection,
					Objects:        objects,
				},
				Options: &migratorpb.MigrationOptions{
					SkipSchemaCopy: skipSchemaCopy,
				},
				QuiesceRoles: quiesceRoles,
			}
			// Only touch skip_copy_data when the operator set --copy-data, so an
			// unset flag keeps the server-side default (perform the copy).
			if f.Changed("copy-data") {
				copyData, _ := f.GetBool("copy-data")
				req.Options.SkipCopyData = !copyData
			}
			resp, err := client.CreateMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to create migration: %w", err)
			}
			return printJSON(cmd, resp)
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("connection", "", "name of a stored Connection to read the source from (create one first via gateway SQL CREATE CONNECTION)")
	cmd.Flags().String("target-database", "", "target multigres database")
	cmd.Flags().String("target-shard", "", "target shard (optional)")
	cmd.Flags().String("name", "", "optional migration name, unique per target database (addresses the migration in place of its id)")
	cmd.Flags().StringSlice("tables", nil, "tables to migrate (comma-separated); use '*' for all owned tables or 'schema.*' for all owned tables in a schema")
	cmd.Flags().Bool("copy-data", true, "run the initial COPY at subscription setup (false subscribes without a copy; target seeded out-of-band)")
	cmd.Flags().Bool("skip-schema-copy", false, "skip pg_dump --schema-only (the target schema already exists)")
	cmd.Flags().StringSlice("quiesce-roles", nil, "source application role(s) (comma-separated) whose CONNECT is revoked during the ACTIVATE cutover so they cannot write to the source once it becomes a subscriber")
	return cmd
}

// AddSetMigrationDirectionCommand sets a migration's active direction. On a
// not-yet-started migration, --direction=import runs the setup pipeline
// (replacing the old start-migration verb); otherwise it performs the
// symmetric cutover (IMPORT -> EXPORT, replacing activate-migration) or
// rollback (EXPORT -> IMPORT, replacing deactivate-migration). Setting the
// current direction is a no-op.
func AddSetMigrationDirectionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-migration-direction",
		Short: "Set a migration's active direction (import|export): begins, cuts over, or rolls back",
		RunE: func(cmd *cobra.Command, args []string) error {
			f := cmd.Flags()
			id, _ := f.GetString("id")
			direction, _ := f.GetString("direction")
			var dir migratorpb.MigrationDirection
			switch strings.ToLower(direction) {
			case "import":
				dir = migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT
			case "export":
				dir = migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT
			default:
				return fmt.Errorf("--direction must be \"import\" or \"export\", got %q", direction)
			}
			maxLagStr, _ := f.GetString("max-lag-bytes")
			waitTimeout, _ := f.GetInt64("wait-timeout")
			var maxLagBytes uint64
			if maxLagStr != "" {
				n, err := units.ParseBytes(maxLagStr)
				if err != nil {
					return fmt.Errorf("--max-lag-bytes: %w", err)
				}
				maxLagBytes = n
			}
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			refID, refName := splitRef(id)
			resp, err := client.SetMigrationDirection(cmd.Context(), &migratorpb.SetMigrationDirectionRequest{
				Ref:                toRef(refID, refName),
				Direction:          dir,
				MaxLagBytes:        &maxLagBytes,
				WaitTimeoutSeconds: &waitTimeout,
			})
			if err != nil {
				return fmt.Errorf("failed to set migration direction: %w", err)
			}
			return printJSON(cmd, resp)
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	cmd.Flags().String("direction", "", "direction to set: \"import\" or \"export\"")
	cmd.Flags().String("max-lag-bytes", "", "readiness threshold for a cutover to export: wait until replication lag is at or below this size before cutting over, so the cutover fits the gateway buffer window. Accepts a byte count or a size literal like '8 MiB' (empty = server default; ignored for --direction=import)")
	cmd.Flags().Int64("wait-timeout", 0, "timeout in seconds to wait for the lag to fall to --max-lag-bytes before failing (0 = server default; ignored for --direction=import)")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("direction")
	return cmd
}

// AddListMigrationsCommand lists all migrations, printing full details for
// each: it lists ids via ListMigrations, then fetches each one's details via
// GetMigration.
func AddListMigrationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-migrations",
		Short: "List all migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			list, err := client.ListMigrations(cmd.Context(), &migratorpb.ListMigrationsRequest{})
			if err != nil {
				return fmt.Errorf("failed to list migrations: %w", err)
			}
			migrations := make([]*migratorpb.GetMigrationResponse, 0, len(list.GetIds()))
			for _, id := range list.GetIds() {
				resp, err := client.GetMigration(cmd.Context(), &migratorpb.GetMigrationRequest{
					Ref: &migratorpb.MigrationRef{Ref: &migratorpb.MigrationRef_Id{Id: id}},
				})
				if err != nil {
					return fmt.Errorf("failed to get migration %d: %w", id, err)
				}
				migrations = append(migrations, resp)
			}
			marshaler := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}
			for _, mig := range migrations {
				data, err := marshaler.Marshal(mig)
				if err != nil {
					return fmt.Errorf("failed to marshal response to JSON: %w", err)
				}
				cmd.Println(string(data))
			}
			return nil
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	return cmd
}

// AddGetMigrationCommand shows one migration by id or name.
func AddGetMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-migration",
		Short: "Get a single migration by id",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			refID, refName := splitRef(id)
			req := &migratorpb.GetMigrationRequest{Ref: toRef(refID, refName)}
			resp, err := client.GetMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to get migration: %w", err)
			}
			return printJSON(cmd, resp)
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

// AddDropMigrationCommand tears down and removes a migration.
func AddDropMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "drop-migration",
		Short: "Drop a migration",
		RunE: func(cmd *cobra.Command, args []string) error {
			f := cmd.Flags()
			id, _ := f.GetString("id")
			wait, _ := f.GetBool("wait")
			waitTimeout, _ := f.GetInt64("wait-timeout")
			force, _ := f.GetBool("force")
			if wait && force {
				return errors.New("--wait and --force are mutually exclusive")
			}
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			refID, refName := splitRef(id)
			req := &migratorpb.DropMigrationRequest{
				Ref:                toRef(refID, refName),
				Wait:               wait,
				WaitTimeoutSeconds: waitTimeout,
				Force:              force,
			}
			resp, err := client.DropMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to drop migration: %w", err)
			}
			return printJSON(cmd, resp)
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	_ = cmd.MarkFlagRequired("id")
	cmd.Flags().Bool("wait", false, "block until the migration is caught up, then drain and tear down (for scripts)")
	cmd.Flags().Int64("wait-timeout", 0, "timeout in seconds for --wait (0 = no timeout)")
	cmd.Flags().Bool("force", false, "skip the drain and tear down from any phase")
	return cmd
}
