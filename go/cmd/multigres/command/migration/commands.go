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
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// markersToSelection converts the CLI's flat --tables markers into the request's
// structured selection: "*" -> all_tables, "schema.*" -> a schema object, and a
// plain "schema.table" -> a table object.
func markersToSelection(markers []string) (allTables bool, objects []*migratorpb.SelectionObject) {
	for _, m := range markers {
		switch {
		case m == "*":
			allTables = true
		case strings.HasSuffix(m, ".*"):
			objects = append(objects, &migratorpb.SelectionObject{
				Object: &migratorpb.SelectionObject_Schema{Schema: strings.TrimSuffix(m, ".*")},
			})
		default:
			objects = append(objects, &migratorpb.SelectionObject{
				Object: &migratorpb.SelectionObject_Table{Table: &migratorpb.TableSpec{QualifiedName: m}},
			})
		}
	}
	return allTables, objects
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
			sourceDSN, _ := f.GetString("source-dsn")
			targetDB, _ := f.GetString("target-database")
			targetShard, _ := f.GetString("target-shard")
			name, _ := f.GetString("name")
			tables, _ := f.GetStringSlice("tables")
			skipSchemaCopy, _ := f.GetBool("skip-schema-copy")

			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			allTables, objects := markersToSelection(tables)
			req := &migratorpb.CreateMigrationRequest{
				SourceDsn:      sourceDSN,
				TargetDatabase: targetDB,
				TargetShard:    targetShard,
				Name:           name,
				AllTables:      allTables,
				Objects:        objects,
				SkipSchemaCopy: skipSchemaCopy,
			}
			// Only send copy_data when the operator set it, so an unset flag keeps
			// the server-side default (true).
			if f.Changed("copy-data") {
				copyData, _ := f.GetBool("copy-data")
				req.CopyData = &copyData
			}
			resp, err := client.CreateMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to create migration: %w", err)
			}
			return printJSON(cmd, resp.GetMigration())
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("source-dsn", "", "libpq conninfo of the source postgres (keyword form, e.g. 'host=... port=... user=... password=... dbname=...')")
	cmd.Flags().String("target-database", "", "target multigres database")
	cmd.Flags().String("target-shard", "", "target shard (optional)")
	cmd.Flags().String("name", "", "optional migration name, unique per target database (addresses the migration in place of its id)")
	cmd.Flags().StringSlice("tables", nil, "tables to migrate (comma-separated); use '*' for all owned tables or 'schema.*' for all owned tables in a schema")
	cmd.Flags().Bool("copy-data", true, "run the initial COPY at subscription setup (false subscribes without a copy; target seeded out-of-band)")
	cmd.Flags().Bool("skip-schema-copy", false, "skip pg_dump --schema-only (the target schema already exists)")
	return cmd
}

// AddStartMigrationCommand begins a recorded migration.
func AddStartMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-migration",
		Short: "Start a previously created migration",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()
			req := &migratorpb.StartMigrationRequest{Id: id}
			resp, err := client.StartMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to start migration: %w", err)
			}
			return printJSON(cmd, resp.GetMigration())
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

// AddUpdateMigrationCommand changes mutable fields of a migration. Only flags
// the operator sets are sent (a field mask), so unset fields are left unchanged.
func AddUpdateMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-migration",
		Short: "Update mutable fields of a migration (source connection, sequence margin, or tables while CREATED)",
		RunE: func(cmd *cobra.Command, args []string) error {
			f := cmd.Flags()
			id, _ := f.GetString("id")
			req := &migratorpb.UpdateMigrationRequest{Id: id}
			var paths []string
			if f.Changed("source-dsn") {
				req.SourceDsn, _ = f.GetString("source-dsn")
				paths = append(paths, "source_dsn")
			}
			if f.Changed("sequence-margin") {
				req.SequenceMargin, _ = f.GetInt64("sequence-margin")
				paths = append(paths, "sequence_margin")
			}
			if f.Changed("tables") {
				req.Tables, _ = f.GetStringSlice("tables")
				paths = append(paths, "tables")
			}
			if len(paths) == 0 {
				return errors.New("no fields to update; set at least one of --source-dsn/--sequence-margin/--tables")
			}
			req.UpdateMask = &fieldmaskpb.FieldMask{Paths: paths}

			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()
			resp, err := client.UpdateMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to update migration: %w", err)
			}
			return printJSON(cmd, resp.GetMigration())
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	_ = cmd.MarkFlagRequired("id")
	cmd.Flags().String("source-dsn", "", "new source libpq conninfo (host/port/user/password/TLS may change; the source database may not)")
	cmd.Flags().Int64("sequence-margin", 0, "margin added past each sequence max at a direction switch")
	cmd.Flags().StringSlice("tables", nil, "tables to migrate (only while CREATED)")
	return cmd
}

// AddActivateMigrationCommand cuts a migration over to serving (IMPORT -> EXPORT).
func AddActivateMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activate-migration",
		Short: "Activate a migration: cut over to serving (switch to EXPORT)",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			resp, err := client.ActivateMigration(cmd.Context(), &migratorpb.ActivateMigrationRequest{Id: id})
			if err != nil {
				return fmt.Errorf("failed to activate migration: %w", err)
			}
			return printJSON(cmd, resp.GetMigration())
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

// AddDeactivateMigrationCommand rolls a migration back to non-serving (EXPORT -> IMPORT).
func AddDeactivateMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deactivate-migration",
		Short: "Deactivate a migration: roll back to non-serving (switch to IMPORT)",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			client, err := admin.NewClient(cmd)
			if err != nil {
				return err
			}
			defer client.Close()

			resp, err := client.DeactivateMigration(cmd.Context(), &migratorpb.DeactivateMigrationRequest{Id: id})
			if err != nil {
				return fmt.Errorf("failed to deactivate migration: %w", err)
			}
			return printJSON(cmd, resp.GetMigration())
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	cmd.Flags().String("id", "", "migration id or name")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

// AddListMigrationsCommand lists all migrations.
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

			req := &migratorpb.GetMigrationsRequest{}
			resp, err := client.GetMigrations(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to list migrations: %w", err)
			}
			return printJSON(cmd, resp)
		},
	}
	cmd.Flags().String("admin-server", "", "Address of the multiadmin server (overrides config)")
	return cmd
}

// AddGetMigrationCommand shows one migration by id.
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

			req := &migratorpb.GetMigrationsRequest{Id: id}
			resp, err := client.GetMigrations(cmd.Context(), req)
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

			req := &migratorpb.DropMigrationRequest{
				Id:                 id,
				Wait:               wait,
				WaitTimeoutSeconds: waitTimeout,
				Force:              force,
			}
			resp, err := client.DropMigration(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("failed to drop migration: %w", err)
			}
			return printJSON(cmd, resp.GetMigration())
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
