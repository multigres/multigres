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

package topo

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/prototext"
)

// CreateClusterMetadataCommand adds the createclustermetadataCommand subcommand
func CreateClusterMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "createclustermetadata",
		Short: "Create the initial metadata for a new cluster",
		Long:  "To be invoked by the operator to create the initial metadata needed for a new cluster",
		RunE:  createClusterMetadata,
	}

	// Add command-specific flags
	cmd.Flags().String("global-topo-address", "localhost:2379", "Address of the of the global topo server (e.g., localhost:2379)")
	cmd.Flags().String("global-topo-root", "/multigres/test/global", "Root path for the cluster in the global topo server")
	cmd.Flags().StringSlice("cells", []string{"zone1"}, "List of cells")
	cmd.Flags().String("durability-policy", "none", "Cluster Durability Policy")
	cmd.Flags().String("backup-location", "", "Backup location")

	_ = cmd.MarkFlagRequired("backup-location")

	return cmd
}

// createClusterMetadata executes the createclustermetadata command
func createClusterMetadata(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	globalTopoAddress, _ := cmd.Flags().GetString("global-topo-address")
	globalTopoRoot, _ := cmd.Flags().GetString("global-topo-root")
	cells, _ := cmd.Flags().GetStringSlice("cells")
	durabilityPolicy, _ := cmd.Flags().GetString("durability-policy")
	backupLocation, _ := cmd.Flags().GetString("backup-location")

	// Create topology store using configured backend
	ts, err := topoclient.OpenServer("etcd", globalTopoRoot, []string{globalTopoAddress}, topoclient.NewDefaultTopoConfig())
	if err != nil {
		return fmt.Errorf("failed to connect to topology server: %w", err)
	}
	defer ts.Close()

	// Create each cell
	for _, cellName := range cells {
		if err := createCell(ctx, ts, cellName, globalTopoAddress, globalTopoRoot); err != nil {
			return fmt.Errorf("failed to create cell %s: %w", cellName, err)
		}
	}

	// Provision the database
	if err := provisionDatabase(ctx, ts, "postgres", cells, backupLocation, durabilityPolicy); err != nil {
		return fmt.Errorf("failed to provision database: %w", err)
	}

	return nil
}

// createCell creates a single cell in the topology
func createCell(ctx context.Context, ts topoclient.Store, cellName, etcdAddress, globalTopoRoot string) error {
	fmt.Printf("Configuring cell: %s\n", cellName)

	// Check if cell already exists
	cell, err := ts.GetCell(ctx, cellName)
	if err == nil {
		fmt.Printf("Cell \"%s\" detected — reusing existing cell:\n%s\n", cellName, prototext.Format(cell))
		return nil
	}

	// Create the cell if it doesn't exist
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		fmt.Printf("Creating cell \"%s\"...\n", cellName)

		// Construct cell root path based on global root and cell name
		cellRoot := globalTopoRoot + "/" + cellName

		cellConfig := &clustermetadatapb.Cell{
			Name:            cellName,
			ServerAddresses: []string{etcdAddress},
			Root:            cellRoot,
		}

		if err := ts.CreateCell(ctx, cellName, cellConfig); err != nil {
			return fmt.Errorf("failed to create cell '%s': %w", cellName, err)
		}

		fmt.Printf("Cell \"%s\" created successfully\n", cellName)
		return nil
	}

	// Some other error occurred
	return fmt.Errorf("failed to check cell '%s': %w", cellName, err)
}

// provisionDatabase registers a database in the global topology
func provisionDatabase(ctx context.Context, ts topoclient.Store, databaseName string, cellNames []string, backupLocation, durabilityPolicy string) error {
	fmt.Printf("Registering database: %s\n", databaseName)

	// Check if database already exists
	db, err := ts.GetDatabase(ctx, databaseName)
	if err == nil {
		fmt.Printf("Database \"%s\" detected — reusing existing database\n%s\n", databaseName, prototext.Format(db))
		return nil
	}

	// Create the database if it doesn't exist
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		fmt.Printf("Creating database \"%s\" with cells: [%s]...\n", databaseName, cellNames)

		databaseConfig := &clustermetadatapb.Database{
			Name: databaseName,
			BackupLocation: &clustermetadatapb.BackupLocation{
				Location: &clustermetadatapb.BackupLocation_Filesystem{
					Filesystem: &clustermetadatapb.FilesystemBackup{
						Path: backupLocation,
					},
				},
			},
			DurabilityPolicy: durabilityPolicy,
			Cells:            cellNames,
		}

		if err := ts.CreateDatabase(ctx, databaseName, databaseConfig); err != nil {
			return fmt.Errorf("failed to create database '%s' in topology: %w", databaseName, err)
		}

		fmt.Printf("Database \"%s\" registered successfully\n", databaseName)
		return nil
	}

	// Some other error occurred
	return fmt.Errorf("failed to check database '%s': %w", databaseName, err)
}
