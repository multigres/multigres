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

// failover-test is a tool for testing Multigres failover in local clusters.
//
// Usage:
//
//	go run failover-test.go                # Interactive mode
//	go run failover-test.go --yes          # Automatic mode
//	go run failover-test.go --yes --debug  # With debug logging
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"

	_ "github.com/lib/pq"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

const (
	checkInterval = 1 * time.Second
)

// ANSI color codes
const (
	colorRed    = "\033[0;31m"
	colorGreen  = "\033[0;32m"
	colorYellow = "\033[1;33m"
	colorBlue   = "\033[0;34m"
	colorReset  = "\033[0m"
)

// PoolerInfo holds information about a pooler instance
type PoolerInfo struct {
	Cell      string
	ServiceID string
	PoolerDir string
	PgPort    int
}

// Config holds the loaded cluster configuration
type Config struct {
	Cells       map[string]local.CellServicesConfig
	AdminServer string
	RepoRoot    string
	ConfigPath  string
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}

var rootCmd = &cobra.Command{
	Use:   "failover-test",
	Short: "Multigres failover test script for local clusters",
	Long: `Continuously test failover by stopping the primary pooler and waiting
for a new primary to be elected and the old primary to become a replica.

Examples:
  # Run interactive mode (ask before each failover)
  ./failover-test

  # Run automatic mode (continuous failover without prompts)
  ./failover-test --yes

  # Enable debug logging
  ./failover-test --yes --debug`,
	RunE: runFailoverTest,
}

var (
	autoYes bool
	debug   bool
)

func init() {
	rootCmd.Flags().BoolVarP(&autoYes, "yes", "y", false, "Automatically proceed with failovers without confirmation")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
}

func runFailoverTest(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) //nolint:gocritic // entry point for CLI tool
	defer cancel()

	logInfo("Multigres Failover Test Script (Local Cluster)")

	// Find repository root
	repoRoot, err := findRepoRoot()
	if err != nil {
		return fmt.Errorf("failed to find repository root: %w", err)
	}

	// Load configuration
	configPath := filepath.Join(repoRoot, "multigres_local", "multigres.yaml")
	logInfo("Config path: " + configPath)

	if autoYes {
		logWarn("Auto-yes mode enabled - will continuously kill primaries without confirmation")
	}
	if debug {
		logInfo("Debug mode enabled - will show detailed diagnostic information")
	}
	fmt.Fprintln(os.Stderr)

	config, err := loadConfig(repoRoot, configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	logSuccess(fmt.Sprintf("Loaded configuration with %d cells", len(config.Cells)))

	// Test connectivity
	if err := testConnectivity(config); err != nil {
		return err
	}
	logSuccess("All prerequisites satisfied")
	fmt.Fprintln(os.Stderr)

	// Disable PostgreSQL monitoring on all poolers
	if err := disablePostgresMonitoring(ctx, config); err != nil {
		return fmt.Errorf("failed to disable postgres monitoring: %w", err)
	}

	// Start the failover loop
	return failoverLoop(ctx, config)
}

func findRepoRoot() (string, error) {
	// Start from current directory
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Walk up until we find go.mod or reach root
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("could not find repository root (looking for go.mod)")
		}
		dir = parent
	}
}

func loadConfig(repoRoot, configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			logError("Config file not found: " + configPath)
			logError("Please run './bin/multigres cluster init' first")
		}
		return nil, err
	}

	var rawConfig struct {
		Provisioner       string         `yaml:"provisioner"`
		ProvisionerConfig map[string]any `yaml:"provisioner-config"`
	}

	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if rawConfig.Provisioner != "local" {
		return nil, fmt.Errorf("unsupported provisioner: %s", rawConfig.Provisioner)
	}

	// Convert to LocalProvisionerConfig
	yamlData, err := yaml.Marshal(rawConfig.ProvisionerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal provisioner config: %w", err)
	}

	var localConfig local.LocalProvisionerConfig
	if err := yaml.Unmarshal(yamlData, &localConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal local provisioner config: %w", err)
	}

	return &Config{
		Cells:       localConfig.Cells,
		AdminServer: fmt.Sprintf("localhost:%d", localConfig.Multiadmin.GrpcPort),
		RepoRoot:    repoRoot,
		ConfigPath:  configPath,
	}, nil
}

func testConnectivity(config *Config) error {
	client, err := newAdminClient(config.AdminServer)
	if err != nil {
		logError(fmt.Sprintf("Failed to connect to cluster: %v", err))
		logError("Make sure the cluster is running: ./bin/multigres cluster up")
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:gocritic // short-lived connectivity check
	defer cancel()

	_, err = client.GetPoolers(ctx, &multiadminpb.GetPoolersRequest{})
	if err != nil {
		logError(fmt.Sprintf("Failed to connect to cluster: %v", err))
		logError("Make sure the cluster is running: ./bin/multigres cluster up")
		return err
	}

	return nil
}

func newAdminClient(addr string) (*adminClient, error) {
	conn, err := grpccommon.NewClient(addr, grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin server at %s: %w", addr, err)
	}
	return &adminClient{
		MultiAdminServiceClient: multiadminpb.NewMultiAdminServiceClient(conn),
		conn:                    conn,
	}, nil
}

type adminClient struct {
	multiadminpb.MultiAdminServiceClient
	conn *grpc.ClientConn
}

func (c *adminClient) Close() error {
	return c.conn.Close()
}

func disablePostgresMonitoring(ctx context.Context, config *Config) error {
	logInfo("Disabling PostgreSQL monitoring on all poolers...")

	client, err := newAdminClient(config.AdminServer)
	if err != nil {
		return err
	}
	defer client.Close()

	poolers, err := getPoolers(ctx, client)
	if err != nil {
		return err
	}

	if len(poolers) == 0 {
		logWarn("No poolers found")
		return nil
	}

	for _, pooler := range poolers {
		cell := pooler.Id.Cell
		serviceID := pooler.Id.Name
		poolerName := fmt.Sprintf("multipooler-%s-%s", cell, serviceID)

		logInfo(fmt.Sprintf("  Disabling monitoring on: %s (cell=%s, service_id=%s)", poolerName, cell, serviceID))

		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.SetPostgresMonitor(reqCtx, &multiadminpb.SetPostgresMonitorRequest{
			PoolerId: pooler.Id,
			Enabled:  false,
		})
		cancel()

		if err != nil {
			logError(fmt.Sprintf("    ✗ Failed to disable monitoring on %s: %v", poolerName, err))
			return err
		}
		logSuccess("    ✓ Disabled monitoring on " + poolerName)
	}

	logSuccess(fmt.Sprintf("Disabled monitoring on %d pooler(s)", len(poolers)))
	fmt.Fprintln(os.Stderr)
	return nil
}

func getPoolers(ctx context.Context, client *adminClient) ([]*clustermetadatapb.MultiPooler, error) {
	resp, err := client.GetPoolers(ctx, &multiadminpb.GetPoolersRequest{})
	if err != nil {
		return nil, fmt.Errorf("GetPoolers RPC failed: %w", err)
	}
	return resp.Poolers, nil
}

func getPoolerStatus(ctx context.Context, client *adminClient, cell, serviceID string) (*multiadminpb.GetPoolerStatusResponse, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return client.GetPoolerStatus(reqCtx, &multiadminpb.GetPoolerStatusRequest{
		PoolerId: &clustermetadatapb.ID{
			Cell: cell,
			Name: serviceID,
		},
	})
}

func findPrimary(ctx context.Context, config *Config) (*PoolerInfo, error) {
	logInfo("Searching for current primary...")

	client, err := newAdminClient(config.AdminServer)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	poolers, err := getPoolers(ctx, client)
	if err != nil {
		return nil, err
	}

	var primaries []*clustermetadatapb.MultiPooler
	for _, p := range poolers {
		if p.Type == clustermetadatapb.PoolerType_PRIMARY {
			primaries = append(primaries, p)
		}
	}

	if len(primaries) == 0 {
		logError("No primary found!")
		return nil, errors.New("no primary found")
	}

	if len(primaries) > 1 {
		logWarn("Multiple primaries found! This indicates a split-brain situation.")
	}

	// Check each primary to find a healthy one
	for _, primary := range primaries {
		cell := primary.Id.Cell
		serviceID := primary.Id.Name

		status, err := getPoolerStatus(ctx, client, cell, serviceID)
		if err != nil {
			continue
		}

		postgresRunning := status.Status.PostgresRunning
		isReady := status.Status.PrimaryStatus != nil && status.Status.PrimaryStatus.Ready

		if postgresRunning && isReady {
			poolerInfo := getPoolerInfo(cell, serviceID, config)
			logSuccess(fmt.Sprintf("Found primary: %s/%s", cell, serviceID))
			logInfo(fmt.Sprintf("  - PostgreSQL running: %v", postgresRunning))
			logInfo(fmt.Sprintf("  - Ready: %v", isReady))
			return poolerInfo, nil
		}
	}

	logError("No healthy primary found!")
	return nil, errors.New("no healthy primary found")
}

func getPoolerInfo(cell, serviceID string, config *Config) *PoolerInfo {
	cellConfig := config.Cells[cell]
	return &PoolerInfo{
		Cell:      cell,
		ServiceID: serviceID,
		PoolerDir: cellConfig.Multipooler.PoolerDir,
		PgPort:    cellConfig.Pgctld.PgPort,
	}
}

func stopPooler(poolerInfo *PoolerInfo, config *Config) error {
	logInfo(fmt.Sprintf("Stopping pooler: %s/%s", poolerInfo.Cell, poolerInfo.ServiceID))

	pgctldBin := filepath.Join(config.RepoRoot, "bin", "pgctld")
	cmd := exec.Command(pgctldBin, "stop", "--pooler-dir", poolerInfo.PoolerDir)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop pooler: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05.000") + " UTC"
	logSuccess("Pooler stopped at " + timestamp)
	return nil
}

func waitForNewPrimary(ctx context.Context, config *Config, oldServiceID string, maxAttempts int) error {
	logInfo("Waiting for new primary to be elected...")

	client, err := newAdminClient(config.AdminServer)
	if err != nil {
		return err
	}
	defer client.Close()

	for attempt := range maxAttempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		poolers, err := getPoolers(ctx, client)
		if err != nil {
			fmt.Fprint(os.Stderr, ".")
			time.Sleep(checkInterval)
			continue
		}

		if debug && attempt%10 == 0 {
			var primaryCount int
			for _, p := range poolers {
				if p.Type == clustermetadatapb.PoolerType_PRIMARY {
					primaryCount++
				}
			}
			fmt.Fprintf(os.Stderr, "\n  [DEBUG] Found %d PRIMARY pooler(s)\n", primaryCount)
		}

		for _, primary := range poolers {
			if primary.Type != clustermetadatapb.PoolerType_PRIMARY {
				continue
			}

			cell := primary.Id.Cell
			serviceID := primary.Id.Name

			if serviceID == oldServiceID {
				if debug && attempt%10 == 0 {
					fmt.Fprintf(os.Stderr, "  [DEBUG] Skipping old primary: %s\n", serviceID)
				}
				continue
			}

			if debug && attempt%10 == 0 {
				fmt.Fprintf(os.Stderr, "  [DEBUG] Checking candidate: %s\n", serviceID)
			}

			status, err := getPoolerStatus(ctx, client, cell, serviceID)
			if err != nil {
				if debug && attempt%10 == 0 {
					fmt.Fprintf(os.Stderr, "  [DEBUG]   error getting status: %v\n", err)
				}
				continue
			}

			postgresRunning := status.Status.PostgresRunning
			isReady := status.Status.PrimaryStatus != nil && status.Status.PrimaryStatus.Ready

			if debug && attempt%10 == 0 {
				fmt.Fprintf(os.Stderr, "  [DEBUG]   postgres_running=%v, ready=%v\n", postgresRunning, isReady)
			}

			if postgresRunning && isReady {
				fmt.Fprintln(os.Stderr)
				logSuccess(fmt.Sprintf("New primary elected: %s/%s", cell, serviceID))
				return nil
			}
		}

		fmt.Fprint(os.Stderr, ".")
		time.Sleep(checkInterval)
	}

	fmt.Fprintln(os.Stderr)
	logError("Timeout waiting for new primary")
	return errors.New("timeout waiting for new primary")
}

func waitForReplicaHealth(ctx context.Context, config *Config, cell, serviceID string, maxAttempts int) error {
	logInfo(fmt.Sprintf("Waiting for %s/%s to become a healthy replica...", cell, serviceID))

	client, err := newAdminClient(config.AdminServer)
	if err != nil {
		return err
	}
	defer client.Close()

	var lastLSN string

	for range maxAttempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		status, err := getPoolerStatus(ctx, client, cell, serviceID)
		if err != nil {
			fmt.Fprint(os.Stderr, ".")
			time.Sleep(checkInterval)
			continue
		}

		poolerType := status.Status.PoolerType
		postgresRunning := status.Status.PostgresRunning
		replStatus := status.Status.ReplicationStatus

		if poolerType == clustermetadatapb.PoolerType_REPLICA && postgresRunning && replStatus != nil {
			lastReceiveLSN := replStatus.LastReceiveLsn
			lastReplayLSN := replStatus.LastReplayLsn
			isPaused := replStatus.IsWalReplayPaused

			if lastReceiveLSN != "" && lastReplayLSN != "" && !isPaused {
				// Verify this replica is connected to the primary
				poolers, err := getPoolers(ctx, client)
				if err != nil {
					fmt.Fprint(os.Stderr, ".")
					time.Sleep(checkInterval)
					continue
				}

				// Find the healthy primary
				for _, primary := range poolers {
					if primary.Type != clustermetadatapb.PoolerType_PRIMARY {
						continue
					}

					primaryCell := primary.Id.Cell
					primaryServiceID := primary.Id.Name

					primaryStatus, err := getPoolerStatus(ctx, client, primaryCell, primaryServiceID)
					if err != nil {
						continue
					}

					if !primaryStatus.Status.PostgresRunning {
						continue
					}

					if primaryStatus.Status.PrimaryStatus == nil || !primaryStatus.Status.PrimaryStatus.Ready {
						continue
					}

					// Check if this replica is in the primary's connected followers
					isConnected := false
					for _, follower := range primaryStatus.Status.PrimaryStatus.ConnectedFollowers {
						if follower.Cell == cell && follower.Name == serviceID {
							isConnected = true
							break
						}
					}

					if isConnected {
						// Verify LSN is advancing
						if lastLSN != "" && lastReplayLSN == lastLSN {
							// LSN hasn't advanced, keep waiting
							lastLSN = lastReplayLSN
						} else {
							fmt.Fprintln(os.Stderr)
							logSuccess(fmt.Sprintf("Replica %s/%s is healthy and replicating", cell, serviceID))
							logInfo(fmt.Sprintf("  - Connected to primary: %s/%s", primaryCell, primaryServiceID))
							logInfo("  - Last receive LSN: " + lastReceiveLSN)
							logInfo("  - Last replay LSN: " + lastReplayLSN)
							return nil
						}
					}
				}
			}
		}

		fmt.Fprint(os.Stderr, ".")
		time.Sleep(checkInterval)
	}

	fmt.Fprintln(os.Stderr)
	logError("Timeout waiting for replica to become healthy")
	return errors.New("timeout waiting for replica to become healthy")
}

func printReplicationStatus(ctx context.Context, config *Config) {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "============================================================")
	logInfo("Replication Status")
	fmt.Fprintln(os.Stderr, "============================================================")

	client, err := newAdminClient(config.AdminServer)
	if err != nil {
		logError(fmt.Sprintf("Failed to get replication status: %v", err))
		return
	}
	defer client.Close()

	poolers, err := getPoolers(ctx, client)
	if err != nil {
		logError(fmt.Sprintf("Failed to get replication status: %v", err))
		return
	}

	// Find the healthy primary
	var primaryCell, primaryServiceID string
	for _, pooler := range poolers {
		if pooler.Type != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}

		cell := pooler.Id.Cell
		serviceID := pooler.Id.Name

		status, err := getPoolerStatus(ctx, client, cell, serviceID)
		if err != nil {
			continue
		}

		if status.Status.PostgresRunning && status.Status.PrimaryStatus != nil && status.Status.PrimaryStatus.Ready {
			primaryCell = cell
			primaryServiceID = serviceID
			break
		}
	}

	// Print primary info
	if primaryServiceID != "" {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "%sPRIMARY: %s/%s%s\n", colorGreen, primaryCell, primaryServiceID, colorReset)

		poolerInfo := getPoolerInfo(primaryCell, primaryServiceID, config)
		if result := runSQLQuery(poolerInfo, "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"); result != "" {
			parts := strings.Split(result, "|")
			if len(parts) == 2 {
				fmt.Fprintf(os.Stderr, "  Timeline ID: %s\n", parts[0])
				fmt.Fprintf(os.Stderr, "  Checkpoint Redo LSN: %s\n", parts[1])
			} else {
				fmt.Fprintf(os.Stderr, "  Checkpoint: %s[unexpected format]%s\n", colorYellow, colorReset)
			}
		} else {
			fmt.Fprintf(os.Stderr, "  Checkpoint: %s[query failed]%s\n", colorRed, colorReset)
		}
	} else {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "%sNo healthy primary found!%s\n", colorRed, colorReset)
	}

	// Print replica info
	for _, pooler := range poolers {
		cell := pooler.Id.Cell
		serviceID := pooler.Id.Name

		// Skip the primary
		if cell == primaryCell && serviceID == primaryServiceID {
			continue
		}

		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "%sREPLICA: %s/%s%s\n", colorBlue, cell, serviceID, colorReset)

		status, err := getPoolerStatus(ctx, client, cell, serviceID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %sCannot get status%s\n", colorRed, colorReset)
			continue
		}

		if !status.Status.PostgresRunning {
			fmt.Fprintf(os.Stderr, "  %sPostgreSQL not running%s\n", colorRed, colorReset)
			continue
		}

		poolerInfo := getPoolerInfo(cell, serviceID, config)

		// Get replication receiver status
		if result := runSQLQuery(poolerInfo, "SELECT status, received_tli FROM pg_stat_wal_receiver;"); result != "" {
			parts := strings.Split(result, "|")
			if len(parts) == 2 {
				fmt.Fprintf(os.Stderr, "  Receiver Status: %s\n", parts[0])
				fmt.Fprintf(os.Stderr, "  Received Timeline: %s\n", parts[1])
			} else {
				fmt.Fprintf(os.Stderr, "  Receiver: %s[no active receiver]%s\n", colorYellow, colorReset)
			}
		} else {
			fmt.Fprintf(os.Stderr, "  Receiver: %s[query failed]%s\n", colorRed, colorReset)
		}

		// Get checkpoint info
		if result := runSQLQuery(poolerInfo, "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"); result != "" {
			parts := strings.Split(result, "|")
			if len(parts) == 2 {
				fmt.Fprintf(os.Stderr, "  Checkpoint Timeline: %s\n", parts[0])
				fmt.Fprintf(os.Stderr, "  Checkpoint Redo LSN: %s\n", parts[1])
			} else {
				fmt.Fprintf(os.Stderr, "  Checkpoint: %s[unexpected format]%s\n", colorYellow, colorReset)
			}
		} else {
			fmt.Fprintf(os.Stderr, "  Checkpoint: %s[query failed]%s\n", colorRed, colorReset)
		}
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "============================================================")
}

func runSQLQuery(poolerInfo *PoolerInfo, query string) string {
	socketPath := filepath.Join(poolerInfo.PoolerDir, "pg_sockets")
	connStr := fmt.Sprintf("host=%s port=%d user=postgres database=postgres sslmode=disable", socketPath, poolerInfo.PgPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return ""
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:gocritic // short-lived query timeout
	defer cancel()

	var result string
	err = db.QueryRowContext(ctx, query).Scan(&result)
	if err != nil {
		// For queries that return multiple columns, try scanning them
		var col1, col2 string
		err = db.QueryRowContext(ctx, query).Scan(&col1, &col2)
		if err != nil {
			return ""
		}
		return col1 + "|" + col2
	}
	return result
}

func failoverLoop(ctx context.Context, config *Config) error {
	iteration := 1

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr)
			logInfo("Interrupted by user")
			return nil
		default:
		}

		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "======================================")
		logInfo(fmt.Sprintf("Failover Test - Iteration %d", iteration))
		fmt.Fprintln(os.Stderr, "======================================")
		fmt.Fprintln(os.Stderr)

		// Find current primary
		primaryInfo, err := findPrimary(ctx, config)
		if err != nil {
			logError("Could not find primary. Exiting.")
			return err
		}

		fmt.Fprintln(os.Stderr)
		logWarn(fmt.Sprintf("About to kill primary: %s/%s", primaryInfo.Cell, primaryInfo.ServiceID))
		fmt.Fprintln(os.Stderr)

		// Ask user for confirmation (unless --yes flag is used)
		if !autoYes {
			fmt.Fprint(os.Stderr, "Kill this primary? (y/n): ")
			var response string
			if _, err := fmt.Scanln(&response); err != nil {
				logInfo("Skipping this iteration")
				continue
			}
			response = strings.TrimSpace(strings.ToLower(response))
			if response != "y" {
				logInfo("Skipping this iteration")
				continue
			}
		} else {
			logInfo("Auto-yes enabled, proceeding automatically...")
		}

		// Stop the primary
		if err := stopPooler(primaryInfo, config); err != nil {
			logError(fmt.Sprintf("Failed to stop pooler: %v", err))
			return err
		}

		// Wait for new primary
		if err := waitForNewPrimary(ctx, config, primaryInfo.ServiceID, 60); err != nil {
			logError("Failed to detect new primary. Manual intervention required.")
			return err
		}

		// Let the system restart postgres organically
		logInfo("Waiting for system to restart postgres organically...")
		fmt.Fprintln(os.Stderr)

		// Wait for the old primary to become a healthy replica
		if err := waitForReplicaHealth(ctx, config, primaryInfo.Cell, primaryInfo.ServiceID, 60); err != nil {
			logError("Replica did not become healthy within 60 seconds!")
			logError("Printing final replication status for diagnostics...")
			printReplicationStatus(ctx, config)
			return err
		}

		logSuccess(fmt.Sprintf("Failover iteration %d complete!", iteration))

		// Print detailed replication status
		printReplicationStatus(ctx, config)

		// Re-disable monitoring for the next iteration
		if err := disablePostgresMonitoring(ctx, config); err != nil {
			logError(fmt.Sprintf("Failed to re-disable postgres monitoring: %v", err))
			return err
		}

		iteration++
		time.Sleep(2 * time.Second)
	}
}

func logInfo(msg string) {
	fmt.Fprintf(os.Stderr, "%s[INFO]%s %s\n", colorBlue, colorReset, msg)
}

func logSuccess(msg string) {
	fmt.Fprintf(os.Stderr, "%s[SUCCESS]%s %s\n", colorGreen, colorReset, msg)
}

func logWarn(msg string) {
	fmt.Fprintf(os.Stderr, "%s[WARN]%s %s\n", colorYellow, colorReset, msg)
}

func logError(msg string) {
	fmt.Fprintf(os.Stderr, "%s[ERROR]%s %s\n", colorRed, colorReset, msg)
}
