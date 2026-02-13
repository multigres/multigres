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

// failover-test is a tool for testing Multigres failover in Kubernetes clusters.
//
// Usage:
//
//	go run failover-test.go                # Interactive mode
//	go run failover-test.go --yes          # Automatic mode
//	go run failover-test.go --yes --debug  # With debug logging
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

const (
	checkInterval = 1 * time.Second
)

// Kubernetes configuration
const (
	pgctldBin = "/usr/local/bin/pgctld"
	poolerDir = "/data"
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
	PodName   string
}

// Config holds environment configuration
type Config struct {
	MultiadminURL       string
	MultiadminGRPC      string
	KubectlContext      string
	KubernetesNamespace string
}

// PoolerStatus represents the status returned from the HTTP API
type PoolerStatus struct {
	Status struct {
		PoolerType      string `json:"pooler_type"`
		PostgresRunning bool   `json:"postgres_running"`
		PrimaryStatus   *struct {
			Ready              bool `json:"ready"`
			ConnectedFollowers []struct {
				Cell string `json:"cell"`
				Name string `json:"name"`
			} `json:"connected_followers"`
		} `json:"primary_status"`
		ReplicationStatus *struct {
			LastReceiveLSN    string `json:"last_receive_lsn"`
			LastReplayLSN     string `json:"last_replay_lsn"`
			IsWALReplayPaused bool   `json:"is_wal_replay_paused"`
		} `json:"replication_status"`
	} `json:"status"`
}

// PoolersResponse represents the poolers list from HTTP API
type PoolersResponse struct {
	Poolers []struct {
		ID struct {
			Cell string `json:"cell"`
			Name string `json:"name"`
		} `json:"id"`
		Type string `json:"type"`
	} `json:"poolers"`
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}

var rootCmd = &cobra.Command{
	Use:   "failover-test",
	Short: "Multigres failover test script for Kubernetes clusters",
	Long: `Continuously test failover by stopping the primary pooler and waiting
for a new primary to be elected and the old primary to become a replica.

Environment variables:
  MULTIADMIN_URL          MultiAdmin API URL (default: http://localhost:18000)
  MULTIADMIN_GRPC         MultiAdmin gRPC address (default: localhost:18070)
  KUBECTL_CONTEXT         kubectl context to use (default: kind-multidemo)
  KUBERNETES_NAMESPACE    Kubernetes namespace (default: default)

Examples:
  # Run interactive mode
  go run failover-test.go

  # Run automatic mode
  go run failover-test.go --yes

  # Use custom settings
  MULTIADMIN_URL=http://localhost:8000 go run failover-test.go --yes`,
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

	config := loadConfig()

	logInfo("Multigres Failover Test Script (Kubernetes)")
	logInfo("MultiAdmin API: " + config.MultiadminURL)
	logInfo("Kubectl context: " + config.KubectlContext)
	logInfo("Namespace: " + config.KubernetesNamespace)

	if autoYes {
		logWarn("Auto-yes mode enabled - will continuously kill primaries without confirmation")
	}
	if debug {
		logInfo("Debug mode enabled - will show detailed diagnostic information")
	}
	fmt.Fprintln(os.Stderr)

	// Verify prerequisites
	if err := verifyPrerequisites(config); err != nil {
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

func loadConfig() *Config {
	return &Config{
		MultiadminURL:       getEnvOrDefault("MULTIADMIN_URL", "http://localhost:18000"),
		MultiadminGRPC:      getEnvOrDefault("MULTIADMIN_GRPC", "localhost:18070"),
		KubectlContext:      getEnvOrDefault("KUBECTL_CONTEXT", "kind-multidemo"),
		KubernetesNamespace: getEnvOrDefault("KUBERNETES_NAMESPACE", "default"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func verifyPrerequisites(config *Config) error {
	// Check kubectl connectivity
	cmd := exec.Command("kubectl", "--context", config.KubectlContext, "cluster-info")
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		logError("kubectl cannot connect to cluster")
		logError(fmt.Sprintf("Please verify the cluster context '%s' exists", config.KubectlContext))
		return err
	}

	// Test API connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:gocritic // short-lived connectivity check
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", config.MultiadminURL+"/api/v1/poolers", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logError("Cannot connect to MultiAdmin API at " + config.MultiadminURL)
		logError("Make sure port-forwarding is set up:")
		logError("  kubectl --context " + config.KubectlContext + " port-forward service/multiadmin 18000:18000")
		return err
	}
	resp.Body.Close()

	return nil
}

func disablePostgresMonitoring(ctx context.Context, config *Config) error {
	logInfo("Disabling PostgreSQL monitoring on all poolers...")

	poolers, err := getPoolers(config)
	if err != nil {
		return err
	}

	if len(poolers.Poolers) == 0 {
		logWarn("No poolers found")
		return nil
	}

	client, err := newAdminClient(config.MultiadminGRPC)
	if err != nil {
		return err
	}
	defer client.Close()

	for _, pooler := range poolers.Poolers {
		cell := pooler.ID.Cell
		serviceID := pooler.ID.Name
		poolerName := fmt.Sprintf("multipooler-%s-%s", cell, serviceID)

		logInfo(fmt.Sprintf("  Disabling monitoring on: %s (cell=%s, service_id=%s)", poolerName, cell, serviceID))

		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.SetPostgresMonitor(reqCtx, &multiadminpb.SetPostgresMonitorRequest{
			PoolerId: &clustermetadatapb.ID{
				Cell: cell,
				Name: serviceID,
			},
			Enabled: false,
		})
		cancel()

		if err != nil {
			logError(fmt.Sprintf("    ✗ Failed to disable monitoring on %s: %v", poolerName, err))
			return err
		}
		logSuccess("    ✓ Disabled monitoring on " + poolerName)
	}

	logSuccess(fmt.Sprintf("Disabled monitoring on %d pooler(s)", len(poolers.Poolers)))
	fmt.Fprintln(os.Stderr)
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

func getPoolers(config *Config) (*PoolersResponse, error) {
	resp, err := http.Get(config.MultiadminURL + "/api/v1/poolers")
	if err != nil {
		return nil, fmt.Errorf("failed to get poolers: %w", err)
	}
	defer resp.Body.Close()

	var poolers PoolersResponse
	if err := json.NewDecoder(resp.Body).Decode(&poolers); err != nil {
		return nil, fmt.Errorf("failed to decode poolers response: %w", err)
	}

	return &poolers, nil
}

func getPoolerStatus(config *Config, cell, serviceID string) (*PoolerStatus, error) {
	url := fmt.Sprintf("%s/api/v1/poolers/%s/%s/status", config.MultiadminURL, cell, serviceID)
	resp, err := http.Get(url) //nolint:gosec // URL constructed from trusted config and validated inputs
	if err != nil {
		return nil, fmt.Errorf("failed to get pooler status: %w", err)
	}
	defer resp.Body.Close()

	var status PoolerStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode status response: %w", err)
	}

	return &status, nil
}

func findPrimary(config *Config) (*PoolerInfo, error) {
	logInfo("Searching for current primary...")

	poolers, err := getPoolers(config)
	if err != nil {
		return nil, err
	}

	var primaries []struct {
		ID struct {
			Cell string `json:"cell"`
			Name string `json:"name"`
		} `json:"id"`
		Type string `json:"type"`
	}

	for _, p := range poolers.Poolers {
		if p.Type == "PRIMARY" {
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
		cell := primary.ID.Cell
		serviceID := primary.ID.Name

		status, err := getPoolerStatus(config, cell, serviceID)
		if err != nil {
			continue
		}

		postgresRunning := status.Status.PostgresRunning
		isReady := status.Status.PrimaryStatus != nil && status.Status.PrimaryStatus.Ready

		if postgresRunning && isReady {
			poolerInfo := getPoolerInfo(cell, serviceID)
			logSuccess(fmt.Sprintf("Found primary: %s/%s (pod: %s)", cell, serviceID, poolerInfo.PodName))
			logInfo(fmt.Sprintf("  - PostgreSQL running: %v", postgresRunning))
			logInfo(fmt.Sprintf("  - Ready: %v", isReady))
			return poolerInfo, nil
		}
	}

	logError("No healthy primary found!")
	return nil, errors.New("no healthy primary found")
}

func getPoolerInfo(cell, serviceID string) *PoolerInfo {
	return &PoolerInfo{
		Cell:      cell,
		ServiceID: serviceID,
		PodName:   fmt.Sprintf("multipooler-%s-%s", cell, serviceID),
	}
}

func stopPooler(poolerInfo *PoolerInfo, config *Config) error {
	logInfo("Stopping pooler: " + poolerInfo.PodName)

	cmd := exec.Command("kubectl",
		"--context", config.KubectlContext,
		"exec", "-n", config.KubernetesNamespace,
		"pod/"+poolerInfo.PodName,
		"-c", "pgctld", "--",
		pgctldBin, "stop",
		"--pooler-dir", poolerDir,
	)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop pooler: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05.000") + " UTC"
	logSuccess("Pooler stopped at " + timestamp)
	return nil
}

func waitForNewPrimary(config *Config, oldServiceID string, maxAttempts int) error {
	logInfo("Waiting for new primary to be elected...")

	for attempt := range maxAttempts {
		poolers, err := getPoolers(config)
		if err != nil {
			fmt.Fprint(os.Stderr, ".")
			time.Sleep(checkInterval)
			continue
		}

		if debug && attempt%10 == 0 {
			var primaryCount int
			for _, p := range poolers.Poolers {
				if p.Type == "PRIMARY" {
					primaryCount++
				}
			}
			fmt.Fprintf(os.Stderr, "\n  [DEBUG] Found %d PRIMARY pooler(s)\n", primaryCount)
		}

		for _, primary := range poolers.Poolers {
			if primary.Type != "PRIMARY" {
				continue
			}

			cell := primary.ID.Cell
			serviceID := primary.ID.Name

			if serviceID == oldServiceID {
				if debug && attempt%10 == 0 {
					fmt.Fprintf(os.Stderr, "  [DEBUG] Skipping old primary: %s\n", serviceID)
				}
				continue
			}

			if debug && attempt%10 == 0 {
				fmt.Fprintf(os.Stderr, "  [DEBUG] Checking candidate: %s\n", serviceID)
			}

			status, err := getPoolerStatus(config, cell, serviceID)
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

func waitForReplicaHealth(config *Config, cell, serviceID string, maxAttempts int) error {
	logInfo(fmt.Sprintf("Waiting for %s/%s to become a healthy replica...", cell, serviceID))

	var lastLSN string

	for range maxAttempts {
		status, err := getPoolerStatus(config, cell, serviceID)
		if err != nil {
			fmt.Fprint(os.Stderr, ".")
			time.Sleep(checkInterval)
			continue
		}

		poolerType := status.Status.PoolerType
		postgresRunning := status.Status.PostgresRunning
		replStatus := status.Status.ReplicationStatus

		if poolerType == "REPLICA" && postgresRunning && replStatus != nil {
			lastReceiveLSN := replStatus.LastReceiveLSN
			lastReplayLSN := replStatus.LastReplayLSN
			isPaused := replStatus.IsWALReplayPaused

			if lastReceiveLSN != "" && lastReplayLSN != "" && !isPaused {
				// Verify this replica is connected to the primary
				poolers, err := getPoolers(config)
				if err != nil {
					fmt.Fprint(os.Stderr, ".")
					time.Sleep(checkInterval)
					continue
				}

				// Find the healthy primary
				for _, primary := range poolers.Poolers {
					if primary.Type != "PRIMARY" {
						continue
					}

					primaryCell := primary.ID.Cell
					primaryServiceID := primary.ID.Name

					primaryStatus, err := getPoolerStatus(config, primaryCell, primaryServiceID)
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

func printReplicationStatus(config *Config) {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "============================================================")
	logInfo("Replication Status")
	fmt.Fprintln(os.Stderr, "============================================================")

	poolers, err := getPoolers(config)
	if err != nil {
		logError(fmt.Sprintf("Failed to get replication status: %v", err))
		return
	}

	// Find the healthy primary
	var primaryCell, primaryServiceID, primaryPodName string
	for _, pooler := range poolers.Poolers {
		if pooler.Type != "PRIMARY" {
			continue
		}

		cell := pooler.ID.Cell
		serviceID := pooler.ID.Name

		status, err := getPoolerStatus(config, cell, serviceID)
		if err != nil {
			continue
		}

		if status.Status.PostgresRunning && status.Status.PrimaryStatus != nil && status.Status.PrimaryStatus.Ready {
			primaryCell = cell
			primaryServiceID = serviceID
			primaryPodName = serviceID // Pod name is same as service ID
			break
		}
	}

	// Print primary info
	if primaryServiceID != "" {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "%sPRIMARY: %s/%s (pod: %s)%s\n", colorGreen, primaryCell, primaryServiceID, primaryPodName, colorReset)

		if result := runSQLQueryInPod(config, primaryPodName, "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"); result != "" {
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
	for _, pooler := range poolers.Poolers {
		cell := pooler.ID.Cell
		serviceID := pooler.ID.Name
		podName := serviceID

		// Skip the primary
		if cell == primaryCell && serviceID == primaryServiceID {
			continue
		}

		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "%sREPLICA: %s/%s (pod: %s)%s\n", colorBlue, cell, serviceID, podName, colorReset)

		status, err := getPoolerStatus(config, cell, serviceID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %sCannot get status%s\n", colorRed, colorReset)
			continue
		}

		if !status.Status.PostgresRunning {
			fmt.Fprintf(os.Stderr, "  %sPostgreSQL not running%s\n", colorRed, colorReset)
			continue
		}

		// Get replication receiver status
		if result := runSQLQueryInPod(config, podName, "SELECT status, received_tli FROM pg_stat_wal_receiver;"); result != "" {
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
		if result := runSQLQueryInPod(config, podName, "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"); result != "" {
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

func runSQLQueryInPod(config *Config, podName, query string) string {
	cmd := exec.Command("kubectl",
		"--context", config.KubectlContext,
		"exec", "-n", config.KubernetesNamespace,
		"pod/"+podName,
		"-c", "pgctld", "--",
		"psql", "-h", poolerDir+"/pg_sockets", "-p", "5432",
		"-U", "postgres", "-d", "postgres",
		"-t", "-A", "-c", query,
	)

	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(output))
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
		primaryInfo, err := findPrimary(config)
		if err != nil {
			logError("Could not find primary. Exiting.")
			return err
		}

		fmt.Fprintln(os.Stderr)
		logWarn(fmt.Sprintf("About to kill primary: %s/%s", primaryInfo.Cell, primaryInfo.ServiceID))
		logInfo("  Pod: " + primaryInfo.PodName)
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
		if err := waitForNewPrimary(config, primaryInfo.ServiceID, 60); err != nil {
			logError("Failed to detect new primary. Manual intervention required.")
			return err
		}

		// Let the system restart postgres organically
		logInfo("Waiting for system to restart postgres organically...")
		fmt.Fprintln(os.Stderr)

		// Wait for the old primary to become a healthy replica
		if err := waitForReplicaHealth(config, primaryInfo.Cell, primaryInfo.ServiceID, 60); err != nil {
			logError("Replica did not become healthy within 60 seconds!")
			logError("Printing final replication status for diagnostics...")
			printReplicationStatus(config)
			return err
		}

		logSuccess(fmt.Sprintf("Failover iteration %d complete!", iteration))

		// Print detailed replication status
		printReplicationStatus(config)

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
