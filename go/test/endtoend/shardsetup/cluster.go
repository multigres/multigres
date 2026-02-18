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

package shardsetup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/utils"
)

// MultipoolerInstance represents a multipooler instance, which is a pair of pgctld + multipooler processes.
// In multigres, a multipooler always has both: pgctld manages PostgreSQL, multipooler handles pooling.
type MultipoolerInstance struct {
	Name        string
	Pgctld      *ProcessInstance
	Multipooler *ProcessInstance
	PgBackRest  *PgBackRestInstance
}

// ShardSetup holds shared test infrastructure for a single shard.
// MultipoolerInstances are stored in a map by name for flexible access.
type ShardSetup struct {
	TempDir        string
	TempDirCleanup func()
	EtcdClientAddr string
	EtcdCmd        *exec.Cmd
	TopoServer     topoclient.Store
	CellName       string

	// MultipoolerInstances indexed by name (e.g., "pooler-1", "pooler-2", "pooler-3")
	Multipoolers map[string]*MultipoolerInstance

	// PrimaryName is the name of the node elected as primary after bootstrap.
	// Set by initializeWithMultiOrch. Use GetPrimary() to access.
	PrimaryName string

	// Multiorch instances (can have multiple)
	MultiOrchInstances map[string]*ProcessInstance

	// Multigateway instance (optional, enabled via WithMultigateway)
	Multigateway       *ProcessInstance
	MultigatewayPgPort int // PostgreSQL protocol port for multigateway

	// PgBackRestCertPaths stores the paths to pgBackRest TLS certificates
	PgBackRestCertPaths *local.PgBackRestCertPaths

	// BaselineGucs stores the GUC values captured after bootstrap completes.
	// These are the "clean state" values that ValidateCleanState checks against
	// and that cleanup restores to. Structure: node name → GUC name → value.
	// After bootstrap with replication configured, this includes:
	// - Primary: synchronous_standby_names, synchronous_commit
	// - Replicas: primary_conninfo
	BaselineGucs map[string]map[string]string
}

// GetMultipoolerInstance returns a multipooler instance by name, or nil if not found.
func (s *ShardSetup) GetMultipoolerInstance(name string) *MultipoolerInstance {
	if s == nil || s.Multipoolers == nil {
		return nil
	}
	return s.Multipoolers[name]
}

// GetMultipooler returns the multipooler process for an instance by name.
// Convenience method for tests that need just the multipooler process.
func (s *ShardSetup) GetMultipooler(name string) *ProcessInstance {
	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		return nil
	}
	return inst.Multipooler
}

// GetPgctld returns the pgctld process for an instance by name.
// Convenience method for tests that need just the pgctld process.
func (s *ShardSetup) GetPgctld(name string) *ProcessInstance {
	inst := s.GetMultipoolerInstance(name)
	if inst == nil {
		return nil
	}
	return inst.Pgctld
}

// GetMultiOrch returns a multiorch instance by name, or nil if not found.
func (s *ShardSetup) GetMultiOrch(name string) *ProcessInstance {
	if s == nil || s.MultiOrchInstances == nil {
		return nil
	}
	return s.MultiOrchInstances[name]
}

// GetPrimary returns the multipooler instance that was elected as primary.
// Fails the test if no primary has been set (e.g., before bootstrap).
func (s *ShardSetup) GetPrimary(t *testing.T) *MultipoolerInstance {
	t.Helper()
	if s == nil || s.PrimaryName == "" {
		t.Fatal("GetPrimary: no primary has been elected yet")
	}
	return s.GetMultipoolerInstance(s.PrimaryName)
}

// RefreshPrimary queries all multipoolers to find the current primary and updates PrimaryName.
func (s *ShardSetup) RefreshPrimary(t *testing.T) *MultipoolerInstance {
	t.Helper()

	for name, inst := range s.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		cancel()
		client.Close()

		if err != nil {
			continue
		}

		if resp.Status.IsInitialized && resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
			s.PrimaryName = name
			t.Logf("RefreshPrimary: current primary is %s", name)
			return inst
		}
	}

	t.Fatal("RefreshPrimary: no primary found in cluster")
	return nil
}

// GetStandbys returns all multipooler instances that are not the primary.
func (s *ShardSetup) GetStandbys() []*MultipoolerInstance {
	var standbys []*MultipoolerInstance
	for name, inst := range s.Multipoolers {
		if name != s.PrimaryName {
			standbys = append(standbys, inst)
		}
	}
	return standbys
}

// PrimaryMultipooler returns the multipooler process for the elected primary.
func (s *ShardSetup) PrimaryMultipooler(t *testing.T) *ProcessInstance {
	t.Helper()
	return s.GetPrimary(t).Multipooler
}

// PrimaryPgctld returns the pgctld process for the elected primary.
func (s *ShardSetup) PrimaryPgctld(t *testing.T) *ProcessInstance {
	t.Helper()
	return s.GetPrimary(t).Pgctld
}

// CreateMultipoolerInstance creates a new multipooler instance (pgctld + multipooler pair) with the given name.
// The instance is added to the setup's Multipoolers map.
// Follows the patterns from multipooler/setup_test.go.
func (s *ShardSetup) CreateMultipoolerInstance(t *testing.T, name string, grpcPort, pgPort, multipoolerPort int) *MultipoolerInstance {
	t.Helper()

	if s.Multipoolers == nil {
		s.Multipoolers = make(map[string]*MultipoolerInstance)
	}

	// Generate pgBackRest certificates once for the entire setup (shared across all multipoolers)
	if s.PgBackRestCertPaths == nil {
		s.PgBackRestCertPaths = s.generatePgBackRestCerts(t)
	}

	// Allocate a port for pgBackRest server (one per multipooler)
	pgbackrestPort := utils.GetFreePort(t)

	// Create pgctld instance
	pgctld := CreatePgctldInstance(t, name, s.TempDir, grpcPort, pgPort)

	// Create multipooler instance with pgBackRest cert paths and port
	// The name (e.g., "primary") is used as the service-id, combined with cell in the topology
	multipooler := CreateMultipoolerProcessInstance(t, name, s.TempDir, multipoolerPort,
		"localhost:"+strconv.Itoa(grpcPort), pgctld.DataDir, pgPort, s.EtcdClientAddr, s.CellName,
		s.PgBackRestCertPaths, pgbackrestPort)

	inst := &MultipoolerInstance{
		Name:        name,
		Pgctld:      pgctld,
		Multipooler: multipooler,
	}

	s.Multipoolers[name] = inst
	return inst
}

// CreatePgctldInstance creates a new pgctld process instance configuration.
// Follows the pattern from multipooler/setup_test.go:createPgctldInstance.
func CreatePgctldInstance(t *testing.T, name, baseDir string, grpcPort, pgPort int) *ProcessInstance {
	t.Helper()

	dataDir := filepath.Join(baseDir, name, "data")
	logFile := filepath.Join(baseDir, name, "pgctld.log")

	// Create data directory
	err := os.MkdirAll(filepath.Dir(logFile), 0o755)
	require.NoError(t, err)

	return &ProcessInstance{
		Name:        name,
		DataDir:     dataDir,
		LogFile:     logFile,
		GrpcPort:    grpcPort,
		PgPort:      pgPort,
		Binary:      "pgctld",
		Environment: append(os.Environ(), "PGCONNECT_TIMEOUT=5", "LC_ALL=en_US.UTF-8", "PGPASSWORD="+TestPostgresPassword),
	}
}

// CreateMultipoolerProcessInstance creates a new multipooler process instance configuration.
// Follows the pattern from multipooler/setup_test.go:createMultipoolerInstance.
func CreateMultipoolerProcessInstance(t *testing.T, name, baseDir string, grpcPort int, pgctldAddr string, pgctldDataDir string, pgPort int, etcdAddr string, cell string, certPaths *local.PgBackRestCertPaths, pgbackrestPort int) *ProcessInstance {
	t.Helper()

	logFile := filepath.Join(baseDir, name, "multipooler.log")
	// Create log directory
	err := os.MkdirAll(filepath.Dir(logFile), 0o755)
	require.NoError(t, err)

	inst := &ProcessInstance{
		Name:        name,
		Cell:        cell,
		LogFile:     logFile,
		GrpcPort:    grpcPort,
		PgPort:      pgPort,
		PgctldAddr:  pgctldAddr,
		DataDir:     pgctldDataDir,
		EtcdAddr:    etcdAddr,
		Binary:      "multipooler",
		Environment: append(os.Environ(), "PGCONNECT_TIMEOUT=5"),
	}

	// Store pgBackRest cert paths struct and port for later use when starting multipooler
	inst.PgBackRestCertPaths = certPaths
	inst.PgBackRestPort = pgbackrestPort

	return inst
}

// CreateMultiOrchInstance creates a new multiorch instance and adds it to the setup.
// Returns the instance and a cleanup function that should be deferred or called manually.
// The cleanup function gracefully terminates the process if it's still running.
func (s *ShardSetup) CreateMultiOrchInstance(t *testing.T, name string, watchTargets []string, config *SetupConfig) (*ProcessInstance, func()) {
	t.Helper()

	if s.MultiOrchInstances == nil {
		s.MultiOrchInstances = make(map[string]*ProcessInstance)
	}

	orchDataDir := filepath.Join(s.TempDir, name)
	logFile := filepath.Join(orchDataDir, "multiorch.log")

	// Create data directory
	err := os.MkdirAll(orchDataDir, 0o755)
	require.NoError(t, err)

	grpcPort := utils.GetFreePort(t)
	httpPort := utils.GetFreePort(t)

	instance := &ProcessInstance{
		Name:                                name,
		DataDir:                             orchDataDir,
		LogFile:                             logFile,
		GrpcPort:                            grpcPort,
		HttpPort:                            httpPort,
		Cell:                                config.CellName,
		EtcdAddr:                            s.EtcdClientAddr,
		WatchTargets:                        watchTargets,
		ServiceID:                           name, // Use the instance name as the service ID
		Binary:                              "multiorch",
		Environment:                         os.Environ(),
		PrimaryFailoverGracePeriodBase:      config.PrimaryFailoverGracePeriodBase,
		PrimaryFailoverGracePeriodMaxJitter: config.PrimaryFailoverGracePeriodMaxJitter,
	}

	// Apply defaults if not specified (0s for fast tests)
	if instance.PrimaryFailoverGracePeriodBase == "" {
		instance.PrimaryFailoverGracePeriodBase = "0s"
	}
	if instance.PrimaryFailoverGracePeriodMaxJitter == "" {
		instance.PrimaryFailoverGracePeriodMaxJitter = "0s"
	}

	s.MultiOrchInstances[name] = instance

	return instance, instance.CleanupFunc(t)
}

// CreateMultigatewayInstance creates a multigateway process instance.
// Returns the created ProcessInstance. Does not start the process.
// Call Start() on the returned instance to start it, and waitForMultigatewayQueryServing() after bootstrap.
func (s *ShardSetup) CreateMultigatewayInstance(t *testing.T, name string, pgPort, httpPort, grpcPort int) *ProcessInstance {
	t.Helper()

	inst := &ProcessInstance{
		Name:        name,
		Binary:      "multigateway",
		Cell:        s.CellName,
		ServiceID:   fmt.Sprintf("%s-%s", name, s.CellName),
		PgPort:      pgPort,
		HttpPort:    httpPort,
		GrpcPort:    grpcPort,
		EtcdAddr:    s.EtcdClientAddr,
		GlobalRoot:  "/multigres/global",
		LogFile:     filepath.Join(s.TempDir, name+".log"),
		Environment: os.Environ(),
	}

	s.Multigateway = inst
	s.MultigatewayPgPort = pgPort

	return inst
}

// WaitForMultigatewayQueryServing waits for multigateway to be able to execute queries.
// This verifies that multigateway has discovered poolers from topology and can route queries.
// Should be called AFTER bootstrap completes.
//
// The timeout is generous (30s) because after bootstrap, the multigateway needs time to:
// 1. Receive the topology watch notification that pooler-1 was promoted to PRIMARY
// 2. Update its LoadBalancer with the new PRIMARY pooler
// This typically takes a few seconds but can be longer under load or slow CI environments.
func (s *ShardSetup) WaitForMultigatewayQueryServing(t *testing.T) {
	t.Helper()

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=2",
		s.MultigatewayPgPort, TestPostgresPassword)

	ctx := utils.WithTimeout(t, 60*time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(startTime)
			t.Fatalf("timeout waiting for multigateway to execute queries after %v (multigateway may not have discovered poolers from topology yet)", elapsed)
		case <-ticker.C:
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				continue
			}

			var result int
			queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = db.QueryRowContext(queryCtx, "SELECT 1").Scan(&result)
			cancel()
			db.Close()

			if err == nil && result == 1 {
				elapsed := time.Since(startTime)
				t.Logf("Multigateway can execute queries (ready after %v)", elapsed)
				return
			}
		}
	}
}

// Cleanup cleans up the shared test infrastructure.
// If testsFailed is true, preserves the temp directory with logs for debugging.
// Follows the pattern from multipooler/setup_test.go:cleanupSharedTestSetup.
func (s *ShardSetup) Cleanup(testsFailed bool) {
	if s == nil {
		return
	}

	// Stop multigateway first (before multipoolers it routes to)
	if s.Multigateway != nil {
		s.Multigateway.Stop()
	}

	// Stop multiorch instances (they orchestrate the shard)
	for _, mo := range s.MultiOrchInstances {
		if mo != nil {
			mo.Stop()
		}
	}

	// Stop multipooler instances (pgbackrest, multipooler, then pgctld)
	for _, inst := range s.Multipoolers {
		if inst.PgBackRest != nil {
			inst.PgBackRest.Stop()
		}
		if inst.Multipooler != nil {
			inst.Multipooler.Stop()
		}
		if inst.Pgctld != nil {
			inst.Pgctld.Stop()
		}
	}

	// Close topology server
	if s.TopoServer != nil {
		s.TopoServer.Close()
	}

	// Stop etcd
	if s.EtcdCmd != nil && s.EtcdCmd.Process != nil {
		_ = s.EtcdCmd.Process.Kill()
		_ = s.EtcdCmd.Wait()
	}

	// Clean up temp directory only if tests passed
	if s.TempDirCleanup != nil && !testsFailed {
		s.TempDirCleanup()
	}
}

// PrintLogLocation prints the temp directory location for debugging.
// If TEST_PRINT_LOGS env var is set, also prints all log contents from the temp directory.
func PrintLogLocation(tempDir string) {
	println("\n" + "=" + "=== TEST LOGS PRESERVED ===" + "=")
	println("Logs available at: " + tempDir)

	// Only print log contents if TEST_PRINT_LOGS is set
	if os.Getenv("TEST_PRINT_LOGS") == "" {
		println("Set TEST_PRINT_LOGS=1 to print log contents")
		println("=" + "=========================" + "=")
		return
	}

	// Print all .log files found in the temp directory
	println("\n" + "=" + "=== SERVICE LOGS (test failure) ===" + "=")
	err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		println("\n--- " + path + " ---")
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			println("  [error reading log: " + readErr.Error() + "]")
			return nil //nolint:nilerr // Continue walking even if one file fails
		}
		if len(content) == 0 {
			println("  [empty log file]")
			return nil
		}
		println(string(content))
		return nil
	})
	if err != nil {
		println("  [error walking log directory: " + err.Error() + "]")
	}

	println("\n" + "=" + "=== END SERVICE LOGS ===" + "=")
}

// DumpServiceLogs prints the location of service log files to help debug test failures.
// Call this before cleanup so logs are available.
// Always prints the temp directory location. If TEST_PRINT_LOGS env var is set, also prints log contents.
// Follows the pattern from multipooler/setup_test.go:dumpServiceLogs.
func (s *ShardSetup) DumpServiceLogs() {
	if s == nil {
		return
	}

	// Use the shared utility function which prints location and optionally all logs
	PrintLogLocation(s.TempDir)
}

// CheckSharedProcesses verifies all shared test processes are still running.
// This catches crashes from previous tests early, before confusing timeout errors.
// Follows the pattern from multipooler/setup_test.go:checkSharedProcesses.
func (s *ShardSetup) CheckSharedProcesses(t *testing.T) {
	t.Helper()

	if s == nil {
		return
	}

	var dead []string

	// Check multigateway
	if s.Multigateway != nil && !s.Multigateway.IsRunning() {
		dead = append(dead, "multigateway")
	}

	// Check multipooler instances
	for name, inst := range s.Multipoolers {
		if inst.Pgctld != nil && !inst.Pgctld.IsRunning() {
			dead = append(dead, name+"-pgctld")
		}
		if inst.Multipooler != nil && !inst.Multipooler.IsRunning() {
			dead = append(dead, name+"-multipooler")
		}
		if inst.PgBackRest != nil && !inst.PgBackRest.IsRunning() {
			dead = append(dead, name+"-pgbackrest")
		}
	}

	// TODO (@rafa): We can check multiorch processes once
	// we are able to disable them on a shard basis.

	if len(dead) > 0 {
		t.Fatalf("Shared test process(es) died: %v. A previous test likely crashed them. Check service logs above.", dead)
	}
}
