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

// Package minigressetup starts a Minigres topology for end-to-end tests: etcd,
// the topology records, one pgctld and one minigres process (the multigateway
// and the multipooler in one process). It implements clustersetup.Cluster, so
// the query-serving tests run against it unchanged.
package minigressetup

import (
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/testconst"
	"github.com/multigres/multigres/go/test/utils"
)

const (
	cellName  = "test-cell"
	database  = "postgres"
	serviceID = "minigres"

	// durabilityPolicy is the bootstrap policy of the database record. A
	// static leader does not use it, and topology creation accepts no
	// single-pooler policy yet (MUL-1716).
	durabilityPolicy = "AT_LEAST_2"
)

// baselineGucNames are the settings saved after bootstrap and restored after
// each test, as in the Multigres topology.
var baselineGucNames = []string{
	"synchronous_standby_names",
	"synchronous_commit",
	"primary_conninfo",
}

type config struct {
	tls       bool
	extraArgs []string
	logLevel  string
}

// Option configures a Minigres setup.
type Option func(*config)

// WithTLS serves the client port over TLS with generated certificates.
func WithTLS() Option {
	return func(c *config) { c.tls = true }
}

// WithRequireSSL serves the client port over TLS and rejects plaintext clients.
func WithRequireSSL() Option {
	return func(c *config) {
		c.tls = true
		c.extraArgs = append(c.extraArgs, "--pg-require-ssl=true")
	}
}

// WithExtraArgs passes extra command-line arguments to the minigres process.
func WithExtraArgs(args ...string) Option {
	return func(c *config) { c.extraArgs = append(c.extraArgs, args...) }
}

// WithLogLevel sets the minigres log level.
func WithLogLevel(level string) Option {
	return func(c *config) { c.logLevel = level }
}

// Setup is a running Minigres topology.
type Setup struct {
	TempDir        string
	EtcdClientAddr string
	TopoServer     topoclient.Store
	CellName       string

	// Pgctld runs PostgreSQL; Minigres runs the gateway and the pooler.
	Pgctld   *clustersetup.ProcessInstance
	Minigres *clustersetup.ProcessInstance

	// TLSCertPaths is set when the client port serves TLS.
	TLSCertPaths *clustersetup.MultigatewayTLSCertPaths

	baselineGucs   map[string]string
	tempDirCleanup func()
	runningCtx     context.Context
	cancel         context.CancelFunc
}

var _ clustersetup.Cluster = (*Setup)(nil)

// New starts a Minigres topology and waits until it serves queries: the pooler
// has bootstrapped, promoted itself, and a write succeeds through the client
// port. Call Cleanup when done (SharedSetupManager does so from TestMain).
func New(t *testing.T, opts ...Option) *Setup {
	t.Helper()
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	for _, binary := range []string{"minigres", "pgctld"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("%s binary not found in PATH - ensure TestMain calls clustersetup.RunTestMain", binary)
		}
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Fatalf("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
	}

	tempDir, tempDirCleanup := testutil.TempDir(t, "minigres_test")
	// Processes live until Cleanup, not until the creating test ends.
	runningCtx, cancel := context.WithCancel(context.Background())
	s := &Setup{
		TempDir:        tempDir,
		CellName:       cellName,
		tempDirCleanup: tempDirCleanup,
		runningCtx:     runningCtx,
		cancel:         cancel,
	}

	t.Logf("Starting etcd for topology...")
	etcdDataDir := filepath.Join(tempDir, "etcd_data")
	require.NoError(t, os.MkdirAll(etcdDataDir, 0o755))
	etcdClientAddr, _, err := clustersetup.StartEtcd(runningCtx, t, etcdDataDir)
	if err != nil {
		cancel()
		t.Fatalf("failed to start etcd: %v", err)
	}
	s.EtcdClientAddr = etcdClientAddr
	s.TopoServer = clustersetup.CreateTopologyCell(t, etcdClientAddr, cellName)

	backupDir := filepath.Join(tempDir, "backup-repo")
	backupLocation := utils.FilesystemBackupLocation(backupDir)
	if err := clustersetup.CreateDatabaseRecord(s.TopoServer, database, backupLocation, durabilityPolicy); err != nil {
		cancel()
		t.Fatalf("%v", err)
	}
	t.Logf("Created database '%s' in topology with filesystem backup: path=%s", database, backupDir)

	if cfg.tls {
		s.TLSCertPaths = clustersetup.GenerateMultigatewayTLSCerts(t, filepath.Join(tempDir, "minigres-tls"))
	}

	// pgctld runs PostgreSQL. No pgBackRest TLS server: with one pooler the
	// first backup and restore are local (MUL-1714).
	s.Pgctld = clustersetup.CreatePgctldInstance(t, serviceID, tempDir,
		utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t), 0, "", backupLocation)
	s.Minigres = s.newMinigresInstance(t, cfg)

	if err := s.Pgctld.Start(runningCtx, t); err != nil {
		cancel()
		t.Fatalf("failed to start pgctld: %v", err)
	}
	if err := s.Minigres.Start(runningCtx, t); err != nil {
		cancel()
		t.Fatalf("failed to start minigres: %v", err)
	}

	s.waitForPrimary(t)
	s.saveBaselineGucs(t)
	s.WaitForQueryServing(t)
	return s
}

// NewIsolated starts a Minigres topology owned by one test. The returned
// function cleans it up, keeping the logs if the test failed.
func NewIsolated(t *testing.T, opts ...Option) (*Setup, func()) {
	t.Helper()
	s := New(t, opts...)
	return s, func() {
		if t.Failed() {
			s.DumpServiceLogs()
		}
		s.Cleanup(t.Failed())
	}
}

// newMinigresInstance describes the minigres process: the client port, the
// gRPC and HTTP ports, pgctld's address, and the same environment the harness
// gives a multipooler.
func (s *Setup) newMinigresInstance(t *testing.T, cfg *config) *clustersetup.ProcessInstance {
	t.Helper()
	logFile := filepath.Join(s.TempDir, serviceID, "minigres.log")
	require.NoError(t, os.MkdirAll(filepath.Dir(logFile), 0o755))

	inst := &clustersetup.ProcessInstance{
		Name:       serviceID,
		Binary:     "minigres",
		Cell:       cellName,
		ServiceID:  serviceID,
		LogFile:    logFile,
		PgPort:     utils.GetFreePort(t),
		GrpcPort:   utils.GetFreePort(t),
		HttpPort:   utils.GetFreePort(t),
		PgctldAddr: fmt.Sprintf("localhost:%d", s.Pgctld.GrpcPort),
		EtcdAddr:   s.EtcdClientAddr,
		GlobalRoot: clustersetup.GlobalTopologyRoot,
		ExtraArgs:  cfg.extraArgs,
		LogLevel:   cfg.logLevel,
		Environment: append(utils.BaseTestEnv(),
			"PGCONNECT_TIMEOUT=5",
			"POSTGRES_PASSWORD="+clustersetup.TestPostgresPassword,
			constants.PgDataDirEnvVar+"="+filepath.Join(s.Pgctld.PoolerDir, "pg_data")),
	}
	if s.TLSCertPaths != nil {
		inst.TLSCertFile = s.TLSCertPaths.ServerCertFile
		inst.TLSKeyFile = s.TLSCertPaths.ServerKeyFile
	}
	return inst
}

// waitForPrimary waits until the pooler has bootstrapped and promoted itself:
// the manager reports it initialized, PostgreSQL ready, and the pooler PRIMARY.
func (s *Setup) waitForPrimary(t *testing.T) {
	t.Helper()
	client, err := clustersetup.NewMultipoolerClient(s.Minigres.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	start := time.Now()
	deadline := start.Add(testconst.ShardBootstrapTimeout)
	var last string
	for {
		ctx, cancel := context.WithTimeout(s.runningCtx, 2*time.Second)
		resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		cancel()
		if err != nil {
			last = err.Error()
		} else {
			st := resp.GetStatus()
			if st.GetIsInitialized() && st.GetPostgresReady() && st.GetPoolerType() == clustermetadatapb.PoolerType_PRIMARY {
				break
			}
			last = fmt.Sprintf("initialized=%v postgres_ready=%v type=%s action=%s",
				st.GetIsInitialized(), st.GetPostgresReady(), st.GetPoolerType(), st.GetPostgresAction())
		}
		if time.Now().After(deadline) {
			t.Fatalf("minigres pooler did not become PRIMARY within %v; last status: %s", testconst.ShardBootstrapTimeout, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("Minigres pooler is PRIMARY (after %v)", time.Since(start).Round(time.Millisecond))
}

// saveBaselineGucs records the clean-state settings after bootstrap.
func (s *Setup) saveBaselineGucs(t *testing.T) {
	t.Helper()
	client, err := clustersetup.NewMultipoolerClient(s.Minigres.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(s.runningCtx, 10*time.Second)
	defer cancel()
	s.baselineGucs = clustersetup.SaveGUCs(ctx, client.Pooler, baselineGucNames)
}

// ClientPort returns the minigres client port.
func (s *Setup) ClientPort() int {
	return s.Minigres.PgPort
}

// ClientTLSCertPaths returns the client port's TLS certificates, or nil when it
// serves plaintext.
func (s *Setup) ClientTLSCertPaths() *clustersetup.MultigatewayTLSCertPaths {
	return s.TLSCertPaths
}

// ComparisonTargets returns PostgreSQL and the minigres client port. The proxy
// target keeps the name "multigateway", which tests use to tell the proxy from
// PostgreSQL; in Minigres it is the multigateway half of the process.
func (s *Setup) ComparisonTargets(t *testing.T) []clustersetup.TestTarget {
	t.Helper()
	return []clustersetup.TestTarget{
		{Name: "postgres", Port: s.Pgctld.PgPort},
		{Name: "multigateway", Port: s.Minigres.PgPort},
	}
}

// WaitForQueryServing waits until a write succeeds through the client port.
func (s *Setup) WaitForQueryServing(t *testing.T) {
	t.Helper()
	clustersetup.WaitForQueryServingOnPort(t, s.Minigres.PgPort, s.TLSCertPaths != nil)
}

// PostgresPort returns the PostgreSQL port.
func (s *Setup) PostgresPort(t *testing.T) int {
	t.Helper()
	return s.Pgctld.PgPort
}

// PostgresSocketDir returns the PostgreSQL Unix socket directory.
func (s *Setup) PostgresSocketDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(s.Pgctld.PoolerDir, "pg_sockets")
}

// GatewayLogFile returns the minigres log, which holds the gateway's log.
func (s *Setup) GatewayLogFile() string {
	return s.Minigres.LogFile
}

// PoolerLogFile returns the minigres log, which holds the pooler's log.
func (s *Setup) PoolerLogFile(t *testing.T) string {
	t.Helper()
	return s.Minigres.LogFile
}

// SetupTest checks the clean state before a test and restores it afterwards.
// Clean state: minigres and pgctld are running, PostgreSQL is out of recovery,
// the pooler is PRIMARY and SERVING, and the baseline settings match. Minigres
// has no replication, so WithoutReplication and WithPausedReplication fail the
// test.
func (s *Setup) SetupTest(t *testing.T, opts ...clustersetup.SetupTestOption) {
	t.Helper()
	cfg := &clustersetup.SetupTestConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.NoReplication || cfg.PauseReplication {
		t.Fatalf("SetupTest: replication options are not supported on Minigres (no replication)")
	}

	s.checkProcesses(t)
	if err := s.validateCleanState(); err != nil {
		t.Fatalf("SetupTest: %v. Previous test leaked state.", err)
	}

	client, err := clustersetup.NewMultipoolerClient(s.Minigres.GrpcPort)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	restore := maps.Clone(s.baselineGucs)
	maps.Copy(restore, clustersetup.SaveGUCs(ctx, client.Pooler, cfg.GucsToReset))
	cancel()
	client.Close()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		client, err := clustersetup.NewMultipoolerClient(s.Minigres.GrpcPort)
		if err != nil {
			t.Errorf("Cleanup: failed to connect to minigres: %v", err)
			return
		}
		clustersetup.RestoreGUCs(cleanupCtx, t, client.Pooler, restore, serviceID)
		client.Close()

		require.Eventually(t, func() bool {
			return s.validateCleanState() == nil
		}, 15*time.Second, 50*time.Millisecond, "Test cleanup failed: state did not return to clean state")
	})
}

// checkProcesses fails the test if minigres or pgctld died.
func (s *Setup) checkProcesses(t *testing.T) {
	t.Helper()
	var dead []string
	if !s.Minigres.IsRunningOrZombie() {
		dead = append(dead, "minigres")
	}
	if !s.Pgctld.IsRunningOrZombie() {
		dead = append(dead, "pgctld")
	}
	if len(dead) > 0 {
		t.Fatalf("Shared test process(es) died: %v. A previous test likely crashed them. Check service logs above.", dead)
	}
}

// validateCleanState checks that PostgreSQL is out of recovery, the pooler is
// PRIMARY and SERVING, and the baseline settings match.
func (s *Setup) validateCleanState() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := clustersetup.NewMultipoolerClient(s.Minigres.GrpcPort)
	if err != nil {
		return fmt.Errorf("failed to connect to minigres: %w", err)
	}
	defer client.Close()

	inRecovery, err := clustersetup.QueryStringValue(ctx, client.Pooler, "SELECT pg_is_in_recovery()")
	if err != nil {
		return fmt.Errorf("failed to query pg_is_in_recovery: %w", err)
	}
	if inRecovery != "f" {
		return fmt.Errorf("pg_is_in_recovery=%s (expected f)", inRecovery)
	}
	if err := clustersetup.ValidatePoolerType(ctx, client.Manager, clustermetadatapb.PoolerType_PRIMARY, serviceID); err != nil {
		return err
	}

	pooler, err := s.TopoServer.GetMultipooler(ctx, &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cellName,
		Name:      serviceID,
	})
	if err != nil {
		return fmt.Errorf("failed to read the pooler record: %w", err)
	}
	if pooler.ServingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
		return fmt.Errorf("pooler serving status=%s (expected SERVING)", pooler.ServingStatus)
	}

	for _, name := range slices.Sorted(maps.Keys(s.baselineGucs)) {
		if err := clustersetup.ValidateGUCValue(ctx, client.Pooler, name, s.baselineGucs[name], serviceID); err != nil {
			return err
		}
	}
	return nil
}

// DumpServiceLogs prints where the service logs are kept.
func (s *Setup) DumpServiceLogs() {
	clustersetup.PrintLogLocation(s.TempDir)
}

// Cleanup stops minigres, then pgctld (which stops PostgreSQL), then etcd. The
// temporary directory is kept when testsFailed is true.
func (s *Setup) Cleanup(testsFailed bool) {
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
	if s.Minigres != nil {
		s.Minigres.TerminateGracefully(logf, 5*time.Second)
	}
	if s.Pgctld != nil {
		// pgctld stops PostgreSQL through gRPC before it is signalled.
		s.Pgctld.TerminateGracefully(logf, 15*time.Second)
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.TopoServer != nil {
		s.TopoServer.Close()
	}
	if s.tempDirCleanup != nil && !testsFailed {
		s.tempDirCleanup()
	}
}
