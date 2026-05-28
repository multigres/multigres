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

package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/tools/retry"

	_ "github.com/lib/pq" // PostgreSQL driver for the readiness probe
	"github.com/spf13/cobra"
)

// defaultPostgresUser, defaultPostgresPassword, and defaultPostgresDatabase
// are fallback connection parameters used when the loaded config doesn't
// override them. They match what the local provisioner writes by default.
const (
	defaultPostgresUser     = "postgres"
	defaultPostgresPassword = "postgres"
	defaultPostgresDatabase = "postgres"
)

// bootstrapCredentials are the postgres connection parameters the
// readiness probe uses to authenticate against multigateway. We pull these
// from the loaded config so a rotated password still works — hardcoding
// "postgres" would break the moment an operator changed it.
type bootstrapCredentials struct {
	user     string
	password string
	database string
}

// extractBootstrapCredentials walks the provisioner config to find the
// postgres user/password/database. It picks the first cell it sees (the
// local provisioner writes the same credentials across cells); falls back
// to defaults for any field that's missing.
func extractBootstrapCredentials(provConfig map[string]any) bootstrapCredentials {
	creds := bootstrapCredentials{
		user:     defaultPostgresUser,
		password: defaultPostgresPassword,
		database: defaultPostgresDatabase,
	}
	cells, ok := provConfig["cells"].(map[string]any)
	if !ok {
		return creds
	}
	for _, cellRaw := range cells {
		cell, ok := cellRaw.(map[string]any)
		if !ok {
			continue
		}
		if pgctld, ok := cell["pgctld"].(map[string]any); ok {
			if v, ok := pgctld["pg-user"].(string); ok && v != "" {
				creds.user = v
			}
			if v, ok := pgctld["pg-password"].(string); ok && v != "" {
				creds.password = v
			}
		}
		if mp, ok := cell["multipooler"].(map[string]any); ok {
			if v, ok := mp["database"].(string); ok && v != "" {
				creds.database = v
			}
		}
		return creds
	}
	return creds
}

// gatewayProbeFunc is the signature waitForGatewaysReady expects. Tests
// can pass stubs directly to waitForGatewaysReady; start() wraps
// runGatewayProbeFn in a closure that captures the bootstrap credentials.
type gatewayProbeFunc func(ctx context.Context, host string, port int) error

// runGatewayProbeFn runs `SELECT 1` through a multigateway as the
// operator's configured postgres user. Going through the gateway's PG
// protocol exercises the entire path psql will use moments later —
// gateway PoolerDiscovery → primary multipooler → GetAuthCredentials →
// pg_authid → SCRAM compare — so a successful probe is the strongest
// practical guarantee that user queries will succeed. It is a var (not a
// func) so tests can swap it out without spinning up a real cluster.
var runGatewayProbeFn = func(ctx context.Context, host string, port int, creds bootstrapCredentials) error {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=1",
		host, port, creds.user, creds.password, creds.database,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	probeCtx, cancel := context.WithTimeout(ctx, constants.LocalGatewayBootstrapProbeTimeout)
	defer cancel()
	_, err = db.ExecContext(probeCtx, "SELECT 1")
	return err
}

// bootstrapHeartbeatInterval controls how often waitForGatewaysReady prints a
// "still waiting" line when no gateway status has changed. It's a var (not a
// constant) so tests can shrink it to avoid 30s real-time waits. Not
// user-tunable via a flag.
var bootstrapHeartbeatInterval = 30 * time.Second

// ServiceInfo holds information about a provisioned service
type ServiceInfo struct {
	Name    string
	FQDN    string
	Ports   map[string]int
	LogFile string
}

// ServiceSummary holds all provisioned services
type ServiceSummary struct {
	Services []ServiceInfo
}

// AddService adds a service to the summary
func (s *ServiceSummary) AddService(name string, result *provisioner.ProvisionResult) {
	// Extract log file path from metadata if available
	logFile := ""
	if result.Metadata != nil {
		if logPath, ok := result.Metadata["log_file"].(string); ok {
			logFile = logPath
		}
	}

	s.Services = append(s.Services, ServiceInfo{
		Name:    name,
		FQDN:    result.FQDN,
		Ports:   result.Ports,
		LogFile: logFile,
	})
}

// PrintSummary prints a formatted summary of all provisioned services
func (s *ServiceSummary) PrintSummary() {
	fmt.Println(strings.Repeat("=", 65))
	fmt.Println("🎉 - Multigres cluster started successfully!")
	fmt.Println(strings.Repeat("=", 65))
	fmt.Println()
	fmt.Println("Provisioned Services")
	fmt.Println("--------------------")
	fmt.Println()

	for _, service := range s.Services {
		fmt.Printf("%s\n", service.Name)
		fmt.Printf("   Host: %s\n", service.FQDN)

		if len(service.Ports) == 1 {
			// Single port format
			for portName, portNum := range service.Ports {
				if portName == "http_port" {
					fmt.Printf("   Port: %d → http://%s:%d\n", portNum, service.FQDN, portNum)
				} else {
					fmt.Printf("   Port: %d\n", portNum)
				}
			}
		} else if len(service.Ports) > 1 {
			// Multiple ports format
			fmt.Printf("   Ports:\n")
			for portName, portNum := range service.Ports {
				displayPortName := strings.ToUpper(strings.Replace(portName, "_port", "", 1))
				if portName == "http_port" {
					fmt.Printf("     - %s: %d → http://%s:%d\n", displayPortName, portNum, service.FQDN, portNum)
				} else {
					fmt.Printf("     - %s: %d\n", displayPortName, portNum)
				}
			}
		}

		if service.LogFile != "" {
			fmt.Printf("   Log: %s\n", service.LogFile)
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("=", 65))
	fmt.Println("✨ - Next steps:")

	// Find services with HTTP ports and add direct links
	for _, service := range s.Services {
		if httpPort, exists := service.Ports["http_port"]; exists {
			hostPort := net.JoinHostPort(service.FQDN, strconv.Itoa(httpPort))
			url := "http://" + hostPort
			fmt.Printf("- Open %s in your browser: %s\n", service.Name, url)
		}
	}
	// Find the first multigateway service and show connection command
	for _, service := range s.Services {
		if strings.HasPrefix(service.Name, "multigateway") {
			if pgPort, exists := service.Ports["pg_port"]; exists {
				fmt.Printf("- 🐘 Connect to PostgreSQL: PGPASSWORD=postgres psql -h %s -p %d -U postgres\n",
					service.FQDN, pgPort)
				break // Show only the first gateway
			}
		}
	}
	fmt.Println("- 🟢 Cluster started successfully. Enjoy!")
	fmt.Println("- To stop the cluster: \"multigres cluster stop\"")
	fmt.Println(strings.Repeat("=", 65))
}

// gatewayEndpoint identifies a single multigateway's PostgreSQL endpoint.
type gatewayEndpoint struct {
	name string
	host string
	port int
}

// collectGateways returns the PostgreSQL endpoint for every multigateway in
// results. Results without a pg_port are skipped.
func collectGateways(results []*provisioner.ProvisionResult) []gatewayEndpoint {
	var gws []gatewayEndpoint
	for _, r := range results {
		if r.ServiceName != "multigateway" {
			continue
		}
		port, ok := r.Ports["pg_port"]
		if !ok {
			continue
		}
		gws = append(gws, gatewayEndpoint{name: r.ServiceName, host: r.FQDN, port: port})
	}
	return gws
}

// gwStatus is the latest known readiness state of a single gateway, shared
// between its probe goroutine (the writer) and the printer goroutine (the
// reader) under a mutex.
type gwStatus struct {
	ready   bool   // probe has succeeded at least once
	lastErr string // sanitized text of the most recent failed probe; shown only in the timeout summary
}

// sanitizeProbeErr renders a probe error as a single trimmed line so it fits on
// one terminal row when included in the timeout summary.
func sanitizeProbeErr(err error) string {
	return strings.TrimSpace(strings.ReplaceAll(err.Error(), "\n", " "))
}

// progressPrinter owns all progress output for waitForGatewaysReady. Keeping
// every write in one place (driven by a single goroutine) guarantees stdout
// lines never interleave. It remembers what it last printed per gateway so it
// emits each gateway's "waiting" and "ready" lines exactly once.
type progressPrinter struct {
	w            io.Writer
	gateways     []gatewayEndpoint
	printedReady []bool
	printedWait  []bool
	lastActivity time.Time // when we last printed anything
}

func newProgressPrinter(w io.Writer, gateways []gatewayEndpoint, start time.Time) *progressPrinter {
	return &progressPrinter{
		w:            w,
		gateways:     gateways,
		printedReady: make([]bool, len(gateways)),
		printedWait:  make([]bool, len(gateways)),
		lastActivity: start,
	}
}

// renderChanges prints each gateway's state the first time it is seen: a
// "waiting" line when a gateway is still pending, then a "ready" line once it
// answers. Probe errors are intentionally not shown here — they only surface in
// the timeout summary — so the live output stays terse.
func (p *progressPrinter) renderChanges(snapshot []gwStatus) {
	for i, gw := range p.gateways {
		switch {
		case snapshot[i].ready && !p.printedReady[i]:
			p.printedReady[i] = true
			fmt.Fprintf(p.w, "✅ - %s ready at %s:%d\n", gw.name, gw.host, gw.port)
			p.lastActivity = time.Now()
		case !snapshot[i].ready && !p.printedWait[i]:
			p.printedWait[i] = true
			fmt.Fprintf(p.w, "⏳ - waiting for %s at %s:%d to become available\n",
				gw.name, gw.host, gw.port)
			p.lastActivity = time.Now()
		}
	}
}

// renderHeartbeat prints a single "still waiting" line listing every pending
// gateway, but only when nothing has been printed for at least
// bootstrapHeartbeatInterval. This reassures the operator that a long-running,
// unchanged wait hasn't hung.
func (p *progressPrinter) renderHeartbeat(snapshot []gwStatus, elapsed time.Duration) {
	if time.Since(p.lastActivity) < bootstrapHeartbeatInterval {
		return
	}
	var pending []string
	for i, gw := range p.gateways {
		if snapshot[i].ready {
			continue
		}
		pending = append(pending, fmt.Sprintf("%s (%s:%d)", gw.name, gw.host, gw.port))
	}
	if len(pending) == 0 {
		return
	}
	fmt.Fprintf(p.w, "⏳ - Still waiting (%s elapsed): %s\n",
		elapsed.Round(time.Second), strings.Join(pending, ", "))
	p.lastActivity = time.Now()
}

// waitForGatewaysReady probes every gateway until each succeeds or ctx is
// cancelled. Gateways are probed sequentially in a single goroutine: every
// probe self-caps (LocalGatewayBootstrapProbeTimeout), so on localhost a full
// cycle is fast and there's no need for concurrency, channels, or locks. The
// printer runs inline, so progress lines never interleave. On success it prints
// a summary line and returns nil; on ctx expiry it returns an error listing the
// gateways that never became ready and how long it waited.
func waitForGatewaysReady(ctx context.Context, w io.Writer, gateways []gatewayEndpoint, probe gatewayProbeFunc) error {
	start := time.Now()
	statuses := make([]gwStatus, len(gateways))
	printer := newProgressPrinter(w, gateways, start)

	r := retry.New(constants.LocalGatewayBootstrapPollInterval, constants.LocalGatewayBootstrapPollInterval)
	for _, rerr := range r.Attempts(ctx) {
		if rerr != nil {
			break // ctx cancelled or timed out
		}
		allUp := true
		for i, gw := range gateways {
			if statuses[i].ready {
				continue
			}
			if perr := probe(ctx, gw.host, gw.port); perr == nil {
				statuses[i].ready = true
			} else {
				allUp = false
				statuses[i].lastErr = sanitizeProbeErr(perr)
			}
		}
		printer.renderChanges(statuses)
		if allUp {
			break
		}
		printer.renderHeartbeat(statuses, time.Since(start))
	}

	// On timeout the last probe error is the most useful diagnostic, so include
	// it per pending gateway here even though the live progress lines omit it.
	var pending []string
	for i, gw := range gateways {
		if statuses[i].ready {
			continue
		}
		if statuses[i].lastErr != "" {
			pending = append(pending, fmt.Sprintf("%s (%s:%d, last error: %s)", gw.name, gw.host, gw.port, statuses[i].lastErr))
		} else {
			pending = append(pending, fmt.Sprintf("%s (%s:%d)", gw.name, gw.host, gw.port))
		}
	}

	elapsed := time.Since(start).Round(time.Second)
	if len(pending) > 0 {
		return fmt.Errorf(
			"cluster did not become ready to serve queries within %s; gateways not ready: %s. Check the multigateway and multipooler log files listed above for details (waited %s)",
			constants.LocalBootstrapWaitTimeout, strings.Join(pending, ", "), elapsed,
		)
	}
	fmt.Fprintf(w, "✅ - Cluster ready to serve queries (total wait: %s)\n", elapsed)
	return nil
}

// start handles the cluster up command
func start(cmd *cobra.Command, args []string) error {
	fmt.Println("Multigres — Distributed Postgres made easy")
	fmt.Println("=================================================================")
	fmt.Println("✨ Bootstrapping your local Multigres cluster — this may take a few moments ✨")

	// Get config paths from flags
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		configPaths = []string{"."}
	}

	// Load configuration to determine provisioner type
	config, configFile, err := LoadConfig(configPaths)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	fmt.Println("📄 - Config loaded from: " + configFile)

	// Create provisioner instance
	p, err := provisioner.GetProvisioner(config.Provisioner)
	if err != nil {
		return fmt.Errorf("failed to create provisioner '%s': %w", config.Provisioner, err)
	}

	// Let provisioner load its own configuration
	if err := p.LoadConfig(configPaths); err != nil {
		return fmt.Errorf("failed to load provisioner config: %w", err)
	}

	fmt.Println("🛠️  - Provisioner: " + p.Name())
	fmt.Println()
	fmt.Println("👋 Here we go! Starting core services...")
	fmt.Println(strings.Repeat("=", 65))
	fmt.Println()

	ctx := cmd.Context()

	// Initialize service summary to track all provisioned services
	summary := &ServiceSummary{}

	// Use the provisioner's Bootstrap method to provision all services
	allResults, err := p.Bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("cluster bootstrap failed: %w", err)
	}

	// Add all returned services to summary dynamically
	for _, result := range allResults {
		summary.AddService(result.ServiceName, result)
	}

	waitForBootstrap, err := cmd.Flags().GetBool("wait-for-bootstrap")
	if err != nil {
		return fmt.Errorf("failed to get wait-for-bootstrap flag: %w", err)
	}
	if waitForBootstrap {
		gateways := collectGateways(allResults)
		if len(gateways) == 0 {
			return errors.New("cluster bootstrap returned no multigateways with a pg_port; cluster cannot serve queries")
		}

		creds := extractBootstrapCredentials(config.ProvisionerConfig)
		probe := func(ctx context.Context, host string, port int) error {
			return runGatewayProbeFn(ctx, host, port, creds)
		}

		fmt.Println()
		fmt.Printf("⏳ - Waiting up to %s for the cluster to start serving queries...\n", constants.LocalBootstrapWaitTimeout)
		waitCtx, cancel := context.WithTimeout(ctx, constants.LocalBootstrapWaitTimeout)
		defer cancel()
		if err := waitForGatewaysReady(waitCtx, os.Stdout, gateways, probe); err != nil {
			return err
		}
	}

	// Print comprehensive summary
	fmt.Println()
	summary.PrintSummary()
	return nil
}

// AddStartCommand adds the start subcommand to the cluster command
func AddStartCommand(clusterCmd *cobra.Command) {
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start local cluster",
		Long:  "Start a local Multigres cluster using the configuration created with 'multigres cluster init'.",
		RunE:  start,
	}

	startCmd.Flags().Bool("wait-for-bootstrap", true,
		"Wait for every multigateway to execute SELECT 1 successfully (using the postgres password from the loaded config) before returning; on timeout the command exits with an error")

	clusterCmd.AddCommand(startCmd)
}
