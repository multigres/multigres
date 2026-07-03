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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	mpmdpb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

const statusRPCTimeout = 30 * time.Second

const (
	labelHealthy         = "(healthy)"
	labelUnhealthy       = "(unhealthy)"
	labelStatusUnavail   = "(status unavailable)"
	labelConnected       = "connected"
	labelUnreachable     = "unreachable"
	labelPostgresStopped = "(stopped)"
	labelPostgresRunning = "running"
	labelPostgresStream  = "streaming"
)

var (
	// flags
	adminServer  string
	cell         string
	database     string
	outputFormat string
)

// AddStatusCommand adds the status subcommand to the cluster command
func AddStatusCommand(clusterCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster health",
		Long:  "Display the current health and status of the Multigres cluster.",
		RunE:  runStatus,
	}
	cmd.Flags().StringVar(&adminServer, "admin-server", "", "gRPC address of the multiadmin server (overrides config)")
	cmd.Flags().StringVar(&cell, "cell", "", "Filter results to a specific cell")
	cmd.Flags().StringVar(&database, "database", "", "Filter poolers to a specific database")
	cmd.Flags().StringVar(&outputFormat, "output", "text", "Output format: text or json")
	clusterCmd.AddCommand(cmd)
}

// poolerStatusResult pairs a pooler's topology info with its live status.
type poolerStatusResult struct {
	pooler *clustermetadatapb.MultiPooler
	status *mpmdpb.Status // nil when GetPoolerStatus RPC failed
}

type componentStatus[T any] struct {
	component   T
	healthy     bool
	connCount   uint32
	connCountOK bool
}

func runStatus(cmd *cobra.Command, args []string) error {
	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), statusRPCTimeout)
	defer cancel()

	var cells []string
	if cell != "" {
		cells = []string{cell}
	}

	// Fetch topology in parallel.
	var (
		cellNames []string
		poolers   []*clustermetadatapb.MultiPooler
		gateways  []*clustermetadatapb.MultiGateway
		orchs     []*clustermetadatapb.MultiOrch
	)

	g, _ := errgroup.WithContext(ctx)

	g.Go(func() error {
		resp, err := client.GetCellNames(ctx, &multiadminpb.GetCellNamesRequest{})
		if err != nil {
			return fmt.Errorf("GetCellNames: %w", err)
		}
		cellNames = resp.GetNames()
		return nil
	})

	g.Go(func() error {
		resp, err := client.GetPoolers(ctx, &multiadminpb.GetPoolersRequest{Cells: cells, Database: database})
		if err != nil {
			return fmt.Errorf("GetPoolers: %w", err)
		}
		poolers = resp.GetPoolers()
		return nil
	})

	g.Go(func() error {
		resp, err := client.GetGateways(ctx, &multiadminpb.GetGatewaysRequest{Cells: cells})
		if err != nil {
			return fmt.Errorf("GetGateways: %w", err)
		}
		gateways = resp.GetGateways()
		return nil
	})

	g.Go(func() error {
		resp, err := client.GetOrchs(ctx, &multiadminpb.GetOrchsRequest{Cells: cells})
		if err != nil {
			return fmt.Errorf("GetOrchs: %w", err)
		}
		orchs = resp.GetOrchs()
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Fetch per-pooler live status in parallel; tolerate individual failures.
	poolerStatuses := make([]*poolerStatusResult, len(poolers))
	pg, _ := errgroup.WithContext(ctx)
	for i, p := range poolers {
		pg.Go(func() error {
			result := &poolerStatusResult{pooler: p}
			resp, err := client.GetPoolerStatus(ctx, &multiadminpb.GetPoolerStatusRequest{
				PoolerId: p.GetId(),
			})
			if err == nil && resp != nil {
				result.status = resp.GetStatus()
			}
			poolerStatuses[i] = result
			return nil
		})
	}
	// skip error check
	_ = pg.Wait()

	// Sort poolers: PRIMARY first, REPLICA second.
	priority := func(t clustermetadatapb.PoolerType) int {
		switch t {
		case clustermetadatapb.PoolerType_PRIMARY:
			return 0
		case clustermetadatapb.PoolerType_REPLICA:
			return 1
		default:
			return 2
		}
	}
	slices.SortStableFunc(poolerStatuses, func(a, b *poolerStatusResult) int {
		return cmp.Compare(priority(a.pooler.GetType()), priority(b.pooler.GetType()))
	})

	// Fetch health for gateways and orchs in parallel via gRPC health API.
	gwStatuses := make([]*componentStatus[*clustermetadatapb.MultiGateway], len(gateways))
	orchStatuses := make([]*componentStatus[*clustermetadatapb.MultiOrch], len(orchs))
	hg, _ := errgroup.WithContext(ctx)
	for i, gw := range gateways {
		hg.Go(func() error {
			cs := &componentStatus[*clustermetadatapb.MultiGateway]{
				component: gw,
				healthy:   grpcHealthy(ctx, gw.GetHostname(), int(gw.GetPortMap()["grpc"])),
			}
			if resp, err := client.GetGatewayConsolidator(ctx, &multiadminpb.GetGatewayConsolidatorRequest{
				GatewayId: gw.GetId(),
			}); err == nil && resp != nil {
				cs.connCount = resp.GetStats().GetConnectionCount()
				cs.connCountOK = true
			}
			gwStatuses[i] = cs
			return nil
		})
	}
	for i, orch := range orchs {
		hg.Go(func() error {
			orchStatuses[i] = &componentStatus[*clustermetadatapb.MultiOrch]{
				component: orch,
				healthy:   grpcHealthy(ctx, orch.GetHostname(), int(orch.GetPortMap()["grpc"])),
			}
			return nil
		})
	}
	_ = hg.Wait()

	// Resolve etcd address and connectivity for the Topology line.
	etcdAddr := ""
	etcdConnected := false
	if localCfg, err := admin.GetLocalConfig(cmd); err == nil && localCfg.Etcd.Port > 0 {
		etcdAddr = fmt.Sprintf("localhost:%d", localCfg.Etcd.Port)
		if conn, err := net.DialTimeout("tcp", etcdAddr, 2*time.Second); err == nil {
			conn.Close()
			etcdConnected = true
		}
	}

	if outputFormat == "json" {
		return printStatusJSON(cmd, cellNames, poolerStatuses, gwStatuses, orchStatuses, etcdAddr, etcdConnected)
	}
	return printStatusText(cmd, cellNames, poolerStatuses, gwStatuses, orchStatuses, etcdAddr, etcdConnected)
}

// --- text output ---

func printStatusText(
	cmd *cobra.Command,
	cells []string,
	poolerStatuses []*poolerStatusResult,
	gateways []*componentStatus[*clustermetadatapb.MultiGateway],
	orchs []*componentStatus[*clustermetadatapb.MultiOrch],
	etcdAddr string,
	etcdConnected bool,
) error {
	cmd.Println("Multigres Cluster Status")
	cmd.Println("========================")
	cmd.Println()

	cmd.Printf("Cells: %s\n", strings.Join(cells, ", "))
	cmd.Println()

	// Group poolers by database, preserving first-seen order.
	dbOrder := []string{}
	dbPoolers := map[string][]*poolerStatusResult{}
	for _, ps := range poolerStatuses {
		db := ps.pooler.GetShardKey().GetDatabase()
		if _, seen := dbPoolers[db]; !seen {
			dbOrder = append(dbOrder, db)
		}
		dbPoolers[db] = append(dbPoolers[db], ps)
	}

	for _, db := range dbOrder {
		pss := dbPoolers[db]
		cmd.Printf("Database: %s\n", db)
		for i, ps := range pss {
			branch := treeBranch(i, len(pss))
			child := treeChild(i, len(pss))
			typeStr := strings.TrimPrefix(ps.pooler.GetType().String(), "PoolerType_")
			cmd.Printf("%s%s: %s %s\n", branch, typeStr, ps.pooler.GetId().GetName(), poolerHealthLabel(ps.status))
			cmd.Printf("%sPostgreSQL: %s\n", child, postgresLabel(ps.status, ps.pooler.GetType()))
		}
		cmd.Println()
	}

	cmd.Println("Gateways:")
	for i, gs := range gateways {
		branch := treeBranch(i, len(gateways))
		pgPort := int(gs.component.GetPortMap()["postgres"])
		label := labelUnhealthy
		if gs.healthy {
			label = labelHealthy
		}
		if gs.connCountOK {
			label = fmt.Sprintf("%s, connections: %d)", strings.TrimSuffix(label, ")"), gs.connCount)
		}
		cmd.Printf("%s%s:%d %s\n", branch, gs.component.GetHostname(), pgPort, label)
	}
	cmd.Println()

	cmd.Println("Orchestrators:")
	for i, os := range orchs {
		branch := treeBranch(i, len(orchs))
		label := labelUnhealthy
		if os.healthy {
			label = labelHealthy
		}
		cmd.Printf("%s%s %s\n", branch, os.component.GetId().GetName(), label)
	}
	cmd.Println()

	if etcdAddr != "" {
		connStr := labelConnected
		if !etcdConnected {
			connStr = labelUnreachable
		}
		cmd.Printf("Topology: etcd://%s (%s)\n", etcdAddr, connStr)
	}

	return nil
}

func treeBranch(i, total int) string {
	if i == total-1 {
		return "└── "
	}
	return "├── "
}

func treeChild(i, total int) string {
	if i == total-1 {
		return "    └── "
	}
	return "│   └── "
}

func poolerHealthLabel(s *mpmdpb.Status) string {
	if s == nil {
		return labelStatusUnavail
	}
	if s.GetPostgresRunning() && s.GetPostgresReady() {
		return labelHealthy
	}
	return labelUnhealthy
}

func postgresLabel(s *mpmdpb.Status, poolerType clustermetadatapb.PoolerType) string {
	if s == nil {
		return labelStatusUnavail
	}
	if !s.GetPostgresRunning() {
		return labelPostgresStopped
	}
	var state string
	switch poolerType {
	case clustermetadatapb.PoolerType_PRIMARY:
		state = labelPostgresRunning
	case clustermetadatapb.PoolerType_REPLICA:
		state = labelPostgresStream
	default:
		state = strings.ToLower(strings.TrimPrefix(s.GetPostgresStatus().String(), "POSTGRES_STATUS_"))
	}
	parts := []string{state}
	if wal := s.GetWalPosition(); wal != "" {
		parts = append(parts, "LSN: "+wal)
	}
	if poolerType == clustermetadatapb.PoolerType_REPLICA {
		if lag := s.GetReplicationStatus().GetLag(); lag != nil {
			parts = append(parts, "lag: "+lag.AsDuration().Truncate(time.Millisecond).String())
		}
	}
	detail := "(" + strings.Join(parts, ", ") + ")"
	if v := s.GetPgVersion(); v != "" {
		return v + " " + detail
	}
	return detail
}

func grpcHealthy(ctx context.Context, host string, port int) bool {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := grpccommon.NewClient(addr, grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		return false
	}
	defer conn.Close()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
}

// --- JSON output ---

type clusterStatusJSON struct {
	Cells         []string       `json:"cells"`
	Databases     []databaseJSON `json:"databases"`
	Gateways      []gatewayJSON  `json:"gateways"`
	Orchestrators []orchJSON     `json:"orchestrators"`
	Topology      topologyJSON   `json:"topology"`
}

type databaseJSON struct {
	Name    string       `json:"name"`
	Poolers []poolerJSON `json:"poolers"`
}

type poolerJSON struct {
	Cell             string `json:"cell"`
	Name             string `json:"name"`
	Type             string `json:"type"`
	ServingStatus    string `json:"serving_status"`
	StatusAvailable  bool   `json:"status_available"`
	Healthy          bool   `json:"healthy"`
	PostgresRunning  bool   `json:"postgres_running"`
	PostgresStatus   string `json:"postgres_status"`
	WalPosition      string `json:"wal_position"`
	PgVersion        string `json:"pg_version,omitempty"`
	ReplicationLagMs *int64 `json:"replication_lag_ms,omitempty"`
}

type gatewayJSON struct {
	Cell            string  `json:"cell"`
	Name            string  `json:"name"`
	Hostname        string  `json:"hostname"`
	PgPort          int     `json:"pg_port"`
	Healthy         bool    `json:"healthy"`
	ConnectionCount *uint32 `json:"connection_count,omitempty"`
}

type orchJSON struct {
	Cell     string `json:"cell"`
	Name     string `json:"name"`
	Hostname string `json:"hostname"`
	Healthy  bool   `json:"healthy"`
}

type topologyJSON struct {
	Etcd      string `json:"etcd"`
	Connected bool   `json:"connected"`
}

func printStatusJSON(
	cmd *cobra.Command,
	cells []string,
	poolerStatuses []*poolerStatusResult,
	gateways []*componentStatus[*clustermetadatapb.MultiGateway],
	orchs []*componentStatus[*clustermetadatapb.MultiOrch],
	etcdAddr string,
	etcdConnected bool,
) error {
	out := clusterStatusJSON{
		Cells:    cells,
		Topology: topologyJSON{Etcd: etcdAddr, Connected: etcdConnected},
	}

	dbOrder := []string{}
	dbPoolers := map[string][]poolerJSON{}
	for _, ps := range poolerStatuses {
		db := ps.pooler.GetShardKey().GetDatabase()
		if _, seen := dbPoolers[db]; !seen {
			dbOrder = append(dbOrder, db)
		}
		pj := poolerJSON{
			Cell:            ps.pooler.GetId().GetCell(),
			Name:            ps.pooler.GetId().GetName(),
			Type:            strings.TrimPrefix(ps.pooler.GetType().String(), "PoolerType_"),
			ServingStatus:   strings.TrimPrefix(ps.pooler.GetServingStatus().String(), "PoolerServingStatus_"),
			StatusAvailable: ps.status != nil,
		}
		if ps.status != nil {
			pj.Healthy = ps.status.GetPostgresRunning() && ps.status.GetPostgresReady()
			pj.PostgresRunning = ps.status.GetPostgresRunning()
			pj.PostgresStatus = strings.TrimPrefix(ps.status.GetPostgresStatus().String(), "POSTGRES_STATUS_")
			pj.WalPosition = ps.status.GetWalPosition()
			pj.PgVersion = ps.status.GetPgVersion()
			if lag := ps.status.GetReplicationStatus().GetLag(); lag != nil {
				ms := lag.AsDuration().Milliseconds()
				pj.ReplicationLagMs = &ms
			}
		}
		dbPoolers[db] = append(dbPoolers[db], pj)
	}
	for _, db := range dbOrder {
		out.Databases = append(out.Databases, databaseJSON{Name: db, Poolers: dbPoolers[db]})
	}

	for _, gs := range gateways {
		pgPort := int(gs.component.GetPortMap()["postgres"])
		gj := gatewayJSON{
			Cell:     gs.component.GetId().GetCell(),
			Name:     gs.component.GetId().GetName(),
			Hostname: gs.component.GetHostname(),
			PgPort:   pgPort,
			Healthy:  gs.healthy,
		}
		if gs.connCountOK {
			cc := gs.connCount
			gj.ConnectionCount = &cc
		}
		out.Gateways = append(out.Gateways, gj)
	}

	for _, os := range orchs {
		out.Orchestrators = append(out.Orchestrators, orchJSON{
			Cell:     os.component.GetId().GetCell(),
			Name:     os.component.GetId().GetName(),
			Hostname: os.component.GetHostname(),
			Healthy:  os.healthy,
		})
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal status to JSON: %w", err)
	}
	cmd.Println(string(data))
	return nil
}
