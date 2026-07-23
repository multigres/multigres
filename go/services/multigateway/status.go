// Copyright 2025 Supabase, Inc.
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

// multigateway is the top-level proxy that masquerades as a PostgreSQL server,
// handling client connections and routing queries to multipooler instances.

package multigateway

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/web"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerStatus represents the status of a multipooler instance.
type PoolerStatus struct {
	Name     string `json:"name"`
	Database string `json:"database"`
	// Leadership is the gateway's merged consensus view of the pooler's role
	// (leader/stale-leader/follower), empty when the gateway is not connected to
	// it. Distinct from Lifecycle: a pooler's consensus role and its process
	// lifecycle state are orthogonal.
	Leadership string `json:"leadership"`
	// Lifecycle is the pooler's process lifecycle state (e.g. active, shutdown),
	// from its topology record.
	Lifecycle string `json:"lifecycle"`
}

// CellStatus represents the status of pooler discovery for a single cell.
type CellStatus struct {
	Cell        string         `json:"cell"`
	LastRefresh time.Time      `json:"last_refresh"`
	Poolers     []PoolerStatus `json:"poolers"`
}

// Link represents a link on the status page.
type Link struct {
	Title       string
	Description string
	Link        string
}

// Status contains information for serving the HTML status page.
type Status struct {
	mu sync.Mutex

	Title string `json:"title"`

	InitError  string            `json:"init_error"`
	TopoStatus map[string]string `json:"topo_status"`

	LocalCell string       `json:"local_cell"`
	ServiceID string       `json:"service_id"`
	Cells     []CellStatus `json:"cells"`

	Links []Link `json:"links"`
}

// handleIndex serves the index page
func (mg *Multigateway) handleIndex(w http.ResponseWriter, r *http.Request) {
	ts := mg.ts.Status()
	cells := mg.collectCellStatuses()

	mg.serverStatus.mu.Lock()
	defer mg.serverStatus.mu.Unlock()

	mg.serverStatus.LocalCell = mg.cell.Get()
	mg.serverStatus.ServiceID = mg.serviceID.Get()
	mg.serverStatus.TopoStatus = ts
	mg.serverStatus.Cells = cells

	err := web.Templates.ExecuteTemplate(w, "gateway_index.html", &mg.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// collectCellStatuses joins the two sources behind the status page: the
// pooler cache supplies the full per-cell pooler list (including poolers the
// gateway is not connected to), and the load balancer supplies each connected
// pooler's live consensus leadership. They are merged on serialized pooler ID.
func (mg *Multigateway) collectCellStatuses() []CellStatus {
	cellStatuses := mg.poolerGateway.CellStatuses()
	cells := make([]CellStatus, 0, len(cellStatuses))
	for _, cs := range cellStatuses {
		cellStatus := CellStatus{
			Cell:        cs.Cell,
			LastRefresh: cs.LastActivity,
			Poolers:     make([]PoolerStatus, 0, len(cs.Poolers)),
		}
		for _, pooler := range cs.Poolers {
			cellStatus.Poolers = append(cellStatus.Poolers, PoolerStatus{
				Name:       pooler.Id.GetName(),
				Database:   pooler.GetShardKey().GetDatabase(),
				Leadership: mg.poolerGateway.LeadershipForID(topoclient.ComponentIDString(pooler.Id)),
				Lifecycle:  lifecycleLabel(pooler.GetLifecycleStatus()),
			})
		}
		cells = append(cells, cellStatus)
	}
	return cells
}

// lifecycleLabel renders a pooler's lifecycle status for display, e.g.
// LIFECYCLE_SHUTDOWN -> "shutdown". A nil or unset status reads as "unknown".
func lifecycleLabel(lc *clustermetadatapb.PoolerLifecycle) string {
	status := lc.GetStatus()
	if status == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(status.String(), "LIFECYCLE_"))
}
