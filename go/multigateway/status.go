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
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/web"
)

// PoolerStatus represents the status of a multipooler instance.
type PoolerStatus struct {
	Name     string `json:"name"`
	Database string `json:"database"`
	Type     string `json:"type"`
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
func (mg *MultiGateway) handleIndex(w http.ResponseWriter, r *http.Request) {
	ts := mg.ts.Status()
	cellStatuses := mg.poolerDiscovery.GetCellStatusesForAdmin()

	mg.serverStatus.mu.Lock()
	defer mg.serverStatus.mu.Unlock()

	mg.serverStatus.LocalCell = mg.cell.Get()
	mg.serverStatus.ServiceID = mg.serviceID.Get()
	mg.serverStatus.TopoStatus = ts
	mg.serverStatus.Cells = make([]CellStatus, 0, len(cellStatuses))
	for _, cs := range cellStatuses {
		cellStatus := CellStatus{
			Cell:        cs.Cell,
			LastRefresh: cs.LastRefresh,
			Poolers:     make([]PoolerStatus, 0, len(cs.Poolers)),
		}
		for _, pooler := range cs.Poolers {
			cellStatus.Poolers = append(cellStatus.Poolers, PoolerStatus{
				Name:     pooler.Id.GetName(),
				Database: pooler.GetDatabase(),
				Type:     pooler.GetType().String(),
			})
		}
		mg.serverStatus.Cells = append(mg.serverStatus.Cells, cellStatus)
	}

	err := web.Templates.ExecuteTemplate(w, "gateway_index.html", &mg.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleReady serves the readiness check
func (mg *MultiGateway) handleReady(w http.ResponseWriter, r *http.Request) {
	mg.serverStatus.mu.Lock()
	defer mg.serverStatus.mu.Unlock()

	isReady := (len(mg.serverStatus.InitError) == 0)
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
