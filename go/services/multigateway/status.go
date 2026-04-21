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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/web"
	"github.com/multigres/multigres/go/services/multigateway/handler/queryregistry"
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

// handleReady serves the readiness check.
// The gateway is ready only when both conditions are met:
// 1. No init errors (topology registration succeeded)
// 2. At least one pooler has been discovered (can actually serve queries)
func (mg *MultiGateway) handleReady(w http.ResponseWriter, r *http.Request) {
	mg.serverStatus.mu.Lock()
	defer mg.serverStatus.mu.Unlock()

	hasNoInitError := len(mg.serverStatus.InitError) == 0
	hasPoolers := mg.poolerDiscovery.PoolerCount() > 0
	isReady := hasNoInitError && hasPoolers
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// ConsolidatorDebugStatus contains data for the consolidator debug page.
type ConsolidatorDebugStatus struct {
	Title string
	Stats preparedstatement.ConsolidatorStats
}

// QueryView is a presentation-ready view of a queryregistry.Snapshot with
// pre-formatted millisecond fields so the HTML template doesn't get mixed
// units from time.Duration.String() (µs, ms, s).
type QueryView struct {
	Fingerprint   string    `json:"fingerprint"`
	NormalizedSQL string    `json:"normalized_sql"`
	Calls         uint64    `json:"calls"`
	Errors        uint64    `json:"errors"`
	AvgMs         string    `json:"avg_ms"`
	MinMs         string    `json:"min_ms"`
	MaxMs         string    `json:"max_ms"`
	TotalRows     uint64    `json:"total_rows"`
	LastSeen      time.Time `json:"last_seen"`
}

// QueriesDebugStatus is the view-model for the /debug/queries HTML page.
type QueriesDebugStatus struct {
	Title               string      `json:"title"`
	TrackedFingerprints int         `json:"tracked_fingerprints"`
	Returned            int         `json:"returned"`
	Limit               int         `json:"limit"`
	SortBy              string      `json:"sort_by"`
	SortLinks           []SortLink  `json:"-"`
	Queries             []QueryView `json:"queries"`
}

// SortLink represents one sort option on the /debug/queries page.
// Precomputed in the handler so the HTML template can iterate without
// using Go-template string literals inside attribute values (which
// breaks HTML linters that don't understand Go templates).
type SortLink struct {
	Key    string
	Label  string
	Active bool
}

// durationToMs renders a duration as a fixed-width millisecond string with
// 3 decimal places so the dashboard columns align. Zero durations print as
// "0.000".
func durationToMs(d time.Duration) string {
	return fmt.Sprintf("%.3f", float64(d.Microseconds())/1000.0)
}

// handleQueriesDebug serves per-query-shape statistics.
// Renders HTML by default; pass ?format=json for machine-readable output.
// Query params:
//
//	limit: max entries to return (default 50, 0 = unlimited)
//	sort:  one of calls (default), total_time, avg_time, errors, last_seen
func (mg *MultiGateway) handleQueriesDebug(w http.ResponseWriter, r *http.Request) {
	registry := mg.pgHandler.QueryRegistry()

	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			limit = n
		}
	}

	sortName := r.URL.Query().Get("sort")
	if sortName == "" {
		sortName = "calls"
	}
	var sortBy queryregistry.SortKey
	switch sortName {
	case "total_time":
		sortBy = queryregistry.SortByTotalTime
	case "avg_time":
		sortBy = queryregistry.SortByAverageTime
	case "errors":
		sortBy = queryregistry.SortByErrors
	case "last_seen":
		sortBy = queryregistry.SortByLastSeen
	default:
		sortBy = queryregistry.SortByCalls
		sortName = "calls"
	}

	var snapshots []queryregistry.Snapshot
	if registry != nil {
		snapshots = registry.Top(limit, sortBy)
	}

	queries := make([]QueryView, len(snapshots))
	for i, s := range snapshots {
		queries[i] = QueryView{
			Fingerprint:   s.Fingerprint,
			NormalizedSQL: s.NormalizedSQL,
			Calls:         s.Calls,
			Errors:        s.Errors,
			AvgMs:         durationToMs(s.AverageDuration),
			MinMs:         durationToMs(s.MinDuration),
			MaxMs:         durationToMs(s.MaxDuration),
			TotalRows:     s.TotalRows,
			LastSeen:      s.LastSeen,
		}
	}

	sortOptions := []struct{ Key, Label string }{
		{"calls", "calls"},
		{"total_time", "total time"},
		{"avg_time", "avg time"},
		{"errors", "errors"},
		{"last_seen", "last seen"},
	}
	sortLinks := make([]SortLink, len(sortOptions))
	for i, opt := range sortOptions {
		sortLinks[i] = SortLink{
			Key:    opt.Key,
			Label:  opt.Label,
			Active: opt.Key == sortName,
		}
	}

	view := QueriesDebugStatus{
		Title:               "Per-Query Metrics",
		TrackedFingerprints: registry.Len(),
		Returned:            len(snapshots),
		Limit:               limit,
		SortBy:              sortName,
		SortLinks:           sortLinks,
		Queries:             queries,
	}

	if r.URL.Query().Get("format") == "json" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(view); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode JSON: %v", err), http.StatusInternalServerError)
		}
		return
	}

	if err := web.Templates.ExecuteTemplate(w, "queries_debug.html", &view); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
	}
}

// handleConsolidatorDebug serves the prepared statement consolidator debug page.
func (mg *MultiGateway) handleConsolidatorDebug(w http.ResponseWriter, r *http.Request) {
	stats := mg.pgHandler.Consolidator().Stats()

	// Check if JSON format is requested
	if r.URL.Query().Get("format") == "json" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode JSON: %v", err), http.StatusInternalServerError)
		}
		return
	}

	status := ConsolidatorDebugStatus{
		Title: "Prepared Statement Consolidator",
		Stats: stats,
	}
	if err := web.Templates.ExecuteTemplate(w, "consolidator_debug.html", &status); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
