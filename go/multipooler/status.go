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

// Package multipooler provides multipooler functionality.
package multipooler

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/multigres/multigres/go/common/web"
)

// Link represents a link on the status page.
type Link struct {
	Title       string
	Description string
	Link        string
}

// Status represents the response from the temporary status endpoint
type Status struct {
	mu sync.Mutex

	Title string `json:"title"`

	InitError  string            `json:"init_error"`
	TopoStatus map[string]string `json:"topo_status"`

	Cell           string `json:"cell"`
	ServiceID      string `json:"service_id"`
	Database       string `json:"database"`
	TableGroup     string `json:"table_group"`
	PgctldAddr     string `json:"pgctld_addr"`
	SocketFilePath string `json:"socket_file_path"`

	Links []Link `json:"links"`
}

// handleIndex serves the index page
func (mp *MultiPooler) handleIndex(w http.ResponseWriter, r *http.Request) {
	mp.serverStatus.mu.Lock()
	defer mp.serverStatus.mu.Unlock()

	mp.serverStatus.Cell = mp.cell.Get()
	mp.serverStatus.ServiceID = mp.serviceID.Get()
	mp.serverStatus.Database = mp.database.Get()
	mp.serverStatus.TableGroup = mp.tableGroup.Get()
	mp.serverStatus.PgctldAddr = mp.pgctldAddr.Get()
	mp.serverStatus.SocketFilePath = mp.socketFilePath.Get()
	mp.serverStatus.TopoStatus = mp.ts.Status()
	err := web.Templates.ExecuteTemplate(w, "pooler_index.html", &mp.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleReady serves the readiness check
func (mp *MultiPooler) handleReady(w http.ResponseWriter, r *http.Request) {
	mp.serverStatus.mu.Lock()
	defer mp.serverStatus.mu.Unlock()

	isReady := (len(mp.serverStatus.InitError) == 0)
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
