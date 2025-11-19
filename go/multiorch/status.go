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

package multiorch

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/multigres/multigres/go/web"
)

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

	Cell string `json:"cell"`

	Links []Link `json:"links"`
}

// handleIndex serves the index page
func (mo *MultiOrch) handleIndex(w http.ResponseWriter, r *http.Request) {
	mo.serverStatus.mu.Lock()
	defer mo.serverStatus.mu.Unlock()

	mo.serverStatus.Cell = mo.cfg.GetCell()
	mo.serverStatus.TopoStatus = mo.ts.Status()
	err := web.Templates.ExecuteTemplate(w, "orch_index.html", &mo.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleReady serves the readiness check
func (mo *MultiOrch) handleReady(w http.ResponseWriter, r *http.Request) {
	mo.serverStatus.mu.Lock()
	defer mo.serverStatus.mu.Unlock()

	isReady := (len(mo.serverStatus.InitError) == 0)
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
