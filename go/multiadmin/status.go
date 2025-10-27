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

package multiadmin

import (
	"fmt"
	"net/http"

	"github.com/multigres/multigres/go/web"
)

// Link represents a link on the status page.
type Link struct {
	Title       string
	Description string
	Link        string
}

// Status represents the response from the temporary status endpoint
type Status struct {
	Title string `json:"title"`

	InitError  string            `json:"init_error"`
	TopoStatus map[string]string `json:"topo_status"`

	Links []Link `json:"links"`
}

// handleIndex serves the index page
func (ma *MultiAdmin) handleIndex(w http.ResponseWriter, r *http.Request) {
	ma.serverStatus.TopoStatus = ma.ts.Status()
	err := web.Templates.ExecuteTemplate(w, "admin_index.html", ma.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleServices discovers and displays all cluster services
func (ma *MultiAdmin) handleServices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Discover services from topology (may be slow, that's okay for this endpoint)
	services, err := ma.DiscoverServices(ctx)
	if err != nil {
		// Show error but still try to render what we have
		if services == nil {
			services = &ServiceList{
				Error: fmt.Sprintf("Failed to discover services: %v", err),
			}
		}
	}

	// Render services template
	if err := web.Templates.ExecuteTemplate(w, "admin_services.html", services); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleReady serves the readiness check
func (ma *MultiAdmin) handleReady(w http.ResponseWriter, r *http.Request) {
	isReady := (len(ma.serverStatus.InitError) == 0)
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
