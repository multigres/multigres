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
	"time"

	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/web"
)

type PoolerStatus struct {
	Name     string `json:"name"`
	Database string `json:"database"`
	Type     string `json:"type"`
}

type Link struct {
	Title       string
	Description string
	Link        string
}

// Status contains information for serving the HTML status page.
type Status struct {
	Title string `json:"title"`

	InitError   []string       `json:"init_error"`
	Cell        string         `json:"cell"`
	PoolerCount int            `json:"pooler_count"`
	LastRefresh time.Time      `json:"last_refresh"`
	Poolers     []PoolerStatus `json:"poolers"`

	Links []Link `json:"links"`
}

var serverStatus = Status{
	Title: "Multigateway",
	Links: []Link{
		{"Config", "Server configuration details", "/config"},
		{"Live", "URL for liveness check", "/live"},
		{"Ready", "URL for readiness check", "/ready"},
	},
}

func init() {
	servenv.HTTPHandleFunc("/status", handleStatus)
	servenv.HTTPHandleFunc("/ready", handleReady)
}

func handleReady(w http.ResponseWriter, r *http.Request) {
	isReady := (len(serverStatus.InitError) == 0)
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleStatus handles the HTTP endpoint that shows overall status
func handleStatus(w http.ResponseWriter, r *http.Request) {
	serverStatus.PoolerCount = poolerDiscovery.PoolerCount()
	serverStatus.LastRefresh = poolerDiscovery.LastRefresh()
	poolers := poolerDiscovery.GetPoolers()
	serverStatus.Poolers = make([]PoolerStatus, 0, len(poolers))
	for _, pooler := range poolers {
		serverStatus.Poolers = append(serverStatus.Poolers, PoolerStatus{
			Name:     pooler.Id.GetName(),
			Database: pooler.GetDatabase(),
			Type:     pooler.GetType().String(),
		})
	}

	err := web.Templates.ExecuteTemplate(w, "gateway_index.html", serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
