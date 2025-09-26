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
	"net/http"
	"time"
)

type HealthInfo struct {
	InitError   string    `json:"init_error"`
	Cell        string    `json:"cell"`
	PoolerCount int       `json:"pooler_count"`
	LastRefresh time.Time `json:"last_refresh"`
	PoolerNames []string  `json:"pooler_names"`
}

var health HealthInfo

func (h *HealthInfo) addInitError(s string) {
	if h.InitError == "" {
		health.InitError = s
	} else {
		health.InitError += "\n" + s
	}
}

// handleStatus handles the HTTP endpoint that shows overall status
func handleStatus(w http.ResponseWriter, r *http.Request) {
	if poolerDiscovery != nil {
		health.PoolerCount = poolerDiscovery.PoolerCount()
		health.LastRefresh = poolerDiscovery.LastRefresh()
		health.PoolerNames = poolerDiscovery.GetPoolersName()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(health); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}
