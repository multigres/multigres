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

package multiadmin

import (
	"context"
	"fmt"
	"sort"

	"github.com/multigres/multigres/go/common/clustermetadata/topo"
)

// ServiceInfo represents a discoverable service in the cluster
type ServiceInfo struct {
	Name       string // Service name (e.g., "multigateway")
	Cell       string // Cell name (e.g., "zone1"), empty for global services
	ProxiedURL string // Proxied URL (e.g., "/proxy/gate/zone1/multigateway")
	DirectURL  string // Direct URL if available (e.g., "http://localhost:15001/")
}

// ServiceList holds all discovered services organized by scope
type ServiceList struct {
	GlobalServices []ServiceInfo            // Global services (multiadmin, etc.)
	CellServices   map[string][]ServiceInfo // Cell name -> services in that cell
	Error          string                   // Error message if discovery failed
}

// DiscoverServices queries the topology store and builds a list of all services
// in the cluster. It returns a ServiceList with services organized by scope.
// This function may be slow (topo queries), so it should be called from a
// dedicated endpoint, not the fast-path root handler.
func (ma *MultiAdmin) DiscoverServices(ctx context.Context) (*ServiceList, error) {
	result := &ServiceList{
		GlobalServices: []ServiceInfo{},
		CellServices:   make(map[string][]ServiceInfo),
	}

	// Add multiadmin as a global service
	// Multiadmin is always available if this code is running
	multiadminProxyURL := fmt.Sprintf("/proxy/admin/%s/multiadmin", topo.GlobalCell)
	multiadminDirectURL := ""
	if httpPort := ma.senv.GetHTTPPort(); httpPort > 0 && ma.senv.GetHostname() != "" {
		multiadminDirectURL = fmt.Sprintf("http://%s:%d/", ma.senv.GetHostname(), httpPort)
	}
	result.GlobalServices = append(result.GlobalServices, ServiceInfo{
		Name:       "multiadmin",
		Cell:       topo.GlobalCell,
		ProxiedURL: multiadminProxyURL,
		DirectURL:  multiadminDirectURL,
	})

	// Discover cells from topology
	cells, err := ma.ts.GetCellNames(ctx)
	if err != nil {
		// If we can't get cells, still return multiadmin but note the error
		result.Error = fmt.Sprintf("Failed to discover cells: %v", err)
		return result, nil
	}

	// For each cell, discover actual registered services from topology
	for _, cell := range cells {
		result.CellServices[cell] = []ServiceInfo{}

		// Discover multigateway services
		gateways, err := ma.ts.GetMultiGatewaysByCell(ctx, cell)
		if err == nil && len(gateways) > 0 {
			// Use the first registered gateway
			gw := gateways[0].MultiGateway
			gatewayName := gateways[0].Id.Name
			proxyURL := fmt.Sprintf("/proxy/gate/%s/%s", cell, gatewayName)
			directURL := ""
			if httpPort, ok := gw.PortMap["http"]; ok && httpPort > 0 {
				directURL = fmt.Sprintf("http://%s:%d/", gw.Hostname, httpPort)
			}
			result.CellServices[cell] = append(result.CellServices[cell], ServiceInfo{
				Name:       "multigateway",
				Cell:       cell,
				ProxiedURL: proxyURL,
				DirectURL:  directURL,
			})
		}

		// Discover multipooler services
		poolers, err := ma.ts.GetMultiPoolersByCell(ctx, cell, nil)
		if err == nil && len(poolers) > 0 {
			// Use the first registered pooler
			mp := poolers[0].MultiPooler
			poolerName := poolers[0].Id.Name
			proxyURL := fmt.Sprintf("/proxy/pool/%s/%s", cell, poolerName)
			directURL := ""
			if httpPort, ok := mp.PortMap["http"]; ok && httpPort > 0 {
				directURL = fmt.Sprintf("http://%s:%d/", mp.Hostname, httpPort)
			}
			result.CellServices[cell] = append(result.CellServices[cell], ServiceInfo{
				Name:       "multipooler",
				Cell:       cell,
				ProxiedURL: proxyURL,
				DirectURL:  directURL,
			})
		}

		// Discover multiorch services
		orchs, err := ma.ts.GetMultiOrchsByCell(ctx, cell)
		if err == nil && len(orchs) > 0 {
			// Use the first registered orch
			mo := orchs[0].MultiOrch
			orchName := orchs[0].Id.Name
			proxyURL := fmt.Sprintf("/proxy/orch/%s/%s", cell, orchName)
			directURL := ""
			if httpPort, ok := mo.PortMap["http"]; ok && httpPort > 0 {
				directURL = fmt.Sprintf("http://%s:%d/", mo.Hostname, httpPort)
			}
			result.CellServices[cell] = append(result.CellServices[cell], ServiceInfo{
				Name:       "multiorch",
				Cell:       cell,
				ProxiedURL: proxyURL,
				DirectURL:  directURL,
			})
		}

		// Sort services within cell alphabetically
		sort.Slice(result.CellServices[cell], func(i, j int) bool {
			return result.CellServices[cell][i].Name < result.CellServices[cell][j].Name
		})
	}

	// Sort global services alphabetically
	sort.Slice(result.GlobalServices, func(i, j int) bool {
		return result.GlobalServices[i].Name < result.GlobalServices[j].Name
	})

	return result, nil
}
