/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// multiadmin provides administrative services for the multigres cluster,
// exposing both HTTP and gRPC endpoints for cluster management operations.
package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/cmd/multiadmin/internal/server"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var (
	// adminServer holds the gRPC admin server instance
	adminServer *server.MultiAdminServer

	Main = &cobra.Command{
		Use:     "multiadmin",
		Short:   "Multiadmin provides administrative services for the multigres cluster, exposing both HTTP and gRPC endpoints for cluster management operations.",
		Long:    "Multiadmin provides administrative services for the multigres cluster, exposing both HTTP and gRPC endpoints for cluster management operations.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

func main() {
	if err := Main.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	servenv.Init()

	// Get the configured logger
	logger := servenv.GetLogger()

	// Open topo connection to discover other components
	ts := topo.Open()
	defer func() { _ = ts.Close() }()

	servenv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multiadmin starting up",
			"http_port", servenv.HTTPPort(),
			"grpc_port", servenv.GRPCPort(),
		)

		// Start the multiadmin gRPC server
		adminServer = server.NewMultiAdminServer(ts, logger)
		go func() {
			if err := adminServer.Start(servenv.GRPCPort()); err != nil {
				logger.Error("Failed to start multiadmin gRPC server", "error", err)
			}
		}()

		// Add HTTP endpoints for cluster management
		servenv.HTTPHandleFunc("/admin/status", handleStatusEndpoint)
		servenv.HTTPHandleFunc("/admin/clusters", handleClustersEndpoint)
		logger.Info("Admin HTTP endpoints available at /admin/status and /admin/clusters")
	})

	servenv.OnClose(func() {
		logger.Info("multiadmin shutting down")

		// Stop the multiadmin gRPC server
		if adminServer != nil {
			adminServer.Stop()
			logger.Info("MultiAdmin gRPC server stopped")
		}
	})

	servenv.RunDefault()

	return nil
}

func init() {
	servenv.RegisterServiceCmd(Main)
}

// AdminStatusResponse represents the response from the admin status endpoint
type AdminStatusResponse struct {
	ServiceType string        `json:"service_type"`
	Status      string        `json:"status"`
	HTTPPort    int           `json:"http_port"`
	GRPCPort    int           `json:"grpc_port"`
	Uptime      time.Duration `json:"uptime"`
}

// AdminClustersResponse represents the response from the admin clusters endpoint
type AdminClustersResponse struct {
	Clusters []string `json:"clusters"`
	Count    int      `json:"count"`
}

// handleStatusEndpoint handles the HTTP endpoint that shows multiadmin status
func handleStatusEndpoint(w http.ResponseWriter, r *http.Request) {
	response := AdminStatusResponse{
		ServiceType: "multiadmin",
		Status:      "running",
		HTTPPort:    servenv.HTTPPort(),
		GRPCPort:    servenv.GRPCPort(),
		Uptime:      time.Since(servenv.GetInitStartTime()),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// handleClustersEndpoint handles the HTTP endpoint that shows cluster information
func handleClustersEndpoint(w http.ResponseWriter, r *http.Request) {
	// For now, return a simple response - this would be enhanced to use the admin server
	// to get actual cluster information
	if adminServer == nil {
		http.Error(w, "Admin server not initialized", http.StatusServiceUnavailable)
		return
	}

	// TODO: Use adminServer to get actual cluster list
	response := AdminClustersResponse{
		Clusters: []string{}, // Will be populated with actual cluster data
		Count:    0,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}
