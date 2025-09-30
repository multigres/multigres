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

package local

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/spf13/cobra"
)

var (
	proxyPort      int
	proxyLogLevel  string
	proxyLogOutput string
)

// proxyConfig holds the parsed configuration for routing
type proxyConfig struct {
	GlobalServices map[string]int            // service name -> http port
	CellServices   map[string]map[string]int // cell name -> service name -> http port
}

// proxy starts a local HTTP proxy that routes subdomain requests to backend services
func proxy(cmd *cobra.Command, args []string) error {
	// Load configuration to get service ports
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		configPaths = []string{"."}
	}

	// Load and parse multigres.yaml
	cfg, configFile, err := loadProxyConfig(configPaths)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Setup logging if log output specified
	if proxyLogOutput != "" {
		logFile, err := os.OpenFile(proxyLogOutput, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		defer logFile.Close()
		log.SetOutput(logFile)
	}

	log.Printf("Starting localproxy on port %d", proxyPort)
	log.Printf("Loaded config from: %s", configFile)

	// Create proxy handler
	handler := createProxyHandler(cfg)

	// Start HTTP server
	addr := fmt.Sprintf(":%d", proxyPort)
	log.Printf("Listening on %s", addr)
	fmt.Printf("Local proxy listening on http://localhost:%d\n", proxyPort)
	fmt.Printf("Route requests like: http://multiadmin.localhost:%d/...\n", proxyPort)
	fmt.Printf("Or: http://multigateway.zone1.localhost:%d/...\n", proxyPort)

	if err := http.ListenAndServe(addr, handler); err != nil {
		return fmt.Errorf("proxy server error: %w", err)
	}

	return nil
}

// loadProxyConfig loads the multigres.yaml configuration and extracts routing info
func loadProxyConfig(configPaths []string) (*proxyConfig, string, error) {
	// Find config file
	var configFile string
	for _, path := range configPaths {
		testPath := filepath.Join(path, "multigres.yaml")
		if _, err := os.Stat(testPath); err == nil {
			configFile = testPath
			break
		}
	}

	if configFile == "" {
		return nil, "", fmt.Errorf("multigres.yaml not found in any of the provided paths: %v", configPaths)
	}

	// Read and parse config
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig struct {
		ProvisionerConfig struct {
			Multiadmin struct {
				HttpPort int `yaml:"http-port"`
			} `yaml:"multiadmin"`
			Cells map[string]struct {
				Multigateway struct {
					HttpPort int `yaml:"http-port"`
				} `yaml:"multigateway"`
				Multipooler struct {
					HttpPort int `yaml:"http-port"`
				} `yaml:"multipooler"`
				Multiorch struct {
					HttpPort int `yaml:"http-port"`
				} `yaml:"multiorch"`
			} `yaml:"cells"`
		} `yaml:"provisioner-config"`
	}

	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		return nil, "", fmt.Errorf("failed to parse config: %w", err)
	}

	// Build proxy config
	cfg := &proxyConfig{
		GlobalServices: make(map[string]int),
		CellServices:   make(map[string]map[string]int),
	}

	// Add global services
	if rawConfig.ProvisionerConfig.Multiadmin.HttpPort > 0 {
		cfg.GlobalServices["multiadmin"] = rawConfig.ProvisionerConfig.Multiadmin.HttpPort
	}

	// Add cell services
	for cellName, cellConfig := range rawConfig.ProvisionerConfig.Cells {
		cfg.CellServices[cellName] = make(map[string]int)

		if cellConfig.Multigateway.HttpPort > 0 {
			cfg.CellServices[cellName]["multigateway"] = cellConfig.Multigateway.HttpPort
		}
		if cellConfig.Multipooler.HttpPort > 0 {
			cfg.CellServices[cellName]["multipooler"] = cellConfig.Multipooler.HttpPort
		}
		if cellConfig.Multiorch.HttpPort > 0 {
			cfg.CellServices[cellName]["multiorch"] = cellConfig.Multiorch.HttpPort
		}
	}

	return cfg, configFile, nil
}

// createProxyHandler creates an HTTP handler that routes based on subdomain
func createProxyHandler(cfg *proxyConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract subdomain from Host header
		host := r.Host
		if colonIdx := strings.Index(host, ":"); colonIdx != -1 {
			host = host[:colonIdx]
		}

		// Health check endpoint for proxy itself - on multigres.localhost or plain localhost
		cleanHost := strings.TrimSuffix(host, ".localhost")
		if (cleanHost == "multigres" || cleanHost == "localhost") && r.URL.Path == "/live" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		}

		// Remove .localhost suffix
		host = strings.TrimSuffix(host, ".localhost")

		// Parse subdomain parts
		parts := strings.Split(host, ".")
		if len(parts) == 0 {
			http.Error(w, "Invalid host format. Expected: service.localhost or service.cell.localhost", http.StatusBadRequest)
			return
		}

		// Route based on service name
		var targetURL *url.URL
		var err error

		if len(parts) == 1 {
			// Global service: multiadmin.localhost
			serviceName := parts[0]
			targetURL, err = getGlobalServiceURL(cfg, serviceName)
		} else if len(parts) == 2 {
			// Cell service: multigateway.zone1.localhost
			serviceName := parts[0]
			cellName := parts[1]
			targetURL, err = getCellServiceURL(cfg, serviceName, cellName)
		} else {
			http.Error(w, fmt.Sprintf("Unknown host format: %s. Expected: service.localhost or service.cell.localhost", r.Host), http.StatusNotFound)
			return
		}

		if err != nil {
			log.Printf("Failed to route %s: %v", r.Host, err)
			http.Error(w, fmt.Sprintf("Service not found: %v", err), http.StatusNotFound)
			return
		}

		// Log the request
		log.Printf("%s %s -> %s%s", r.Method, r.Host, targetURL.String(), r.URL.Path)

		// Create reverse proxy
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		proxy.ServeHTTP(w, r)
	})
}

// getGlobalServiceURL returns the target URL for a global service
func getGlobalServiceURL(cfg *proxyConfig, serviceName string) (*url.URL, error) {
	port, ok := cfg.GlobalServices[serviceName]
	if !ok || port == 0 {
		return nil, fmt.Errorf("unknown or unconfigured global service: %s", serviceName)
	}

	return url.Parse(fmt.Sprintf("http://localhost:%d", port))
}

// getCellServiceURL returns the target URL for a cell-scoped service
func getCellServiceURL(cfg *proxyConfig, serviceName, cellName string) (*url.URL, error) {
	cellServices, ok := cfg.CellServices[cellName]
	if !ok {
		return nil, fmt.Errorf("unknown cell: %s", cellName)
	}

	port, ok := cellServices[serviceName]
	if !ok || port == 0 {
		return nil, fmt.Errorf("unknown or unconfigured service %s in cell %s", serviceName, cellName)
	}

	return url.Parse(fmt.Sprintf("http://localhost:%d", port))
}

var ProxyCommand = &cobra.Command{
	Use:   "proxy",
	Short: "Start local HTTP proxy for service routing",
	Long: `Start a local HTTP proxy that routes subdomain-based requests to backend services.

Examples:
  http://multiadmin.localhost:8080/     -> routes to multiadmin HTTP port
  http://multigateway.zone1.localhost:8080/ -> routes to multigateway in zone1`,
	RunE: proxy,
}

func init() {
	ProxyCommand.Flags().IntVar(&proxyPort, "port", 8080, "Port to listen on")
	ProxyCommand.Flags().StringVar(&proxyLogLevel, "log-level", "info", "Log level")
	ProxyCommand.Flags().StringVar(&proxyLogOutput, "log-output", "", "Log output file (default: stderr)")
}
