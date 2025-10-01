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

// Package local implements the local proxy command for routing HTTP
// requests to backend services. The proxy uses subdomain routing to
// route to a particular service, like
// http://multigateway-cell1.localhost:<local proxy port>/
// The root path redirects to multiadmin for service navigation.
// The local proxy won't be necessary for production, where we can
// use k8s Ingress instead.
package local

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// No custom flags needed - servenv provides --http-port, --log-level, --log-output

// proxyConfig holds the parsed configuration for routing
type proxyConfig struct {
	GlobalServices map[string]int            // service name -> http port
	CellServices   map[string]map[string]int // cell name -> service name -> http port
}

// normalizeForSubdomain removes hyphens from a name to avoid conflicts with the separator.
// This allows using a single hyphen as the service-cell separator in subdomains while
// supporting hyphens in service and cell names.
func normalizeForSubdomain(name string) string {
	return strings.ReplaceAll(name, "-", "")
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

	fmt.Printf("Starting localproxy on port %d\n", servenv.HTTPPort())
	fmt.Printf("Loaded config from: %s\n", configFile)
	fmt.Printf("Route requests like: http://multiadmin.localhost:%d/...\n", servenv.HTTPPort())
	fmt.Printf("Or: http://multigateway-zone1.localhost:%d/...\n", servenv.HTTPPort())

	// Initialize servenv
	servenv.Init()

	// Register proxy handlers for each configured service subdomain
	// This ensures requests to service.localhost/* get proxied, not handled by servenv

	// Helper to create a proxy handler for a specific target URL
	createProxyHandler := func(targetURL *url.URL) http.HandlerFunc {
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		return func(w http.ResponseWriter, r *http.Request) {
			proxy.ServeHTTP(w, r)
		}
	}

	// Register proxy handlers for global services
	for serviceName, port := range cfg.GlobalServices {
		if port > 0 {
			targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", port))
			host := fmt.Sprintf("%s.localhost/", serviceName)
			servenv.HTTPHandleFunc(host, createProxyHandler(targetURL))
		}
	}

	// Register proxy handlers for cell services
	for cellName, services := range cfg.CellServices {
		for serviceName, port := range services {
			if port > 0 {
				targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", port))
				// Normalize names to remove hyphens, use single hyphen as separator
				normalizedService := normalizeForSubdomain(serviceName)
				normalizedCell := normalizeForSubdomain(cellName)
				host := fmt.Sprintf("%s-%s.localhost/", normalizedService, normalizedCell)
				servenv.HTTPHandleFunc(host, createProxyHandler(targetURL))
			}
		}
	}

	// Register redirect for proxy's own host - redirect root to multiadmin
	// Only redirect when accessing localhost directly, not subdomains
	servenv.HTTPHandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Extract hostname without port
		hostname := r.Host
		if colonIdx := strings.Index(hostname, ":"); colonIdx != -1 {
			hostname = hostname[:colonIdx]
		}

		// Only redirect if accessing localhost or 127.0.0.1 directly
		if hostname == "localhost" || hostname == "127.0.0.1" {
			// Redirect to multiadmin via the proxy
			multiadminPort := cfg.GlobalServices["multiadmin"]
			if multiadminPort == 0 {
				http.Error(w, "Multiadmin not configured", http.StatusServiceUnavailable)
				return
			}

			// Redirect to multiadmin through the proxy (using proxy's port)
			proxyPort := servenv.HTTPPort()
			redirectURL := fmt.Sprintf("http://multiadmin.localhost:%d/", proxyPort)
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return
		}

		// For other hosts (subdomains), return 404 - they should be handled by subdomain handlers
		http.NotFound(w, r)
	})

	// Use servenv to run the HTTP server
	servenv.RunDefault()

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

var ProxyCommand = &cobra.Command{
	Use:   "proxy",
	Short: "Start local HTTP proxy for service routing",
	Long: `Start a local HTTP proxy that routes subdomain-based requests to backend services.

Examples:
  http://multiadmin.localhost:8080/     -> routes to multiadmin HTTP port
  http://multigateway-zone1.localhost:8080/ -> routes to multigateway in zone1`,
	PreRunE: servenv.CobraPreRunE,
	RunE:    proxy,
}

func init() {
	// Register servenv flags for service commands (--http-port, etc.)
	servenv.RegisterServiceCmd(ProxyCommand)
}
