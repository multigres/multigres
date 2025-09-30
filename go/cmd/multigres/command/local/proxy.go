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
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/web"

	"github.com/spf13/cobra"
)

// No custom flags needed - servenv provides --http-port, --log-level, --log-output

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

	fmt.Printf("Starting localproxy on port %d\n", servenv.HTTPPort())
	fmt.Printf("Loaded config from: %s\n", configFile)
	fmt.Printf("Route requests like: http://multiadmin.localhost:%d/...\n", servenv.HTTPPort())
	fmt.Printf("Or: http://multigateway.zone1.localhost:%d/...\n", servenv.HTTPPort())

	// Initialize servenv
	servenv.Init()

	// Register landing page and proxy handler
	// servenv already registered /css/, /live, /favicon.ico, /config
	// We just need to handle the root path and proxy requests
	servenv.HTTPHandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Check if this is for the proxy's own host
		host := r.Host
		if colonIdx := strings.Index(host, ":"); colonIdx != -1 {
			host = host[:colonIdx]
		}
		cleanHost := strings.TrimSuffix(host, ".localhost")

		// For proxy's own host, only handle root path (landing page)
		// Other paths like /css/, /live, etc. are handled by servenv's registered handlers
		if (cleanHost == "multigres" || cleanHost == "localhost") && r.URL.Path == "/" {
			renderLandingPage(w, r, cfg)
			return
		}

		// For all other requests, proxy them to backend services
		proxyRequest(w, r, cfg)
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

// ServiceLink represents a service with its URL for the landing page
type ServiceLink struct {
	Name string
	URL  string
}

// CellServiceGroup represents services grouped by cell
type CellServiceGroup struct {
	CellName string
	Services []ServiceLink
}

// LandingPageData holds data for the landing page template
type LandingPageData struct {
	GlobalServices []ServiceLink
	CellServices   []CellServiceGroup
}

// renderLandingPage renders an HTML page listing all available services
func renderLandingPage(w http.ResponseWriter, r *http.Request, cfg *proxyConfig) {
	// Determine the port to use in URLs
	// If request came with explicit port, use it; otherwise omit (default port)
	portSuffix := ""
	if colonIdx := strings.Index(r.Host, ":"); colonIdx != -1 {
		port := r.Host[colonIdx+1:]
		// Only include port in URL if it's not the default port (80 for http, 443 for https)
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		defaultPort := "80"
		if scheme == "https" {
			defaultPort = "443"
		}
		if port != defaultPort {
			portSuffix = ":" + port
		}
	}

	data := LandingPageData{
		GlobalServices: []ServiceLink{},
		CellServices:   []CellServiceGroup{},
	}

	// Build global services list
	for serviceName, servicePort := range cfg.GlobalServices {
		if servicePort > 0 {
			serviceURL := fmt.Sprintf("http://%s.localhost%s/", serviceName, portSuffix)
			data.GlobalServices = append(data.GlobalServices, ServiceLink{
				Name: serviceName,
				URL:  serviceURL,
			})
		}
	}

	// Build cell services list
	for cellName, services := range cfg.CellServices {
		cellGroup := CellServiceGroup{
			CellName: cellName,
			Services: []ServiceLink{},
		}
		for serviceName, servicePort := range services {
			if servicePort > 0 {
				serviceURL := fmt.Sprintf("http://%s.%s.localhost%s/", serviceName, cellName, portSuffix)
				displayName := fmt.Sprintf("%s (%s)", serviceName, cellName)
				cellGroup.Services = append(cellGroup.Services, ServiceLink{
					Name: displayName,
					URL:  serviceURL,
				})
			}
		}
		if len(cellGroup.Services) > 0 {
			data.CellServices = append(data.CellServices, cellGroup)
		}
	}

	// Execute template using web.Templates
	err := web.Templates.ExecuteTemplate(w, "proxy_landing.html", data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// proxyRequest proxies a request to the appropriate backend service based on subdomain
func proxyRequest(w http.ResponseWriter, r *http.Request, cfg *proxyConfig) {
	// Extract subdomain from Host header
	host := r.Host
	if colonIdx := strings.Index(host, ":"); colonIdx != -1 {
		host = host[:colonIdx]
	}

	// Remove .localhost suffix for routing
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
		http.Error(w, fmt.Sprintf("Service not found: %v", err), http.StatusNotFound)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ServeHTTP(w, r)
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
	PreRunE: servenv.CobraPreRunE,
	RunE:    proxy,
}

func init() {
	// Register servenv flags for service commands (--http-port, etc.)
	servenv.RegisterServiceCmd(ProxyCommand)
}
