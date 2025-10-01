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

	// Register proxy handlers for each configured service subdomain
	// This ensures requests to service.localhost/* get proxied, not handled by servenv

	// Helper to create a proxy handler for a specific target URL
	createProxyHandler := func(targetURL *url.URL) http.HandlerFunc {
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		return func(w http.ResponseWriter, r *http.Request) {
			proxy.ServeHTTP(w, r)
		}
	}

	// Register for global services
	for serviceName, port := range cfg.GlobalServices {
		if port > 0 {
			targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", port))
			host := fmt.Sprintf("%s.localhost/", serviceName)
			servenv.HTTPHandleFunc(host, createProxyHandler(targetURL))
		}
	}

	// Register for cell services
	for cellName, services := range cfg.CellServices {
		for serviceName, port := range services {
			if port > 0 {
				targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", port))
				host := fmt.Sprintf("%s.%s.localhost/", serviceName, cellName)
				servenv.HTTPHandleFunc(host, createProxyHandler(targetURL))
			}
		}
	}

	// Register landing page for proxy's own host
	servenv.HTTPHandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		renderLandingPage(w, r, cfg)
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
	Name      string
	URL       string // Proxied URL (through subdomain)
	DirectURL string // Direct URL (actual host:port)
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
			directURL := fmt.Sprintf("http://localhost:%d/", servicePort)
			data.GlobalServices = append(data.GlobalServices, ServiceLink{
				Name:      serviceName,
				URL:       serviceURL,
				DirectURL: directURL,
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
				directURL := fmt.Sprintf("http://localhost:%d/", servicePort)
				displayName := fmt.Sprintf("%s (%s)", serviceName, cellName)
				cellGroup.Services = append(cellGroup.Services, ServiceLink{
					Name:      displayName,
					URL:       serviceURL,
					DirectURL: directURL,
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
