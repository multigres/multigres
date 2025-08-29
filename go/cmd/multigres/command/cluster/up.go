// Copyright 2025 The Multigres Authors.
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

package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// ServiceInfo holds information about a provisioned service
type ServiceInfo struct {
	Name    string
	FQDN    string
	Ports   map[string]int
	LogFile string
}

// ServiceSummary holds all provisioned services
type ServiceSummary struct {
	Services []ServiceInfo
}

// AddService adds a service to the summary
func (s *ServiceSummary) AddService(name string, result *provisioner.ProvisionResult) {
	// Extract log file path from metadata if available
	logFile := ""
	if result.Metadata != nil {
		if logPath, ok := result.Metadata["log_file"].(string); ok {
			logFile = logPath
		}
	}

	s.Services = append(s.Services, ServiceInfo{
		Name:    name,
		FQDN:    result.FQDN,
		Ports:   result.Ports,
		LogFile: logFile,
	})
}

// PrintSummary prints a formatted summary of all provisioned services
func (s *ServiceSummary) PrintSummary() {
	fmt.Println(strings.Repeat("=", 65))
	fmt.Println("🎉 - Multigres cluster started successfully!")
	fmt.Println(strings.Repeat("=", 65))
	fmt.Println()
	fmt.Println("Provisioned Services")
	fmt.Println("--------------------")
	fmt.Println()

	for _, service := range s.Services {
		displayName := service.Name
		switch service.Name {
		case "etcd":
			displayName = "etcd"
		case "multigateway":
			displayName = "Multigateway"
		case "multipooler":
			displayName = "Multipooler"
		case "multiorch":
			displayName = "MultiOrchestrator"
		}
		fmt.Printf("%s\n", displayName)
		fmt.Printf("   Host: %s\n", service.FQDN)

		if len(service.Ports) == 1 {
			// Single port format
			for portName, portNum := range service.Ports {
				if portName == "http_port" {
					fmt.Printf("   Port: %d → http://%s:%d\n", portNum, service.FQDN, portNum)
				} else {
					fmt.Printf("   Port: %d\n", portNum)
				}
			}
		} else if len(service.Ports) > 1 {
			// Multiple ports format
			fmt.Printf("   Ports:\n")
			for portName, portNum := range service.Ports {
				displayPortName := strings.ToUpper(strings.Replace(portName, "_port", "", 1))
				if portName == "http_port" {
					fmt.Printf("     - %s: %d → http://%s:%d\n", displayPortName, portNum, service.FQDN, portNum)
				} else {
					fmt.Printf("     - %s: %d\n", displayPortName, portNum)
				}
			}
		}

		if service.LogFile != "" {
			fmt.Printf("   Log: %s\n", service.LogFile)
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("=", 65))
	fmt.Println("✨ - Next steps:")

	// Find services with HTTP ports and add direct links
	for _, service := range s.Services {
		if httpPort, exists := service.Ports["http_port"]; exists {
			url := fmt.Sprintf("http://%s:%d", service.FQDN, httpPort)
			fmt.Printf("- Open %s in your browser: %s\n", service.Name, url)
		}
	}
	fmt.Println("- 🐘 Connect to PostgreSQL via Multigateway")
	fmt.Println("- 🔍 Run `multigres cluster status` to check health")
	fmt.Println("- 🛑 Run `multigres cluster down` to stop the cluster")
	fmt.Println(strings.Repeat("=", 65))
}

// runUp handles the cluster up command
func runUp(cmd *cobra.Command, args []string) error {
	servenv.FireRunHooks()

	fmt.Println("Multigres — Distributed Postgres made easy")
	fmt.Println("=================================================================")
	fmt.Println("✨ Bootstrapping your local Multigres cluster — this may take a few moments ✨")

	// Get config paths from flags
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		configPaths = []string{"."}
	}

	// Load configuration to determine provisioner type
	config, configFile, err := LoadConfig(configPaths)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	fmt.Println("📄 - Config loaded from: " + configFile)

	// Create provisioner instance
	p, err := provisioner.GetProvisioner(config.Provisioner)
	if err != nil {
		return fmt.Errorf("failed to create provisioner '%s': %w", config.Provisioner, err)
	}

	// Let provisioner load its own configuration
	if err := p.LoadConfig(configPaths); err != nil {
		return fmt.Errorf("failed to load provisioner config: %w", err)
	}

	fmt.Println("🛠️  - Provisioner: " + p.Name())
	fmt.Println()
	fmt.Println("👋 Here we go! Starting core services...")
	fmt.Println(strings.Repeat("=", 65))
	fmt.Println()

	ctx := context.Background()

	// Initialize service summary to track all provisioned services
	summary := &ServiceSummary{}

	// Use the provisioner's Bootstrap method to provision all services
	allResults, err := p.Bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("cluster bootstrap failed: %w", err)
	}

	// Add all returned services to summary dynamically
	for _, result := range allResults {
		summary.AddService(result.ServiceName, result)
	}

	// Print comprehensive summary
	fmt.Println()
	summary.PrintSummary()
	return nil
}

var UpCommand = &cobra.Command{
	Use:   "up",
	Short: "Start local cluster",
	Long:  "Start a local Multigres cluster using the configuration created with 'multigres cluster init'.",
	RunE:  runUp,
}

func init() {
	// No additional flags needed - config-path is provided by viperutil via root command
}
