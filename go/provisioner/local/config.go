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
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/stringutil"

	"gopkg.in/yaml.v3"
)

// CellConfig holds the configuration for a single cell
type CellConfig struct {
	Name     string `yaml:"name"`
	RootPath string `yaml:"root-path"`
}

// TopologyConfig holds the configuration for cluster topology
type TopologyConfig struct {
	Backend        string       `yaml:"backend"`
	GlobalRootPath string       `yaml:"global-root-path"`
	Cells          []CellConfig `yaml:"cells"`
}

// CellServicesConfig holds the service configuration for a specific cell
type CellServicesConfig struct {
	Multigateway MultigatewayConfig `yaml:"multigateway"`
	Multipooler  MultipoolerConfig  `yaml:"multipooler"`
	Multiorch    MultiorchConfig    `yaml:"multiorch"`
	Pgctld       PgctldConfig       `yaml:"pgctld"`
}

// LocalProvisionerConfig represents the typed configuration for the local provisioner
type LocalProvisionerConfig struct {
	RootWorkingDir string                        `yaml:"root-working-dir"`
	DefaultDbName  string                        `yaml:"default-db-name"`
	Etcd           EtcdConfig                    `yaml:"etcd"`
	Topology       TopologyConfig                `yaml:"topology"`
	Multiadmin     MultiadminConfig              `yaml:"multiadmin"`
	Cells          map[string]CellServicesConfig `yaml:"cells,omitempty"`
}

// EtcdConfig holds etcd service configuration
type EtcdConfig struct {
	Version string `yaml:"version"`
	DataDir string `yaml:"data-dir"`
	Port    int    `yaml:"port"`
}

// MultigatewayConfig holds multigateway service configuration
type MultigatewayConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	PgPort   int    `yaml:"pg-port"`
	LogLevel string `yaml:"log-level"`
}

// MultipoolerConfig holds multipooler service configuration
type MultipoolerConfig struct {
	Path           string `yaml:"path"`
	Database       string `yaml:"database"`
	TableGroup     string `yaml:"table-group"`
	ServiceID      string `yaml:"service-id"`
	PoolerDir      string `yaml:"pooler-dir"` // Directory path for PostgreSQL socket files
	PgPort         int    `yaml:"pg-port"`    // PostgreSQL port number (same as pgctld)
	HttpPort       int    `yaml:"http-port"`
	GrpcPort       int    `yaml:"grpc-port"`
	GRPCSocketFile string `yaml:"grpc-socket-file"` // Unix socket file path for gRPC
	LogLevel       string `yaml:"log-level"`
}

// MultiorchConfig holds multiorch service configuration
type MultiorchConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	LogLevel string `yaml:"log-level"`
}

// MultiadminConfig holds multiadmin service configuration
type MultiadminConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	LogLevel string `yaml:"log-level"`
}

// PgctldConfig holds pgctld service configuration
type PgctldConfig struct {
	Path           string `yaml:"path"`
	PoolerDir      string `yaml:"pooler-dir"`       // Base directory for this pgctld instance
	GrpcPort       int    `yaml:"grpc-port"`        // gRPC port for pgctld server
	GRPCSocketFile string `yaml:"grpc-socket-file"` // Unix socket file path for gRPC
	PgPort         int    `yaml:"pg-port"`          // PostgreSQL port
	PgDatabase     string `yaml:"pg-database"`      // PostgreSQL database name
	PgUser         string `yaml:"pg-user"`          // PostgreSQL username
	PgPwfile       string `yaml:"pg-pwfile"`        // PostgreSQL password file path (optional)
	Timeout        int    `yaml:"timeout"`          // Operation timeout in seconds
	LogLevel       string `yaml:"log-level"`        // Log level
}

// LoadConfig loads the provisioner-specific configuration from the given config paths
func (p *localProvisioner) LoadConfig(configPaths []string) error {
	// Try to find the config file in the provided paths
	for _, configPath := range configPaths {
		configFile := filepath.Join(configPath, "multigres.yaml")
		if _, err := os.Stat(configFile); err == nil {
			data, err := os.ReadFile(configFile)
			if err != nil {
				return fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}

			// Parse the full config file
			var fullConfig struct {
				Provisioner       string         `yaml:"provisioner"`
				ProvisionerConfig map[string]any `yaml:"provisioner-config,omitempty"`
			}
			if err := yaml.Unmarshal(data, &fullConfig); err != nil {
				return fmt.Errorf("failed to parse config file %s: %w", configFile, err)
			}

			// Validate that this is for the local provisioner
			if fullConfig.Provisioner != "local" {
				return fmt.Errorf("config file %s is for provisioner '%s', not 'local'", configFile, fullConfig.Provisioner)
			}

			if err := p.ValidateConfig(fullConfig.ProvisionerConfig); err != nil {
				return fmt.Errorf("failed to validate config file %s: %w", configFile, err)
			}

			// Convert the provisioner-config section to our typed config
			yamlData, err := yaml.Marshal(fullConfig.ProvisionerConfig)
			if err != nil {
				return fmt.Errorf("failed to marshal provisioner config: %w", err)
			}

			p.config = &LocalProvisionerConfig{}
			if err := yaml.Unmarshal(yamlData, p.config); err != nil {
				return fmt.Errorf("failed to unmarshal provisioner config: %w", err)
			}

			return nil
		}
	}

	return fmt.Errorf("multigres.yaml not found in any of the provided paths: %v", configPaths)
}

// DefaultConfig returns the default configuration for the local provisioner
func (p *localProvisioner) DefaultConfig(configPaths []string) map[string]any {
	baseDir := configPaths[0]
	binDir, err := getExecutablePath()
	if err != nil {
		binDir = "./bin"
		fmt.Println("Warning: Could not determine executable path, will use ./bin to find binaries")
	}

	// Generate service IDs for each cell using the same method as topo components
	serviceIDZone1 := stringutil.RandomString(8)
	serviceIDZone2 := stringutil.RandomString(8)
	tableGroup := "default"
	dbName := "postgres"

	// Create typed configuration with defaults
	localConfig := LocalProvisionerConfig{
		RootWorkingDir: baseDir,
		DefaultDbName:  dbName,
		Etcd: EtcdConfig{
			Version: "3.5.9",
			DataDir: filepath.Join(baseDir, "data", "etcd-data"),
			Port:    ports.DefaultEtcdPort,
		},
		Topology: TopologyConfig{
			Backend:        "etcd2",
			GlobalRootPath: "/multigres/global",
			Cells: []CellConfig{
				{
					Name:     "zone1",
					RootPath: "/multigres/zone1",
				},
				{
					Name:     "zone2",
					RootPath: "/multigres/zone2",
				},
			},
		},
		Multiadmin: MultiadminConfig{
			Path:     filepath.Join(binDir, "multiadmin"),
			HttpPort: ports.DefaultMultiadminHTTP,
			GrpcPort: ports.DefaultMultiadminGRPC,
			LogLevel: "info",
		},
		Cells: map[string]CellServicesConfig{
			"zone1": {
				Multigateway: MultigatewayConfig{
					Path:     filepath.Join(binDir, "multigateway"),
					HttpPort: ports.DefaultMultigatewayHTTP,
					GrpcPort: ports.DefaultMultigatewayGRPC,
					PgPort:   ports.DefaultMultigatewayPG,
					LogLevel: "info",
				},
				Multipooler: MultipoolerConfig{
					Path:           filepath.Join(binDir, "multipooler"),
					Database:       dbName,
					TableGroup:     tableGroup,
					ServiceID:      serviceIDZone1,
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone1),
					PgPort:         ports.DefaultPostgresPort, // Same as pgctld for this zone
					HttpPort:       ports.DefaultMultipoolerHTTP,
					GrpcPort:       ports.DefaultMultipoolerGRPC,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "multipooler-zone1.sock"),
					LogLevel:       "info",
				},
				Multiorch: MultiorchConfig{
					Path:     filepath.Join(binDir, "multiorch"),
					HttpPort: ports.DefaultMultiorchHTTP,
					GrpcPort: ports.DefaultMultiorchGRPC,
					LogLevel: "info",
				},
				Pgctld: PgctldConfig{
					Path:           filepath.Join(binDir, "pgctld"),
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone1),
					GrpcPort:       ports.DefaultPgctldGRPC,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "pgctld-zone1.sock"),
					PgPort:         ports.DefaultPostgresPort,
					PgDatabase:     dbName,
					PgUser:         "postgres",
					PgPwfile:       filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone1), "pgpassword.txt"),
					Timeout:        30,
					LogLevel:       "info",
				},
			},
			"zone2": {
				Multigateway: MultigatewayConfig{
					Path:     filepath.Join(binDir, "multigateway"),
					HttpPort: ports.DefaultMultigatewayHTTP + 100,
					GrpcPort: ports.DefaultMultigatewayGRPC + 100,
					PgPort:   ports.DefaultPostgresPort + 100,
					LogLevel: "info",
				},
				Multipooler: MultipoolerConfig{
					Path:           filepath.Join(binDir, "multipooler"),
					Database:       dbName,
					TableGroup:     tableGroup,
					ServiceID:      serviceIDZone2,
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone2),
					PgPort:         ports.DefaultPostgresPort + 100,
					HttpPort:       ports.DefaultMultipoolerHTTP + 100,
					GrpcPort:       ports.DefaultMultipoolerGRPC + 100,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "multipooler-zone2.sock"),
					LogLevel:       "info",
				},
				Multiorch: MultiorchConfig{
					Path:     filepath.Join(binDir, "multiorch"),
					HttpPort: ports.DefaultMultiorchHTTP + 100,
					GrpcPort: ports.DefaultMultiorchGRPC + 100,
					LogLevel: "info",
				},
				Pgctld: PgctldConfig{
					Path:           filepath.Join(binDir, "pgctld"),
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone2),
					GrpcPort:       ports.DefaultPgctldGRPC + 100,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "pgctld-zone2.sock"),
					PgPort:         ports.DefaultPostgresPort + 100,
					PgDatabase:     dbName,
					PgUser:         "postgres",
					PgPwfile:       filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone2), "pgpassword.txt"),
					Timeout:        30,
					LogLevel:       "info",
				},
			},
		},
	}

	// Convert to map[string]any via YAML marshaling to preserve struct ordering
	yamlData, err := yaml.Marshal(localConfig)
	if err != nil {
		// Fallback to empty config if marshaling fails
		fmt.Printf("Warning: failed to marshal default config: %v\n", err)
		return map[string]any{}
	}

	var configMap map[string]any
	if err := yaml.Unmarshal(yamlData, &configMap); err != nil {
		// Fallback to empty config if unmarshaling fails
		fmt.Printf("Warning: failed to unmarshal default config: %v\n", err)
		return map[string]any{}
	}

	return configMap
}

// getServiceConfig gets the configuration for a specific service (global services only)
func (p *localProvisioner) getServiceConfig(service string) map[string]any {
	switch service {
	case "etcd":
		return map[string]any{
			"version":  p.config.Etcd.Version,
			"data-dir": p.config.Etcd.DataDir,
			"port":     p.config.Etcd.Port,
		}
	case "multiadmin":
		return map[string]any{
			"path":      p.config.Multiadmin.Path,
			"http_port": p.config.Multiadmin.HttpPort,
			"grpc_port": p.config.Multiadmin.GrpcPort,
			"log_level": p.config.Multiadmin.LogLevel,
		}
	default:
		// Return empty config if not found
		return map[string]any{}
	}
}

// getCellServiceConfig gets the configuration for a specific service in a specific cell
func (p *localProvisioner) getCellServiceConfig(cellName, service string) (map[string]any, error) {
	cellServices, exists := p.config.Cells[cellName]
	if !exists {
		return nil, fmt.Errorf("cell %s not found in configuration", cellName)
	}

	switch service {
	case "multigateway":
		return map[string]any{
			"path":      cellServices.Multigateway.Path,
			"http_port": cellServices.Multigateway.HttpPort,
			"grpc_port": cellServices.Multigateway.GrpcPort,
			"pg_port":   cellServices.Multigateway.PgPort,
			"log_level": cellServices.Multigateway.LogLevel,
		}, nil
	case "multipooler":
		return map[string]any{
			"path":             cellServices.Multipooler.Path,
			"database":         cellServices.Multipooler.Database,
			"table_group":      cellServices.Multipooler.TableGroup,
			"service-id":       cellServices.Multipooler.ServiceID,
			"http_port":        cellServices.Multipooler.HttpPort,
			"grpc_port":        cellServices.Multipooler.GrpcPort,
			"grpc_socket_file": cellServices.Multipooler.GRPCSocketFile,
			"log_level":        cellServices.Multipooler.LogLevel,
			"pooler_dir":       cellServices.Multipooler.PoolerDir,
			"pg_port":          cellServices.Multipooler.PgPort,
		}, nil
	case "multiorch":
		return map[string]any{
			"path":      cellServices.Multiorch.Path,
			"http_port": cellServices.Multiorch.HttpPort,
			"grpc_port": cellServices.Multiorch.GrpcPort,
			"log_level": cellServices.Multiorch.LogLevel,
		}, nil
	case "pgctld":
		return map[string]any{
			"path":             cellServices.Pgctld.Path,
			"pooler_dir":       cellServices.Pgctld.PoolerDir,
			"grpc_port":        cellServices.Pgctld.GrpcPort,
			"grpc_socket_file": cellServices.Pgctld.GRPCSocketFile,
			"pg_port":          cellServices.Pgctld.PgPort,
			"pg_database":      cellServices.Pgctld.PgDatabase,
			"pg_user":          cellServices.Pgctld.PgUser,
			"pg_pwfile":        cellServices.Pgctld.PgPwfile,
			"timeout":          cellServices.Pgctld.Timeout,
			"log_level":        cellServices.Pgctld.LogLevel,
		}, nil
	default:
		return nil, fmt.Errorf("unknown service %s", service)
	}
}
