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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/provisioner/local/pgbackrest"
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
	BackupRepoPath string                        `yaml:"backup-repo-path,omitempty"`
	Etcd           EtcdConfig                    `yaml:"etcd"`
	Topology       TopologyConfig                `yaml:"topology"`
	Multiadmin     MultiadminConfig              `yaml:"multiadmin"`
	Cells          map[string]CellServicesConfig `yaml:"cells,omitempty"`
}

// EtcdConfig holds etcd service configuration
type EtcdConfig struct {
	Version  string `yaml:"version"`
	DataDir  string `yaml:"data-dir"`
	Port     int    `yaml:"port"`                // Client port
	PeerPort int    `yaml:"peer-port,omitempty"` // Optional peer port, defaults to Port+1
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
	Shard          string `yaml:"shard"`
	ServiceID      string `yaml:"service-id"`
	PoolerDir      string `yaml:"pooler-dir"`  // Directory path for PostgreSQL socket files
	PgPort         int    `yaml:"pg-port"`     // PostgreSQL port number (same as pgctld)
	BackupConf     string `yaml:"backup-conf"` // Path to backup configuration file (pgbackrest.conf)
	HttpPort       int    `yaml:"http-port"`
	GrpcPort       int    `yaml:"grpc-port"`
	GRPCSocketFile string `yaml:"grpc-socket-file"` // Unix socket file path for gRPC
	LogLevel       string `yaml:"log-level"`
}

// MultiorchConfig holds multiorch service configuration
type MultiorchConfig struct {
	Path                           string `yaml:"path"`
	HttpPort                       int    `yaml:"http-port"`
	GrpcPort                       int    `yaml:"grpc-port"`
	LogLevel                       string `yaml:"log-level"`
	ClusterMetadataRefreshInterval string `yaml:"cluster-metadata-refresh-interval,omitempty"`
	PoolerHealthCheckInterval      string `yaml:"pooler-health-check-interval,omitempty"`
	RecoveryCycleInterval          string `yaml:"recovery-cycle-interval,omitempty"`
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
	PgPwfile       string `yaml:"pg-pwfile"`        // Source password file path; copied to pooler-dir/pgpassword.txt during init
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
	serviceIDZone3 := stringutil.RandomString(8)
	tableGroup := "default"
	shard := "0-inf"
	dbName := constants.DefaultPostgresDatabase

	// Create typed configuration with defaults
	localConfig := LocalProvisionerConfig{
		RootWorkingDir: baseDir,
		DefaultDbName:  dbName,
		BackupRepoPath: filepath.Join(baseDir, "data", "backups"),
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
				{
					Name:     "zone3",
					RootPath: "/multigres/zone3",
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
					Shard:          shard,
					ServiceID:      serviceIDZone1,
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone1),
					PgPort:         ports.DefaultLocalPostgresPort, // Same as pgctld for this zone
					BackupConf:     filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone1), "pgbackrest.conf"),
					HttpPort:       ports.DefaultMultipoolerHTTP,
					GrpcPort:       ports.DefaultMultipoolerGRPC,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "multipooler-zone1.sock"),
					LogLevel:       "info",
				},
				Multiorch: MultiorchConfig{
					Path:                           filepath.Join(binDir, "multiorch"),
					HttpPort:                       ports.DefaultMultiorchHTTP,
					GrpcPort:                       ports.DefaultMultiorchGRPC,
					LogLevel:                       "info",
					ClusterMetadataRefreshInterval: "500ms",
					PoolerHealthCheckInterval:      "500ms",
					RecoveryCycleInterval:          "500ms",
				},
				Pgctld: PgctldConfig{
					Path:           filepath.Join(binDir, "pgctld"),
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone1),
					GrpcPort:       ports.DefaultPgctldGRPC,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "pgctld-zone1.sock"),
					PgPort:         ports.DefaultLocalPostgresPort,
					PgDatabase:     dbName,
					PgUser:         constants.DefaultPostgresUser,
					Timeout:        30,
					LogLevel:       "info",
				},
			},
			"zone2": {
				Multigateway: MultigatewayConfig{
					Path:     filepath.Join(binDir, "multigateway"),
					HttpPort: ports.DefaultMultigatewayHTTP + 1,
					GrpcPort: ports.DefaultMultigatewayGRPC + 1,
					PgPort:   ports.DefaultMultigatewayPG + 1,
					LogLevel: "info",
				},
				Multipooler: MultipoolerConfig{
					Path:           filepath.Join(binDir, "multipooler"),
					Database:       dbName,
					TableGroup:     tableGroup,
					Shard:          shard,
					ServiceID:      serviceIDZone2,
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone2),
					PgPort:         ports.DefaultLocalPostgresPort + 1,
					BackupConf:     filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone2), "pgbackrest.conf"),
					HttpPort:       ports.DefaultMultipoolerHTTP + 1,
					GrpcPort:       ports.DefaultMultipoolerGRPC + 1,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "multipooler-zone2.sock"),
					LogLevel:       "info",
				},
				Multiorch: MultiorchConfig{
					Path:                           filepath.Join(binDir, "multiorch"),
					HttpPort:                       ports.DefaultMultiorchHTTP + 1,
					GrpcPort:                       ports.DefaultMultiorchGRPC + 1,
					LogLevel:                       "info",
					ClusterMetadataRefreshInterval: "500ms",
					PoolerHealthCheckInterval:      "500ms",
					RecoveryCycleInterval:          "500ms",
				},
				Pgctld: PgctldConfig{
					Path:           filepath.Join(binDir, "pgctld"),
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone2),
					GrpcPort:       ports.DefaultPgctldGRPC + 1,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "pgctld-zone2.sock"),
					PgPort:         ports.DefaultLocalPostgresPort + 1,
					PgDatabase:     dbName,
					PgUser:         constants.DefaultPostgresUser,
					PgPwfile:       filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone2), "pgpassword.txt"),
					Timeout:        30,
					LogLevel:       "info",
				},
			},
			"zone3": {
				Multigateway: MultigatewayConfig{
					Path:     filepath.Join(binDir, "multigateway"),
					HttpPort: ports.DefaultMultigatewayHTTP + 2,
					GrpcPort: ports.DefaultMultigatewayGRPC + 2,
					PgPort:   ports.DefaultMultigatewayPG + 2,
					LogLevel: "info",
				},
				Multipooler: MultipoolerConfig{
					Path:           filepath.Join(binDir, "multipooler"),
					Database:       dbName,
					TableGroup:     tableGroup,
					Shard:          shard,
					ServiceID:      serviceIDZone3,
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone3),
					PgPort:         ports.DefaultLocalPostgresPort + 2,
					BackupConf:     filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone3), "pgbackrest.conf"),
					HttpPort:       ports.DefaultMultipoolerHTTP + 2,
					GrpcPort:       ports.DefaultMultipoolerGRPC + 2,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "multipooler-zone3.sock"),
					LogLevel:       "info",
				},
				Multiorch: MultiorchConfig{
					Path:                           filepath.Join(binDir, "multiorch"),
					HttpPort:                       ports.DefaultMultiorchHTTP + 2,
					GrpcPort:                       ports.DefaultMultiorchGRPC + 2,
					LogLevel:                       "info",
					ClusterMetadataRefreshInterval: "500ms",
					PoolerHealthCheckInterval:      "500ms",
					RecoveryCycleInterval:          "500ms",
				},
				Pgctld: PgctldConfig{
					Path:           filepath.Join(binDir, "pgctld"),
					PoolerDir:      GeneratePoolerDir(baseDir, serviceIDZone3),
					GrpcPort:       ports.DefaultPgctldGRPC + 2,
					GRPCSocketFile: filepath.Join(baseDir, "sockets", "pgctld-zone3.sock"),
					PgPort:         ports.DefaultLocalPostgresPort + 2,
					PgDatabase:     dbName,
					PgUser:         constants.DefaultPostgresUser,
					PgPwfile:       filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone3), "pgpassword.txt"),
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
			"version":   p.config.Etcd.Version,
			"data-dir":  p.config.Etcd.DataDir,
			"port":      p.config.Etcd.Port,
			"peer-port": p.config.Etcd.PeerPort,
		}
	case constants.ServiceMultiadmin:
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
	case constants.ServiceMultigateway:
		return map[string]any{
			"path":      cellServices.Multigateway.Path,
			"http_port": cellServices.Multigateway.HttpPort,
			"grpc_port": cellServices.Multigateway.GrpcPort,
			"pg_port":   cellServices.Multigateway.PgPort,
			"log_level": cellServices.Multigateway.LogLevel,
		}, nil
	case constants.ServiceMultipooler:
		return map[string]any{
			"path":             cellServices.Multipooler.Path,
			"database":         cellServices.Multipooler.Database,
			"table_group":      cellServices.Multipooler.TableGroup,
			"shard":            cellServices.Multipooler.Shard,
			"service-id":       cellServices.Multipooler.ServiceID,
			"http_port":        cellServices.Multipooler.HttpPort,
			"grpc_port":        cellServices.Multipooler.GrpcPort,
			"grpc_socket_file": cellServices.Multipooler.GRPCSocketFile,
			"log_level":        cellServices.Multipooler.LogLevel,
			"pooler_dir":       cellServices.Multipooler.PoolerDir,
			"pg_port":          cellServices.Multipooler.PgPort,
			"backup_conf":      cellServices.Multipooler.BackupConf,
		}, nil
	case constants.ServiceMultiorch:
		return map[string]any{
			"path":                              cellServices.Multiorch.Path,
			"http_port":                         cellServices.Multiorch.HttpPort,
			"grpc_port":                         cellServices.Multiorch.GrpcPort,
			"log_level":                         cellServices.Multiorch.LogLevel,
			"cluster_metadata_refresh_interval": cellServices.Multiorch.ClusterMetadataRefreshInterval,
			"pooler_health_check_interval":      cellServices.Multiorch.PoolerHealthCheckInterval,
			"recovery_cycle_interval":           cellServices.Multiorch.RecoveryCycleInterval,
		}, nil
	case constants.ServicePgctld:
		return map[string]any{
			"path":             cellServices.Pgctld.Path,
			"pooler_dir":       cellServices.Pgctld.PoolerDir,
			"grpc_port":        cellServices.Pgctld.GrpcPort,
			"grpc_socket_file": cellServices.Pgctld.GRPCSocketFile,
			"pg_port":          cellServices.Pgctld.PgPort,
			"pg_database":      cellServices.Pgctld.PgDatabase,
			"pg_user":          cellServices.Pgctld.PgUser,
			"timeout":          cellServices.Pgctld.Timeout,
			"log_level":        cellServices.Pgctld.LogLevel,
		}, nil
	default:
		return nil, fmt.Errorf("unknown service %s", service)
	}
}

// GeneratePgBackRestConfigs generates pgBackRest configuration files for all poolers
// This should be called after the configuration is loaded and before starting services
func (p *localProvisioner) GeneratePgBackRestConfigs() error {
	if p.config == nil {
		return fmt.Errorf("configuration not loaded")
	}

	// Set default backup repository path if not specified
	if p.config.BackupRepoPath == "" {
		p.config.BackupRepoPath = filepath.Join(p.config.RootWorkingDir, "data", "backups")
	}

	// Create the backup repository directory if it doesn't exist
	if err := os.MkdirAll(p.config.BackupRepoPath, 0o755); err != nil {
		return fmt.Errorf("failed to create backup repository directory %s: %w", p.config.BackupRepoPath, err)
	}

	// Create the pgBackRest log directory if it doesn't exist
	pgBackRestLogPath := filepath.Join(p.config.RootWorkingDir, "logs", "dbs", "postgres", "pgbackrest")
	if err := os.MkdirAll(pgBackRestLogPath, 0o755); err != nil {
		return fmt.Errorf("failed to create pgBackRest log directory %s: %w", pgBackRestLogPath, err)
	}

	// Get sorted list of all cell names for consistent ordering
	var allCells []string
	for cellName := range p.config.Cells {
		allCells = append(allCells, cellName)
	}
	sort.Strings(allCells)

	// Generate TLS certificates for pgBackRest (shared across all nodes)
	tlsDir := filepath.Join(p.config.BackupRepoPath, "tls")
	caCertPath := filepath.Join(tlsDir, "ca.crt")
	caKeyPath := filepath.Join(tlsDir, "ca.key")
	serverCertPath := filepath.Join(tlsDir, "server.crt")
	serverKeyPath := filepath.Join(tlsDir, "server.key")

	// Generate CA certificate
	if err := pgbackrest.GenerateCA(caCertPath, caKeyPath); err != nil {
		return fmt.Errorf("failed to generate pgBackRest CA: %w", err)
	}

	// Generate shared server certificate for all nodes
	if err := pgbackrest.GenerateServerCert(caCertPath, caKeyPath, serverCertPath, serverKeyPath, "pgbackrest"); err != nil {
		return fmt.Errorf("failed to generate pgBackRest server cert: %w", err)
	}
	fmt.Printf("✓ - pgBackRest TLS certificates generated\n")

	for i, cellName := range allCells {
		cellServices := p.config.Cells[cellName]

		// Use default backup config path if not specified in config
		backupConfPath := cellServices.Multipooler.BackupConf
		if backupConfPath == "" {
			backupConfPath = filepath.Join(cellServices.Multipooler.PoolerDir, "pgbackrest.conf")
		}

		// Build AdditionalHosts with all other clusters using TLS
		var additionalHosts []pgbackrest.PgHost
		for j, otherCellName := range allCells {
			if otherCellName == cellName {
				continue // Skip self - this is pg1
			}
			otherCellServices := p.config.Cells[otherCellName]

			additionalHosts = append(additionalHosts, pgbackrest.PgHost{
				DataPath:  filepath.Join(otherCellServices.Multipooler.PoolerDir, "pg_data"),
				Host:      "localhost", // TLS connection to localhost
				Port:      otherCellServices.Multipooler.PgPort,
				SocketDir: filepath.Join(otherCellServices.Multipooler.PoolerDir, "pg_sockets"),
				User:      constants.DefaultPostgresUser,
				Database:  constants.DefaultPostgresDatabase,
				PgBackRestTLS: &pgbackrest.PgBackRestTLSConfig{
					Port:     ports.DefaultPgBackRestTLSPort + j,
					CAFile:   caCertPath,
					CertFile: serverCertPath,
					KeyFile:  serverKeyPath,
				},
			})
		}

		// Create per-pooler spool and lock directories inside backup repo
		poolerID := cellServices.Multipooler.ServiceID
		pgBackRestSpoolPath := filepath.Join(p.config.BackupRepoPath, "spool", "pooler_"+poolerID)
		if err := os.MkdirAll(pgBackRestSpoolPath, 0o755); err != nil {
			return fmt.Errorf("failed to create pgBackRest spool directory %s: %w", pgBackRestSpoolPath, err)
		}

		pgBackRestLockPath := filepath.Join(p.config.BackupRepoPath, "lock", "pooler_"+poolerID)
		if err := os.MkdirAll(pgBackRestLockPath, 0o755); err != nil {
			return fmt.Errorf("failed to create pgBackRest lock directory %s: %w", pgBackRestLockPath, err)
		}

		// Generate pgBackRest config for this pooler with TLS
		backupCfg := pgbackrest.Config{
			StanzaName:      "multigres",
			PgDataPath:      filepath.Join(cellServices.Multipooler.PoolerDir, "pg_data"),
			PgPort:          cellServices.Multipooler.PgPort,
			PgSocketDir:     filepath.Join(cellServices.Multipooler.PoolerDir, "pg_sockets"),
			PgUser:          constants.DefaultPostgresUser,
			PgPassword:      "postgres", // For local development only
			PgDatabase:      constants.DefaultPostgresDatabase,
			AdditionalHosts: additionalHosts,
			LogPath:         pgBackRestLogPath,
			SpoolPath:       pgBackRestSpoolPath,
			LockPath:        pgBackRestLockPath,
			RetentionFull:   7,
			PgBackRestTLS: &pgbackrest.PgBackRestTLSConfig{
				CertFile: serverCertPath,
				KeyFile:  serverKeyPath,
				CAFile:   caCertPath,
				Address:  "0.0.0.0",
				Port:     ports.DefaultPgBackRestTLSPort + i,
			},
		}

		// Write the pgBackRest config file
		if err := pgbackrest.WriteConfigFile(backupConfPath, backupCfg); err != nil {
			return fmt.Errorf("failed to generate pgBackRest config for cell %s: %w", cellName, err)
		}

		fmt.Printf("✓ - pgBackRest config created for cell %s\n", cellName)
	}

	return nil
}

// InitializePgBackRestStanzas initializes pgBackRest stanzas for all poolers
// This should be called after PostgreSQL is running in each pooler
// For HA setups, the shared stanza "multigres" is created from each cluster's perspective
func (p *localProvisioner) InitializePgBackRestStanzas() error {
	if p.config == nil {
		return fmt.Errorf("configuration not loaded")
	}

	ctx := context.TODO()

	for cellName, cellServices := range p.config.Cells {
		// Use default backup config path if not specified in config
		backupConfPath := cellServices.Multipooler.BackupConf
		if backupConfPath == "" {
			backupConfPath = filepath.Join(cellServices.Multipooler.PoolerDir, "pgbackrest.conf")
		}

		// Get backup repository path
		repoPath := p.config.BackupRepoPath
		if repoPath == "" {
			repoPath = filepath.Join(p.config.RootWorkingDir, "data", "backups")
		}

		// Create the shared stanza for this pooler
		// Each cluster will attempt to create the stanza from its perspective (with itself as pg1)
		// This is idempotent - subsequent calls will validate/update the stanza
		if err := pgbackrest.StanzaCreate(ctx, "multigres", backupConfPath, repoPath); err != nil {
			return fmt.Errorf("failed to create stanza for cell %s: %w", cellName, err)
		}
		fmt.Printf("✓ - pgBackRest stanza initialized for cell %s\n", cellName)
	}

	return nil
}
