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

// Package provisioner provides a plugin-based system for provisioning and managing
// database services in the Multigres ecosystem. It defines interfaces and types
// that allow different provisioner implementations to be registered and used
// interchangeably.
//
// The provisioner system supports:
//   - Bootstrap operations to set up initial infrastructure (e.g., etcd)
//   - Database provisioning and deprovisioning
//   - Service lifecycle management
//   - Configuration validation
//
// Example usage:
//
//	// Register a provisioner
//	RegisterProvisioner("local", local.NewProvisioner)
//
//	// Get and use a provisioner
//	prov, err := GetProvisioner("local")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Bootstrap the system
//	results, err := prov.Bootstrap(context.Background())
//	if err != nil {
//	    log.Fatal(err)
//	}
package provisioner

import (
	"context"
	"fmt"
)

// ProvisionResult contains the result of provisioning a service.
// It provides information about where and how the service is accessible.
type ProvisionResult struct {
	// ServiceName is the name of the service that was provisioned
	ServiceName string
	// FQDN is the fully qualified domain name where the service is accessible
	FQDN string
	// Ports maps protocol names to their port numbers (e.g., "grpc" -> 2379, "http" -> 2380)
	Ports map[string]int
	// Metadata contains additional provisioner-specific information
	Metadata map[string]any
}

// ProvisionRequest contains the parameters for provisioning a service.
// It specifies what service to provision and any additional configuration needed.
type ProvisionRequest struct {
	// Service is the name of the service to provision (etcd, multigateway, multiorch, multipooler)
	Service string
	// DatabaseName is the name of the database this service belongs to (empty for global services like etcd)
	DatabaseName string
	// Params contains additional parameters that can be passed to the provisioner
	Params map[string]any
}

// DeprovisionRequest contains the parameters for deprovisioning a service.
// It specifies what service to remove and whether to clean up associated data.
type DeprovisionRequest struct {
	// Service is the name of the service to deprovision (etcd, multigateway, multiorch)
	Service string
	// ServiceID is the unique identifier for the service instance
	ServiceID string
	// DatabaseName is the name of the database this service belongs to (empty for global services like etcd)
	DatabaseName string
	// Clean indicates whether to remove all data associated with the service
	Clean bool
}

// Provisioner defines the interface that all provisioner plugins must implement.
// This interface provides a consistent API for different provisioner implementations,
// allowing them to be used interchangeably in the Multigres system.
type Provisioner interface {
	// Name returns the name of the provisioner.
	// This should be a unique identifier for the provisioner type.
	Name() string

	// DefaultConfig returns the default configuration for this provisioner.
	// The returned map contains key-value pairs that represent the default
	// settings for the provisioner.
	// TODO: We can have better typing in the future here,
	// but we can wait until we start implementing other provisioners
	DefaultConfig() map[string]any

	// LoadConfig loads the provisioner-specific configuration from the given config paths.
	// The provisioner will search for config files in the provided paths and load
	// its own configuration section. This encapsulates all config loading logic
	// within the provisioner implementation.
	LoadConfig(configPaths []string) error

	// Bootstrap sets up etcd and creates the default database.
	// This method performs the initial setup required for the Multigres system
	// to function. It typically involves:
	//   - Starting an etcd instance for metadata storage
	//   - Creating the default database
	//   - Setting up any required infrastructure
	//
	// The method returns a slice of ProvisionResult containing information
	// about all services that were provisioned during bootstrap.
	Bootstrap(ctx context.Context) ([]*ProvisionResult, error)

	// Teardown shuts down all services (reverse of Bootstrap).
	// This method stops all running services and optionally cleans up
	// associated data. The clean parameter determines whether data
	// should be preserved (false) or removed (true).
	Teardown(ctx context.Context, clean bool) error

	// ProvisionDatabase provisions all services required for a specific database.
	// This method creates and starts all the services needed to support
	// a database instance, including multigateway, multiorch, and multipooler.
	// The etcdAddress parameter specifies the address of the etcd instance
	// that should be used for metadata storage.
	//
	// Returns a slice of ProvisionResult containing information about
	// all services that were provisioned for the database.
	ProvisionDatabase(ctx context.Context, databaseName string, etcdAddress string) ([]*ProvisionResult, error)

	// DeprovisionDatabase removes all services associated with a specific database.
	// This method stops and optionally removes all services that were created
	// for the specified database. The etcdAddress parameter specifies the
	// address of the etcd instance used for metadata storage.
	DeprovisionDatabase(ctx context.Context, databaseName string, etcdAddress string) error

	// ValidateConfig validates the provisioner-specific configuration.
	// This method checks that the provided configuration is valid for
	// this provisioner. It should return an error if the configuration
	// is invalid or missing required fields.
	ValidateConfig(config map[string]any) error
}

// Factory is a function that creates a new provisioner instance.
// Factory functions are used to instantiate provisioner implementations
// and are registered with the provisioner system using RegisterProvisioner.
type Factory func() (Provisioner, error)

// provisionerFactories holds the registered provisioner factories.
// This map associates provisioner names with their factory functions,
// allowing the system to create provisioner instances by name.
var provisionerFactories = make(map[string]Factory)

// RegisterProvisioner registers a provisioner factory with the given name.
// This function allows provisioner implementations to be registered with
// the system so they can be created later using GetProvisioner.
//
// The name parameter should be unique across all registered provisioners.
// If a provisioner with the same name is already registered, it will be
// replaced with the new factory.
func RegisterProvisioner(name string, factory Factory) {
	provisionerFactories[name] = factory
}

// GetProvisioner creates a provisioner instance by name.
// This function looks up the registered factory for the given name and
// uses it to create a new provisioner instance.
//
// Returns an error if no provisioner is registered with the given name.
// The error message includes a list of available provisioner names.
func GetProvisioner(name string) (Provisioner, error) {
	factory, exists := provisionerFactories[name]
	if !exists {
		return nil, fmt.Errorf("provisioner '%s' not found. Available provisioners: %v", name, GetAvailableProvisioners())
	}

	return factory()
}

// GetAvailableProvisioners returns a list of registered provisioner names.
// This function provides a way to discover what provisioners are available
// in the system. It returns a slice containing the names of all registered
// provisioners.
func GetAvailableProvisioners() []string {
	names := make([]string, 0, len(provisionerFactories))
	for name := range provisionerFactories {
		names = append(names, name)
	}
	return names
}
