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

package provisioner

import (
	"context"
	"fmt"
)

// ProvisionResult contains the result of provisioning a service
type ProvisionResult struct {
	// FQDN is the fully qualified domain name where the service is accessible
	FQDN string
	// Ports maps protocol names to their port numbers (e.g., "grpc" -> 2379, "http" -> 2380)
	Ports map[string]int
	// Metadata contains additional provisioner-specific information
	Metadata map[string]interface{}
}

// ProvisionRequest contains the parameters for provisioning a service
type ProvisionRequest struct {
	// Service is the name of the service to provision (etcd, multigateway, multiorch)
	Service string
	// Config contains the provisioner-specific configuration
	Config map[string]interface{}
}

// Provisioner defines the interface that all provisioner plugins must implement
type Provisioner interface {
	// Name returns the name of the provisioner
	Name() string

	// DefaultConfig returns the default configuration for this provisioner
	DefaultConfig() map[string]interface{}

	// ProvisionEtcd provisions etcd for the given cell
	// It should be idempotent - calling multiple times should not cause issues
	ProvisionEtcd(ctx context.Context, req *ProvisionRequest) (*ProvisionResult, error)

	// ProvisionMultigateway provisions multigateway for the given cell
	// It should be idempotent - calling multiple times should not cause issues
	ProvisionMultigateway(ctx context.Context, req *ProvisionRequest) (*ProvisionResult, error)

	// ProvisionMultiOrch provisions multi-orchestrator for the given cell
	// It should be idempotent - calling multiple times should not cause issues
	ProvisionMultiOrch(ctx context.Context, req *ProvisionRequest) (*ProvisionResult, error)

	// Deprovision removes/stops services for the given cell
	// It should be idempotent - calling multiple times should not cause issues
	Deprovision(ctx context.Context, req *ProvisionRequest) error

	// ValidateConfig validates the provisioner-specific configuration
	ValidateConfig(config map[string]interface{}) error
}

// ProvisionerFactory is a function that creates a new provisioner instance
type ProvisionerFactory func() (Provisioner, error)

var (
	// provisionerFactories holds the registered provisioner factories
	provisionerFactories = make(map[string]ProvisionerFactory)
)

// RegisterProvisioner registers a provisioner factory with the given name
func RegisterProvisioner(name string, factory ProvisionerFactory) {
	provisionerFactories[name] = factory
}

// GetProvisioner creates a provisioner instance by name
func GetProvisioner(name string) (Provisioner, error) {
	factory, exists := provisionerFactories[name]
	if !exists {
		return nil, fmt.Errorf("provisioner '%s' not found. Available provisioners: %v", name, GetAvailableProvisioners())
	}

	return factory()
}

// GetAvailableProvisioners returns a list of registered provisioner names
func GetAvailableProvisioners() []string {
	names := make([]string, 0, len(provisionerFactories))
	for name := range provisionerFactories {
		names = append(names, name)
	}
	return names
}
