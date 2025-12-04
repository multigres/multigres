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
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/retry"

	"go.opentelemetry.io/otel/attribute"
)

// waitForServiceReady waits for a service to become ready by checking appropriate endpoints
// The provided parentCtx should contain any active span for distributed tracing
func (p *localProvisioner) waitForServiceReady(parentCtx context.Context, serviceName string, host string, servicePorts map[string]int, timeout time.Duration) error {
	// Create span as child of parent context
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, fmt.Sprintf("wait_for_service_ready/%s", serviceName))
	defer span.End()

	span.SetAttributes(attribute.String("service.name", serviceName))

	r := retry.New(10*time.Millisecond, time.Second)
	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			return fmt.Errorf("%s did not become ready within %v after %d attempts: %w", serviceName, timeout, attempt, err)
		}

		// First check TCP connectivity on all advertised ports
		allPortsReady := true
		for _, port := range servicePorts {
			address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
			conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
			if err != nil {
				allPortsReady = false
				break // This port not ready yet
			}
			conn.Close()
		}
		if !allPortsReady {
			continue // Not all ports ready yet, will backoff
		}

		// For services with HTTP endpoints, check debug/config endpoint
		if err := p.checkMultigresServiceHealth(ctx, serviceName, host, servicePorts); err != nil {
			continue // HTTP endpoint not ready yet, will backoff
		}

		return nil // Service is ready
	}
	panic("unreachable")
}

// checkMultigresServiceHealth checks health for all supported service port types
// The context is propagated to all health check calls to maintain trace parent-child relationships
func (p *localProvisioner) checkMultigresServiceHealth(ctx context.Context, serviceName string, host string, servicePorts map[string]int) error {
	// Iterate over service ports and run health checks for supported types
	for portType, port := range servicePorts {
		switch portType {
		case "http_port":
			// Run HTTP health check
			httpAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
			if err := p.checkDebugConfigEndpoint(ctx, httpAddress); err != nil {
				return err
			}
		case "grpc_port":
			// Run gRPC health check for pgctld
			if serviceName == "pgctld" {
				grpcAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				if err := p.checkPgctldGrpcHealth(ctx, grpcAddress); err != nil {
					return err
				}
			}
		case "etcd_port":
			// Run etcd health check
			if serviceName == "etcd" {
				etcdAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				if err := p.checkEtcdHealth(ctx, etcdAddress); err != nil {
					return err
				}
			}
			// Future: Add other gRPC services
		default:
			// No health check implemented for this port type, skip
			continue
		}
	}
	return nil
}

// checkEtcdHealth checks if etcd is ready by querying its health endpoint
func (p *localProvisioner) checkEtcdHealth(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s/health", address)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach etcd health endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("etcd health endpoint returned status %d", resp.StatusCode)
	}
	return nil
}

// checkDebugConfigEndpoint checks if the debug/config endpoint returns 200 OK
func (p *localProvisioner) checkDebugConfigEndpoint(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s/live", address)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach debug endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("debug endpoint returned status %d", resp.StatusCode)
	}
	return nil
}

// checkPgctldGrpcHealth checks if pgctld gRPC server is healthy by calling Status
func (p *localProvisioner) checkPgctldGrpcHealth(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpccommon.LocalClientDialOptions()...)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	_, err = client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("pgctld gRPC status call failed: %w", err)
	}

	return nil
}

// validateProcessRunning checks if a process with the given PID is still running
func (p *localProvisioner) validateProcessRunning(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid PID: %d", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process with PID %d not found: %w", pid, err)
	}

	// Send signal 0 to check if process exists without actually sending a signal
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return fmt.Errorf("process with PID %d is not running: %w", pid, err)
	}

	return nil
}

// checkPortConflict checks if a port is already in use by another process
func (p *localProvisioner) checkPortConflict(port int, serviceName, portName string) error {
	if port <= 0 {
		return nil // Skip invalid ports
	}

	address := fmt.Sprintf("localhost:%d", port)
	conn, err := (&net.Dialer{Timeout: 1 * time.Second}).DialContext(context.TODO(), "tcp", address)
	if err != nil {
		// Port is not in use, this is good
		return nil //nolint:nilerr // Error means port is free, which is success
	}
	conn.Close()

	// Port is in use by some process
	return fmt.Errorf("cannot start %s: port %d (%s) is already in use by another process. "+
		"There is no way to do a clean start. Please kill the process using port %d or change the configuration",
		serviceName, port, portName, port)
}
