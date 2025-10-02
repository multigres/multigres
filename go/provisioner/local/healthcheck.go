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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/grpccommon"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/timertools"
)

// waitForServiceReady waits for a service to become ready by checking appropriate endpoints
func (p *localProvisioner) waitForServiceReady(serviceName string, host string, servicePorts map[string]int, timeout time.Duration) error {
	ticker := timertools.NewBackoffTicker(10*time.Millisecond, time.Second)
	// Trigger an immediate first check
	ticker.C <- time.Now()
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return fmt.Errorf("%s did not become ready within %v", serviceName, timeout)
		case <-ticker.C:
			// First check TCP connectivity on all advertised ports
			allPortsReady := true
			for _, port := range servicePorts {
				address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				conn, err := net.DialTimeout("tcp", address, 2*time.Second)
				if err != nil {
					allPortsReady = false
					break // This port not ready yet
				}
				conn.Close()
			}
			if !allPortsReady {
				continue // Not all ports ready yet
			}
			// For services with HTTP endpoints, check debug/config endpoint
			if err := p.checkMultigresServiceHealth(serviceName, host, servicePorts); err != nil {
				continue // HTTP endpoint not ready yet
			}
			return nil // Service is ready
		}
	}
}

// checkMultigresServiceHealth checks health for all supported service port types
func (p *localProvisioner) checkMultigresServiceHealth(serviceName string, host string, servicePorts map[string]int) error {
	// Iterate over service ports and run health checks for supported types
	for portType, port := range servicePorts {
		switch portType {
		case "http_port":
			// Run HTTP health check
			httpAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
			if err := p.checkDebugConfigEndpoint(httpAddress); err != nil {
				return err
			}
		case "grpc_port":
			// Run gRPC health check for pgctld
			if serviceName == "pgctld" {
				grpcAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				if err := p.checkPgctldGrpcHealth(grpcAddress); err != nil {
					return err
				}
			}
			// Future: Add other gRPC services
		case "etcd_port":
			// Run etcd health check
			if serviceName == "etcd" {
				etcdAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				if err := p.checkEtcdHealth(etcdAddress); err != nil {
					return err
				}
			}
		default:
			// No health check implemented for this port type, skip
			continue
		}
	}
	return nil
}

// checkEtcdHealth checks if etcd is ready by querying its health endpoint
func (p *localProvisioner) checkEtcdHealth(address string) error {
	url := fmt.Sprintf("http://%s/health", address)
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.Get(url)
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
func (p *localProvisioner) checkDebugConfigEndpoint(address string) error {
	url := fmt.Sprintf("http://%s/live", address)
	client := &http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get(url)
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
func (p *localProvisioner) checkPgctldGrpcHealth(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, grpccommon.ClientDialOptions()...)
	conn, err := grpc.NewClient(address, opts...)
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
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		// Port is not in use, this is good
		return nil
	}
	conn.Close()

	// Port is in use by some process
	return fmt.Errorf("cannot start %s: port %d (%s) is already in use by another process. "+
		"There is no way to do a clean start. Please kill the process using port %d or change the configuration",
		serviceName, port, portName, port)
}
