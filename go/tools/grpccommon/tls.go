// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpccommon

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// BuildServerTLSConfig creates a TLS configuration for a gRPC server.
// Returns nil if neither certFile nor keyFile is provided (plaintext mode).
//
// When caFile is provided, mutual TLS is enabled: the server requires and
// verifies client certificates against the given CA.
//
// When serverCAFile is provided, the intermediate CA certificate is appended
// to the server's certificate chain so clients receive the full chain.
func BuildServerTLSConfig(certFile, keyFile, caFile, serverCAFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		// Reject ca/serverCA-only configurations: a user that set them
		// likely intended TLS/mTLS and would otherwise silently get plaintext.
		if caFile != "" || serverCAFile != "" {
			return nil, errors.New("server CA configured without server cert and key")
		}
		return nil, nil
	}
	if certFile == "" {
		return nil, errors.New("server cert is required when server key is set")
	}
	if keyFile == "" {
		return nil, errors.New("server key is required when server cert is set")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load gRPC server certificate: %w", err)
	}

	// Append intermediate CA certificates to the server's certificate chain
	// so clients receive the full chain during the TLS handshake.
	if serverCAFile != "" {
		serverCAPEM, err := os.ReadFile(serverCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read server CA file: %w", err)
		}
		rest := serverCAPEM
		found := false
		for {
			var block *pem.Block
			block, rest = pem.Decode(rest)
			if block == nil {
				break
			}
			// Skip non-certificate blocks (e.g., a stray PRIVATE KEY block in a
			// mixed PEM file). Appending them would corrupt the chain and only
			// surface as a confusing TLS handshake error later.
			if block.Type != "CERTIFICATE" {
				continue
			}
			cert.Certificate = append(cert.Certificate, block.Bytes)
			found = true
		}
		if !found {
			return nil, errors.New("failed to decode any certificates from server CA PEM")
		}
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Enable mutual TLS if a client CA is provided.
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsConfig.ClientCAs = caPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// BuildClientTLSConfig creates a TLS configuration for a gRPC client.
// Returns nil if no TLS parameters are provided (insecure mode).
//
// When caFile is provided, the client verifies the server's certificate
// against the given CA. When certFile and keyFile are provided, the client
// presents a certificate for mutual TLS.
//
// If only serverName is provided (no caFile, certFile, or keyFile), TLS is
// enabled using the system trust store with that server name — useful when
// connecting to a server with a publicly trusted certificate.
func BuildClientTLSConfig(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" && caFile == "" && serverName == "" {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Load CA for server certificate verification.
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caPool
	}

	// Load client certificate for mutual TLS.
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load gRPC client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	} else if certFile != "" || keyFile != "" {
		return nil, errors.New("both client cert and key must be provided for mTLS")
	}

	if serverName != "" {
		tlsConfig.ServerName = serverName
	}

	return tlsConfig, nil
}
