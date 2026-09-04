// Copyright 2019 The Vitess Authors.
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
//
// Modifications Copyright 2026 Supabase, Inc.

/*
Package etcdtopo implements topoclient.Conn with etcd as the backend.

We expect the following behavior from the etcd client library:

  - Get and Delete return ErrorCodeKeyNotFound if the node doesn't exist.
  - Create returns ErrorCodeNodeExist if the node already exists.
  - Intermediate directories are always created automatically if necessary.
  - Set returns ErrorCodeKeyNotFound if the node doesn't exist already.
  - It returns ErrorCodeTestFailed if the provided version index doesn't match.

We follow these conventions within this package:

  - Call convertError(err) on any errors returned from the etcd client library.
    Functions defined in this package can be assumed to have already converted
    errors as necessary.
*/
package etcdtopo

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"go.etcd.io/etcd/client/pkg/v3/tlsutil"
	"google.golang.org/grpc"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
)

var (
	clientCertPath string
	clientKeyPath  string
	serverCaPath   string
)

var (
	_ topoclient.Conn           = (*etcdtopo)(nil)
	_ topoclient.FactoryWithTLS = Factory{}
)

// remoteOperationTimeout is used for operations where we have to
// call out to etcd for initial data fetches (e.g., watch setup).
const remoteOperationTimeout = timeouts.RemoteOperationTimeout

// Factory is the etcd topoclient.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topoclient.Factory interface.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topoclient.Factory interface.
func (f Factory) Create(cell, root string, serverAddrs []string) (topoclient.Conn, error) {
	return NewEtcdTopo(serverAddrs, root)
}

// CreateWithTLS creates an etcd topology client with per-connection TLS options.
func (f Factory) CreateWithTLS(cell, root string, serverAddrs []string, tlsOptions *topoclient.TLSOptions) (topoclient.Conn, error) {
	return NewServerWithTLS(serverAddrs, root, tlsOptions)
}

// etcdtopo is the implementation of topoclient.Conn for etcd.
type etcdtopo struct {
	// cli is the v3 client.
	cli *clientv3.Client

	// root is the root path for this client.
	root string

	running chan struct{}

	// eph tracks lease-backed ephemeral files; see ephemeral.go.
	eph ephemeralState
}

func init() {
	servenv.OnParse(registerEtcdTopoFlags)
	topoclient.RegisterFactory(topoclient.DefaultTopoImplementation, Factory{})
}

func registerEtcdTopoFlags(fs *pflag.FlagSet) {
	fs.StringVar(&clientCertPath, "topo-etcd-tls-cert", clientCertPath, "path to the client cert to use to connect to the etcd topo server, requires topo-etcd-tls-key, enables TLS")
	fs.StringVar(&clientKeyPath, "topo-etcd-tls-key", clientKeyPath, "path to the client key to use to connect to the etcd topo server, enables TLS")
	fs.StringVar(&serverCaPath, "topo-etcd-tls-ca", serverCaPath, "path to the ca to use to validate the server cert when connecting to the etcd topo server")
}

// Close closes the etcd client.
func (s *etcdtopo) Close() error {
	close(s.running)
	if err := s.cli.Close(); err != nil {
		return err
	}
	s.cli = nil
	return nil
}

func newTLSConfig(certPath, keyPath, caPath string) (*tls.Config, error) {
	if certPath == "" || keyPath == "" {
		return nil, nil
	}

	cert, err := tlsutil.NewCert(certPath, keyPath, nil)
	if err != nil {
		return nil, err
	}

	var caPool *x509.CertPool
	if caPath != "" {
		caPool, err = tlsutil.NewCertPool([]string{caPath})
		if err != nil {
			return nil, err
		}
	}

	return newTLSConfigWithCertificate(cert, caPool), nil
}

func newTLSConfigFromPEM(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	if len(certPEM) == 0 && len(keyPEM) == 0 {
		if len(caPEM) != 0 {
			return nil, errors.New("client certificate and key PEM are required when CA PEM is provided")
		}
		return nil, nil
	}
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return nil, errors.New("both client certificate and key PEM must be provided for TLS")
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	var caPool *x509.CertPool
	if len(caPEM) > 0 {
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("failed to parse CA certificate PEM")
		}
	}

	return newTLSConfigWithCertificate(&cert, caPool), nil
}

func newTLSConfigWithCertificate(cert *tls.Certificate, caPool *x509.CertPool) *tls.Config {
	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            caPool,
		InsecureSkipVerify: false,
	}
	if cert != nil {
		config.Certificates = []tls.Certificate{*cert}
	}
	return config
}

func newTLSConfigFromOptions(tlsOptions *topoclient.TLSOptions) (*tls.Config, error) {
	if tlsOptions == nil {
		return nil, nil
	}
	if tlsOptions.Config != nil {
		if len(tlsOptions.CertPEM) != 0 || len(tlsOptions.KeyPEM) != 0 || len(tlsOptions.CAPEM) != 0 {
			return nil, errors.New("TLS config cannot be combined with TLS PEM material")
		}
		config := tlsOptions.Config.Clone()
		if config.MinVersion < tls.VersionTLS12 {
			config.MinVersion = tls.VersionTLS12
		}
		config.InsecureSkipVerify = false
		return config, nil
	}
	if len(tlsOptions.CertPEM) == 0 && len(tlsOptions.KeyPEM) == 0 && len(tlsOptions.CAPEM) == 0 {
		return nil, errors.New("TLS options require a client certificate and key or TLS config")
	}
	return newTLSConfigFromPEM(tlsOptions.CertPEM, tlsOptions.KeyPEM, tlsOptions.CAPEM)
}

func newEtcdClientConfig(serverAddrs []string, tlsConfig *tls.Config) clientv3.Config {
	return clientv3.Config{
		Endpoints:   serverAddrs,
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()}, // grpc.WithBlock is deprecated but required by etcd client
		TLS:         tlsConfig,
	}
}

func validateTLSEndpoints(serverAddrs []string, tlsConfig *tls.Config) error {
	if tlsConfig == nil {
		return nil
	}
	for _, serverAddr := range serverAddrs {
		endpoint, err := url.Parse(serverAddr)
		if err != nil {
			continue
		}
		if strings.EqualFold(endpoint.Scheme, "http") {
			return fmt.Errorf("TLS cannot use HTTP endpoint %q", serverAddr)
		}
	}
	return nil
}

// NewServerWithOpts creates a new server with TLS material loaded from file paths.
func NewServerWithOpts(serverAddrs []string, root, certPath, keyPath, caPath string) (*etcdtopo, error) {
	tlscfg, err := newTLSConfig(certPath, keyPath, caPath)
	if err != nil {
		return nil, err
	}
	config := newEtcdClientConfig(serverAddrs, tlscfg)

	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	return &etcdtopo{
		cli:     cli,
		root:    root,
		running: make(chan struct{}),
	}, nil
}

// NewServerWithTLS creates a new server with in-memory TLS options.
func NewServerWithTLS(serverAddrs []string, root string, tlsOptions *topoclient.TLSOptions) (*etcdtopo, error) {
	tlscfg, err := newTLSConfigFromOptions(tlsOptions)
	if err != nil {
		return nil, err
	}
	if err := validateTLSEndpoints(serverAddrs, tlscfg); err != nil {
		return nil, err
	}
	config := newEtcdClientConfig(serverAddrs, tlscfg)

	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	return &etcdtopo{
		cli:     cli,
		root:    root,
		running: make(chan struct{}),
	}, nil
}

// NewEtcdTopo creates a new server using the TLS paths configured by command-line flags.
func NewEtcdTopo(serverAddrs []string, root string) (*etcdtopo, error) {
	return NewServerWithOpts(serverAddrs, root, clientCertPath, clientKeyPath, serverCaPath)
}
