// Copyright 2026 Supabase, Inc.
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

package servenv

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// Endpoint is where a gRPC server listens (a TCP port, memory, later a Unix
// socket) and how a client reaches it.
type Endpoint interface {
	// Listen opens the listener the server serves on.
	Listen() (net.Listener, error)

	// Target returns the gRPC target and the dial options the endpoint
	// requires; a network endpoint leaves credentials to the client.
	Target() (target string, opts []grpc.DialOption)

	// Local reports whether the endpoint is unreachable from the network, in
	// which case the server uses no TLS and no auth.
	Local() bool
}

// tcpEndpoint is the port from --grpc-port and --grpc-bind-address, read when
// used, after flags are parsed.
type tcpEndpoint struct {
	bindAddress viperutil.Value[string]
	port        viperutil.Value[int]
}

func (e tcpEndpoint) Listen() (net.Listener, error) {
	listener, err := net.Listen("tcp", e.address())
	if err != nil {
		return nil, fmt.Errorf("cannot listen on grpc port %d: %w", e.port.Get(), err)
	}
	return listener, nil
}

func (e tcpEndpoint) Target() (string, []grpc.DialOption) {
	return e.address(), nil
}

func (e tcpEndpoint) Local() bool { return false }

func (e tcpEndpoint) address() string {
	return net.JoinHostPort(e.bindAddress.Get(), strconv.Itoa(e.port.Get()))
}

// inMemoryBufferSize is each in-memory connection's buffer; it does not limit
// message size.
const inMemoryBufferSize = 1 << 20

// inMemoryTarget is passed unresolved to the context dialer, which ignores it.
const inMemoryTarget = "passthrough:///in-memory"

type inMemoryEndpoint struct {
	listener *bufconn.Listener
}

// NewInMemoryEndpoint returns an in-memory endpoint (bufconn) that only the
// same process can reach.
func NewInMemoryEndpoint() Endpoint {
	return inMemoryEndpoint{listener: bufconn.Listen(inMemoryBufferSize)}
}

func (e inMemoryEndpoint) Listen() (net.Listener, error) {
	return e.listener, nil
}

func (e inMemoryEndpoint) Target() (string, []grpc.DialOption) {
	return inMemoryTarget, []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return e.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func (e inMemoryEndpoint) Local() bool { return true }
