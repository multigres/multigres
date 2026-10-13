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

// Endpoint is where a gRPC server listens and how a client reaches it. A
// server serves on whatever its endpoint listens on, so where it listens (a
// TCP port, memory, or later a Unix socket) is decided by the endpoint alone.
type Endpoint interface {
	// Listen opens the listener the server serves on.
	Listen() (net.Listener, error)

	// Target tells a client how to connect: a gRPC target and the dial
	// options the endpoint itself requires. Transport credentials for a
	// network endpoint are the client's own and are not included.
	Target() (target string, opts []grpc.DialOption)

	// Local reports whether the endpoint cannot be reached from the network.
	// A server on a local endpoint serves without TLS or authentication.
	Local() bool
}

// tcpEndpoint is a TCP port, configured by a server's --grpc-port and
// --grpc-bind-address flags. It reads them when used, after flags are parsed.
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

// inMemoryBufferSize is the size of each in-memory connection's buffer. It
// does not limit message size: gRPC flow control streams larger messages
// through it.
const inMemoryBufferSize = 1 << 20

// inMemoryTarget is the gRPC target of an in-memory endpoint. The passthrough
// scheme hands it unresolved to the context dialer, which ignores it.
const inMemoryTarget = "passthrough:///in-memory"

// inMemoryEndpoint is a listener in memory (bufconn): only code in the same
// process can connect, through the dialer its Target returns.
type inMemoryEndpoint struct {
	listener *bufconn.Listener
}

// NewInMemoryEndpoint returns an endpoint in memory, for a server whose
// clients run in the same process (in minigres, the multigateway calling the
// multipooler). Nothing outside the process can reach it.
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
		// The connection never leaves the process.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func (e inMemoryEndpoint) Local() bool { return true }
