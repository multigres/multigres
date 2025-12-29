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
// Modifications Copyright 2025 Supabase, Inc.

package grpccommon

import (
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// maxMessageSize is the maximum message size which the gRPC server will
	// accept. Larger messages will be rejected.
	// Note: We're using 16 MiB as default value because that's the default in MySQL
	maxMessageSize = 16 * 1024 * 1024
	// enablePrometheus sets a flag to enable grpc client/server grpc monitoring.
	enablePrometheus bool
)

// RegisterFlags installs grpccommon flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.IntVar(&maxMessageSize, "grpc-max-message-size", maxMessageSize, "Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'.")
	fs.BoolVar(&grpc.EnableTracing, "grpc-enable-tracing", grpc.EnableTracing, "Enable gRPC tracing.")
	fs.BoolVar(&enablePrometheus, "grpc-prometheus", enablePrometheus, "Enable gRPC monitoring with Prometheus.")
}

// EnableGRPCPrometheus returns the value of the --grpc-prometheus flag.
func EnableGRPCPrometheus() bool {
	return enablePrometheus
}

// MaxMessageSize returns the value of the --grpc-max-message-size flag.
func MaxMessageSize() int {
	return maxMessageSize
}

// LocalClientDialOptions returns a slice of grpc.DialOption to be used when creating a gRPC client.
// These options are used for local clients connecting to the gRPC server.
// They are not intended to be used for production environments.
// The WithDisableServiceConfig is a workaround for a known issue
// in MacOS where localhost host takes too long to resolve.
// See the following PR for more details: https://github.com/multigres/multigres/pull/152
func LocalClientDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableServiceConfig(),
	}
}

// NewClient creates a gRPC client connection with OpenTelemetry instrumentation.
// Use this instead of grpc.NewClient() directly to ensure consistent telemetry.
func NewClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// Prepend our telemetry handler so caller's options can override if needed
	allOpts := append([]grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}, opts...)
	return grpc.NewClient(target, allOpts...)
}
