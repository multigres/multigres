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
	"go.opentelemetry.io/otel/attribute"
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

	// otelInstrumentationEnabled reports whether OpenTelemetry gRPC
	// instrumentation (the otelgrpc stats handler) is attached to clients
	// created by NewClient. It is a function rather than a value so the owner
	// of the process-wide setting (servenv, via --grpc-otel-instrumentation)
	// can register it before flags are parsed while NewClient still sees the
	// parsed value at dial time. The default keeps instrumentation on for
	// callers without a servenv, such as the multigres CLI.
	otelInstrumentationEnabled = func() bool { return true }
)

// SetOTelInstrumentationEnabled registers the process-wide decision on whether
// gRPC clients created by NewClient carry OpenTelemetry instrumentation. The
// function is evaluated on every NewClient call, so it may read configuration
// that is only final after flag parsing. Passing nil restores the default
// (enabled).
func SetOTelInstrumentationEnabled(enabled func() bool) {
	if enabled == nil {
		enabled = func() bool { return true }
	}
	otelInstrumentationEnabled = enabled
}

// OTelInstrumentationEnabled reports the current process-wide decision, see
// SetOTelInstrumentationEnabled.
func OTelInstrumentationEnabled() bool {
	return otelInstrumentationEnabled()
}

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
	return ClientDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// ClientDialOptions returns the standard dial options for a gRPC client given
// a caller-supplied transport credentials dial option. It always includes
// WithDisableServiceConfig, a macOS localhost-resolution workaround (see #152),
// so callers can't forget it when wiring up TLS or insecure credentials.
func ClientDialOptions(transportCreds grpc.DialOption) []grpc.DialOption {
	return []grpc.DialOption{
		transportCreds,
		grpc.WithDisableServiceConfig(),
	}
}

// ClientOption configures OpenTelemetry instrumentation for the gRPC client.
// These options extend the stats handler that NewClient creates.
type ClientOption interface {
	apply(*clientConfig)
}

type clientConfig struct {
	otelOptions []otelgrpc.Option
	dialOptions []grpc.DialOption
}

// WithAttributes adds custom OpenTelemetry attributes to gRPC client spans.
// This is a generic helper that can be used by domain-specific code to add
// custom span attributes without making grpccommon domain-aware.
func WithAttributes(attrs ...attribute.KeyValue) ClientOption {
	return funcOption(func(c *clientConfig) {
		c.otelOptions = append(c.otelOptions,
			otelgrpc.WithSpanAttributes(attrs...),
		)
	})
}

// WithDialOptions adds standard gRPC dial options to the client.
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return funcOption(func(c *clientConfig) {
		c.dialOptions = append(c.dialOptions, opts...)
	})
}

type funcOption func(*clientConfig)

func (f funcOption) apply(c *clientConfig) {
	f(c)
}

// NewClient creates a gRPC client with OpenTelemetry instrumentation.
// Use WithAttributes to add span attributes such as the remote pooler identity.
// Use WithDialOptions to pass standard gRPC dial options.
//
// All ClientOptions are used to configure a single stats handler, preventing
// duplication and ensuring consistent telemetry across the application. The
// stats handler is omitted when OTelInstrumentationEnabled reports false: it
// runs on every RPC (header extraction, span and metric attribute sets), which
// on the gateway to pooler hop means once per SQL statement.
func NewClient(target string, opts ...ClientOption) (*grpc.ClientConn, error) {
	cfg := &clientConfig{}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	// Raise the client-side max message sizes to match the server
	// (grpc.MaxRecvMsgSize/MaxSendMsgSize in servenv). Modern gRPC-Go enforces a
	// 4 MiB default MaxCallRecvMsgSize on the CLIENT, independent of the server
	// limit, so without this a >4 MiB result (e.g. a large row streamed from the
	// pooler to the gateway) fails with RESOURCE_EXHAUSTED "received message larger
	// than max".
	allOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageSize),
			grpc.MaxCallSendMsgSize(maxMessageSize),
		),
	}
	if OTelInstrumentationEnabled() {
		allOpts = append(allOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler(cfg.otelOptions...)))
	}
	allOpts = append(allOpts, cfg.dialOptions...)

	return grpc.NewClient(target, allOpts...)
}
