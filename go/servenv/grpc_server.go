/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications Copyright 2025 The Multigres Authors.
*/

package servenv

import (
	"context"
	"log/slog"
	"math"
	"net"
	"os"
	"strconv"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/multigres/multigres/go/grpccommon"

	"github.com/spf13/pflag"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// This file handles gRPC server, on its own port.
// Clients register servers, based on service map:
//
// servenv.RegisterGRPCFlags()
//
//	servenv.OnRun(func() {
//	  if servenv.GRPCCheckServiceMap("XXX") {
//	    pb.RegisterXXX(servenv.GRPCServer, XXX)
//	  }
//	}
//
// Note servenv.GRPCServer can only be used in servenv.OnRun,
// and not before, as it is initialized right before calling OnRun.
var (
	// gRPCAuth specifies which auth plugin to use. Currently only "static" and
	// "mtls" are supported.
	//
	// To expose this flag, call RegisterGRPCAuthServerFlags before ParseFlags.
	gRPCAuth string

	// GRPCServer is the global server to serve gRPC.
	GRPCServer *grpc.Server

	authPlugin Authenticator
)

// Misc. server variables.
var (
	// gRPCPort is the port to listen on for gRPC. If zero, don't listen.
	gRPCPort int

	// gRPCBindAddress is the address to bind to for gRPC. If empty, bind to all addresses.
	gRPCBindAddress string

	// gRPCMaxConnectionAge is the maximum age of a client connection, before GoAway is sent.
	// This is useful for L4 loadbalancing to ensure rebalancing after scaling.
	gRPCMaxConnectionAge = time.Duration(math.MaxInt64)

	// gRPCMaxConnectionAgeGrace is an additional grace period after GRPCMaxConnectionAge, after which
	// connections are forcibly closed.
	gRPCMaxConnectionAgeGrace = time.Duration(math.MaxInt64)

	// gRPCInitialConnWindowSize ServerOption that sets window size for a connection.
	// The lower bound for window size is 64K and any value smaller than that will be ignored.
	gRPCInitialConnWindowSize int

	// gRPCInitialWindowSize ServerOption that sets window size for stream.
	// The lower bound for window size is 64K and any value smaller than that will be ignored.
	gRPCInitialWindowSize int

	// gRPCKeepAliveEnforcementPolicyMinTime sets the keepalive enforcement policy on the server.
	// This is the minimum amount of time a client should wait before sending a keepalive ping.
	gRPCKeepAliveEnforcementPolicyMinTime = 10 * time.Second

	// gRPCKeepAliveEnforcementPolicyPermitWithoutStream, if true, instructs the server to allow keepalive pings
	// even when there are no active streams (RPCs). If false, and client sends ping when
	// there are no active streams, server will send GOAWAY and close the connection.
	gRPCKeepAliveEnforcementPolicyPermitWithoutStream bool

	gRPCKeepaliveTime    = 10 * time.Second
	gRPCKeepaliveTimeout = 10 * time.Second
)

// TLS variables.
var (
	// gRPCCert is the cert to use if TLS is enabled.
	gRPCCert string
	// gRPCKey is the key to use if TLS is enabled.
	gRPCKey string
	// gRPCCA is the CA to use if TLS is enabled.
	gRPCCA string
	// gRPCCRL is the CRL (Certificate Revocation List) to use if TLS is
	// enabled.
	gRPCCRL string
	// gRPCEnableOptionalTLS enables an optional TLS mode when a server accepts
	// both TLS and plain-text connections on the same port.
	gRPCEnableOptionalTLS bool
	// gRPCServerCA if specified will combine server cert and server CA.
	gRPCServerCA string
)

// RegisterGRPCServerFlags registers flags required to run a gRPC server via Run
// or RunDefault.
//
// `go/cmd/*` entrypoints should call this function before
// ParseFlags(WithArgs)? if they wish to run a gRPC server.
func RegisterGRPCServerFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.IntVar(&gRPCPort, "grpc-port", gRPCPort, "Port to listen on for gRPC calls. If zero, do not listen.")
		fs.StringVar(&gRPCBindAddress, "grpc-bind-address", gRPCBindAddress, "Bind address for gRPC calls. If empty, listen on all addresses.")
		fs.DurationVar(&gRPCMaxConnectionAge, "grpc-max-connection-age", gRPCMaxConnectionAge, "Maximum age of a client connection before GoAway is sent.")
		fs.DurationVar(&gRPCMaxConnectionAgeGrace, "grpc-max-connection-age-grace", gRPCMaxConnectionAgeGrace, "Additional grace period after grpc-max-connection-age, after which connections are forcibly closed.")
		fs.IntVar(&gRPCInitialConnWindowSize, "grpc-server-initial-conn-window-size", gRPCInitialConnWindowSize, "gRPC server initial connection window size")
		fs.IntVar(&gRPCInitialWindowSize, "grpc-server-initial-window-size", gRPCInitialWindowSize, "gRPC server initial window size")
		fs.DurationVar(&gRPCKeepAliveEnforcementPolicyMinTime, "grpc-server-keepalive-enforcement-policy-min-time", gRPCKeepAliveEnforcementPolicyMinTime, "gRPC server minimum keepalive time")
		fs.BoolVar(&gRPCKeepAliveEnforcementPolicyPermitWithoutStream, "grpc-server-keepalive-enforcement-policy-permit-without-stream", gRPCKeepAliveEnforcementPolicyPermitWithoutStream, "gRPC server permit client keepalive pings even when there are no active streams (RPCs)")

		fs.StringVar(&gRPCCert, "grpc-cert", gRPCCert, "server certificate to use for gRPC connections, requires grpc-key, enables TLS")
		fs.StringVar(&gRPCKey, "grpc-key", gRPCKey, "server private key to use for gRPC connections, requires grpc-cert, enables TLS")
		fs.StringVar(&gRPCCA, "grpc-ca", gRPCCA, "server CA to use for gRPC connections, requires TLS, and enforces client certificate check")
		fs.StringVar(&gRPCCRL, "grpc-crl", gRPCCRL, "path to a certificate revocation list in PEM format, client certificates will be further verified against this file during TLS handshake")
		fs.BoolVar(&gRPCEnableOptionalTLS, "grpc-enable-optional-tls", gRPCEnableOptionalTLS, "enable optional TLS mode when a server accepts both TLS and plain-text connections on the same port")
		fs.StringVar(&gRPCServerCA, "grpc-server-ca", gRPCServerCA, "path to server CA in PEM format, which will be combine with server cert, return full certificate chain to clients")
		fs.DurationVar(&gRPCKeepaliveTime, "grpc-server-keepalive-time", gRPCKeepaliveTime, "After a duration of this time, if the server doesn't see any activity, it pings the client to see if the transport is still alive.")
		fs.DurationVar(&gRPCKeepaliveTimeout, "grpc-server-keepalive-timeout", gRPCKeepaliveTimeout, "After having pinged for keepalive check, the server waits for a duration of Timeout and if no activity is seen even after that the connection is closed.")
	})
}

// GRPCCert returns the value of the `--grpc_cert` flag.
func GRPCCert() string {
	return gRPCCert
}

// GRPCCertificateAuthority returns the value of the `--grpc_ca` flag.
func GRPCCertificateAuthority() string {
	return gRPCCA
}

// GRPCKey returns the value of the `--grpc_key` flag.
func GRPCKey() string {
	return gRPCKey
}

// GRPCPort returns the value of the `--grpc_port` flag.
func GRPCPort() int {
	return gRPCPort
}

// GRPCBindAddress returns the value of the `--grpc-bind-address` flag.
func GRPCBindAddress() string {
	return gRPCBindAddress
}

// isGRPCEnabled returns true if gRPC server is set
func isGRPCEnabled() bool {
	if gRPCPort != 0 {
		return true
	}

	if socketFile != "" {
		return true
	}

	return false
}

// createGRPCServer create the gRPC server we will be using.
// It has to be called after flags are parsed, but before
// services register themselves.
func createGRPCServer() {
	// skip if not registered
	if !isGRPCEnabled() {
		slog.Info("GRPC is not enabled (no grpc-port or socket-file set), skipping gRPC server creation")
		return
	}

	var opts []grpc.ServerOption
	if gRPCCert != "" && gRPCKey != "" {
		slog.Error("TLS is not implemented yet")
		os.Exit(1)
	}
	// Override the default max message size for both send and receive
	// (which is 4 MiB in gRPC 1.0.0).
	// Large messages can occur when users try to insert or fetch very big
	// rows. If they hit the limit, they'll see the following error:
	// grpc: received message length XXXXXXX exceeding the max size 4194304
	// Note: For gRPC 1.0.0 it's sufficient to set the limit on the server only
	// because it's not enforced on the client side.
	msgSize := grpccommon.MaxMessageSize()
	slog.Info("Setting grpc max message size", "msgSize", msgSize)
	opts = append(opts, grpc.MaxRecvMsgSize(msgSize))
	opts = append(opts, grpc.MaxSendMsgSize(msgSize))

	if gRPCInitialConnWindowSize != 0 {
		slog.Info("Setting grpc server initial conn window size", "gRPCInitialConnWindowSize", int32(gRPCInitialConnWindowSize))
		opts = append(opts, grpc.InitialConnWindowSize(int32(gRPCInitialConnWindowSize)))
	}

	if gRPCInitialWindowSize != 0 {
		slog.Info("Setting grpc server initial window size", "gRPCInitialWindowSize", int32(gRPCInitialWindowSize))
		opts = append(opts, grpc.InitialWindowSize(int32(gRPCInitialWindowSize)))
	}

	ep := keepalive.EnforcementPolicy{
		MinTime:             gRPCKeepAliveEnforcementPolicyMinTime,
		PermitWithoutStream: gRPCKeepAliveEnforcementPolicyPermitWithoutStream,
	}
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(ep))

	ka := keepalive.ServerParameters{
		MaxConnectionAge:      gRPCMaxConnectionAge,
		MaxConnectionAgeGrace: gRPCMaxConnectionAgeGrace,
		Time:                  gRPCKeepaliveTime,
		Timeout:               gRPCKeepaliveTimeout,
	}
	opts = append(opts, grpc.KeepaliveParams(ka))

	opts = append(opts, interceptors()...)

	GRPCServer = grpc.NewServer(opts...)
}

// We can only set a ServerInterceptor once, so we chain multiple interceptors into one
func interceptors() []grpc.ServerOption {
	interceptors := &serverInterceptorBuilder{}

	if gRPCAuth != "" {
		slog.Info("enabling auth plugin", "plugin", gRPCAuth)
		pluginInitializer := GetAuthenticator(gRPCAuth)
		authPluginImpl, err := pluginInitializer()
		if err != nil {
			slog.Error("Failed to load auth plugin", "err", err)
			os.Exit(1)
		}
		authPlugin = authPluginImpl
		interceptors.Add(authenticatingStreamInterceptor, authenticatingUnaryInterceptor)
	}

	// TODO (@rafael) hook stats and tracing

	return interceptors.Build()
}

func serveGRPC() {
	// skip if not registered
	if gRPCPort == 0 {
		return
	}

	// register reflection to support list calls :)
	reflection.Register(GRPCServer)

	// register health service to support health checks
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(GRPCServer, healthServer)

	for service := range GRPCServer.GetServiceInfo() {
		healthServer.SetServingStatus(service, healthpb.HealthCheckResponse_SERVING)
	}

	// listen on the port
	slog.Info("Listening for gRPC calls on port", "grpcPort", gRPCPort)
	listener, err := net.Listen("tcp", net.JoinHostPort(gRPCBindAddress, strconv.Itoa(gRPCPort)))
	if err != nil {
		slog.Error("Cannot listen on the provided grpc port", "err", err)
		os.Exit(1)
	}

	// and serve on it
	// NOTE: Before we call Serve(), all services must have registered themselves
	//       with "GRPCServer". This is the case because go/vt/servenv/run.go
	//       runs all OnRun() hooks after createGRPCServer() and before
	//       serveGRPC(). If this was not the case, the binary would crash with
	//       the error "grpc: Server.RegisterService after Server.Serve".
	go func() {
		err := GRPCServer.Serve(listener)
		if err != nil {
			slog.Error("Failed to start grpc server", "err", err)
			os.Exit(1)
		}
	}()

	OnTermSync(func() {
		slog.Info("Initiated graceful stop of gRPC server")
		GRPCServer.GracefulStop()
		slog.Info("gRPC server stopped")
	})
}

// GRPCCheckServiceMap returns if we should register a gRPC service
// (and also logs how to enable / disable it)
func GRPCCheckServiceMap(name string) bool {
	// Silently fail individual services if gRPC is not enabled in
	// the first place (either on a grpc port or on the socket file)
	if !isGRPCEnabled() {
		return false
	}

	// then check ServiceMap
	return checkServiceMap("grpc", name)
}

func authenticatingStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	newCtx, err := authPlugin.Authenticate(stream.Context(), info.FullMethod)
	if err != nil {
		return err
	}

	wrapped := WrapServerStream(stream)
	wrapped.WrappedContext = newCtx
	return handler(srv, wrapped)
}

func authenticatingUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	newCtx, err := authPlugin.Authenticate(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	return handler(newCtx, req)
}

// WrappedServerStream is based on the service stream wrapper from: https://github.com/grpc-ecosystem/go-grpc-middleware
type WrappedServerStream struct {
	grpc.ServerStream
	WrappedContext context.Context
}

// Context returns the wrapper's WrappedContext, overwriting the nested grpc.ServerStream.Context()
func (w *WrappedServerStream) Context() context.Context {
	return w.WrappedContext
}

// WrapServerStream returns a ServerStream that has the ability to overwrite context.
func WrapServerStream(stream grpc.ServerStream) *WrappedServerStream {
	if existing, ok := stream.(*WrappedServerStream); ok {
		return existing
	}
	return &WrappedServerStream{ServerStream: stream, WrappedContext: stream.Context()}
}

// serverInterceptorBuilder chains together multiple ServerInterceptors
type serverInterceptorBuilder struct {
	streamInterceptors []grpc.StreamServerInterceptor
	unaryInterceptors  []grpc.UnaryServerInterceptor
}

// Add adds interceptors to the builder
func (collector *serverInterceptorBuilder) Add(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor) {
	collector.streamInterceptors = append(collector.streamInterceptors, s)
	collector.unaryInterceptors = append(collector.unaryInterceptors, u)
}

// AddUnary adds a single unary interceptor to the builder
func (collector *serverInterceptorBuilder) AddUnary(u grpc.UnaryServerInterceptor) {
	collector.unaryInterceptors = append(collector.unaryInterceptors, u)
}

// Build returns DialOptions to add to the grpc.Dial call
func (collector *serverInterceptorBuilder) Build() []grpc.ServerOption {
	slog.Info("Building interceptors", "unary", len(collector.unaryInterceptors), "stream", len(collector.streamInterceptors))
	switch len(collector.unaryInterceptors) + len(collector.streamInterceptors) {
	case 0:
		return []grpc.ServerOption{}
	default:
		return []grpc.ServerOption{
			grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(collector.unaryInterceptors...)),
			grpc.StreamInterceptor(grpcmiddleware.ChainStreamServer(collector.streamInterceptors...)),
		}
	}
}
