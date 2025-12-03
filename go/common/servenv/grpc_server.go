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

	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/pflag"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

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

// GrpcServer holds all gRPC server configuration and the server instance
type GrpcServer struct {
	// auth specifies which auth plugin to use. Currently only "static" and "mtls" are supported.
	auth viperutil.Value[string]

	// port is the port to listen on for gRPC. If zero, don't listen.
	port viperutil.Value[int]

	// bindAddress is the address to bind to for gRPC. If empty, bind to all addresses.
	bindAddress viperutil.Value[string]

	// maxConnectionAge is the maximum age of a client connection, before GoAway is sent.
	maxConnectionAge viperutil.Value[time.Duration]

	// maxConnectionAgeGrace is an additional grace period after maxConnectionAge
	maxConnectionAgeGrace viperutil.Value[time.Duration]

	// initialConnWindowSize sets window size for a connection.
	initialConnWindowSize viperutil.Value[int]

	// initialWindowSize sets window size for stream.
	initialWindowSize viperutil.Value[int]

	// keepAliveEnforcementPolicyMinTime sets the keepalive enforcement policy on the server.
	keepAliveEnforcementPolicyMinTime viperutil.Value[time.Duration]

	// keepAliveEnforcementPolicyPermitWithoutStream allows keepalive pings even when there are no active streams
	keepAliveEnforcementPolicyPermitWithoutStream viperutil.Value[bool]

	// keepaliveTime is the time after which the server pings the client if no activity is seen
	keepaliveTime viperutil.Value[time.Duration]

	// keepaliveTimeout is the wait time after keepalive ping before closing the connection
	keepaliveTimeout viperutil.Value[time.Duration]

	// cert is the certificate file path for TLS
	cert viperutil.Value[string]

	// key is the private key file path for TLS
	key viperutil.Value[string]

	// ca is the CA file path for TLS
	ca viperutil.Value[string]

	// crl is the Certificate Revocation List file path
	crl viperutil.Value[string]

	// enableOptionalTLS enables optional TLS mode
	enableOptionalTLS viperutil.Value[bool]

	// serverCA is the server CA file path
	serverCA viperutil.Value[string]

	// Server is the actual gRPC server instance
	Server *grpc.Server

	// authPlugin is the authenticator plugin
	authPlugin Authenticator

	// socketFile is the named socket for RPCs
	socketFile viperutil.Value[string]
}

// NewGrpcServer creates and initializes a new GrpcServer with viperutil values
func NewGrpcServer(reg *viperutil.Registry) *GrpcServer {
	return &GrpcServer{
		auth: viperutil.Configure(reg, "grpc-auth-mode", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-auth-mode",
			Dynamic:  false,
		}),
		port: viperutil.Configure(reg, "grpc-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "grpc-port",
			Dynamic:  false,
		}),
		bindAddress: viperutil.Configure(reg, "grpc-bind-address", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-bind-address",
			Dynamic:  false,
		}),
		maxConnectionAge: viperutil.Configure(reg, "grpc-max-connection-age", viperutil.Options[time.Duration]{
			Default:  time.Duration(math.MaxInt64),
			FlagName: "grpc-max-connection-age",
			Dynamic:  false,
		}),
		maxConnectionAgeGrace: viperutil.Configure(reg, "grpc-max-connection-age-grace", viperutil.Options[time.Duration]{
			Default:  time.Duration(math.MaxInt64),
			FlagName: "grpc-max-connection-age-grace",
			Dynamic:  false,
		}),
		initialConnWindowSize: viperutil.Configure(reg, "grpc-server-initial-conn-window-size", viperutil.Options[int]{
			Default:  0,
			FlagName: "grpc-server-initial-conn-window-size",
			Dynamic:  false,
		}),
		initialWindowSize: viperutil.Configure(reg, "grpc-server-initial-window-size", viperutil.Options[int]{
			Default:  0,
			FlagName: "grpc-server-initial-window-size",
			Dynamic:  false,
		}),
		keepAliveEnforcementPolicyMinTime: viperutil.Configure(reg, "grpc-server-keepalive-enforcement-policy-min-time", viperutil.Options[time.Duration]{
			Default:  10 * time.Second,
			FlagName: "grpc-server-keepalive-enforcement-policy-min-time",
			Dynamic:  false,
		}),
		keepAliveEnforcementPolicyPermitWithoutStream: viperutil.Configure(reg, "grpc-server-keepalive-enforcement-policy-permit-without-stream", viperutil.Options[bool]{
			Default:  false,
			FlagName: "grpc-server-keepalive-enforcement-policy-permit-without-stream",
			Dynamic:  false,
		}),
		keepaliveTime: viperutil.Configure(reg, "grpc-server-keepalive-time", viperutil.Options[time.Duration]{
			Default:  10 * time.Second,
			FlagName: "grpc-server-keepalive-time",
			Dynamic:  false,
		}),
		keepaliveTimeout: viperutil.Configure(reg, "grpc-server-keepalive-timeout", viperutil.Options[time.Duration]{
			Default:  10 * time.Second,
			FlagName: "grpc-server-keepalive-timeout",
			Dynamic:  false,
		}),
		cert: viperutil.Configure(reg, "grpc-cert", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-cert",
			Dynamic:  false,
		}),
		key: viperutil.Configure(reg, "grpc-key", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-key",
			Dynamic:  false,
		}),
		ca: viperutil.Configure(reg, "grpc-ca", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-ca",
			Dynamic:  false,
		}),
		crl: viperutil.Configure(reg, "grpc-crl", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-crl",
			Dynamic:  false,
		}),
		enableOptionalTLS: viperutil.Configure(reg, "grpc-enable-optional-tls", viperutil.Options[bool]{
			Default:  false,
			FlagName: "grpc-enable-optional-tls",
			Dynamic:  false,
		}),
		serverCA: viperutil.Configure(reg, "grpc-server-ca", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-server-ca",
			Dynamic:  false,
		}),
		socketFile: viperutil.Configure(reg, "grpc-socket-file", viperutil.Options[string]{
			Default:  "",
			FlagName: "grpc-socket-file",
			Dynamic:  false,
		}),
	}
}

// RegisterFlags registers all gRPC server flags with the given FlagSet
func (g *GrpcServer) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("grpc-auth-mode", g.auth.Default(), "gRPC auth plugin to use (e.g., 'static', 'mtls')")
	fs.Int("grpc-port", g.port.Default(), "Port to listen on for gRPC calls. If zero, do not listen.")
	fs.String("grpc-bind-address", g.bindAddress.Default(), "Bind address for gRPC calls. If empty, listen on all addresses.")
	fs.Duration("grpc-max-connection-age", g.maxConnectionAge.Default(), "Maximum age of a client connection before GoAway is sent.")
	fs.Duration("grpc-max-connection-age-grace", g.maxConnectionAgeGrace.Default(), "Additional grace period after grpc-max-connection-age, after which connections are forcibly closed.")
	fs.Int("grpc-server-initial-conn-window-size", g.initialConnWindowSize.Default(), "gRPC server initial connection window size")
	fs.Int("grpc-server-initial-window-size", g.initialWindowSize.Default(), "gRPC server initial window size")
	fs.Duration("grpc-server-keepalive-enforcement-policy-min-time", g.keepAliveEnforcementPolicyMinTime.Default(), "gRPC server minimum keepalive time")
	fs.Bool("grpc-server-keepalive-enforcement-policy-permit-without-stream", g.keepAliveEnforcementPolicyPermitWithoutStream.Default(), "gRPC server permit client keepalive pings even when there are no active streams (RPCs)")
	fs.String("grpc-cert", g.cert.Default(), "server certificate to use for gRPC connections, requires grpc-key, enables TLS")
	fs.String("grpc-key", g.key.Default(), "server private key to use for gRPC connections, requires grpc-cert, enables TLS")
	fs.String("grpc-ca", g.ca.Default(), "server CA to use for gRPC connections, requires TLS, and enforces client certificate check")
	fs.String("grpc-crl", g.crl.Default(), "path to a certificate revocation list in PEM format, client certificates will be further verified against this file during TLS handshake")
	fs.Bool("grpc-enable-optional-tls", g.enableOptionalTLS.Default(), "enable optional TLS mode when a server accepts both TLS and plain-text connections on the same port")
	fs.String("grpc-server-ca", g.serverCA.Default(), "path to server CA in PEM format, which will be combine with server cert, return full certificate chain to clients")
	fs.Duration("grpc-server-keepalive-time", g.keepaliveTime.Default(), "After a duration of this time, if the server doesn't see any activity, it pings the client to see if the transport is still alive.")
	fs.Duration("grpc-server-keepalive-timeout", g.keepaliveTimeout.Default(), "After having pinged for keepalive check, the server waits for a duration of Timeout and if no activity is seen even after that the connection is closed.")
	fs.String("grpc-socket-file", g.socketFile.Default(), "Local unix socket file to listen on")

	viperutil.BindFlags(fs,
		g.auth,
		g.port,
		g.bindAddress,
		g.maxConnectionAge,
		g.maxConnectionAgeGrace,
		g.initialConnWindowSize,
		g.initialWindowSize,
		g.keepAliveEnforcementPolicyMinTime,
		g.keepAliveEnforcementPolicyPermitWithoutStream,
		g.keepaliveTime,
		g.keepaliveTimeout,
		g.cert,
		g.key,
		g.ca,
		g.crl,
		g.enableOptionalTLS,
		g.serverCA,
		g.socketFile,
	)
}

// Cert returns the certificate path
func (g *GrpcServer) Cert() string {
	return g.cert.Get()
}

// CA returns the CA path
func (g *GrpcServer) CA() string {
	return g.ca.Get()
}

// Key returns the key path
func (g *GrpcServer) Key() string {
	return g.key.Get()
}

// Port returns the gRPC port
func (g *GrpcServer) Port() int {
	return g.port.Get()
}

// BindAddress returns the bind address
func (g *GrpcServer) BindAddress() string {
	return g.bindAddress.Get()
}

// IsEnabled returns true if gRPC server is enabled
func (g *GrpcServer) IsEnabled() bool {
	if g.port.Get() != 0 {
		return true
	}

	if g.socketFile.Get() != "" {
		return true
	}

	return false
}

// Create creates the gRPC server instance.
// It has to be called after flags are parsed, but before services register themselves.
func (g *GrpcServer) Create() {
	// skip if not enabled
	if !g.IsEnabled() {
		slog.Info("GRPC is not enabled (no grpc-port or socket-file set), skipping gRPC server creation")
		return
	}

	var opts []grpc.ServerOption
	if g.cert.Get() != "" && g.key.Get() != "" {
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

	if g.initialConnWindowSize.Get() != 0 {
		slog.Info("Setting grpc server initial conn window size", "gRPCInitialConnWindowSize", int32(g.initialConnWindowSize.Get()))
		opts = append(opts, grpc.InitialConnWindowSize(int32(g.initialConnWindowSize.Get())))
	}

	if g.initialWindowSize.Get() != 0 {
		slog.Info("Setting grpc server initial window size", "gRPCInitialWindowSize", int32(g.initialWindowSize.Get()))
		opts = append(opts, grpc.InitialWindowSize(int32(g.initialWindowSize.Get())))
	}

	ep := keepalive.EnforcementPolicy{
		MinTime:             g.keepAliveEnforcementPolicyMinTime.Get(),
		PermitWithoutStream: g.keepAliveEnforcementPolicyPermitWithoutStream.Get(),
	}
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(ep))

	ka := keepalive.ServerParameters{
		MaxConnectionAge:      g.maxConnectionAge.Get(),
		MaxConnectionAgeGrace: g.maxConnectionAgeGrace.Get(),
		Time:                  g.keepaliveTime.Get(),
		Timeout:               g.keepaliveTimeout.Get(),
	}
	opts = append(opts, grpc.KeepaliveParams(ka))

	// Add OpenTelemetry instrumentation for distributed tracing and metrics
	// If no OTEL exporters are configured, noop exporters are used with minimal overhead
	opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))

	opts = append(opts, g.interceptors()...)

	g.Server = grpc.NewServer(opts...)
}

// interceptors builds the list of interceptors for the gRPC server
func (g *GrpcServer) interceptors() []grpc.ServerOption {
	interceptors := &serverInterceptorBuilder{}

	if g.auth.Get() != "" {
		slog.Info("enabling auth plugin", "plugin", g.auth.Get())
		pluginInitializer := GetAuthenticator(g.auth.Get())
		authPluginImpl, err := pluginInitializer()
		if err != nil {
			slog.Error("Failed to load auth plugin", "err", err)
			os.Exit(1)
		}
		g.authPlugin = authPluginImpl
		interceptors.Add(
			func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				return g.authenticatingStreamInterceptor(srv, stream, info, handler)
			},
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				return g.authenticatingUnaryInterceptor(ctx, req, info, handler)
			},
		)
	}

	return interceptors.Build()
}

// Serve starts the gRPC server and begins listening for requests
func (g *GrpcServer) Serve(sv *ServEnv) {
	// skip if not enabled
	if g.port.Get() == 0 {
		return
	}

	// register reflection to support list calls :)
	reflection.Register(g.Server)

	// register health service to support health checks
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(g.Server, healthServer)

	for service := range g.Server.GetServiceInfo() {
		healthServer.SetServingStatus(service, healthpb.HealthCheckResponse_SERVING)
	}

	// listen on the port
	slog.Info("Listening for gRPC calls on port", "grpcPort", g.port.Get())
	listener, err := net.Listen("tcp", net.JoinHostPort(g.bindAddress.Get(), strconv.Itoa(g.port.Get())))
	if err != nil {
		slog.Error("Cannot listen on the provided grpc port", "err", err)
		os.Exit(1)
	}

	// and serve on it
	// NOTE: Before we call Serve(), all services must have registered themselves
	//       with the Server. This is the case because go/common/servenv/run.go
	//       runs all OnRun() hooks after Create() and before Serve().
	//       If this was not the case, the binary would crash with
	//       the error "grpc: Server.RegisterService after Server.Serve".
	go func() {
		err := g.Server.Serve(listener)
		if err != nil {
			slog.Error("Failed to start grpc server", "err", err)
			os.Exit(1)
		}
	}()

	sv.OnTermSync(func() {
		slog.Info("Initiated graceful stop of gRPC server")
		g.Server.GracefulStop()
		slog.Info("gRPC server stopped")
	})
}

// CheckServiceMap returns if we should register a gRPC service
func (g *GrpcServer) CheckServiceMap(name string, sv *ServEnv) bool {
	// Silently fail individual services if gRPC is not enabled in
	// the first place (either on a grpc port or on the socket file)
	if !g.IsEnabled() {
		return false
	}

	// then check ServiceMap
	return sv.checkServiceMap("grpc", name)
}

func (g *GrpcServer) authenticatingStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if g.authPlugin == nil {
		return handler(srv, stream)
	}

	newCtx, err := g.authPlugin.Authenticate(stream.Context(), info.FullMethod)
	if err != nil {
		return err
	}

	wrapped := WrapServerStream(stream)
	wrapped.WrappedContext = newCtx
	return handler(srv, wrapped)
}

func (g *GrpcServer) authenticatingUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if g.authPlugin == nil {
		return handler(ctx, req)
	}

	newCtx, err := g.authPlugin.Authenticate(ctx, info.FullMethod)
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
