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
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var grpcAuthServerFlagHooks []func(*pflag.FlagSet)

// errGRPCAuthFailed is the only detail ever returned to a gRPC caller for a
// rejected request, across every Authenticator implementation (mtls, jwt) -
// see authFailedMessage (http_auth.go) for why the real reason is logged
// server-side instead of returned over the wire.
var errGRPCAuthFailed = status.Error(codes.Unauthenticated, authFailedMessage)

// RegisterGRPCServerAuthFlags registers flags required to enable server-side
// authentication in multigres gRPC services.
//
// `go/cmd/*` entrypoints should call this function before
// ParseFlags(WithArgs)? if they wish to expose Authenticator functionality.
func RegisterGRPCServerAuthFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		for _, fn := range grpcAuthServerFlagHooks {
			fn(fs)
		}
	})
}

// Auth returns the auth mode
func (g *GrpcServer) Auth() string {
	return g.auth.Get()
}

// AuthPlugin returns the resolved Authenticator plugin instance for the
// currently active auth mode, or nil if no auth mode is configured. Callers
// that need it before GrpcServer.Create() has run - e.g. HTTP routes
// registered during a service's Init(), before Create() runs during Run() -
// must resolve it lazily, per-call, rather than capturing the result once at
// wiring time, since resolution happens asynchronously with respect to the
// HTTP listener coming up (see run.go).
//
// While an auth mode is configured but resolveAuthPlugin hasn't finished yet,
// this deliberately does NOT return nil - nil means "no auth configured,
// pass every request through" everywhere it's checked (AuthenticateBearer,
// the gRPC interceptors), so returning it here during the resolution window
// would silently authenticate every HTTP/Connect/REST/pprof request that
// happens to race startup. Instead it returns pendingAuthPlugin, which always
// fails, so requests are rejected (fail closed) until resolution completes.
func (g *GrpcServer) AuthPlugin() Authenticator {
	if plugin := g.resolvedAuthPlugin(); plugin != nil {
		return plugin
	}
	if g.auth.Get() == "" {
		return nil
	}
	return pendingAuthPlugin{}
}

// resolvedAuthPlugin returns the plugin resolved by resolveAuthPlugin, or nil
// if none is configured or resolution hasn't completed. Unlike AuthPlugin,
// this has no "pending" case: it's only used by the gRPC interceptors built
// in Create(), after resolveAuthPlugin has already run to completion as part
// of that same call - see interceptors().
func (g *GrpcServer) resolvedAuthPlugin() Authenticator {
	box := g.authPluginBox.Load()
	if box == nil {
		return nil
	}
	return box.authenticator
}

// authenticatorBox lets GrpcServer store an Authenticator behind an
// atomic.Pointer. atomic.Pointer[Authenticator] would itself need to hold a
// *Authenticator, which is awkward to populate for an interface value;
// boxing it behind a concrete struct avoids that.
type authenticatorBox struct {
	authenticator Authenticator
}

// pendingAuthPlugin is what GrpcServer.AuthPlugin returns while an auth mode
// is configured but not yet resolved (see its doc comment for why this must
// fail closed rather than act like "no auth configured"). It implements both
// Authenticator and TokenVerifier so it fails closed on every transport this
// registry is used from.
type pendingAuthPlugin struct{}

var (
	_ Authenticator = pendingAuthPlugin{}
	_ TokenVerifier = pendingAuthPlugin{}
)

func (pendingAuthPlugin) Authenticate(context.Context, string) (context.Context, error) {
	// codes.Unauthenticated, not codes.Unavailable: a client with a retry
	// policy that treats Unavailable as transient/retryable would otherwise
	// retry a deliberate auth rejection instead of surfacing it as a failure.
	return nil, errGRPCAuthFailed
}

func (pendingAuthPlugin) VerifyToken(string) (jwt.MapClaims, error) {
	return nil, errors.New("auth plugin still initializing")
}

// Authenticator provides an interface to implement auth in Multigres in
// grpc server
type Authenticator interface {
	Authenticate(ctx context.Context, fullMethod string) (context.Context, error)
}

// authPlugins is a registry of AuthPlugin initializers.
var authPlugins = make(map[string]func() (Authenticator, error))

// RegisterAuthPlugin registers an implementation of AuthServer.
// Returns an error if a plugin with the same name is already registered.
func RegisterAuthPlugin(name string, authPlugin func() (Authenticator, error)) error {
	if _, ok := authPlugins[name]; ok {
		return fmt.Errorf("AuthPlugin %q already registered", name)
	}
	authPlugins[name] = authPlugin
	return nil
}

// GetAuthenticator returns an AuthPlugin by name.
func GetAuthenticator(name string) (func() (Authenticator, error), error) {
	authPlugin, ok := authPlugins[name]
	if !ok {
		return nil, fmt.Errorf("no AuthPlugin %q registered", name)
	}
	return authPlugin, nil
}

// FakeAuthStreamInterceptor fake interceptor to test plugin
func FakeAuthStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if fakeDummyAuthenticate(stream.Context()) {
		return handler(srv, stream)
	}
	return status.Errorf(codes.Unauthenticated, "username and password must be provided")
}

// FakeAuthUnaryInterceptor fake interceptor to test plugin
func FakeAuthUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if fakeDummyAuthenticate(ctx) {
		return handler(ctx, req)
	}
	return nil, status.Errorf(codes.Unauthenticated, "username and password must be provided")
}

func fakeDummyAuthenticate(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md["username"]) == 0 || len(md["password"]) == 0 {
			return false
		}
		username := md["username"][0]
		password := md["password"][0]
		if username == "valid" && password == "valid" {
			return true
		}
		return false
	}
	return false
}
