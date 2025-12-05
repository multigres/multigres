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
	"fmt"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var grpcAuthServerFlagHooks []func(*pflag.FlagSet)

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
