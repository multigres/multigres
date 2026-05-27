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

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/version"
	serviceinfopb "github.com/multigres/multigres/go/pb/serviceinfo"
)

// serviceInfoServer implements the ServiceInfo gRPC service. servenv
// registers this on every gRPC server it spins up so callers can fetch
// build identity from any multigres process without per-service plumbing.
type serviceInfoServer struct {
	serviceinfopb.UnimplementedServiceInfoServer
}

func (s *serviceInfoServer) GetBuildInfo(_ context.Context, _ *serviceinfopb.GetBuildInfoRequest) (*serviceinfopb.GetBuildInfoResponse, error) {
	snap := version.Read()
	bi := &serviceinfopb.BuildInfo{
		Version:   version.Version,
		Revision:  snap.Revision,
		Modified:  snap.Modified,
		GoVersion: snap.GoVersion,
		MainPath:  snap.MainPath,
	}
	if !snap.CommitTime.IsZero() {
		bi.CommitTime = timestamppb.New(snap.CommitTime)
	}
	return &serviceinfopb.GetBuildInfoResponse{BuildInfo: bi}, nil
}

// registerServiceInfo wires the ServiceInfo gRPC service onto a server.
// Called from GrpcServer.Serve so every multigres process exposes it.
func registerServiceInfo(s *grpc.Server) {
	serviceinfopb.RegisterServiceInfoServer(s, &serviceInfoServer{})
}
