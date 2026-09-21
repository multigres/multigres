// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package grpcpoolerservice

import (
	"github.com/multigres/multigres/go/common/queryrpc"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func (s *poolerService) ExecuteStream(stream pb.MultipoolerService_ExecuteStreamServer) error {
	return queryrpc.Serve(stream, s.StreamExecute)
}
