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

package grpcpoolerservice

import (
	"github.com/multigres/multigres/go/common/mterrors"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// StreamReplication tunnels the raw PostgreSQL replication wire protocol as
// opaque bytes between the gateway and a dedicated replication=database backend
// (poolerserver.ServeReplication, the same code minigres's direct stream runs).
func (s *poolerService) StreamReplication(stream multipoolerpb.MultipoolerService_StreamReplicationServer) error {
	return mterrors.ToGRPC(s.pooler.ServeReplication(stream.Context(), stream.Recv, stream.Send))
}
