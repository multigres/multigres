// Copyright 2026 Supabase, Inc.
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

package grpcpoolerservice

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
)

// eofNotificationStream is a gateway that opens a stream and immediately
// closes its send side.
type eofNotificationStream struct {
	multipoolerpb.MultipoolerService_NotificationStreamServer
	ctx context.Context
}

func (s *eofNotificationStream) Context() context.Context { return s.ctx }

func (s *eofNotificationStream) Recv() (*multipoolerpb.NotificationStreamRequest, error) {
	return nil, io.EOF
}

// The manager replaces the listener on every connection reopen, after the gRPC
// service is registered. The service must serve the listener current at the
// time of the stream, not the one (here: none) present at registration.
func TestNotificationStream_UsesListenerSetAfterRegistration(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	p := poolerserver.NewQueryPoolerServer(logger, nil, &clustermetadatapb.ID{Name: "p"}, "tg", "0", nil, 0, false)
	srv := &poolerService{pooler: p}

	p.SetPubSubListener(pubsub.NewListener(nil, logger, nil))

	err := srv.NotificationStream(&eofNotificationStream{ctx: t.Context()})
	require.NoError(t, err)
}
