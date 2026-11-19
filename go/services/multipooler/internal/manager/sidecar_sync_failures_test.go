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

package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

func TestSidecarSyncRepeatedRefusalIsBounded(t *testing.T) {
	s, updates := newSyncFixture(t, 3, true, 0)
	primaryMock(s, "f")
	tableMock(s, 5)
	s.pm.sidecarWalSegBytes.Store(123)
	s.fake.configureReply = func(*pb.ConfigureRequest) *pb.ConfigureResponse {
		return &pb.ConfigureResponse{Accepted: false, HeldRevision: 200}
	}
	require.ErrorContains(t, s.pm.syncSidecar(t.Context()), "refused revision 100, holding 200")
	require.Len(t, *updates, 1, "one floor reconciliation per pass, not an unbounded retry loop")
	require.Equal(t, 2, s.fake.configureCount())
	require.Equal(t, uint32(123), s.pm.sidecarWalSegBytes.Load(), "failed Configure must not acknowledge a segment-size update")
}

func TestSidecarSyncStandbyDoesNotRaiseRejectedRevision(t *testing.T) {
	s, updates := newSyncFixture(t, 3, true, 0)
	primaryMock(s, "t")
	tableMock(s, 5)
	s.fake.configureReply = func(*pb.ConfigureRequest) *pb.ConfigureResponse {
		return &pb.ConfigureResponse{Accepted: false, HeldRevision: 200}
	}
	require.NoError(t, s.pm.syncSidecar(t.Context()))
	require.Empty(t, *updates, "standby must wait for replicated metadata instead of updating it")
	require.Equal(t, 1, s.fake.configureCount(), "no resend of stale metadata")
}

func TestReplicationPassfileFailureDoesNotPublishPath(t *testing.T) {
	for _, obstacle := range []string{"missing pooler", "destination directory"} {
		t.Run(obstacle, func(t *testing.T) {
			volume := t.TempDir()
			pooler := filepath.Join(volume, "pooler")
			pm := volumeRootTestManager(t, volume, pooler)
			if obstacle == "destination directory" {
				require.NoError(t, os.MkdirAll(filepath.Join(pooler, replicationPassfileName), 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(pooler, replicationPassfileName, "keep"), []byte("evidence"), 0o600))
			}
			require.Error(t, pm.writeReplicationPassfile())
			require.Empty(t, pm.pgpassFilePath(), "failed writes must not advertise a usable replication credential")
			if obstacle == "destination directory" {
				data, err := os.ReadFile(filepath.Join(pooler, replicationPassfileName, "keep"))
				require.NoError(t, err)
				require.Equal(t, "evidence", string(data))
			}
		})
	}
}
