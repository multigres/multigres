// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcmigrationservice

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

// TestMigrationServiceIsRegisteredOnlyOnAManagedPooler: an unmanaged pooler fronts an
// external database that multigres does not own, so it serves no migration RPCs and starts no
// migration coordinator that could create migration state in that database. A managed pooler
// still serves them.
func TestMigrationServiceIsRegisteredOnlyOnAManagedPooler(t *testing.T) {
	for name, tc := range map[string]struct {
		mode       clustermetadata.LeadershipMode
		tablegroup string
		registered bool
	}{
		"unmanaged": {clustermetadata.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, "migrateTG", false},
		"managed":   {clustermetadata.LeadershipMode_LEADERSHIP_MODE_CONSENSUS, "default", true},
	} {
		t.Run(name, func(t *testing.T) {
			server := grpc.NewServer()
			registerMigrationService(server, manager.NewManagerForTesting(t, tc.mode, tc.tablegroup))
			_, registered := server.GetServiceInfo()[migratorpb.Migrator_ServiceDesc.ServiceName]
			require.Equal(t, tc.registered, registered)
		})
	}
}
