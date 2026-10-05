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

package multigateway

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/readonly"
)

// watchReadOnly mirrors the read-only flags of every topo Database record
// into mg.readOnly for the gateway's lifetime, and terminates write sessions
// of a database when its force flag is first observed. Blocks until ctx is
// cancelled; run it in a goroutine.
func (mg *Multigateway) watchReadOnly(ctx context.Context, logger *slog.Logger) {
	apply := func(wd *topoclient.WatchDataRecursive) {
		database, ok := databaseFromRecordPath(wd.Path)
		if !ok {
			return
		}
		var mode readonly.Mode
		if wd.Err == nil {
			rec := &clustermetadatapb.Database{}
			if err := proto.Unmarshal(wd.Contents, rec); err != nil {
				logger.ErrorContext(ctx, "ignoring undecodable database record", "path", wd.Path, "error", err)
				return
			}
			mode = readonly.Mode{Enabled: rec.GetReadOnly(), Force: rec.GetReadOnly() && rec.GetReadOnlyForce()}
		}
		prev := mg.readOnly.Set(database, mode)
		if mode == prev {
			return
		}
		logger.InfoContext(ctx, "database read-only mode changed", "database", database, "read_only", mode.Enabled, "force", mode.Force)
		if mode.Force && !prev.Force {
			mg.terminateWriteSessions(ctx, logger, database)
		}
	}
	topoclient.WatchPathWithRetry(ctx, mg.ts, topoclient.GlobalCell, topoclient.DatabasesPath, logger,
		func(initial []*topoclient.WatchDataRecursive) {
			for _, wd := range initial {
				apply(wd)
			}
		},
		func(changes <-chan *topoclient.WatchDataRecursive) {
			for {
				select {
				case <-ctx.Done():
					return
				case wd, ok := <-changes:
					if !ok {
						return
					}
					apply(wd)
				}
			}
		})
}

// databaseFromRecordPath extracts the database name from a watched path of
// the form .../databases/<name>/Database.
func databaseFromRecordPath(watchPath string) (string, bool) {
	_, after, found := strings.Cut(watchPath, topoclient.DatabasesPath+"/")
	if !found {
		return "", false
	}
	database, file, _ := strings.Cut(after, "/")
	if database == "" || file != topoclient.DatabaseFile {
		return "", false
	}
	return database, true
}

// terminateWriteSessions closes, with FATAL 57P01, every primary-port session
// on database that is inside a transaction or holds a pinned backend. Those
// sessions keep the read-write default they started with, so read-only mode
// cannot reach them any other way. Sessions on the replica port never write.
func (mg *Multigateway) terminateWriteSessions(ctx context.Context, logger *slog.Logger, database string) {
	terminated := 0
	for _, conn := range mg.pgListener.Conns() {
		if conn.Database() != database || !writeSession(conn) {
			continue
		}
		conn.Terminate()
		terminated++
	}
	logger.InfoContext(ctx, "terminated write sessions for read-only database", "database", database, "sessions", terminated)
}

func writeSession(conn *server.Conn) bool {
	state, _ := conn.GetConnectionState().(*handler.MultigatewayConnectionState)
	return engine.SessionPinned(conn, state, constants.DefaultTableGroup, constants.DefaultShard)
}
