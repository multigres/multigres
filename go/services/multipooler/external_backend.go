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

package multipooler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

// backingConnection is the resolved configuration of the external PostgreSQL an
// unmanaged pooler fronts. It is loaded by name (--backing-connection); no
// endpoint or credential is ever supplied directly on the command line.
type backingConnection struct {
	Name           string
	Host           string
	Port           int
	Database       string
	User           string
	Password       string
	SSLMode        string
	SSLRootCert    string
	SSLNegotiation string
}

// Validate checks that the connection is complete enough to dial, by the rules the default
// primary applies to the connections it serves (backingconn.Validate).
func (c *backingConnection) Validate() error {
	return backingconn.Validate(c.Host, c.Port, c.Database, c.User)
}

// checkBackingDatabase refuses a backing connection that serves another database than the
// pooler's own: the pooler advertises --database to the cluster and dials the connection's.
func checkBackingDatabase(name string, backend *manager.ExternalBackend, database string) error {
	if backend.Database != database {
		return fmt.Errorf("backing connection %q serves database %q, which does not match --database %q", name, backend.Database, database)
	}
	return nil
}

// connectionSource gives a pooler the configuration of the external PostgreSQL it
// fronts, by the name given in --backing-connection. Where that configuration lives is
// not decided here: the default is a source that refuses, so an unmanaged pooler does not
// start, with a clear error, until one is set.
type connectionSource interface {
	BackingConnection(ctx context.Context, name string) (*backingConnection, error)
}

// unconfiguredSource is the default source.
type unconfiguredSource struct{}

func (unconfiguredSource) BackingConnection(_ context.Context, name string) (*backingConnection, error) {
	return nil, fmt.Errorf("backing connection %q: no source of backing connections is configured", name)
}

// errInvalidBackingConnection marks a connection that a source returned and that can never be
// used as it is: none at all, incomplete, or naming more than one server. Asking again gets
// the same answer.
var errInvalidBackingConnection = errors.New("invalid backing connection")

// fetchBackingConnection asks a source for a connection and returns it only if it is usable:
// a source that answers with nothing, or with a connection that is not complete or names more
// than one server, is refused, whoever the source is.
func fetchBackingConnection(ctx context.Context, source connectionSource, name string) (*backingConnection, error) {
	conn, err := source.BackingConnection(ctx, name)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, fmt.Errorf("%w: backing connection %q: the source returned none", errInvalidBackingConnection, name)
	}
	if err := conn.Validate(); err != nil {
		return nil, fmt.Errorf("%w: backing connection %q: %w", errInvalidBackingConnection, name, err)
	}
	return conn, nil
}

// leadershipModeFor derives the pooler's leadership mode. A pooler that names a
// backing connection fronts an external PostgreSQL and is unmanaged; a static
// leader (minigres) leads its shard without consensus; every other pooler owns
// its postgres through pgctld and is elected by consensus. A static leader
// manages its own postgres, so it cannot also name a backing connection.
func leadershipModeFor(backingConnection string, staticLeader bool) (clustermetadatapb.LeadershipMode, error) {
	switch {
	case backingConnection != "" && staticLeader:
		return 0, errors.New("--backing-connection cannot be used by a static leader, which manages its own postgres")
	case backingConnection != "":
		return clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, nil
	case staticLeader:
		return clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC, nil
	default:
		return clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS, nil
	}
}

// The flags that describe a postgres managed through pgctld. A managed pooler adopts the first
// two groups from pgctld's Status unless they are set (adoptPgctldValues); an unmanaged pooler
// fronts an external postgres that has no pgctld, no pgBackRest and no data directory of its own,
// so it refuses every one of them (validateBackendFlags).
var (
	// pgctldRequiredFlags are the identity of the postgres pgctld runs: Init fails when pgctld
	// is unreachable and these are not set.
	pgctldRequiredFlags = []string{"pg-port", "pooler-dir"}
	// pgctldOptionalFlags have workable defaults: Init starts degraded when pgctld is
	// unreachable and these are not set.
	pgctldOptionalFlags = []string{"pgbackrest-port", "pgbackrest-cert-file", "pgbackrest-key-file", "pgbackrest-ca-file"}
	// managedOnlyFlags are never adopted from pgctld and still mean nothing without it.
	managedOnlyFlags = []string{"socket-file", "pgctld-addr", "pgbackrest-cipher-key-file"}
)

// managedPostgresFlags returns every flag that belongs to pgctld or to a postgres it manages.
func managedPostgresFlags() []string {
	return slices.Concat(pgctldRequiredFlags, pgctldOptionalFlags, managedOnlyFlags)
}

// validateBackendFlags rejects flags that describe a pgctld-managed local
// postgres when --backing-connection is set, rather than silently ignoring
// them: the endpoint of an unmanaged pooler comes only from its backing
// connection, and it has no pgctld or backups to configure.
func validateBackendFlags(backingConnection string, explicit func(name string) bool) error {
	if backingConnection == "" {
		return nil
	}
	for _, name := range managedPostgresFlags() {
		if explicit(name) {
			return fmt.Errorf("--%s configures a pgctld-managed postgres and cannot be combined with --backing-connection; an unmanaged pooler takes its endpoint from the backing connection and has no pgctld or backups", name)
		}
	}
	return nil
}

// requireUnchangedManagement refuses to start a pooler under an id whose topology record is
// unmanaged when the pooler is managed, or the other way round. A managed member of a cohort
// and an unmanaged front for an external database are different components even under the
// same name; letting a restart flip one into the other would leave the orchestrator's view of
// that id describing the wrong thing. A missing record is a first start.
//
// A read that fails for another reason refuses an unmanaged pooler, whose bootstrap needs the
// topology anyway, and lets a managed one start, so that a managed pooler still starts while the
// topology is unreachable, as it does without this check. The invariant does not rest on this
// read: RegisterMultipooler refuses to change whether an existing record is managed
// (topoclient.ErrManagementChanged), so such a pooler never overwrites the record once the
// topology is back; this check only makes the common case fail at startup.
func requireUnchangedManagement(ctx context.Context, ts topoclient.Store, id *clustermetadatapb.ID, mode clustermetadatapb.LeadershipMode, logger *slog.Logger) error {
	if ts == nil {
		return nil
	}
	info, err := ts.GetMultipooler(ctx, id)
	switch {
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}):
		return nil
	case err != nil && !topoclient.IsManagedLeadershipMode(mode):
		return fmt.Errorf("read the topology record of pooler %s to check whether it is unmanaged: %w", topoclient.ComponentIDString(id), err)
	case err != nil:
		logger.WarnContext(ctx, "could not read this pooler's topology record to check whether it is unmanaged; starting anyway",
			"pooler_id", topoclient.ComponentIDString(id), "error", err)
		return nil
	}
	recorded := info.GetLeadershipMode()
	if topoclient.IsManagedLeadershipMode(recorded) != topoclient.IsManagedLeadershipMode(mode) {
		return fmt.Errorf("pooler %s is registered in the topology as %s and cannot start as %s: "+
			"start it under a new name, or delete its topology record first",
			topoclient.ComponentIDString(id), recorded, mode)
	}
	return nil
}
