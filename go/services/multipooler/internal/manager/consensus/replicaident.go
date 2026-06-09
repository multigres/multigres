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

package consensus

import (
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// MaxApplicationNameLength is the maximum length of a PostgreSQL application_name (NAMEDATALEN - 1).
const MaxApplicationNameLength = 63

// ReplicaID is a validated MULTIPOOLER identity for use in PostgreSQL replication.
// Construct via NewReplicaID(). Holds both the canonical cluster ID and its derived
// PostgreSQL application_name. The appName field is the only serialization format
// used internally; other formats (e.g., JSON, gRPC ID) are accessible via the id field.
type ReplicaID struct {
	id      *clustermetadatapb.ID
	appName string
}

// AppName returns the PostgreSQL application_name for this replica.
func (r ReplicaID) AppName() string { return r.appName }

// ID returns the canonical cluster ID for this replica.
func (r ReplicaID) ID() *clustermetadatapb.ID { return r.id }

// NewReplicaID generates the ReplicaID for a multipooler from its ID.
// Format: {cell}_{name}
// This is used consistently for:
// - primary_conninfo: standby's application_name when connecting to primary
// - synchronous_standby_names: standby identities on the primary
//
// On validation failure an approximate ReplicaID is returned alongside the error.
// The approximate appName is "{cell}_{name}" with missing fields replaced by
// "<unknown>". For overlong names the full (invalid) string is returned as-is.
// Callers that require a strictly valid appName must check the error. Callers that
// can tolerate an approximate name (e.g. for logging or informational responses)
// may use the returned value regardless.
func NewReplicaID(id *clustermetadatapb.ID) (ReplicaID, error) {
	if id == nil {
		return ReplicaID{appName: "<nil>"}, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "nil ID")
	}

	cell := id.Cell
	if cell == "" {
		cell = "<unknown>"
	}
	name := id.Name
	if name == "" {
		name = "<unknown>"
	}
	appName := fmt.Sprintf("%s_%s", cell, name)

	if id.Cell == "" {
		return ReplicaID{id: id, appName: appName}, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "empty cell")
	}
	if id.Name == "" {
		return ReplicaID{id: id, appName: appName}, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "empty name")
	}
	// Underscores are not allowed in Cell or Name because they are used as delimiters
	// in the application_name format (cell_name). Allowing underscores would break parsing.
	if strings.Contains(id.Cell, "_") {
		return ReplicaID{id: id, appName: appName}, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"cell contains underscore: %q (underscores not allowed)", id.Cell)
	}
	if strings.Contains(id.Name, "_") {
		return ReplicaID{id: id, appName: appName}, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"name contains underscore: %q (underscores not allowed)", id.Name)
	}
	if len(appName) > MaxApplicationNameLength {
		return ReplicaID{id: id, appName: appName}, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"application name %q exceeds maximum length of %d characters", appName, MaxApplicationNameLength)
	}
	return ReplicaID{id: id, appName: appName}, nil
}

// ReplicaIDsToAppNames converts a slice of ReplicaID to their application name strings for use in APIs
// that accept []string (e.g., history records).
func ReplicaIDsToAppNames(ids []ReplicaID) []string {
	strs := make([]string, len(ids))
	for i, n := range ids {
		strs[i] = n.appName
	}
	return strs
}

// ToReplicaIDs converts a slice of IDs to their ReplicaID representations.
// If any ID fails strict validation the corresponding ReplicaID contains an
// approximate appName and the first error is returned alongside the full slice.
// Callers that require strict validity must check the error. Callers that can
// tolerate approximate names (e.g. for logging or informational responses) may
// use the returned slice regardless.
func ToReplicaIDs(ids []*clustermetadatapb.ID) ([]ReplicaID, error) {
	result := make([]ReplicaID, len(ids))
	var firstErr error
	for i, id := range ids {
		rid, err := NewReplicaID(id)
		result[i] = rid
		if err != nil && firstErr == nil {
			firstErr = mterrors.Wrapf(err, "ids[%d]", i)
		}
	}
	return result, firstErr
}

// NewReplicaIDFromAppName parses a PostgreSQL application_name (format: "cell_name") into
// a ReplicaID. This is the inverse of NewReplicaID's appName generation.
// On parse failure an error is returned alongside a best-effort ReplicaID that
// preserves the raw appName in the Name field so callers can include it rather
// than silently dropping the member.
//
// TODO: once leadership_history stores serialized clustermetadata.ID values directly
// instead of application_name strings, this parsing and its error path can be removed.
func NewReplicaIDFromAppName(appName string) (ReplicaID, error) {
	id, err := ParseApplicationName(appName)
	if err != nil {
		return ReplicaID{
			id:      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Name: appName},
			appName: appName,
		}, err
	}
	return ReplicaID{id: id, appName: appName}, nil
}

// ParseApplicationName parses a postgres replication application_name back
// into a clustermetadata ID. Application names are always set by multipoolers
// (postgres replication clients), so Component is always MULTIPOOLER.
// Format: {cell}_{name}
// Example: "us-west_replica-1" -> ID{Component: MULTIPOOLER, Cell: "us-west", Name: "replica-1"}
//
// For decoding non-pooler IDs that happen to share the cell_name encoding
// (e.g. coordinator_id in rule_history), use topoclient.SplitClusterID directly.
func ParseApplicationName(appName string) (*clustermetadatapb.ID, error) {
	cell, name, err := topoclient.SplitClusterID(appName)
	if err != nil {
		return nil, err
	}
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}, nil
}

// ParseReplicaIDStrings converts a slice of "cell_name" app name strings into ReplicaIDs.
// Returns nil for nil input, preserving the distinction between "not set" and "empty".
func ParseReplicaIDStrings(names []string) ([]ReplicaID, error) {
	if names == nil {
		return nil, nil
	}
	result := make([]ReplicaID, 0, len(names))
	for _, s := range names {
		id, err := ParseApplicationName(s)
		if err != nil {
			return nil, err
		}
		result = append(result, ReplicaID{id: id, appName: s})
	}
	return result, nil
}
