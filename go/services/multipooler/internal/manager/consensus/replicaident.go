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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// MaxApplicationNameLength is the maximum length of a PostgreSQL application_name (NAMEDATALEN - 1).
const MaxApplicationNameLength = 63

// ReplicaID is a validated pooler identity that can be safely encoded as a
// PostgreSQL application_name (format: {cell}_{name}). The validation rules
// (no underscores, ≤63 chars) are driven by PostgreSQL's application_name
// constraints. Construct via NewReplicaID or NewReplicaIDFromAppName.
type ReplicaID struct {
	id      *clustermetadatapb.ID
	appName string
}

// AppName returns the PostgreSQL application_name representation.
func (r ReplicaID) AppName() string { return r.appName }

// ID returns the canonical cluster ID.
func (r ReplicaID) ID() *clustermetadatapb.ID { return r.id }

// NewReplicaID constructs a ReplicaID from a cluster ID, validating that it can
// be safely encoded as a PostgreSQL application_name.
// On validation failure an approximate ReplicaID is returned alongside the error;
// callers requiring strict validity must check the error.
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

// NewReplicaIDFromAppName parses a PostgreSQL application_name back into a ReplicaID.
// Format: {cell}_{name}  (e.g. "us-west_replica-1")
// On parse failure an error is returned alongside a best-effort ReplicaID that
// preserves the raw appName in the Name field so callers can include it rather
// than silently dropping the member.
func NewReplicaIDFromAppName(appName string) (ReplicaID, error) {
	id, err := ParseApplicationName(appName)
	if err != nil {
		return ReplicaID{
			id:      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Name: appName},
			appName: appName,
		}, err
	}
	id.Component = clustermetadatapb.ID_MULTIPOOLER
	return ReplicaID{id: id, appName: appName}, nil
}

// ToReplicaIDs converts a slice of IDs to ReplicaID values.
// Returns the first validation error alongside the full slice; callers requiring
// strict validity must check the error.
func ToReplicaIDs(ids []*clustermetadatapb.ID) ([]ReplicaID, error) {
	result := make([]ReplicaID, len(ids))
	var firstErr error
	for i, id := range ids {
		rid, err := NewReplicaID(id)
		result[i] = rid
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return result, firstErr
}

// ReplicaIDsToAppNames converts a slice of ReplicaID to their application name strings.
func ReplicaIDsToAppNames(ids []ReplicaID) []string {
	strs := make([]string, len(ids))
	for i, n := range ids {
		strs[i] = n.appName
	}
	return strs
}

// ParseApplicationName parses a PostgreSQL application_name back into a cluster ID.
// Format: {cell}_{name}
// Example: "us-west_replica-1" -> ID{Cell: "us-west", Name: "replica-1"}
func ParseApplicationName(appName string) (*clustermetadatapb.ID, error) {
	parts := strings.SplitN(appName, "_", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid application name format: %q (expected cell_name)", appName)
	}

	cell := parts[0]
	name := parts[1]

	if cell == "" {
		return nil, fmt.Errorf("invalid application name: cell cannot be empty in %q", appName)
	}
	if name == "" {
		return nil, fmt.Errorf("invalid application name: name cannot be empty in %q", appName)
	}

	return &clustermetadatapb.ID{
		Cell: cell,
		Name: name,
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
