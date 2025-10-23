// Copyright 2025 Supabase, Inc.
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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// SyncStandbyConfig represents a parsed synchronous_standby_names configuration
type SyncStandbyConfig struct {
	Method     multipoolermanagerdata.SynchronousMethod // FIRST, ANY, or special UNSPECIFIED for wildcard
	NumSync    int32                                    // Number of synchronous standbys
	StandbyIDs []*clustermetadatapb.ID                  // List of standby IDs
	IsWildcard bool                                     // True if configuration is "*"
}

// parseApplicationName parses an application name back into a clustermetadata ID
// Format: {cell}_{name}
// Example: "us-west_replica-1" -> ID{Cell: "us-west", Name: "replica-1"}
func parseApplicationName(appName string) (*clustermetadatapb.ID, error) {
	parts := strings.SplitN(appName, "_", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid application name format: %q (expected cell_name)", appName)
	}
	return &clustermetadatapb.ID{
		Cell: parts[0],
		Name: parts[1],
	}, nil
}

// parseSynchronousStandbyNames parses a PostgreSQL synchronous_standby_names string
// Examples:
//   - "FIRST 2 (\"cell_replica1\", \"cell_replica2\", \"cell_replica3\")"
//   - "ANY 1 (\"cell_replica1\", \"cell_replica2\")"
//   - "*" (wildcard - all connected standbys)
//   - "" (empty - no synchronous replication)
func parseSynchronousStandbyNames(value string) (*SyncStandbyConfig, error) {
	value = strings.TrimSpace(value)

	// Handle empty case
	if value == "" {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "synchronous replication not configured")
	}

	// Handle wildcard case
	if value == "*" {
		return &SyncStandbyConfig{
			Method:     multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_UNSPECIFIED,
			NumSync:    1,
			StandbyIDs: nil,
			IsWildcard: true,
		}, nil
	}

	// Parse format: METHOD NUM (member1, member2, ...)
	// Regex: ^(FIRST|ANY)\s+(\d+)\s*\(([^)]*)\)$
	re := regexp.MustCompile(`^(FIRST|ANY)\s+(\d+)\s*\(([^)]*)\)$`)
	matches := re.FindStringSubmatch(value)
	if matches == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous_standby_names format: %q", value))
	}

	methodStr := matches[1]
	numSync, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid num_sync value in synchronous_standby_names: %q", matches[2]))
	}

	// Convert string method to enum
	var method multipoolermanagerdata.SynchronousMethod
	switch methodStr {
	case "FIRST":
		method = multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST
	case "ANY":
		method = multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unsupported synchronous method: %q", methodStr))
	}

	// Parse member list
	membersStr := strings.TrimSpace(matches[3])
	if membersStr == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"empty member list in synchronous_standby_names")
	}

	// Split by comma and clean up each member
	membersParts := strings.Split(membersStr, ",")
	standbyIDs := make([]*clustermetadatapb.ID, 0, len(membersParts))
	for _, part := range membersParts {
		part = strings.TrimSpace(part)
		// Remove surrounding quotes if present
		part = strings.Trim(part, `"`)
		if part != "" {
			// Parse application name back to ID
			id, err := parseApplicationName(part)
			if err != nil {
				return nil, mterrors.Wrap(err, fmt.Sprintf("failed to parse application name %q", part))
			}
			standbyIDs = append(standbyIDs, id)
		}
	}

	if len(standbyIDs) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"no valid members found in synchronous_standby_names")
	}

	return &SyncStandbyConfig{
		Method:     method,
		NumSync:    int32(numSync),
		StandbyIDs: standbyIDs,
		IsWildcard: false,
	}, nil
}

// parseAndRedactPrimaryConnInfo parses a PostgreSQL primary_conninfo connection string into structured fields
// Example input: "host=localhost port=5432 user=postgres application_name=cell_name"
// Returns a PrimaryConnInfo message with parsed fields, or an error if parsing fails
// Note: Passwords are redacted in the raw field for security
func parseAndRedactPrimaryConnInfo(connInfoStr string) (*multipoolermanagerdata.PrimaryConnInfo, error) {
	connInfo := &multipoolermanagerdata.PrimaryConnInfo{}

	// Simple space-based parsing of key=value pairs
	parts := strings.Split(connInfoStr, " ")
	redactedParts := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			// Not a key=value pair - parsing failed
			return nil, fmt.Errorf("invalid key=value format in primary_conninfo: %q", part)
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		if key == "" {
			return nil, fmt.Errorf("empty key in primary_conninfo: %q", part)
		}

		// Redact sensitive fields in the raw string
		if key == "password" {
			redactedParts = append(redactedParts, key+"=[REDACTED]")
		} else {
			redactedParts = append(redactedParts, part)
		}

		// Parse specific fields we care about
		switch key {
		case "host":
			connInfo.Host = value
		case "port":
			if port, err := strconv.ParseInt(value, 10, 32); err == nil {
				connInfo.Port = int32(port)
			}
		case "user":
			connInfo.User = value
		case "application_name":
			connInfo.ApplicationName = value
		}
	}

	// Set the redacted raw string
	connInfo.Raw = strings.Join(redactedParts, " ")

	return connInfo, nil
}
