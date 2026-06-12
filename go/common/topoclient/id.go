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

package topoclient

import (
	"fmt"
	"strings"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ComponentTypeToString converts a ComponentType enum to its string representation.
// This function uses the generated name map to be resilient to refactors.
// It's not specific to any single component type and can be used across the topology system.
func ComponentTypeToString(component clustermetadatapb.ID_ComponentType) string {
	// Use the generated name map for resilience - this automatically updates when the proto changes
	if name, exists := clustermetadatapb.ID_ComponentType_name[int32(component)]; exists {
		// Convert the generated name (e.g., "MULTIPOOLER") to lowercase for consistency
		return strings.ToLower(name)
	}
	return "unknown"
}

// ClusterIDString returns a string representation of the cluster ID in cell_name format.
// Returns empty string if id is nil.
func ClusterIDString(id *clustermetadatapb.ID) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("%s_%s", id.Cell, id.Name)
}

// SplitClusterID is the inverse of ClusterIDString. It splits the cell_name
// encoding into its two parts. The encoding assumes cell names cannot contain
// underscores, which matches the cluster's naming convention. Returns an error
// for malformed input (missing separator, empty cell, or empty name).
func SplitClusterID(s string) (cell, name string, err error) {
	parts := strings.SplitN(s, "_", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid cell_name format: %q (expected cell_name)", s)
	}
	cell, name = parts[0], parts[1]
	if cell == "" {
		return "", "", fmt.Errorf("invalid cell_name: cell cannot be empty in %q", s)
	}
	if name == "" {
		return "", "", fmt.Errorf("invalid cell_name: name cannot be empty in %q", s)
	}
	return cell, name, nil
}
