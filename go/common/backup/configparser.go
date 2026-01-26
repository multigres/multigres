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

package backup

import (
	"bufio"
	"fmt"
	"strings"
)

// credentialKeys are the keys that should be updated during credential refresh
var credentialKeys = map[string]bool{
	"repo1-s3-key":        true,
	"repo1-s3-key-secret": true,
	"repo1-s3-key-type":   true,
	"repo1-s3-token":      true,
}

// UpdateCredentialsInConfig updates only S3 credential lines in pgbackrest.conf
// while preserving all other configuration, comments, and formatting.
func UpdateCredentialsInConfig(configContent string, newCredentials map[string]string) (string, error) {
	var result strings.Builder
	scanner := bufio.NewScanner(strings.NewReader(configContent))

	updatedKeys := make(map[string]bool)

	for scanner.Scan() {
		line := scanner.Text()

		// Check if this line is a credential key=value pair
		if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])

				// If this is a credential key and we have a new value, replace it
				if credentialKeys[key] {
					if newValue, exists := newCredentials[key]; exists {
						result.WriteString(key + "=" + newValue + "\n")
						updatedKeys[key] = true
						continue
					}
				}
			}
		}

		// Preserve all other lines as-is
		result.WriteString(line + "\n")
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed to scan config: %w", err)
	}

	return result.String(), nil
}
