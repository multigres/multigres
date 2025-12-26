// Copyright 2025 Supabase, Inc.
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

// Package safepath provides utilities for safely encoding path components
// to prevent path traversal attacks while supporting UTF-8 identifiers.
package safepath

import (
	"fmt"
	"path/filepath"
	"strings"
)

// IsSafePathChar returns true if the rune is safe to use in file paths without encoding.
// Safe characters are: a-z, A-Z, 0-9, underscore, hyphen, and dot.
//
// This set of characters is conservative. Many of the characters not allowed are safe,
// but we err on the side of caution and encode everything that is not explicitly allowed.
func IsSafePathChar(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '_' || r == '-' || r == '.'
}

// EncodePathComponent URL-encodes a path component, preserving safe characters.
// Safe characters (a-z, A-Z, 0-9, _, -, .) are preserved, everything else is
// percent-encoded. Consecutive dots (..) are always encoded to prevent path traversal.
// This prevents path traversal while supporting UTF-8 identifiers.
func EncodePathComponent(s string) string {
	if s == "" {
		return ""
	}

	var result strings.Builder
	runes := []rune(s)
	i := 0
	for i < len(runes) {
		r := runes[i]
		// Check for consecutive dots (path traversal defense)
		if r == '.' && i+1 < len(runes) && runes[i+1] == '.' {
			// Encode all consecutive dots
			for i < len(runes) && runes[i] == '.' {
				result.WriteString("%2E")
				i++
			}
			continue
		}
		if IsSafePathChar(r) {
			result.WriteRune(r)
		} else {
			// URL encode: convert rune to UTF-8 bytes, then hex encode each byte
			utf8Bytes := []byte(string(r))
			for _, b := range utf8Bytes {
				result.WriteString(fmt.Sprintf("%%%02X", b))
			}
		}
		i++
	}
	return result.String()
}

// Join safely joins a base path with components by encoding each component
// and verifying the result is contained within the base path. This prevents
// path traversal attacks while supporting UTF-8 identifiers.
func Join(basePath string, components ...string) (string, error) {
	// Encode each component
	encoded := make([]string, len(components))
	for i, c := range components {
		encoded[i] = EncodePathComponent(c)
	}

	// Build the full path
	fullPath := filepath.Join(append([]string{basePath}, encoded...)...)

	// Verify containment: ensure the final path is still under basePath
	// This is a defense-in-depth check to catch any edge cases in encoding
	cleanBase := filepath.Clean(basePath) + string(filepath.Separator)
	cleanFull := filepath.Clean(fullPath) + string(filepath.Separator)
	if !strings.HasPrefix(cleanFull, cleanBase) {
		return "", fmt.Errorf("path traversal detected: base=%s, full=%s", basePath, fullPath)
	}

	return fullPath, nil
}
