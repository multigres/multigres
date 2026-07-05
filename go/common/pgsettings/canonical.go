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

// Package pgsettings contains helpers for PostgreSQL session setting names.
package pgsettings

// CanonicalGUCName returns the PostgreSQL-compatible canonical spelling used
// for GUC/settings keys in Multigres: ASCII A-Z are folded to a-z and all other
// bytes are preserved.
//
// PostgreSQL's guc_name_compare folds only ASCII A-Z. Do not use
// strings.ToLower here: PostgreSQL treats high-bit custom GUC names like
// "my.Ä" and "my.ä" as distinct, while Unicode lowercasing may collapse them.
func CanonicalGUCName(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			b := []byte(s)
			for j := i; j < len(b); j++ {
				if b[j] >= 'A' && b[j] <= 'Z' {
					b[j] += 'a' - 'A'
				}
			}
			return string(b)
		}
	}
	return s
}
