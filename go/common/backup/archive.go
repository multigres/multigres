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

import "regexp"

var (
	// archiveIDRe is a pgBackRest archive id, <pg major>-<history id>.
	archiveIDRe = regexp.MustCompile(`^[0-9]+-[0-9]+$`)
	// walDirRe is a WAL directory of an archive id: the timeline and log of the
	// segment names beneath it.
	walDirRe = regexp.MustCompile(`^[0-9A-F]{16}$`)
)

// ValidArchiveID reports whether s is a pgBackRest archive id as the sidecar's
// repo-ls accepts it.
func ValidArchiveID(s string) bool { return archiveIDRe.MatchString(s) }

// ValidWALDir reports whether s is a WAL directory name as the sidecar's repo-ls
// accepts it.
func ValidWALDir(s string) bool { return walDirRe.MatchString(s) }
