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

import "strings"

// SidecarArchiveCommand is the archive_command that hands a segment to the
// pgbackrestd sidecar over its WAL socket. The one renderer both multipooler
// (the command it writes to postgresql.auto.conf) and the sidecar (the command
// restore leaves behind) use, so the two cannot drift. The socket path and the
// URL host both contain "pgbackrest", which is what makes pgBackRest's own
// archive_command check accept the command.
func SidecarArchiveCommand(walSocket string) string {
	return curlWALPrefix(walSocket) + " -X POST http://pgbackrest/wal/%f"
}

// SidecarRestoreCommand is the matching restore_command. pidfile is the
// restore-wrapper pidfile that consensus operations use to stop archive replay.
func SidecarRestoreCommand(walSocket, pidfile string) string {
	return "pgctld restore-wrapper " + ShellWord(pidfile) +
		` --rename "%p.fetch" "%p" -- ` + curlWALPrefix(walSocket) +
		` -o "%p.fetch" http://pgbackrest/wal/%f`
}

// curlWALPrefix is the curl invocation shared by both commands.
func curlWALPrefix(walSocket string) string {
	return "curl -sS --fail-with-body --unix-socket " + ShellWord(walSocket) + " --connect-timeout 5 --max-time 600"
}

// ShellWord quotes s for /bin/sh only when it has to be, so ordinary paths
// render exactly as the documented command text.
func ShellWord(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/._-+:@%=,") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
