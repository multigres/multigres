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

package pgbackrestd

import (
	"os"
	"slices"

	"github.com/multigres/multigres/go/common/sandbox"
)

// Commands pgbackrestd runs. Anything else is rejected before it starts.
const (
	cmdRestore      = "restore"
	cmdArchivePush  = "archive-push"
	cmdArchiveGet   = "archive-get"
	cmdBackup       = "backup"
	cmdExpire       = "expire"
	cmdStanzaCreate = "stanza-create"
	cmdStanzaDelete = "stanza-delete"
	cmdRepoLs       = "repo-ls"
	cmdInfo         = "info"
	cmdCheck        = "check"
	cmdVerify       = "verify"
	cmdStop         = "stop"
	cmdStart        = "start"
	cmdVersion      = "version"
	cmdServer       = "server"
)

// s3Commands are the commands an S3-command child may run: every command
// except the server, which has its own kind and no token.
var s3Commands = []string{
	cmdRestore, cmdArchivePush, cmdArchiveGet, cmdBackup, cmdExpire,
	cmdStanzaCreate, cmdStanzaDelete, cmdRepoLs, cmdInfo, cmdCheck, cmdVerify, cmdStop, cmdStart, cmdVersion,
}

func knownCommand(command string) bool {
	return command == cmdServer || slices.Contains(s3Commands, command)
}

// opensRepository reports whether a command reads or writes the backup
// repository, and therefore connects to the storage endpoint.
func opensRepository(command string) bool {
	switch command {
	case cmdServer, cmdStart, cmdStop, cmdVersion:
		return false
	}
	return knownCommand(command)
}

// policyFor returns the sandbox policy for one pgBackRest command. Anything
// absent is denied, so /proc, /sys, the secrets mounts, token paths and the
// cipher key file are unreachable by construction. An unknown command gets
// only the system grants, not even the data volume.
//
// resolve_unix (connecting to the postgres socket and creds.sock) is added by
// the sandbox package to every Read or Write rule on kernels that have it.
func policyFor(command string, cfg Config, self string) sandbox.Policy {
	const rx = sandbox.Read | sandbox.Exec
	rules := []sandbox.Rule{
		{Path: "/usr", Access: rx},
		{Path: "/lib", Access: rx},
		{Path: "/etc", Access: rx},
		{Path: self, Access: rx},
		{Path: "/dev/null", Access: sandbox.Read | sandbox.Write},
		{Path: "/dev/urandom", Access: sandbox.Read | sandbox.Write},
	}
	if !knownCommand(command) {
		return sandbox.Policy{Rules: rules}
	}
	rules = append(rules,
		sandbox.Rule{Path: cfg.DataVolume, Access: sandbox.Read},
		sandbox.Rule{Path: cfg.stateSub(dirConf), Access: sandbox.Read},
		sandbox.Rule{Path: cfg.stateSub(filePgpass), Access: sandbox.Read},
	)
	// The server presents the certificates; a backup from a standby presents
	// them as the --pg2-host TLS client (control.go adds the three file
	// flags for BACKUP only). No other command takes pg2 arguments.
	//
	// A rule on a missing path fails the sandbox, which is right for the
	// server (it must not run without its certificates) but would break every
	// backup that has no remote, so backup gets the rule only when the
	// directory exists.
	if cfg.TLSCertDir != "" && (command == cmdServer || (command == cmdBackup && dirExists(cfg.TLSCertDir))) {
		rules = append(rules, sandbox.Rule{Path: cfg.TLSCertDir, Access: sandbox.Read})
	}
	if command != cmdServer {
		// The server never holds the token, so it has no business at the
		// credential socket.
		rules = append(rules, sandbox.Rule{Path: cfg.stateSub(dirCreds), Access: sandbox.Read})
	}
	// The CA that verifies the storage endpoint (render.go pins it with
	// repo<n>-storage-ca-file), for the commands that open the repository.
	// start, stop and version do not, and neither does the server.
	// Like the cert dir, only when it is in use and present: a rule on a
	// missing path would fail the child. The path is resolved again for every
	// child, because the file behind it may have been replaced since startup
	// (a projected volume swaps a symbolic link): a child is never granted a
	// file that lies on the data volume, which postgres can write. Without the
	// rule the command cannot read its CA and fails.
	//
	// This is the only rule added for the CA. A CA kept below a tree that
	// every child may read anyway (/etc) is readable by all of them; a CA
	// certificate is not a secret.
	if ca := cfg.storageCAFile(); ca != "" && opensRepository(command) && fileExists(ca) {
		if onVolume, _, err := cfg.caOnDataVolume(); err == nil && !onVolume {
			rules = append(rules, sandbox.Rule{Path: ca, Access: sandbox.Read})
		}
	}

	lock := cfg.stateSub(dirLock)
	spool := cfg.stateSub(dirSpool)
	tmp := cfg.stateSub(dirTmp)
	write := func(paths ...string) {
		for _, p := range paths {
			rules = append(rules, sandbox.Rule{Path: p, Access: sandbox.Write})
		}
	}
	// The private scratch directories get Read as well: pgBackRest opens its
	// lock files O_RDWR and lists its spool, and Write alone (no read_file,
	// no read_dir) makes every lock acquisition fail with EACCES. Nothing
	// secret lives in them. The data volume and the repository keep exactly
	// the access each command had.
	scratch := func(paths ...string) {
		for _, p := range paths {
			rules = append(rules, sandbox.Rule{Path: p, Access: sandbox.Read | sandbox.Write})
		}
	}

	repoWrite, repoRead := false, false
	switch command {
	case cmdRestore:
		// restore writes only to its own spool (see restoreSpoolRel), never to
		// spool/archive where the push acknowledgements live.
		write(cfg.DataVolume)
		scratch(lock, cfg.stateSub(restoreSpoolRel))
		repoRead = true
	case cmdArchivePush:
		scratch(spool, lock)
		repoWrite = true
	case cmdArchiveGet:
		scratch(spool, tmp, lock)
		repoRead = true
	case cmdBackup, cmdExpire, cmdStanzaCreate, cmdStanzaDelete:
		scratch(lock)
		repoWrite = true
	case cmdInfo, cmdCheck, cmdVerify, cmdRepoLs:
		scratch(lock)
		repoRead = true
	case cmdStop, cmdStart, cmdVersion:
		// stop and start create and remove <stanza>.stop in lock/, and stop
		// --force reads the lock files to find the workers to signal.
		rules = append(rules, sandbox.Rule{Path: lock, Access: sandbox.Read | sandbox.Write})
	case cmdServer:
		// Read on the data volume (granted to every command above) and no
		// more. The server speaks pgBackRest's remote protocol to any peer
		// holding a shard client certificate, and that protocol can write and
		// remove files under pg1-path. The one remote use here is a standby's
		// backup through this primary: SQL over the local socket and reads of
		// PGDATA. With Write, a peer with the certificate could overwrite
		// PGDATA through the server.
		scratch(lock)
	}

	if cfg.LocationType == LocationFilesystem && (repoRead || repoWrite) {
		access := sandbox.Read
		if repoWrite {
			access |= sandbox.Write
		}
		rules = append(rules, sandbox.Rule{Path: cfg.FSRepoPath, Access: access})
	}
	return sandbox.Policy{Rules: rules}
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func fileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.Mode().IsRegular()
}
