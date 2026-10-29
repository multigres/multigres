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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sandbox"
)

var allCommands = []string{
	cmdRestore, cmdArchivePush, cmdArchiveGet, cmdBackup, cmdExpire,
	cmdStanzaCreate, cmdInfo, cmdCheck, cmdVerify, cmdStop, cmdStart, cmdVersion, cmdServer,
}

// grants reports whether policy gives access to target through a rule on
// target itself or on one of its parents.
func grants(p sandbox.Policy, target string, access sandbox.Access) bool {
	for _, r := range p.Rules {
		if r.Access&access == access && pathWithin(r.Path, target) {
			return true
		}
	}
	return false
}

func TestPolicyFor_NoProc(t *testing.T) {
	cfg := testConfig(t)
	for _, cmd := range allCommands {
		p := policyFor(cmd, cfg, "/multigres/bin/pgbackrestd")
		for _, r := range p.Rules {
			for _, banned := range []string{"/proc", "/sys", "/secrets", cfg.CipherKeyFile, cfg.S3StaticKeyDir, "/var/run/secrets"} {
				assert.False(t, pathWithin(banned, r.Path) || pathWithin(r.Path, banned),
					"%s: rule %q touches %q", cmd, r.Path, banned)
			}
		}
	}
}

func TestPolicyFor_BackupCannotWriteDataVolume(t *testing.T) {
	cfg := testConfig(t)
	for _, cmd := range []string{cmdBackup, cmdArchivePush, cmdArchiveGet, cmdExpire, cmdStanzaCreate, cmdInfo, cmdCheck, cmdVerify, cmdStop, cmdStart, cmdVersion} {
		p := policyFor(cmd, cfg, "/self")
		assert.False(t, grants(p, filepath.Join(cfg.DataVolume, "pg_data"), sandbox.Write), cmd)
		assert.True(t, grants(p, filepath.Join(cfg.DataVolume, "pg_data"), sandbox.Read), cmd)
	}
	for _, cmd := range []string{cmdRestore, cmdServer} {
		assert.True(t, grants(policyFor(cmd, cfg, "/self"), cfg.DataVolume, sandbox.Write), cmd)
	}
}

func TestPolicyFor_ArchivePushCannotWriteTmp(t *testing.T) {
	cfg := testConfig(t)
	push := policyFor(cmdArchivePush, cfg, "/self")
	assert.False(t, grants(push, cfg.stateSub(dirTmp), sandbox.Write))
	assert.True(t, grants(push, cfg.stateSub(dirSpool), sandbox.Write))
	assert.True(t, grants(push, cfg.stateSub(dirLock), sandbox.Write))

	get := policyFor(cmdArchiveGet, cfg, "/self")
	assert.True(t, grants(get, cfg.stateSub(dirTmp), sandbox.Write))
}

func TestPolicyFor_RestoreCannotWriteArchiveSpool(t *testing.T) {
	cfg := testConfig(t)
	p := policyFor(cmdRestore, cfg, "/self")
	assert.False(t, grants(p, cfg.stateSub(dirSpool+"/archive"), sandbox.Write))
	assert.True(t, grants(p, cfg.stateSub(restoreSpoolRel), sandbox.Write))
}

func TestPolicyFor_StateIsReadOnlyToChildren(t *testing.T) {
	cfg := testConfig(t)
	for _, cmd := range allCommands {
		p := policyFor(cmd, cfg, "/self")
		for _, d := range []string{dirConf, dirCreds, dirState, filePgpass} {
			assert.False(t, grants(p, cfg.stateSub(d), sandbox.Write), "%s %s", cmd, d)
		}
	}
}

func TestPolicyFor_BaseGrants(t *testing.T) {
	cfg := testConfig(t)
	cfg.TLSCertDir = t.TempDir() // backup gets the rule only for a directory that exists
	for _, cmd := range allCommands {
		p := policyFor(cmd, cfg, "/multigres/bin/pgbackrestd")
		assert.True(t, grants(p, "/multigres/bin/pgbackrestd", sandbox.Read|sandbox.Exec), cmd)
		assert.True(t, grants(p, "/usr/bin/pgbackrest", sandbox.Exec), cmd)
		assert.True(t, grants(p, "/dev/urandom", sandbox.Write), cmd)
		assert.True(t, grants(p, cfg.stateSub(dirLock), sandbox.Write), cmd)
		// The server presents the certificates and backup is the pg2 TLS
		// client; only the server goes without the credential socket.
		assert.Equal(t, cmd == cmdServer || cmd == cmdBackup, grants(p, cfg.TLSCertDir, sandbox.Read), "%s cert dir", cmd)
		assert.Equal(t, cmd != cmdServer, grants(p, cfg.stateSub(dirCreds), sandbox.Read), "%s creds", cmd)
	}
}

func TestPolicyFor_NoRuleOnCertDirExceptServerAndBackup(t *testing.T) {
	cfg := testConfig(t)
	cfg.TLSCertDir = t.TempDir()
	for _, cmd := range s3Commands {
		if cmd == cmdBackup {
			continue
		}
		for _, r := range policyFor(cmd, cfg, "/self").Rules {
			assert.False(t, pathWithin(cfg.TLSCertDir, r.Path) || pathWithin(r.Path, cfg.TLSCertDir), "%s references the cert dir via %q", cmd, r.Path)
		}
	}
}

func TestPolicyFor_StopStartVersionOnlyTouchLock(t *testing.T) {
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	cfg.FSRepoPath = "/backups"
	for _, cmd := range []string{cmdStop, cmdStart, cmdVersion} {
		p := policyFor(cmd, cfg, "/self")
		assert.True(t, grants(p, cfg.stateSub(dirLock), sandbox.Read|sandbox.Write), cmd)
		assert.False(t, grants(p, "/backups", sandbox.Read), cmd)
		assert.False(t, grants(p, cfg.DataVolume, sandbox.Write), cmd)
		assert.False(t, grants(p, cfg.stateSub(dirSpool), sandbox.Write), cmd)
	}
}

func TestPolicyFor_UnknownCommandGetsOnlySystemGrants(t *testing.T) {
	cfg := testConfig(t)
	p := policyFor("rm-rf", cfg, "/self")
	assert.False(t, grants(p, cfg.DataVolume, sandbox.Read))
	assert.False(t, grants(p, cfg.stateSub(dirCreds), sandbox.Read))
	assert.False(t, grants(p, cfg.stateSub(dirLock), sandbox.Write))
	assert.True(t, grants(p, "/usr/bin/pgbackrest", sandbox.Exec))
}

func TestPolicyFor_FilesystemRepoGrants(t *testing.T) {
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	cfg.FSRepoPath = "/backups"

	want := map[string]sandbox.Access{
		cmdArchivePush:  sandbox.Read | sandbox.Write,
		cmdBackup:       sandbox.Read | sandbox.Write,
		cmdExpire:       sandbox.Read | sandbox.Write,
		cmdStanzaCreate: sandbox.Read | sandbox.Write,
		cmdRestore:      sandbox.Read,
		cmdArchiveGet:   sandbox.Read,
		cmdInfo:         sandbox.Read,
		cmdCheck:        sandbox.Read,
		cmdVerify:       sandbox.Read,
		cmdStop:         0,
		cmdStart:        0,
		cmdVersion:      0,
		cmdServer:       0,
	}
	require.Len(t, want, len(allCommands), "table must cover every command")
	for cmd, access := range want {
		p := policyFor(cmd, cfg, "/self")
		assert.Equal(t, access&sandbox.Read != 0, grants(p, "/backups/archive", sandbox.Read), "%s read", cmd)
		assert.Equal(t, access&sandbox.Write != 0, grants(p, "/backups/archive", sandbox.Write), "%s write", cmd)
	}

	cfg.LocationType = LocationS3
	for _, cmd := range allCommands {
		assert.False(t, grants(policyFor(cmd, cfg, "/self"), "/backups", sandbox.Read), cmd)
	}
}

func TestConfigValidate_DataVolumeMustNotContainState(t *testing.T) {
	cfg := testConfig(t)
	require.NoError(t, cfg.Validate())

	for name, mutate := range map[string]func(*Config){
		"equals state dir":   func(c *Config) { setDataVolume(c, c.StateDir) },
		"contains state dir": func(c *Config) { setDataVolume(c, filepath.Dir(c.StateDir)) },
		"contains wal sock":  func(c *Config) { setDataVolume(c, "/run") },
		"contains ctl sock":  func(c *Config) { setDataVolume(c, "/run/pgbackrest-control") },
	} {
		t.Run(name, func(t *testing.T) {
			c := testConfig(t)
			mutate(&c)
			err := c.Validate()
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "contains"), err.Error())
		})
	}
}

func setDataVolume(c *Config, dv string) {
	c.DataVolume = dv
	c.PGData = dv + "/pg_data"
	c.PGSocketDir = dv + "/pg_sockets"
}

func TestConfigValidate_FSRepoPathIsBounded(t *testing.T) {
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	for name, path := range map[string]string{
		"root":           "/",
		"relative":       "backups",
		"unclean":        "/backups/../backups",
		"state dir":      cfg.StateDir,
		"inside state":   cfg.StateDir + "/conf",
		"contains state": filepath.Dir(cfg.StateDir),
		"data volume":    cfg.DataVolume,
		"inside volume":  cfg.DataVolume + "/backups",
		"secrets":        "/secrets/pgbackrest-s3",
		"proc":           "/proc",
		"etc":            "/etc/backups",
		"parent of etc":  "/",
	} {
		c := cfg
		c.FSRepoPath = path
		assert.Error(t, c.Validate(), name)
	}
	c := cfg
	c.FSRepoPath = "/backups"
	assert.NoError(t, c.Validate())
}

// A socket directory outside the denylist, so only the overlap check with the
// socket dirs can reject these.
func TestConfigValidate_FSRepoPathRejectsSocketDirOverlap(t *testing.T) {
	socks := t.TempDir()
	for name, mutate := range map[string]func(*Config){
		"wal socket dir equal":       func(c *Config) { c.WALSocket = socks + "/wal/wal.sock"; c.FSRepoPath = socks + "/wal" },
		"wal socket dir inside repo": func(c *Config) { c.WALSocket = socks + "/wal/wal.sock"; c.FSRepoPath = socks },
		"repo inside wal socket dir": func(c *Config) { c.WALSocket = socks + "/wal/wal.sock"; c.FSRepoPath = socks + "/wal/repo" },
		"control socket dir equal":   func(c *Config) { c.ControlSocket = socks + "/ctl/c.sock"; c.FSRepoPath = socks + "/ctl" },
		"repo inside control dir":    func(c *Config) { c.ControlSocket = socks + "/ctl/c.sock"; c.FSRepoPath = socks + "/ctl/repo" },
	} {
		c := testConfig(t)
		c.LocationType = LocationFilesystem
		c.FSRepoPath = "/backups"
		require.NoError(t, c.Validate(), "control: the base config is valid")
		mutate(&c)
		err := c.Validate()
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "overlaps", name)
	}
}

func TestConfigValidate_PostgresPathsLieWithinDataVolume(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"pgdata outside":   func(c *Config) { c.PGData = "/elsewhere/pg_data" },
		"socket outside":   func(c *Config) { c.PGSocketDir = "/tmp" },
		"pgdata missing":   func(c *Config) { c.PGData = "" },
		"socket missing":   func(c *Config) { c.PGSocketDir = "" },
		"pgdata traversal": func(c *Config) { c.PGData = c.DataVolume + "/../x" },
	} {
		c := testConfig(t)
		mutate(&c)
		assert.Error(t, c.Validate(), name)
	}
}

func TestConfigValidate_Required(t *testing.T) {
	cfg := testConfig(t)
	cfg.RestorePIDFile = ""
	assert.ErrorContains(t, cfg.Validate(), "--restore-pidfile")

	cfg = testConfig(t)
	cfg.DataVolume = ""
	assert.ErrorContains(t, cfg.Validate(), "--data-volume")
}

func TestPolicyFor_BackupWithoutCertDirHasNoRuleOnIt(t *testing.T) {
	cfg := testConfig(t)
	cfg.TLSCertDir = "/nonexistent/certs"
	assert.False(t, grants(policyFor(cmdBackup, cfg, "/self"), cfg.TLSCertDir, sandbox.Read))
	assert.True(t, grants(policyFor(cmdServer, cfg, "/self"), cfg.TLSCertDir, sandbox.Read), "the server still names it so it fails closed")
}

// The CA rule follows the cert dir's: only when an endpoint is set and the
// file exists, so a missing file can never break start or any other child.
func TestPolicyFor_CredCAFileRuleIsConditional(t *testing.T) {
	ca := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(ca, []byte("pem"), 0o600))
	cfg := testConfig(t)
	cfg.LocationType, cfg.S3Endpoint, cfg.S3CredCAFile = LocationS3, "127.0.0.1:9000", ca
	for _, cmd := range s3Commands {
		assert.True(t, grants(policyFor(cmd, cfg, "/self"), ca, sandbox.Read), cmd)
	}

	missing := cfg
	missing.S3CredCAFile = filepath.Join(t.TempDir(), "absent.pem")
	noEndpoint := cfg
	noEndpoint.S3Endpoint = ""
	for name, c := range map[string]Config{"missing file": missing, "no endpoint": noEndpoint} {
		for _, cmd := range s3Commands {
			assert.False(t, grants(policyFor(cmd, c, "/self"), c.S3CredCAFile, sandbox.Read), "%s %s", name, cmd)
		}
	}
}

// pgBackRest opens lock files O_RDWR and lists its spool, so the scratch
// directories need Read with Write, while the data volume and the repository
// keep exactly what each command had.
func TestPolicyFor_ScratchDirsAreReadWrite(t *testing.T) {
	cfg := testConfig(t)
	rw := sandbox.Read | sandbox.Write
	for _, cmd := range []string{cmdRestore, cmdArchivePush, cmdArchiveGet, cmdBackup, cmdExpire, cmdStanzaCreate, cmdInfo, cmdCheck, cmdVerify, cmdStop, cmdStart, cmdVersion, cmdServer} {
		assert.True(t, grants(policyFor(cmd, cfg, "/self"), cfg.stateSub(dirLock), rw), "%s lock", cmd)
	}
	assert.True(t, grants(policyFor(cmdArchivePush, cfg, "/self"), cfg.stateSub(dirSpool), rw))
	assert.True(t, grants(policyFor(cmdArchiveGet, cfg, "/self"), cfg.stateSub(dirSpool), rw))
	assert.True(t, grants(policyFor(cmdArchiveGet, cfg, "/self"), cfg.stateSub(dirTmp), rw))
	assert.True(t, grants(policyFor(cmdRestore, cfg, "/self"), cfg.stateSub(restoreSpoolRel), rw))

	// Nothing else widened: the data volume is Write only where it was, and
	// never Read-Write-able through the scratch rules.
	for _, cmd := range []string{cmdBackup, cmdArchivePush, cmdArchiveGet, cmdExpire, cmdStanzaCreate, cmdInfo, cmdCheck, cmdVerify} {
		assert.False(t, grants(policyFor(cmd, cfg, "/self"), cfg.DataVolume, sandbox.Write), cmd)
	}
	assert.False(t, grants(policyFor(cmdRestore, cfg, "/self"), cfg.stateSub(dirSpool), sandbox.Read), "restore must not reach the push acknowledgements")
}
