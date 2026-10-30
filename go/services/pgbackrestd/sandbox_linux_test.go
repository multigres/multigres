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

//go:build linux

package pgbackrestd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sandbox"
)

// TestMain makes the test binary answer the sandbox-exec re-execution that
// sandbox.Command performs, the same routing main() does in production.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == sandbox.SubcommandName {
		// The child's environment is built by envFor, so a test asks for an
		// older ABI through a file named after its parent instead.
		if b, err := os.ReadFile(abiMarker(os.Getppid())); err == nil {
			if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				sandbox.LimitABIForTest(n)
			}
		}
		if err := sandbox.ExecMain(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "sandbox-exec:", err)
		}
		os.Exit(1) //nolint:forbidigo // TestMain stands in for main()
	}
	os.Exit(m.Run()) //nolint:forbidigo // TestMain may exit
}

const probeScript = `#!/bin/sh
for a in "$@"; do
  case "$a" in
    w:*) p=${a#w:}; if ( : > "$p/probe" ) 2>/dev/null; then echo "W-OK $p"; else echo "W-DENY $p"; fi;;
    r:*) p=${a#r:}; if ( cat "$p" ) >/dev/null 2>&1; then echo "R-OK $p"; else echo "R-DENY $p"; fi;;
    plant:*) rest=${a#plant:}; d=${rest%%=*}; t=${rest#*=}
      if ln -s "$t" "$d/planted" 2>/dev/null && mkdir "$d/sub" 2>/dev/null && ln -s "$t" "$d/sub/link" 2>/dev/null; then echo "PLANT-OK $d"; else echo "PLANT-FAIL $d"; fi;;
  esac
done
env | sed 's/^/ENV /'
`

// installProbe writes the probe under /usr, the only executable location the
// policy grants, so it can run as the pgBackRest child.
func installProbe(t *testing.T) string {
	t.Helper()
	path := filepath.Join("/usr/local/bin", fmt.Sprintf("pgbackrestd-probe-%d", os.Getpid()))
	if err := os.WriteFile(path, []byte(probeScript), 0o755); err != nil {
		t.Skipf("cannot install the probe under /usr/local/bin (run as root in a container): %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })
	return path
}

// sandboxedServer builds a Server with the sandbox required, so runChild takes
// the real Landlock path.
func sandboxedServer(t *testing.T, mutate func(*Config)) *Server {
	return sandboxedServerWithProbe(t, true, mutate)
}

func sandboxedServerWithProbe(t *testing.T, probe bool, mutate func(*Config)) *Server {
	t.Helper()
	if err := sandbox.CheckAvailable(); err != nil {
		t.Skipf("Landlock unavailable: %v", err)
	}
	s := newTestServerOn(t, func(c *Config) {
		c.SandboxAnchor = AnchorDirFlag
		if mutate != nil {
			mutate(c)
		}
	}, New)
	if probe {
		s.bin = installProbe(t)
	}
	require.NoError(t, os.MkdirAll(s.cfg.stateSub(dirSpool+"/archive"), 0o700))
	return s
}

// runProbe runs the probe as the given kind and command and returns its
// stdout lines.
func runProbe(t *testing.T, s *Server, kind Kind, command string, args ...string) []string {
	t.Helper()
	var out, errOut bytes.Buffer
	code, err := s.runChild(context.Background(), kind, command, args, &out, &errOut)
	require.NoError(t, err, errOut.String())
	require.Zero(t, code, "stdout=%s stderr=%s", out.String(), errOut.String())
	return strings.Split(strings.TrimSpace(out.String()), "\n")
}

func TestSandbox_WriteSetPerCommand(t *testing.T) {
	s := sandboxedServer(t, nil)
	cfg := s.cfg
	repo := cfg.FSRepoPath

	targets := map[string]string{
		"datavol":       cfg.DataVolume,
		"lock":          cfg.stateSub(dirLock),
		"spool":         cfg.stateSub(dirSpool),
		"spool/archive": cfg.stateSub(dirSpool + "/archive"),
		"spool-restore": cfg.stateSub(restoreSpoolRel),
		"tmp":           cfg.stateSub(dirTmp),
		"creds":         cfg.stateSub(dirCreds),
		"conf":          cfg.stateSub(dirConf),
		"state":         cfg.StateDir,
		"repo":          repo,
	}
	// Written out independently of policyFor, from the design's table.
	writable := map[string][]string{
		cmdRestore:     {"datavol", "lock", "spool-restore"},
		cmdArchivePush: {"lock", "spool", "spool/archive", "repo"},
		cmdArchiveGet:  {"lock", "spool", "spool/archive", "tmp"},
		cmdBackup:      {"lock", "repo"},
		cmdInfo:        {"lock"},
		cmdStop:        {"lock"},
		cmdStart:       {"lock"},
		cmdVersion:     {"lock"},
		cmdServer:      {"datavol", "lock"},
	}

	for command, want := range writable {
		t.Run(command, func(t *testing.T) {
			kind := KindS3Command
			if command == cmdServer {
				kind = KindServer
			}
			var args []string
			for _, p := range targets {
				args = append(args, "w:"+p)
			}
			lines := runProbe(t, s, kind, command, args...)
			for name, path := range targets {
				got := slices.Contains(lines, "W-OK "+path)
				assert.Equal(t, slices.Contains(want, name), got, "%s writing %s (%s)", command, name, path)
				if !got {
					assert.True(t, slices.Contains(lines, "W-DENY "+path), "no verdict for %s", path)
				}
			}
		})
	}
}

func TestSandbox_ReadSetAndSecrets(t *testing.T) {
	secrets := t.TempDir()
	cipher := filepath.Join(secrets, "cipher")
	require.NoError(t, os.WriteFile(cipher, []byte(`{"1":"hunter2"}`), 0o600))
	token := filepath.Join(secrets, "token")
	require.NoError(t, os.WriteFile(token, []byte("web-identity-token"), 0o600))
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("x"), 0o600))

	s := sandboxedServer(t, func(c *Config) { c.CipherKeyFile = cipher })
	cfg := s.cfg
	inVol := filepath.Join(cfg.DataVolume, "PG_VERSION")
	require.NoError(t, os.WriteFile(inVol, []byte("17"), 0o600))
	inConf := filepath.Join(cfg.stateSub(dirConf), "pgbackrest.conf")
	require.NoError(t, os.WriteFile(inConf, []byte("[global]\n"), 0o600))
	inCreds := filepath.Join(cfg.stateSub(dirCreds), "marker")
	require.NoError(t, os.WriteFile(inCreds, []byte("x"), 0o600))

	for _, command := range []string{cmdArchivePush, cmdArchiveGet, cmdBackup, cmdRestore, cmdInfo, cmdStop, cmdStart, cmdServer} {
		t.Run(command, func(t *testing.T) {
			kind := KindS3Command
			if command == cmdServer {
				kind = KindServer
			}
			lines := runProbe(t, s, kind, command,
				"r:"+inVol, "r:"+inConf, "r:"+cfg.stateSub(filePgpass), "r:"+inCreds,
				"r:"+cipher, "r:"+token, "r:"+outside, "r:/proc/self/status", "r:/etc/passwd")
			for _, p := range []string{inVol, inConf, cfg.stateSub(filePgpass), "/etc/passwd"} {
				assert.Contains(t, lines, "R-OK "+p, p)
			}
			for _, p := range []string{cipher, token, outside, "/proc/self/status"} {
				assert.Contains(t, lines, "R-DENY "+p, p)
			}
			// The server never holds the token, so it gets no way to the
			// credential socket directory.
			if command == cmdServer {
				assert.Contains(t, lines, "R-DENY "+inCreds)
			} else {
				assert.Contains(t, lines, "R-OK "+inCreds)
			}
		})
	}
}

func envLines(lines []string) []string {
	var env []string
	for _, l := range lines {
		if e, ok := strings.CutPrefix(l, "ENV "); ok {
			env = append(env, e)
		}
	}
	return env
}

func TestSandbox_ChildEnvironment(t *testing.T) {
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::1:role/pid1")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token")
	t.Setenv("PID1_ONLY", "x")
	cipher := filepath.Join(t.TempDir(), "cipher")
	require.NoError(t, os.WriteFile(cipher, []byte(`{"1":"hunter2"}`), 0o600))
	s := sandboxedServer(t, func(c *Config) { c.CipherKeyFile = cipher })
	snap, err := s.buildSnapshot(map[uint32]uint32{1: 1})
	require.NoError(t, err)
	s.setRepoSnapshot(snap)

	for _, command := range []string{cmdArchivePush, cmdArchiveGet, cmdBackup, cmdRestore, cmdInfo} {
		env := envLines(runProbe(t, s, KindS3Command, command))
		assert.Contains(t, env, "PGBACKRESTD_CREDS_TOKEN="+s.credsToken(), command)
		assert.Contains(t, env, "PGBACKREST_REPO1_CIPHER_PASS=hunter2", command)
		for _, e := range env {
			assert.False(t, strings.HasPrefix(e, "AWS_"), "%s leaked %q", command, e)
			assert.NotContains(t, e, "PID1_ONLY", command)
		}
	}

	env := envLines(runProbe(t, s, KindServer, cmdServer))
	assert.Contains(t, env, "PGBACKREST_CONFIG="+s.cfg.stateSub("conf/pgbackrest-server.conf"))
	for _, e := range env {
		assert.NotContains(t, e, "PGBACKRESTD_CREDS_TOKEN")
		assert.NotContains(t, e, "CIPHER_PASS")
		assert.NotContains(t, e, "hunter2")
		assert.False(t, strings.HasPrefix(e, "AWS_"), "server leaked %q", e)
		assert.NotContains(t, e, "PID1_ONLY")
	}
}

func TestSandbox_PlantedLinksAreNotFollowedWhenClearingTmp(t *testing.T) {
	victim := t.TempDir()
	victimFile := filepath.Join(victim, "precious")
	require.NoError(t, os.WriteFile(victimFile, []byte("keep"), 0o600))

	s := sandboxedServer(t, nil)
	cfg := s.cfg
	// archive-get is the one command with write on lock, spool and tmp.
	lines := runProbe(t, s, KindS3Command, cmdArchiveGet,
		"plant:"+cfg.stateSub(dirTmp)+"="+victim,
		"plant:"+cfg.stateSub(dirSpool)+"="+victim,
		"plant:"+cfg.stateSub(dirLock)+"="+victim)
	for _, d := range []string{dirTmp, dirSpool, dirLock} {
		require.Contains(t, lines, "PLANT-OK "+cfg.stateSub(d), "the child must be able to plant in %s for this test to mean anything", d)
	}
	_, err := os.Lstat(filepath.Join(cfg.stateSub(dirTmp), "sub", "link"))
	require.NoError(t, err)

	// PID 1 restarts and clears tmp/.
	s2, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	require.NoError(t, s2.clearTmp(), "startup duties clear tmp")

	entries, err := os.ReadDir(cfg.stateSub(dirTmp))
	require.NoError(t, err)
	assert.Empty(t, entries)
	got, err := os.ReadFile(victimFile)
	require.NoError(t, err, "clearing tmp followed a planted link")
	assert.Equal(t, "keep", string(got))
}

// TestSandbox_RealPgbackrestInfo is a smoke test of the policy against the
// real binary and a filesystem repository.
func TestSandbox_RealPgbackrestInfo(t *testing.T) {
	real, err := exec.LookPath("pgbackrest")
	if err != nil {
		t.Skip("pgbackrest is not installed")
	}
	if filepath.Dir(real) != "/usr/bin" && filepath.Dir(real) != "/bin" {
		t.Skipf("pgbackrest at %s is outside the child PATH", real)
	}
	if os.Geteuid() == 0 {
		t.Skip("pgBackRest refuses to run as root; run this test as an unprivileged user")
	}
	s := sandboxedServerWithProbe(t, false, nil)
	conf := fmt.Sprintf("[global]\nrepo1-path=%s\nlock-path=%s\nlog-level-file=off\nlog-level-console=info\nspool-path=%s\n",
		s.cfg.FSRepoPath, s.cfg.stateSub(dirLock), s.cfg.stateSub(dirSpool))
	require.NoError(t, os.WriteFile(s.cfg.stateSub("conf/pgbackrest.conf"), []byte(conf), 0o600))

	var out, errOut bytes.Buffer
	code, err := s.runChild(context.Background(), KindS3Command, cmdInfo, []string{"--output=json"}, &out, &errOut)
	require.NoError(t, err)
	assert.Zero(t, code, "stdout=%s stderr=%s", out.String(), errOut.String())
	t.Logf("pgbackrest info: %s", strings.TrimSpace(out.String()))
}

// The default (nil-location) cluster passes --tls-cert-dir but mounts nothing
// there. A rule on a missing path fails every sandboxed child, so only the
// server may reference the directory.
func TestSandbox_AbsentCertDirOnlyStopsTheServer(t *testing.T) {
	s := sandboxedServer(t, func(c *Config) { c.TLSCertDir = "/nonexistent/certs" })
	for _, command := range s3Commands {
		runProbe(t, s, KindS3Command, command)
	}
	_, err := s.runChild(context.Background(), KindServer, cmdServer, nil, io.Discard, io.Discard)
	require.Error(t, err)
}

func TestNew_ValidatesTheAnchor(t *testing.T) {
	requireLandlockForNew(t)
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	cfg.FSRepoPath = "/backups"
	cfg.S3CredSource, cfg.CipherKeyFile = "", ""
	cfg.SandboxAnchor = AnchorMountFlag // the data volume is a plain directory
	_, err := New(cfg)
	require.ErrorContains(t, err, "not a mount point")

	cfg.SandboxAnchor = AnchorDirFlag
	s, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func requireLandlockForNew(t *testing.T) {
	t.Helper()
	if err := sandbox.CheckAvailable(); err != nil {
		t.Skipf("Landlock unavailable: %v", err)
	}
}

// Before the first Configure no config has been rendered and the lock path
// would default to /tmp/pgbackrest, outside the policy. start and stop --force
// must still work, under the real sandbox, with real pgBackRest.
func TestSandbox_RealStartStopBeforeConfigure(t *testing.T) {
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest is not installed")
	}
	if os.Geteuid() == 0 {
		t.Skip("pgBackRest refuses to run as root; run this test as an unprivileged user")
	}
	s := sandboxedServerWithProbe(t, false, nil)

	run := func(command string, args ...string) string {
		var out, errOut bytes.Buffer
		code, err := s.runChild(context.Background(), KindS3Command, command, args, &out, &errOut)
		require.NoError(t, err)
		require.Zero(t, code, "%s: stdout=%s stderr=%s", command, out.String(), errOut.String())
		return out.String() + errOut.String()
	}
	stopFile := filepath.Join(s.cfg.stateSub(dirLock), Stanza+".stop")

	run(cmdVersion)
	run(cmdStart)
	run(cmdStop, "--force")
	_, err := os.Stat(stopFile)
	require.NoError(t, err, "stop must leave the stop file in lock/")
	run(cmdStart)
	_, err = os.Stat(stopFile)
	require.ErrorIs(t, err, os.ErrNotExist, "start must remove the stop file")
	run(cmdStop)
	run(cmdStart)
}

// After a container restart the emptyDir still holds the conf the last
// Configure rendered, which names a cipher repo, but the snapshot is empty
// until the next Configure. The boot-time start must still succeed, or
// Configure can never arrive.
func TestSandbox_RealBootStartWithRenderedCipherConf(t *testing.T) {
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest is not installed")
	}
	if os.Geteuid() == 0 {
		t.Skip("pgBackRest refuses to run as root; run this test as an unprivileged user")
	}
	first := sandboxedServerWithProbe(t, false, nil)
	cfg := first.cfg
	require.NoError(t, first.Close())

	rendered := fmt.Sprintf(`[global]
repo1-path=%s
repo1-cipher-type=aes-256-cbc
lock-path=%s
log-level-file=off
spool-path=%s

[multigres]
pg1-path=%s
pg1-socket-path=%s
`, cfg.FSRepoPath, cfg.stateSub(dirLock), cfg.stateSub(dirSpool), cfg.PGData, cfg.PGSocketDir)
	require.NoError(t, os.WriteFile(cfg.stateSub(fileCommandConf), []byte(rendered), 0o600))

	// The restart: same state dir, empty snapshot, no cipher pass anywhere.
	s, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	require.Empty(t, s.snapshot().CipherPass)
	conf, err := os.ReadFile(cfg.stateSub(fileCommandConf))
	require.NoError(t, err)
	// A rendered conf with no persisted state behind it is stale: the restart
	// replaces it with the minimal conf, which is what lets start read it.
	require.NotContains(t, string(conf), "repo1-cipher-type", "a stale rendered conf must not survive an unconfigured restart")

	var out, errOut bytes.Buffer
	code, err := s.runChild(context.Background(), KindS3Command, cmdStart, nil, &out, &errOut)
	require.NoError(t, err)
	assert.Zero(t, code, "stdout=%s stderr=%s", out.String(), errOut.String())
	t.Logf("boot start with rendered cipher conf: code=%d out=%q err=%q", code, out.String(), errOut.String())
}

// A sandboxed backup is the pg2 TLS client, so it must read the three client
// files; no other command may, and a path outside the cert dir stays denied.
func TestSandbox_BackupReadsPg2ClientCerts(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "other.key")
	require.NoError(t, os.WriteFile(outside, []byte("x"), 0o600))
	s := sandboxedServer(t, nil)
	cert, key, ca := constants.PgBackRestCertFiles(s.cfg.TLSCertDir)

	for _, command := range []string{cmdBackup, cmdInfo, cmdRestore, cmdArchivePush, cmdStart} {
		t.Run(command, func(t *testing.T) {
			sibling := filepath.Join(filepath.Dir(s.cfg.TLSCertDir), "sibling.key")
			require.NoError(t, os.WriteFile(sibling, []byte("x"), 0o600))
			lines := runProbe(t, s, KindS3Command, command, "r:"+cert, "r:"+key, "r:"+ca, "r:"+outside, "r:"+sibling)
			want := "R-DENY "
			if command == cmdBackup {
				want = "R-OK "
			}
			for _, p := range []string{cert, key, ca} {
				assert.Contains(t, lines, want+p, p)
			}
			assert.Contains(t, lines, "R-DENY "+outside)
			assert.Contains(t, lines, "R-DENY "+sibling, "a path beside the cert dir stays denied")
		})
	}
}

func abiMarker(pid int) string { return fmt.Sprintf("/tmp/pgbackrestd-test-abi.%d", pid) }
