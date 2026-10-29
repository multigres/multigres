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
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sandbox"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// pgToolDir finds initdb and pg_ctl, which Debian keeps off PATH.
func pgToolDir(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("initdb"); err == nil {
		return filepath.Dir(p)
	}
	matches, _ := filepath.Glob("/usr/lib/postgresql/*/bin/initdb")
	if len(matches) == 0 {
		t.Skip("postgres binaries are not installed")
	}
	return filepath.Dir(matches[len(matches)-1])
}

// realCluster is a throwaway postgres whose WAL goes to the repository through
// an unsandboxed archive-push, the way postgres runs archive_command, plus the
// sandboxed Server whose children the tests drive.
type realCluster struct {
	s    *Server
	port int
	user string
	bin  string
}

func (c *realCluster) psql(t *testing.T, sql string) string {
	t.Helper()
	out, err := exec.Command(filepath.Join(c.bin, "psql"), "-h", c.s.cfg.PGSocketDir, "-p", strconv.Itoa(c.port),
		"-U", c.user, "-d", "postgres", "-Atc", sql).CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

// run executes one command as a sandboxed S3-command child, the way every API
// request does, and requires success.
func (c *realCluster) run(t *testing.T, command string, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	code, err := c.s.runChild(ctx, KindS3Command, command, args, &out, &out)
	require.NoError(t, err)
	require.Zero(t, code, "%s %v:\n%s", command, args, out.String())
	return out.String()
}

func (c *realCluster) try(t *testing.T, command string, args ...string) (int, string) {
	t.Helper()
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	code, err := c.s.runChild(ctx, KindS3Command, command, args, &out, &out)
	require.NoError(t, err)
	return code, out.String()
}

// archivedSegments lists the WAL segment names in a filesystem repository.
func archivedSegments(t *testing.T, repo string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(repo, "archive", Stanza, "*", "*", "*"))
	require.NoError(t, err)
	var names []string
	for _, f := range files {
		base := filepath.Base(f)
		if len(base) > 25 && base[24] == '-' {
			names = append(names, base[:24])
		}
	}
	sort.Strings(names)
	return names
}

func newRealCluster(t *testing.T, abi int) *realCluster {
	t.Helper()
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest is not installed")
	}
	if os.Geteuid() == 0 {
		t.Skip("pgBackRest refuses to run as root; run this test as an unprivileged user")
	}
	bin := pgToolDir(t)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	if abi > 0 {
		if sandbox.ABI() < abi {
			t.Skipf("kernel Landlock ABI %d < %d", sandbox.ABI(), abi)
		}
		marker := abiMarker(os.Getpid())
		require.NoError(t, os.WriteFile(marker, []byte(strconv.Itoa(abi)), 0o600))
		t.Cleanup(func() { os.Remove(marker) })
	}

	// Short paths: the postgres socket path must fit in sun_path.
	dv, err := os.MkdirTemp("", "pbv")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dv) })
	u, err := user.Current()
	require.NoError(t, err)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	s := sandboxedServerWithProbe(t, false, func(c *Config) {
		c.DataVolume = dv
		c.PGData = filepath.Join(dv, "pg_data")
		c.PGSocketDir = filepath.Join(dv, "pg_sockets")
		c.PGPort = port
		c.PGUser = u.Username
	})
	cfg := s.cfg
	require.NoError(t, os.MkdirAll(cfg.PGSocketDir, 0o700))
	cl := &realCluster{s: s, port: port, user: u.Username, bin: bin}

	conf := fmt.Sprintf(`[global]
repo1-path=%s
repo1-retention-full=1
lock-path=%s
spool-path=%s
log-level-file=off
log-level-console=info
archive-async=y
archive-get-queue-max=64MiB
start-fast=y
compress-type=none

[multigres]
pg1-path=%s
pg1-socket-path=%s
pg1-port=%d
pg1-user=%s
`, cfg.FSRepoPath, cfg.stateSub(dirLock), cfg.stateSub(dirSpool), cfg.PGData, cfg.PGSocketDir, port, u.Username)
	require.NoError(t, fileutil.AtomicWriteFile(cfg.stateSub(fileCommandConf), []byte(conf), 0o600))

	out, err := exec.Command(filepath.Join(bin, "initdb"), "-D", cfg.PGData, "-U", u.Username, "--auth=trust", "--no-locale", "-E", "UTF8").CombinedOutput()
	require.NoError(t, err, string(out))
	extra := fmt.Sprintf(`
listen_addresses=''
port=%d
unix_socket_directories='%s'
wal_level=replica
archive_mode=on
archive_command='pgbackrest --stanza=multigres --config=%s archive-push %%p'
`, port, cfg.PGSocketDir, cfg.stateSub(fileCommandConf))
	f, err := os.OpenFile(filepath.Join(cfg.PGData, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString(extra)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	out, err = exec.Command(filepath.Join(bin, "pg_ctl"), "-D", cfg.PGData, "-l", filepath.Join(dv, "pg.log"), "-w", "start").CombinedOutput()
	require.NoError(t, err, string(out))
	t.Cleanup(func() {
		_ = exec.Command(filepath.Join(bin, "pg_ctl"), "-D", cfg.PGData, "-m", "immediate", "stop").Run()
	})
	return cl
}

// TestSandbox_RealCommandsUnderLandlock runs real pgBackRest for every command
// the allowlist permits that touches lock/, spool/, tmp/ or the restore spool,
// under the real Landlock policy, then repeats it on the rulesets of older
// ABIs. The probes in the other sandbox tests cannot show that pgBackRest
// itself needs Read on those directories (it opens lock files O_RDWR and lists
// its spool), so this is the test that does.
func TestSandbox_RealCommandsUnderLandlock(t *testing.T) {
	for _, abi := range []int{0, 8, 7} {
		name := "kernel-abi"
		if abi > 0 {
			name = "abi" + strconv.Itoa(abi)
		}
		t.Run(name, func(t *testing.T) { realCommandSequence(t, abi) })
	}
}

func realCommandSequence(t *testing.T, abi int) {
	c := newRealCluster(t, abi)
	cfg := c.s.cfg
	t.Logf("kernel Landlock ABI %d, enforcing ABI %d", sandbox.ABI(), max(abi, 0))

	c.run(t, cmdStart)
	c.run(t, cmdStanzaCreate)
	c.run(t, cmdInfo, "--output=json")

	// Make WAL, then let postgres archive it (unsandboxed, as in production
	// before the API existed) so check and backup's archive checks pass.
	c.psql(t, "create table t as select generate_series(1,1000) i")
	c.psql(t, "select pg_switch_wal()")
	c.run(t, cmdCheck)
	c.run(t, cmdBackup, "--type=full")

	// Async archive-push in the sandbox: the foreground process and the worker
	// it forks list and write spool/archive and lock/. The segment is already
	// in the repository, which pgBackRest accepts when the content is equal.
	c.psql(t, "insert into t select generate_series(1,1000)")
	seg2 := c.psql(t, "select pg_walfile_name(pg_switch_wal())")
	c.run(t, cmdArchivePush, filepath.Join(cfg.PGData, "pg_wal", seg2))

	// Postgres's own archiver must have stored the segments before they are
	// fetched back; fetch whichever segments the repository really holds.
	var segments []string
	require.Eventually(t, func() bool {
		segments = archivedSegments(t, cfg.FSRepoPath)
		return len(segments) >= 2
	}, 60*time.Second, 500*time.Millisecond, "the repository should hold archived WAL")

	// Async archive-get into tmp/, as the WAL fetch handler does.
	for _, name := range []string{segments[0], segments[len(segments)-1]} {
		dst := filepath.Join(cfg.stateSub(dirTmp), name+".fetch")
		if code, out := c.try(t, cmdArchiveGet, name, dst); code != 0 {
			tree, _ := exec.Command("find", cfg.FSRepoPath, cfg.stateSub(dirSpool), "-type", "f").CombinedOutput()
			t.Fatalf("archive-get %s exit %d:\n%s\nfiles:\n%s", name, code, out, tree)
		}
		fi, err := os.Stat(dst)
		require.NoError(t, err, name)
		assert.Equal(t, int64(16<<20), fi.Size(), name)
		require.NoError(t, os.Remove(dst))
	}

	c.run(t, cmdBackup, "--type=full")
	c.run(t, cmdExpire)
	c.run(t, cmdVerify)

	// Restore writes the data volume and its own spool only.
	target := filepath.Join(cfg.DataVolume, "restored")
	c.run(t, cmdRestore, "--pg1-path="+target, "--type=none")
	_, err := os.Stat(filepath.Join(target, "PG_VERSION"))
	require.NoError(t, err, "restore must have produced a data directory")

	c.run(t, cmdStop)
	c.run(t, cmdStart)
	c.run(t, cmdInfo)
}

// A sandboxed child's file rule is bound to one inode. The 8432 server holds a
// rule on pgpass, and pgpass is replaced by rename, so a server that keeps
// running across a Configure loses the file (libpq skips an unreadable pgpass
// silently, and the replica backup then fails with "no password supplied").
func serverProbe(t *testing.T, s *Server) (runLog string) {
	t.Helper()
	runLog = filepath.Join(s.cfg.DataVolume, "probe.log")
	script := fmt.Sprintf(`#!/bin/sh
if cat "$HOME/pgpass" >/dev/null 2>&1; then echo "run ok $$" >> %[1]s; else echo "run deny $$" >> %[1]s; fi
while true; do
  if cat "$HOME/pgpass" >/dev/null 2>&1; then echo "chk ok $$" >> %[1]s; else echo "chk deny $$" >> %[1]s; fi
  sleep 0.2
done
`, runLog)
	bin := filepath.Join("/usr/local/bin", fmt.Sprintf("pgbackrestd-server-probe-%d", os.Getpid()))
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Skipf("cannot install the probe under /usr/local/bin: %v", err)
	}
	t.Cleanup(func() { os.Remove(bin) })
	s.bin = bin
	return runLog
}

func logLines(t *testing.T, path, prefix string) []string {
	t.Helper()
	b, _ := os.ReadFile(path)
	var out []string
	for l := range strings.SplitSeq(string(b), "\n") {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}

func pgpassServer(t *testing.T) (*Server, string) {
	t.Helper()
	pw := filepath.Join(t.TempDir(), "pw")
	require.NoError(t, os.WriteFile(pw, []byte("secret\n"), 0o600))
	s := sandboxedServerWithProbe(t, false, func(c *Config) {
		c.PGPasswordFile = pw
		c.PGUser = "postgres"
	})
	runLog := serverProbe(t, s)
	fastServer(s)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); s.stopBackground() })
	require.NoError(t, s.startPgServer(ctx))
	require.Eventually(t, func() bool { return len(logLines(t, runLog, "run ok")) == 1 }, 20*time.Second, 50*time.Millisecond,
		"the server must start sandboxed and read pgpass")
	return s, runLog
}

func TestSandbox_PgpassReplacementBlindsARunningServer(t *testing.T) {
	s, runLog := pgpassServer(t)
	// The hazard, shown: a replacement behind the server's back.
	require.NoError(t, s.writePgpass())
	require.Eventually(t, func() bool { return len(logLines(t, runLog, "chk deny")) > 0 }, 10*time.Second, 100*time.Millisecond,
		"a rename over pgpass must hide it from a server sandboxed on the old inode")
}

func TestSandbox_ConfigureLeavesAWorkingServer(t *testing.T) {
	s, runLog := pgpassServer(t)
	require.NoError(t, s.applyConfigure(persistedState{}, RepoSnapshot{}))
	require.Eventually(t, func() bool { return len(logLines(t, runLog, "run ")) >= 2 }, 20*time.Second, 50*time.Millisecond,
		"Configure must restart the server")
	runs := logLines(t, runLog, "run ")
	assert.Equal(t, "run ok", runs[len(runs)-1][:6], "the restarted server reads the new pgpass")
	time.Sleep(time.Second)
	// Only the old server's loop may have seen the swap, and only until it was killed.
	after := logLines(t, runLog, "chk deny")
	time.Sleep(time.Second)
	assert.Len(t, logLines(t, runLog, "chk deny"), len(after), "no process may still be denied pgpass")
}

// A restore pauses the server for its whole run; a Configure that lands in the
// middle must nest with that pause, not restart the server under the restore
// or leave it down afterwards.
func TestSandbox_ConfigureNestsWithARestorePause(t *testing.T) {
	s, runLog := pgpassServer(t)
	resume := s.pause()
	require.Empty(t, logLines(t, runLog, "chk deny"))
	before := len(logLines(t, runLog, "run "))

	require.NoError(t, s.applyConfigure(persistedState{}, RepoSnapshot{}))
	time.Sleep(time.Second)
	assert.Len(t, logLines(t, runLog, "run "), before, "the server must stay down while the restore's pause is held")

	resume()
	require.Eventually(t, func() bool { return len(logLines(t, runLog, "run ")) == before+1 }, 20*time.Second, 50*time.Millisecond,
		"the server comes back once the outer pause is released")
	runs := logLines(t, runLog, "run ")
	assert.Equal(t, "run ok", runs[len(runs)-1][:6])
}
