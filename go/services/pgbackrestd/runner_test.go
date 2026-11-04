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
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

// makeCertDir writes the three files the server configuration names.
func makeCertDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cert, key, ca := constants.PgBackRestCertFiles(dir)
	for _, p := range []string{cert, key, ca} {
		require.NoError(t, os.WriteFile(p, []byte("pem"), 0o600))
	}
	return dir
}

func newTestServer(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()
	return newTestServerOn(t, mutate, newForTest)
}

// newTestServerOn builds the server with build: newForTest for tests of
// anything but confinement, New (the real Landlock sandbox) for the rest.
func newTestServerOn(t *testing.T, mutate func(*Config), build func(Config) (*Server, error)) *Server {
	t.Helper()
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	cfg.FSRepoPath = t.TempDir()
	cfg.S3CredSource = ""
	cfg.CipherKeyFile = ""
	cfg.TLSCertDir = makeCertDir(t)
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := build(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestNew_PreparesState(t *testing.T) {
	s := newTestServer(t, nil)
	for _, d := range []string{dirConf, dirSpool, dirLock, dirTmp, dirCreds, dirState, restoreSpoolRel} {
		fi, err := os.Stat(s.cfg.stateSub(d))
		require.NoError(t, err, d)
		assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm(), d)
	}
	assert.Len(t, s.credsToken(), 64)
	// Commands that run before the first Configure need a config to read.
	conf, err := os.ReadFile(s.cfg.stateSub(fileCommandConf))
	require.NoError(t, err)
	assert.Contains(t, string(conf), "lock-path="+s.cfg.stateSub(dirLock))
	assert.Equal(t, uint32(16<<20), s.walSegmentSize())
	assert.False(t, s.configured())
}

func TestChildArgv(t *testing.T) {
	s := newTestServer(t, nil)
	conf := s.cfg.stateSub("conf/pgbackrest.conf")
	bin := s.binPath()

	assert.Equal(t,
		[]string{
			bin, "--stanza=multigres", "--config=" + conf,
			"--lock-path=" + s.cfg.stateSub(dirLock), "--log-level-file=off", "info", "--output=json",
		},
		s.childArgv(KindS3Command, "info", []string{"--output=json"}))

	// check and verify take no lock: pgBackRest 2.60.0 rejects --lock-path on
	// them with error 031.
	for _, command := range []string{cmdCheck, cmdVerify} {
		assert.Equal(t,
			[]string{bin, "--stanza=multigres", "--config=" + conf, "--log-level-file=off", command},
			s.childArgv(KindS3Command, command, nil))
	}

	// version rejects every global option.
	assert.Equal(t, []string{bin, "version"}, s.childArgv(KindS3Command, cmdVersion, nil))

	// The server rejects --stanza, and always runs the server command.
	assert.Equal(t,
		[]string{bin, "--config=" + s.cfg.stateSub("conf/pgbackrest-server.conf"), "server"},
		s.childArgv(KindServer, "ignored", nil))

	restore := s.childArgv(KindS3Command, cmdRestore, nil)
	assert.Contains(t, restore, "--spool-path="+s.cfg.stateSub(restoreSpoolRel))
}

// The asynchronous archive worker re-executes the child's argv[0] on the
// child's fixed PATH, so argv[0] must be a path, not a name that only PID 1's
// PATH can resolve.
func TestBinPathIsAbsoluteWhenFoundOnPath(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "pgbackrest")
	require.NoError(t, os.WriteFile(stub, []byte("#!/bin/sh\n"), 0o755))
	t.Setenv("PATH", dir)

	s := &Server{bin: "pgbackrest"}
	assert.Equal(t, stub, s.binPath())

	assert.Equal(t, "/custom/pgbackrest", (&Server{bin: "/custom/pgbackrest"}).binPath(), "a path is used as given")

	t.Setenv("PATH", t.TempDir())
	assert.Equal(t, "pgbackrest", s.binPath(), "a missing binary keeps the bare name so the spawn reports it")
}

// fakePgbackrest puts a script named pgbackrest first on PATH. The script
// records its grandchild's pid, signals readiness, then sleeps.
func fakePgbackrest(t *testing.T) (pidFile, readyFile string) {
	t.Helper()
	dir := t.TempDir()
	pidFile = filepath.Join(dir, "grandchild.pid")
	readyFile = filepath.Join(dir, "ready")
	script := "#!/bin/sh\nsleep 300 &\necho $! > " + pidFile + "\necho ready > " + readyFile + "\nsleep 300\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return pidFile, readyFile
}

func TestRunChild_KillsGroupOnCancel(t *testing.T) {
	pidFile, readyFile := fakePgbackrest(t)
	s := newTestServer(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := s.runChild(ctx, KindS3Command, "info", nil, &out, &out)
		done <- err
	}()

	require.Eventually(t, func() bool { _, err := os.Stat(readyFile); return err == nil }, 10*time.Second, 20*time.Millisecond)
	raw, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	grandchild, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(grandchild, 0), "grandchild should be alive before cancel")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("runChild did not return after cancel")
	}
	require.Eventually(t, func() bool { return syscall.Kill(grandchild, 0) == syscall.ESRCH },
		10*time.Second, 20*time.Millisecond, "grandchild survived the group kill")
}

func TestRunChild_ReturnsExitCode(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte("#!/bin/sh\necho \"$@\"\nexit 103\n"), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	s := newTestServer(t, nil)

	var out bytes.Buffer
	code, err := s.runChild(context.Background(), KindS3Command, "check", nil, &out, &out)
	require.NoError(t, err)
	assert.Equal(t, 103, code)
	assert.Contains(t, out.String(), "--stanza=multigres")
}

func TestRunChild_EnvIsExact(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte("#!/bin/sh\n/usr/bin/env\n"), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("AWS_ROLE_ARN", "arn:leak")
	s := newTestServer(t, nil)

	var out bytes.Buffer
	_, err := s.runChild(context.Background(), KindServer, "server", nil, &out, &out)
	require.NoError(t, err)
	assert.NotContains(t, out.String(), "AWS_ROLE_ARN")
	assert.NotContains(t, out.String(), "PGBACKRESTD_CREDS_TOKEN")
	assert.Contains(t, out.String(), "PGBACKREST_CONFIG=")
}

func TestGate_CloseIsAtomicWithEnter(t *testing.T) {
	for range 50 {
		g := NewGate()
		var closedDone atomic.Bool
		var violations atomic.Int32
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					// Read before the call: if the gate had already finished
					// closing, TryEnter must refuse.
					wasClosed := closedDone.Load()
					release, ok := g.TryEnter()
					if ok {
						if wasClosed {
							violations.Add(1)
						}
						release()
					}
				}
			})
		}
		time.Sleep(time.Millisecond)
		reopen, err := g.Close(context.Background(), 5*time.Second)
		require.NoError(t, err)
		closedDone.Store(true)
		time.Sleep(2 * time.Millisecond)
		close(stop)
		wg.Wait()
		reopen()
		assert.Zero(t, violations.Load(), "TryEnter succeeded after Close returned")
	}
}

func TestGate_CloseWaitsForInFlight(t *testing.T) {
	g := NewGate()
	release, ok := g.TryEnter()
	require.True(t, ok)

	closed := make(chan struct{})
	var reopen func()
	go func() {
		var err error
		reopen, err = g.Close(context.Background(), 5*time.Second)
		assert.NoError(t, err)
		close(closed)
	}()

	require.Eventually(t, func() bool {
		rel, ok := g.TryEnter()
		if ok {
			rel()
		}
		return !ok
	}, time.Second, time.Millisecond,
		"gate should refuse new entries while draining")
	select {
	case <-closed:
		t.Fatal("Close returned with a request in flight")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the request finished")
	}
	_, ok = g.TryEnter()
	assert.False(t, ok)
	reopen()
	release2, ok := g.TryEnter()
	assert.True(t, ok)
	release2()
}

func TestGate_CloseTimesOutAndReopens(t *testing.T) {
	g := NewGate()
	release, ok := g.TryEnter()
	require.True(t, ok)
	defer release()

	reopen, err := g.Close(context.Background(), 30*time.Millisecond)
	require.Error(t, err)
	assert.Nil(t, reopen)

	release2, ok := g.TryEnter()
	assert.True(t, ok, "gate must be open again after a timed-out Close")
	release2()
}

func TestGate_CloseHonorsContext(t *testing.T) {
	g := NewGate()
	release, _ := g.TryEnter()
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := g.Close(ctx, time.Minute)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBuildSnapshot_StaticKeysFollowTheRepoList(t *testing.T) {
	keys := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(keys, "AWS_ACCESS_KEY_ID"), []byte("AKIAXYZ\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(keys, "AWS_SECRET_ACCESS_KEY"), []byte("sekret\n"), 0o600))

	s := newTestServer(t, func(c *Config) {
		c.LocationType = LocationS3
		c.S3Bucket, c.S3Region = "b", "r"
		c.S3CredSource = CredStatic
		c.S3StaticKeyDir = keys
	})
	snap, err := s.buildSnapshot(map[uint32]uint32{2: 1})
	require.NoError(t, err)
	assert.Equal(t, map[uint32]StaticKey{2: {ID: "AKIAXYZ", Secret: "sekret"}}, snap.StaticKeys)
}

func TestBuildSnapshot_MapsGenerationsAndRejectsMissingOnes(t *testing.T) {
	cipher := filepath.Join(t.TempDir(), "cipher")
	require.NoError(t, os.WriteFile(cipher, []byte(`{"3":"three","7":"seven"}`), 0o600))
	s := newTestServer(t, func(c *Config) { c.CipherKeyFile = cipher })

	snap, err := s.buildSnapshot(map[uint32]uint32{1: 3, 2: 7})
	require.NoError(t, err)
	env := envFor(KindS3Command, s.cfg, Secrets{RepoSnapshot: snap, CredsToken: "t"})
	assert.Contains(t, env, "PGBACKREST_REPO1_CIPHER_PASS=three")
	assert.Contains(t, env, "PGBACKREST_REPO2_CIPHER_PASS=seven")

	_, err = s.buildSnapshot(map[uint32]uint32{1: 4})
	require.ErrorContains(t, err, "generation 4")
}

// A Configure swaps the snapshot while children are being started. Every
// environment must come from exactly one snapshot, and -race must stay quiet.
func TestSnapshot_ConcurrentSwapIsAtomic(t *testing.T) {
	s := newTestServer(t, nil)
	a := RepoSnapshot{Repos: map[uint32]uint32{1: 3, 2: 7}, CipherPass: map[uint32]string{3: "A3", 7: "A7"}}
	b := RepoSnapshot{Repos: map[uint32]uint32{1: 8, 2: 9}, CipherPass: map[uint32]string{8: "B8", 9: "B9"}}
	s.setRepoSnapshot(a)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				env := envFor(KindS3Command, s.cfg, s.secretsNow())
				joined := strings.Join(env, "\n")
				isA := strings.Contains(joined, "REPO1_CIPHER_PASS=A3") && strings.Contains(joined, "REPO2_CIPHER_PASS=A7")
				isB := strings.Contains(joined, "REPO1_CIPHER_PASS=B8") && strings.Contains(joined, "REPO2_CIPHER_PASS=B9")
				if isA == isB {
					t.Errorf("env is not from a single snapshot: %v", env)
					return
				}
			}
		})
	}
	for i := range 2000 {
		if i%2 == 0 {
			s.setRepoSnapshot(b)
		} else {
			s.setRepoSnapshot(a)
		}
	}
	close(stop)
	wg.Wait()

	// The caller's maps are copied on publish.
	a.CipherPass[3] = "mutated"
	s.setRepoSnapshot(a)
	a.CipherPass[3] = "mutated-again"
	assert.Equal(t, "mutated", s.snapshot().CipherPass[3])
}

func TestRunChild_RejectsBadCommands(t *testing.T) {
	s := newTestServer(t, nil)
	for _, command := range []string{cmdServer, "", "rm", "backup --repo=2", "../info"} {
		var out bytes.Buffer
		code, err := s.runChild(context.Background(), KindS3Command, command, nil, &out, &out)
		require.Error(t, err, command)
		assert.Equal(t, -1, code)
	}
	_, err := s.runChild(context.Background(), Kind(42), cmdInfo, nil, io.Discard, io.Discard)
	require.Error(t, err)
}

func TestRunChild_ServerFailsClosedWithoutCertDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte("#!/bin/sh\nexit 0\n"), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	partial := t.TempDir()
	cert, _, _ := constants.PgBackRestCertFiles(partial)
	require.NoError(t, os.WriteFile(cert, []byte("pem"), 0o600))
	unreadableKind := t.TempDir()
	for _, p := range func() []string { c, k, a := constants.PgBackRestCertFiles(unreadableKind); return []string{c, a, k} }() {
		require.NoError(t, os.WriteFile(p, []byte("pem"), 0o600))
	}
	require.NoError(t, os.Remove(filepath.Join(unreadableKind, "pgbackrest.key")))
	require.NoError(t, os.Mkdir(filepath.Join(unreadableKind, "pgbackrest.key"), 0o700))

	for name, certDir := range map[string]string{
		"absent":       "/nonexistent/certs",
		"unset":        "",
		"empty":        t.TempDir(),
		"partial":      partial,
		"key is a dir": unreadableKind,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t, nil)
			s.cfg.TLSCertDir = certDir
			_, err := s.runChild(context.Background(), KindServer, cmdServer, nil, io.Discard, io.Discard)
			require.Error(t, err)

			// The default nil-location layout has no cert volume; commands
			// other than the server must not care.
			code, err := s.runChild(context.Background(), KindS3Command, cmdInfo, nil, io.Discard, io.Discard)
			require.NoError(t, err)
			assert.Zero(t, code)
		})
	}
}

func TestRunChild_ServerRunsWithCompleteCertDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte("#!/bin/sh\nexit 0\n"), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	s := newTestServer(t, nil)
	require.NoError(t, s.checkCertDir())
	code, err := s.runChild(context.Background(), KindServer, cmdServer, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	assert.Zero(t, code)
}

func TestListenUnix_NonEmptyDirectoryAtPathDoesNotBlockStartup(t *testing.T) {
	victim := t.TempDir()
	victimFile := filepath.Join(victim, "precious")
	require.NoError(t, os.WriteFile(victimFile, []byte("keep"), 0o600))

	dir, err := os.MkdirTemp("", "pgbr-sock")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "x.sock")
	require.NoError(t, os.MkdirAll(filepath.Join(path, "a", "b"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "a", "file"), []byte("x"), 0o600))
	require.NoError(t, os.Symlink(victim, filepath.Join(path, "a", "link")))
	require.NoError(t, os.Symlink(victimFile, filepath.Join(path, "a", "b", "filelink")))

	ln, err := listenUnix(path)
	require.NoError(t, err)
	defer ln.Close()
	fi, err := os.Lstat(path)
	require.NoError(t, err)
	assert.NotZero(t, fi.Mode()&os.ModeSocket)
	got, err := os.ReadFile(victimFile)
	require.NoError(t, err, "removal followed a planted link")
	assert.Equal(t, "keep", string(got))
}

func TestListenUnix_ReplacesPlantedEntriesWithoutFollowing(t *testing.T) {
	victim := filepath.Join(t.TempDir(), "victim")
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o640))

	plant := map[string]func(path string) error{
		"symlink":      func(p string) error { return os.Symlink(victim, p) },
		"regular file": func(p string) error { return os.WriteFile(p, []byte("junk"), 0o600) },
		"empty dir":    func(p string) error { return os.Mkdir(p, 0o700) },
		"stale socket": func(p string) error {
			l, err := net.Listen("unix", p)
			if err == nil {
				l.(*net.UnixListener).SetUnlinkOnClose(false)
				l.Close()
			}
			return err
		},
		"nothing there": func(string) error { return nil },
	}
	for name, do := range plant {
		t.Run(name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "pgbr-sock")
			require.NoError(t, err)
			t.Cleanup(func() { os.RemoveAll(dir) })
			path := filepath.Join(dir, "x.sock")
			require.NoError(t, do(path))

			ln, err := listenUnix(path)
			require.NoError(t, err)
			defer ln.Close()

			fi, err := os.Lstat(path)
			require.NoError(t, err)
			assert.NotZero(t, fi.Mode()&os.ModeSocket, "path must now be a socket")
			assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm(), "mode must be right at creation")
			// The umask is restored, and the owner x bit was never cleared.
			d, err := os.MkdirTemp(dir, "after")
			require.NoError(t, err)
			di, err := os.Stat(d)
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o700), di.Mode().Perm())
			vi, err := os.Stat(victim)
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o640), vi.Mode().Perm(), "a link target must not be chmodded")
		})
	}
}

func TestNew_StaticKeyMissingFails(t *testing.T) {
	cfg := testConfig(t)
	cfg.S3StaticKeyDir = t.TempDir()
	_, err := newForTest(cfg)
	require.Error(t, err)
}
