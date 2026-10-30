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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

// fakeBackrest is a stand-in pgbackrest first on PATH. It appends every argv
// to a log, answers version, fails the commands it is told to, and can hold
// restore until released.
type fakeBackrest struct {
	log     string
	started string
	release string
	stop    string
}

// fakeOpts shapes the fake. A stop file is modelled the way pgBackRest does:
// stop on an existing file signals nobody (logged NOKILL, otherwise KILL),
// and start removes it.
type fakeOpts struct {
	failing     []string
	holdRestore bool
	// failStartCalls lists the 1-based numbers of the start invocations that
	// fail and leave the stop file in place.
	failStartCalls []int
	// versionBody replaces the version branch of the script.
	versionBody string
	// hang lists commands that block until killed, after logging their argv.
	hang []string
	// infoBytes makes info print that many bytes to stdout.
	infoBytes int
}

func newFakeBackrest(t *testing.T, failing []string, holdRestore bool) *fakeBackrest {
	t.Helper()
	return newFakeBackrestOpts(t, fakeOpts{failing: failing, holdRestore: holdRestore})
}

func newFakeBackrestOpts(t *testing.T, o fakeOpts) *fakeBackrest {
	t.Helper()
	dir := t.TempDir()
	f := &fakeBackrest{
		log:     filepath.Join(dir, "log"),
		started: filepath.Join(dir, "restore-started"),
		release: filepath.Join(dir, "release"),
		stop:    filepath.Join(dir, "stopfile"),
	}
	hold := ""
	if o.holdRestore {
		hold = fmt.Sprintf("touch %q\nwhile [ ! -f %q ]; do sleep 0.02; done\n", f.started, f.release)
	}
	version := `echo "pgBackRest 2.60.0"`
	if o.versionBody != "" {
		version = o.versionBody
	}
	info := ":"
	if o.infoBytes > 0 {
		info = fmt.Sprintf("head -c %d /dev/zero | tr '\\0' x", o.infoBytes)
	}
	var failStart []string
	for _, n := range o.failStartCalls {
		failStart = append(failStart, strconv.Itoa(n))
	}
	script := fmt.Sprintf(`#!/bin/sh
for a in "$@"; do case "$a" in --*) ;; *) cmd=$a; break;; esac; done
echo "$@" >> %[1]q
case " %[7]s " in *" $cmd "*) sleep 600;; esac
case "$cmd" in
info) %[8]s ;;
version) %[3]s ;;
restore)
%[2]s;;
stop) if [ -f %[4]q ]; then echo NOKILL >> %[1]q; else touch %[4]q; echo KILL >> %[1]q; fi ;;
start)
  n=$(cat %[4]q.calls 2>/dev/null || echo 0); n=$((n+1)); echo $n > %[4]q.calls
  case " %[6]s " in *" $n "*) echo "boom start $n" >&2; exit 7;; esac
  rm -f %[4]q ;;
esac
case " %[5]s " in *" $cmd "*) echo "boom $cmd" >&2; exit 7;; esac
exit 0
`, f.log, hold, version, f.stop, strings.Join(o.failing, " "), strings.Join(failStart, " "), strings.Join(o.hang, " "), info)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return f
}

// lines returns the logged argv lines.
func (f *fakeBackrest) lines(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(f.log)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// commands returns the pgbackrest command of each logged line, in order,
// skipping the one-off version probe.
func (f *fakeBackrest) commands(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, l := range f.lines(t) {
		for w := range strings.FieldsSeq(l) {
			if !strings.HasPrefix(w, "--") {
				if w != "version" && w != "KILL" && w != "NOKILL" {
					out = append(out, w)
				}
				break
			}
		}
	}
	return out
}

func (f *fakeBackrest) waitRestoreStarted(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { _, err := os.Stat(f.started); return err == nil }, 10*time.Second, 10*time.Millisecond)
}

func (f *fakeBackrest) releaseRestore(t *testing.T) {
	t.Helper()
	require.NoError(t, os.WriteFile(f.release, nil, 0o600))
}

type controlHarness struct {
	s      *Server
	client pgbackrestdserviceconnect.PgBackRestdClient
}

func newControlHarness(t *testing.T, mutate func(*Config)) *controlHarness {
	t.Helper()
	s := newTestServer(t, mutate)
	ctx := t.Context()
	s.serveCtx.Store(&ctx)
	s.pg.startRetryMin, s.pg.startRetryMax = time.Millisecond, 5*time.Millisecond
	// Background work (version probe, start retry) ends with the test: the
	// context is cancelled just before cleanups run.
	t.Cleanup(s.stopBackground)
	oldDelay := startRetryDelay
	startRetryDelay = time.Millisecond
	t.Cleanup(func() { startRetryDelay = oldDelay })
	mux := http.NewServeMux()
	s.registerControl(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &controlHarness{s: s, client: pgbackrestdserviceconnect.NewPgBackRestdClient(srv.Client(), srv.URL)}
}

func repos(pairs ...uint32) []*pb.RepoGeneration {
	var out []*pb.RepoGeneration
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &pb.RepoGeneration{Generation: pairs[i], RepoNumber: pairs[i+1]})
	}
	return out
}

func (h *controlHarness) configure(t *testing.T, rev int64, r []*pb.RepoGeneration) *pb.ConfigureResponse {
	t.Helper()
	resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: rev, Repos: r}))
	require.NoError(t, err)
	return resp.Msg
}

type runResult struct {
	stdout, stderr string
	exit           int32
	hasExit        bool
	truncated      bool
}

func (h *controlHarness) run(ctx context.Context, cmd pb.Command, args ...string) (runResult, error) {
	stream, err := h.client.Run(ctx, connect.NewRequest(&pb.RunRequest{Command: cmd, Args: args}))
	if err != nil {
		return runResult{}, err
	}
	defer stream.Close()
	var res runResult
	for stream.Receive() {
		m := stream.Msg()
		switch c := m.Chunk.(type) {
		case *pb.RunOutput_Stdout:
			res.stdout += string(c.Stdout)
		case *pb.RunOutput_Stderr:
			res.stderr += string(c.Stderr)
		case *pb.RunOutput_ExitCode:
			res.exit, res.hasExit = c.ExitCode, true
			res.truncated = m.OutputTruncated
		}
	}
	return res, stream.Err()
}

func writeKeyFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "keys.json")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func TestConfigure_RejectsOlderRevision(t *testing.T) {
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 5, repos(1, 1)).Accepted)

	conf := readFile(t, h.s.cfg.stateSub(fileClientConf))
	state := readFile(t, h.s.cfg.stateSub(fileGenerations))

	resp := h.configure(t, 4, repos(1, 1, 2, 2))
	assert.False(t, resp.Accepted)
	assert.Equal(t, int64(5), resp.HeldRevision)
	assert.Equal(t, conf, readFile(t, h.s.cfg.stateSub(fileClientConf)), "a rejected Configure must not re-render")
	assert.Equal(t, state, readFile(t, h.s.cfg.stateSub(fileGenerations)))
	assert.Equal(t, map[uint32]uint32{1: 1}, h.s.snapshot().Repos)

	// Equal is accepted: a retry of the same Configure must succeed.
	resp = h.configure(t, 5, repos(1, 1, 2, 2))
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(5), resp.HeldRevision)
}

func TestConfigure_RejectsInvalid(t *testing.T) {
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	for name, r := range map[string][]*pb.RepoGeneration{
		"empty":              nil,
		"generation zero":    repos(0, 1),
		"repo number zero":   repos(1, 0),
		"repo number 257":    repos(1, 257),
		"duplicate gen":      repos(1, 1, 1, 2),
		"duplicate repo num": repos(1, 1, 2, 1),
	} {
		_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: r}))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
	}
	_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1), WalSegmentSizeBytes: 3 << 20}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// None of the rejected requests moved the revision.
	st, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), st.Msg.Revision)
}

func TestConfigure_PersistsAcrossRestart(t *testing.T) {
	keys := writeKeyFile(t, `{"3":"cipher-three","7":"cipher-seven"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
	resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{
		Revision: 9, Repos: repos(7, 2, 3, 1), WalSegmentSizeBytes: 64 << 20,
	}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Accepted)

	// Repos are stored sorted by repo number whatever order they arrived in.
	assert.JSONEq(t, `{"revision":9,"repos":[{"generation":3,"repo_number":1},{"generation":7,"repo_number":2}],"wal_segment_size_bytes":67108864}`,
		readFile(t, h.s.cfg.stateSub(fileGenerations)))

	s2, err := newForTest(h.s.cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	assert.True(t, s2.configured())
	assert.Equal(t, uint32(64<<20), s2.walSegmentSize())
	rev, got := s2.held()
	assert.Equal(t, int64(9), rev)
	assert.Equal(t, []repoGen{{3, 1}, {7, 2}}, got)
	assert.Equal(t, map[uint32]uint32{1: 3, 2: 7}, s2.snapshot().Repos)
	assert.Equal(t, "cipher-seven", s2.snapshot().CipherPass[7])

	// The floor survived too.
	mux := http.NewServeMux()
	s2.registerControl(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c2 := &controlHarness{s: s2, client: pgbackrestdserviceconnect.NewPgBackRestdClient(srv.Client(), srv.URL)}
	assert.False(t, c2.configure(t, 8, repos(1, 1)).Accepted)
}

// A restart renders the conf from the current flags and the persisted
// repositories: a conf edited or deleted while the sidecar was down is not
// trusted.
func TestConfigure_RestartRerendersConf(t *testing.T) {
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	confPath := h.s.cfg.stateSub(fileClientConf)
	want := readFile(t, confPath)
	require.NoError(t, os.WriteFile(confPath, []byte("[global]\nrepo1-path=/evil\n"), 0o600))

	s2, err := newForTest(h.s.cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	assert.Equal(t, want, readFile(t, confPath))
}

func TestConfigure_ConfHasNoSecrets(t *testing.T) {
	const pass3, pass7, secret = "cipher-three-PASS", "cipher-seven-PASS", "static-SECRET-value"
	keys := writeKeyFile(t, fmt.Sprintf(`{"3":%q,"7":%q}`, pass3, pass7))
	staticDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(staticDir, staticKeyIDFile), []byte("AKIASTATICID\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(staticDir, staticKeySecretFile), []byte(secret+"\n"), 0o600))
	pwFile := filepath.Join(t.TempDir(), "pw")
	require.NoError(t, os.WriteFile(pwFile, []byte("pg-PASSWORD\n"), 0o600))

	for _, src := range []string{CredStatic, CredWebIdentity, CredPodIdentity, CredNone, CredEndpoint} {
		t.Run(src, func(t *testing.T) {
			h := newControlHarness(t, func(c *Config) {
				c.LocationType = LocationS3
				c.FSRepoPath = ""
				c.S3Bucket, c.S3Region, c.S3KeyPrefix = "bkt", "us-east-1", "pfx/"
				c.S3CredSource = src
				c.S3StaticKeyDir = staticDir
				c.S3CredEndpoint = "http://127.0.0.1:1/creds"
				c.CipherKeyFile = keys
				c.PGPasswordFile = pwFile
				c.PGUser = "pgbackrest"
			})
			require.True(t, h.configure(t, 1, repos(3, 1, 7, 2)).Accepted)

			for _, f := range []string{fileClientConf, fileServerConf} {
				body := readFile(t, h.s.cfg.stateSub(f))
				for _, secretValue := range []string{pass3, pass7, secret, "AKIASTATICID", "pg-PASSWORD", h.s.token} {
					assert.NotContains(t, body, secretValue, f)
				}
			}
			conf := readFile(t, h.s.cfg.stateSub(fileClientConf))
			assert.NotContains(t, conf, "cipher-pass")
			assert.NotContains(t, conf, "s3-key-secret")
			assert.Contains(t, conf, "repo1-cipher-type=aes-256-cbc")
			assert.Contains(t, conf, "repo2-cipher-type=aes-256-cbc")
			assert.Contains(t, conf, "repo2-path=/pfx/multigres/gen-7")
			assert.Contains(t, conf, "repo1-path=/pfx/multigres/gen-3")

			wantType := map[string]string{CredStatic: "shared", CredWebIdentity: "process", CredPodIdentity: "process", CredEndpoint: "process", CredNone: "auto"}[src]
			for _, n := range []string{"1", "2"} {
				assert.Contains(t, conf, "repo"+n+"-s3-key-type="+wantType)
				if wantType == "process" {
					assert.Contains(t, conf, "repo"+n+"-s3-process-cmd="+h.s.self+"\nrepo"+n+"-s3-process-cmd=creds")
				} else {
					assert.NotContains(t, conf, "s3-process-cmd")
				}
			}

			// The password lives only in the 0600 pgpass, in pgpass format.
			fi, err := os.Stat(h.s.cfg.stateSub(filePgpass))
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
			assert.Equal(t, "*:*:*:pgbackrest:pg-PASSWORD\n", readFile(t, h.s.cfg.stateSub(filePgpass)))
		})
	}
}

func TestRender_FixedSettings(t *testing.T) {
	h := newControlHarness(t, func(c *Config) { c.ArchiveGetQueueMax = "512MiB" })
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	conf := readFile(t, h.s.cfg.stateSub(fileClientConf))
	for _, want := range []string{
		"log-level-file=off", "log-level-stderr=info", "archive-get-queue-max=512MiB",
		"lock-path=" + h.s.cfg.stateSub(dirLock), "spool-path=" + h.s.cfg.stateSub(dirSpool),
		"log-path=" + h.s.cfg.stateSub(dirLog),
		"repo1-type=posix", "repo1-path=" + h.s.cfg.FSRepoPath,
		"pg1-path=" + h.s.cfg.PGData, "pg1-socket-path=" + h.s.cfg.PGSocketDir,
	} {
		assert.Contains(t, conf, want)
	}
	assert.NotContains(t, conf, "log-level-file=detail")

	cert, key, ca := constants.PgBackRestCertFiles(h.s.cfg.TLSCertDir)
	server := readFile(t, h.s.cfg.stateSub(fileServerConf))
	for _, want := range []string{
		"tls-server-auth=pgbackrest=multigres", "tls-server-cert-file=" + cert,
		"tls-server-key-file=" + key, "tls-server-ca-file=" + ca, "tls-server-port=8432",
	} {
		assert.Contains(t, server, want)
	}
	assert.NotContains(t, server, "pgbackrest=*")
	assert.Contains(t, server, "log-level-file=off")
	assert.NotContains(t, server, "log-path=")
	// Rendered files are not world or group readable.
	for _, f := range []string{fileClientConf, fileServerConf} {
		fi, err := os.Stat(h.s.cfg.stateSub(f))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), f)
	}
}

// A Configure that fails after the snapshot was swapped must leave the
// previous generation list in force everywhere: memory, conf and disk.
func TestConfigure_FailedConfigureChangesNothing(t *testing.T) {
	keys := writeKeyFile(t, `{"1":"one","2":"two"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	confPath := h.s.cfg.stateSub(fileClientConf)
	statePath := h.s.cfg.stateSub(fileGenerations)
	conf, state := readFile(t, confPath), readFile(t, statePath)
	oldSnap := *h.s.snapshot()

	assertUnchanged := func(t *testing.T) {
		t.Helper()
		assert.Equal(t, conf, readFile(t, confPath))
		assert.Equal(t, state, readFile(t, statePath))
		assert.Equal(t, oldSnap, *h.s.snapshot())
		rev, got := h.s.held()
		assert.Equal(t, int64(1), rev)
		assert.Equal(t, []repoGen{{1, 1}}, got)
	}

	t.Run("generation without a key", func(t *testing.T) {
		_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1, 9, 2)}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		assertUnchanged(t)
	})

	t.Run("key file unreadable", func(t *testing.T) {
		require.NoError(t, os.Rename(keys, keys+".gone"))
		defer func() { require.NoError(t, os.Rename(keys+".gone", keys)) }()
		_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1, 2, 2)}))
		require.Error(t, err)
		assertUnchanged(t)
	})

	t.Run("state file cannot be written", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("directory permissions do not bind root")
		}
		dir := h.s.cfg.stateSub(dirState)
		require.NoError(t, os.Chmod(dir, 0o500))
		defer func() { require.NoError(t, os.Chmod(dir, 0o700)) }()
		_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1, 2, 2)}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
		// The conf was rendered for the new list before the write failed, and
		// must have been put back.
		assertUnchanged(t)
	})

	t.Run("pgpass cannot be read", func(t *testing.T) {
		h.s.cfg.PGPasswordFile = filepath.Join(t.TempDir(), "missing")
		defer func() { h.s.cfg.PGPasswordFile = "" }()
		_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1, 2, 2)}))
		require.Error(t, err)
		assertUnchanged(t)
	})

	// And the next, valid Configure goes through: nothing was left locked or
	// half-applied.
	require.True(t, h.configure(t, 2, repos(1, 1, 2, 2)).Accepted)
	_, got := h.s.held()
	assert.Equal(t, []repoGen{{1, 1}, {2, 2}}, got)
}

// Several fields change in one Configure: the generations, their repo
// numbers, and the segment size. Everything that reads them must see the new
// set, and none the old.
func TestConfigure_SeveralFieldsInOneUpdate(t *testing.T) {
	keys := writeKeyFile(t, `{"1":"one","2":"two","3":"three"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
	require.True(t, h.configure(t, 1, repos(1, 1, 2, 2)).Accepted)

	resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{
		Revision: 2, Repos: repos(2, 1, 3, 3, 1, 2), WalSegmentSizeBytes: 1 << 30,
	}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Accepted)

	assert.Equal(t, map[uint32]uint32{1: 2, 2: 1, 3: 3}, h.s.snapshot().Repos)
	assert.Equal(t, uint32(1<<30), h.s.walSegmentSize())
	conf := readFile(t, h.s.cfg.stateSub(fileClientConf))
	assert.Contains(t, conf, "repo1-path="+h.s.cfg.FSRepoPath+"/gen-2")
	assert.Contains(t, conf, "repo2-path="+h.s.cfg.FSRepoPath+"\n", "generation 1 sits on repo 2, so its path has no gen suffix")
	assert.Contains(t, conf, "repo3-path="+h.s.cfg.FSRepoPath+"/gen-3")

	st, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	assert.Equal(t, []*pb.RepoGeneration{{Generation: 2, RepoNumber: 1}, {Generation: 1, RepoNumber: 2}, {Generation: 3, RepoNumber: 3}}, st.Msg.Repos)

	// An omitted segment size keeps the established one rather than
	// resetting to the default.
	resp, err = h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 3, Repos: repos(1, 1)}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Accepted)
	assert.Equal(t, uint32(1<<30), h.s.walSegmentSize())
}

// A to B to A again: the return trip renders exactly what the first A did,
// and a replayed A at the old revision is refused.
func TestConfigure_RevertedMidFlight(t *testing.T) {
	keys := writeKeyFile(t, `{"1":"one","2":"two"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	confA := readFile(t, h.s.cfg.stateSub(fileClientConf))
	snapA := *h.s.snapshot()

	require.True(t, h.configure(t, 2, repos(1, 1, 2, 2)).Accepted)
	assert.NotEqual(t, confA, readFile(t, h.s.cfg.stateSub(fileClientConf)))

	assert.False(t, h.configure(t, 1, repos(1, 1)).Accepted, "the old revision cannot roll the list back")
	assert.Equal(t, map[uint32]uint32{1: 1, 2: 2}, h.s.snapshot().Repos)

	require.True(t, h.configure(t, 3, repos(1, 1)).Accepted)
	assert.Equal(t, confA, readFile(t, h.s.cfg.stateSub(fileClientConf)))
	assert.Equal(t, snapA, *h.s.snapshot())
}

// Configures racing each other and commands starting meanwhile: at the end
// the memory, the persisted state and the conf describe one and the same
// accepted Configure, and the held revision is the highest one accepted.
func TestConfigure_ConcurrentConsistency(t *testing.T) {
	newFakeBackrest(t, nil, false)
	keys := writeKeyFile(t, `{"1":"one","2":"two","3":"three"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	sets := [][]*pb.RepoGeneration{repos(1, 1), repos(1, 1, 2, 2), repos(3, 1, 2, 2), repos(2, 1, 1, 2, 3, 3)}
	var maxAccepted atomic.Int64
	maxAccepted.Store(1)
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Go(func() {
			rev := int64(2 + i)
			resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: rev, Repos: sets[i%len(sets)]}))
			if err != nil || !resp.Msg.Accepted {
				return
			}
			for {
				cur := maxAccepted.Load()
				if rev <= cur || maxAccepted.CompareAndSwap(cur, rev) {
					return
				}
			}
		})
		wg.Go(func() {
			_, _ = h.run(t.Context(), pb.Command_COMMAND_INFO, "--output=json")
			_, _ = h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		})
	}
	wg.Wait()

	rev, repoList := h.s.held()
	assert.Equal(t, maxAccepted.Load(), rev)
	st, ok, err := h.s.readState()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, rev, st.Revision)
	assert.Equal(t, repoList, st.Repos)
	assert.Equal(t, st.repoMap(), h.s.snapshot().Repos)
	want, err := h.s.clientConfBody(st, *h.s.snapshot())
	require.NoError(t, err)
	assert.Equal(t, string(want), readFile(t, h.s.cfg.stateSub(fileClientConf)))
}

func TestStatus_Reports(t *testing.T) {
	newFakeBackrest(t, nil, false)
	keys := writeKeyFile(t, `{"1":"one","2":""}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys; c.ServerPort = 8433 })

	get := func() *pb.StatusResponse {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg
	}
	st := get()
	assert.False(t, st.Configured)
	assert.Equal(t, int64(0), st.Revision)
	assert.Empty(t, st.Repos)
	assert.Equal(t, uint32(8433), st.ServerPort)
	assert.False(t, st.ServerUp)
	assert.Eventually(t, func() bool { return get().PgbackrestVersion == "2.60.0" }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []uint32{1}, st.GenerationsWithKeys, "a generation declared unencrypted has no key")

	require.True(t, h.configure(t, 4, repos(1, 1)).Accepted)
	st = get()
	assert.True(t, st.Configured)
	assert.Equal(t, int64(4), st.Revision)

	// The key file is read on every call, not as of the last Configure.
	require.NoError(t, os.WriteFile(keys, []byte(`{"1":"one","5":"five","2":"two"}`), 0o600))
	assert.Equal(t, []uint32{1, 2, 5}, get().GenerationsWithKeys)

	// Status never fails: an unreadable key file shows as no keys.
	require.NoError(t, os.Remove(keys))
	assert.Empty(t, get().GenerationsWithKeys)
}

func TestStatus_VersionRunsOnce(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	get := func() string {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg.PgbackrestVersion
	}
	require.Eventually(t, func() bool { return get() == "2.60.0" }, 10*time.Second, 10*time.Millisecond)
	for range 5 {
		assert.Equal(t, "2.60.0", get())
	}
	assert.Equal(t, []string{"version"}, fake.lines(t))
}

func TestStatus_FingerprintDeterministic(t *testing.T) {
	newFakeBackrest(t, nil, false)
	fp := func(mutate func(*Config)) string {
		h := newControlHarness(t, mutate)
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg.LocationFingerprint
	}
	s3 := func(c *Config) {
		c.LocationType, c.FSRepoPath = LocationS3, ""
		c.S3Bucket, c.S3Region, c.S3Endpoint, c.S3KeyPrefix = "bkt", "eu-west-1", "minio:9000", "p/"
		c.S3CredSource = CredNone
	}
	a, b := fp(s3), fp(s3)
	assert.Equal(t, a, b)

	want := sha256.Sum256([]byte(`{"location-type":"s3","s3-bucket":"bkt","s3-region":"eu-west-1","s3-endpoint":"minio:9000","s3-key-prefix":"p/","fs-repo-path":""}`))
	assert.Equal(t, hex.EncodeToString(want[:]), a)

	for name, mutate := range map[string]func(*Config){
		"bucket":   func(c *Config) { s3(c); c.S3Bucket = "other" },
		"region":   func(c *Config) { s3(c); c.S3Region = "us-east-1" },
		"endpoint": func(c *Config) { s3(c); c.S3Endpoint = "" },
		"prefix":   func(c *Config) { s3(c); c.S3KeyPrefix = "q/" },
		"fs":       nil,
	} {
		assert.NotEqual(t, a, fp(mutate), name)
	}
	// Things that are not the location do not change it.
	assert.Equal(t, a, fp(func(c *Config) { s3(c); c.ServerPort = 9000; c.ArchiveGetQueueMax = "1GiB" }))
}

func TestRun_RequiresConfigure(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	for _, cmd := range []pb.Command{pb.Command_COMMAND_INFO, pb.Command_COMMAND_RESTORE} {
		_, err := h.run(t.Context(), cmd)
		require.Error(t, err)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	}
	assert.Empty(t, fake.commands(t))
}

func TestRun_RejectedArgsNeverReachTheChild(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	_, err := h.run(t.Context(), pb.Command_COMMAND_BACKUP, "--repo1-path=/elsewhere")
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	_, err = h.run(t.Context(), pb.Command_COMMAND_RESTORE, "--recovery-option=restore_command=x")
	require.Error(t, err)
	assert.Empty(t, fake.commands(t))
}

func TestRun_StreamsOutputAndExitCode(t *testing.T) {
	newFakeBackrest(t, []string{"check"}, false)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	res, err := h.run(t.Context(), pb.Command_COMMAND_CHECK, "--repo=1")
	require.NoError(t, err)
	assert.True(t, res.hasExit)
	assert.Equal(t, int32(7), res.exit)
	assert.Contains(t, res.stderr, "boom check")

	res, err = h.run(t.Context(), pb.Command_COMMAND_INFO)
	require.NoError(t, err)
	assert.True(t, res.hasExit)
	assert.Equal(t, int32(0), res.exit)
}

func TestRun_FixedFlags(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, func(c *Config) { c.RestorePIDFile = "/var/lib/po'oler/restore.pid" })
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	cert, key, ca := constants.PgBackRestCertFiles(h.s.cfg.TLSCertDir)

	_, err := h.run(t.Context(), pb.Command_COMMAND_BACKUP, "--type=full")
	require.NoError(t, err)
	_, err = h.run(t.Context(), pb.Command_COMMAND_BACKUP, "--pg2-host=10.1.1.1", "--pg2-path=/d")
	require.NoError(t, err)
	_, err = h.run(t.Context(), pb.Command_COMMAND_RESTORE, "--set=latest", "--type=standby")
	require.NoError(t, err)

	lines := fake.lines(t)
	require.Len(t, lines, 7, "backup, backup, start, stop, restore, start plus KILL: %v", lines)
	assert.NotContains(t, lines[0], "--pg2-host-type", "no remote, no TLS flags")
	assert.True(t, strings.HasSuffix(lines[1],
		"--pg2-host=10.1.1.1 --pg2-path=/d --pg2-host-type=tls --pg2-host-ca-file="+ca+" --pg2-host-cert-file="+cert+" --pg2-host-key-file="+key), lines[1])

	wantRestoreCmd := `pgctld restore-wrapper '/var/lib/po'\''oler/restore.pid' --rename "%p.fetch" "%p" -- curl -sS --fail-with-body --unix-socket /run/pgbackrest-wal/wal.sock --connect-timeout 5 --max-time 600 -o "%p.fetch" http://pgbackrest/wal/%f`
	assert.Equal(t, wantRestoreCmd, h.s.restoreCommand())
	assert.True(t, strings.HasSuffix(lines[5],
		"--set=latest --type=standby --recovery-option=restore_command="+strings.ReplaceAll(wantRestoreCmd, "'", "''")), lines[5])
	assert.Contains(t, lines[5], "--spool-path="+h.s.cfg.stateSub(restoreSpoolRel))
	assert.Contains(t, lines[3], "stop --force")
	assert.Contains(t, lines[6], " start")
}

// pauseRecorder logs server pause and resume to the same file the fake
// pgbackrest logs to, so the order of everything shows in one place.
func pauseRecorder(t *testing.T, fake *fakeBackrest) func() func() {
	t.Helper()
	var mu sync.Mutex
	note := func(what string) {
		mu.Lock()
		defer mu.Unlock()
		f, err := os.OpenFile(fake.log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		require.NoError(t, err)
		defer f.Close()
		_, err = f.WriteString(what + "\n")
		require.NoError(t, err)
	}
	return func() func() {
		note("PAUSE")
		return func() { note("RESUME") }
	}
}

func shortened(lines []string) []string {
	var out []string
	for _, l := range lines {
		fields := strings.Fields(l)
		w := fields[0]
		for _, f := range fields {
			if !strings.HasPrefix(f, "--") {
				w = f
				break
			}
		}
		if w != "version" && w != "KILL" && w != "NOKILL" {
			out = append(out, w)
		}
	}
	return out
}

// The first Configure writes the client conf, so it pauses the 8432 server
// around the write: every test that configures first sees one pair, then the
// restore's own pause.
var configurePause = []string{"PAUSE", "RESUME", "PAUSE"}

func TestRestore_PausesAndResumesServer(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.Equal(t, int32(0), res.exit)
	assert.Equal(t, append(configurePause, "start", "stop", "restore", "start", "RESUME"), shortened(fake.lines(t)))

	release, ok := h.s.gate.TryEnter()
	require.True(t, ok, "the gate reopens after a restore")
	release()
}

func TestRestore_StartRunsAfterFailedRestore(t *testing.T) {
	fake := newFakeBackrest(t, []string{"restore"}, false)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.Equal(t, int32(7), res.exit, "the caller sees the restore's failure")
	assert.Equal(t, append(configurePause, "start", "stop", "restore", "start", "RESUME"), shortened(fake.lines(t)))

	release, ok := h.s.gate.TryEnter()
	require.True(t, ok)
	release()
}

// Without the stop barrier restore does not run, but the unwinding still
// does: start, resume, reopen.
func TestRestore_FailedStopSkipsRestoreButUnwinds(t *testing.T) {
	fake := newFakeBackrest(t, []string{"stop"}, false)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.Equal(t, int32(7), res.exit)
	assert.Equal(t, append(configurePause, "start", "stop", "start", "RESUME"), shortened(fake.lines(t)))
	release, ok := h.s.gate.TryEnter()
	require.True(t, ok)
	release()
}

// A start that keeps failing after the restore is handed to the background
// retry, which clears the stop file once start works again; the restore's own
// result stands.
func TestRestore_FailedStartHandsOffToRetry(t *testing.T) {
	// Call 1 is the start before stop, 2 to 4 the in-place attempts, 5 the
	// background retry.
	fake := newFakeBackrestOpts(t, fakeOpts{failStartCalls: []int{2, 3, 4}})
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.Equal(t, int32(0), res.exit, "a successful restore is not reported failed because start was slow")
	release, ok := h.s.gate.TryEnter()
	require.True(t, ok, "the gate reopens; the retry does not hold it")
	release()

	require.Eventually(t, func() bool {
		return strings.Join(shortened(fake.lines(t)), ",") == "start,stop,restore,start,start,start,start"
	}, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { _, err := os.Stat(fake.stop); return os.IsNotExist(err) }, 10*time.Second, 5*time.Millisecond,
		"the retry removed the stop file")
}

func TestRestore_StartPendingShowsInStatus(t *testing.T) {
	var calls []int
	for i := 2; i <= 40; i++ {
		calls = append(calls, i)
	}
	fake := newFakeBackrestOpts(t, fakeOpts{failStartCalls: calls})
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	pending := func() bool {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg.RestoreStartPending
	}
	assert.False(t, pending())
	_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.True(t, pending(), "the retry is still outstanding")
	require.Eventually(t, func() bool { return !pending() }, 20*time.Second, 10*time.Millisecond)
	_, statErr := os.Stat(fake.stop)
	assert.True(t, os.IsNotExist(statErr), "and it cleared the stop file")
}

// "pgbackrest stop --force" on an existing stop file signals nobody, so the
// restore runs start first.
func TestRestore_StaleStopFileDoesNotDisarmStopForce(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{})
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	require.NoError(t, os.WriteFile(fake.stop, nil, 0o600))

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.Equal(t, int32(0), res.exit)
	lines := fake.lines(t)
	assert.Contains(t, lines, "KILL", "stop --force took effect")
	assert.NotContains(t, lines, "NOKILL")
	assert.Equal(t, []string{"start", "stop", "restore", "start"}, shortened(lines))
}

// A restore that hangs is cut off after restoreTimeout; the unwinding still
// runs and the gate reopens.
func TestRestore_ChildIsBoundedByTimeout(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	old := restoreTimeout
	restoreTimeout = 300 * time.Millisecond
	t.Cleanup(func() { restoreTimeout = old })

	_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	assert.Equal(t, append(configurePause, "start", "stop", "restore", "start", "RESUME"), shortened(fake.lines(t)))
	release, ok := h.s.gate.TryEnter()
	require.True(t, ok)
	release()
}

// A wedged stop or start must not hold the gate, the paused server or
// restore_in_progress forever: each is bounded, a timed-out stop skips the
// restore, and the unwinding still runs.
func TestRestore_HungStopAndStartAreBounded(t *testing.T) {
	old := restoreStepTimeout
	restoreStepTimeout = 200 * time.Millisecond
	t.Cleanup(func() { restoreStepTimeout = old })

	for _, hang := range [][]string{{"stop"}, {"start"}, {"stop", "start"}} {
		t.Run(strings.Join(hang, "+"), func(t *testing.T) {
			fake := newFakeBackrestOpts(t, fakeOpts{hang: hang})
			h := newControlHarness(t, nil)
			h.s.pauseFn = pauseRecorder(t, fake)
			require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

			done := make(chan error, 1)
			go func() { _, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE); done <- err }()
			select {
			case err := <-done:
				if slices.Contains(hang, "stop") {
					require.Error(t, err)
					assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
				}
			case <-time.After(15 * time.Second):
				t.Fatal("the restore sequence did not finish within the step bounds")
			}

			cmds := shortened(fake.lines(t))
			if slices.Contains(hang, "stop") {
				assert.NotContains(t, cmds, "restore", "a hung stop skips the restore")
			} else {
				assert.Contains(t, cmds, "restore")
			}
			assert.Equal(t, "RESUME", cmds[len(cmds)-1], "the server resumes")
			resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
			require.NoError(t, err)
			assert.False(t, resp.Msg.RestoreInProgress, "restore_in_progress clears")
			release, ok := h.s.gate.TryEnter()
			require.True(t, ok, "the gate reopens")
			release()
		})
	}
}

// Configure waits out of a restore's way: refused while the gate is closed,
// and a restore waits for a Configure already running.
func TestConfigure_UnavailableDuringRestore(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	conf := readFile(t, h.s.cfg.stateSub(fileClientConf))

	done := make(chan error, 1)
	go func() {
		_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
		done <- err
	}()
	fake.waitRestoreStarted(t)

	_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1, 2, 2)}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	assert.Equal(t, conf, readFile(t, h.s.cfg.stateSub(fileClientConf)))

	fake.releaseRestore(t)
	require.NoError(t, <-done)
	assert.True(t, h.configure(t, 2, repos(1, 1, 2, 2)).Accepted)
}

// A failed first Configure must not leave its conf behind for start and stop
// to read: the minimal conf is back.
func TestConfigure_FailedFirstConfigureLeavesNoNewConf(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not bind root")
	}
	h := newControlHarness(t, nil)
	before := readFile(t, h.s.cfg.stateSub(fileClientConf))
	dir := h.s.cfg.stateSub(dirState)
	require.NoError(t, os.Chmod(dir, 0o500))
	defer func() { require.NoError(t, os.Chmod(dir, 0o700)) }()

	_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)}))
	require.Error(t, err)
	assert.Equal(t, before, readFile(t, h.s.cfg.stateSub(fileClientConf)))
	assert.False(t, h.s.configured())
}

func TestRender_NeverDisablesTLSVerification(t *testing.T) {
	ca := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(ca, []byte("pem"), 0o600))
	for name, caFile := range map[string]string{"with CA": ca, "without CA": ""} {
		t.Run(name, func(t *testing.T) {
			h := newControlHarness(t, func(c *Config) {
				c.LocationType, c.FSRepoPath = LocationS3, ""
				c.S3Bucket, c.S3Region, c.S3Endpoint = "b", "r", "127.0.0.1:9000"
				c.S3CredSource = CredNone
				c.S3CredCAFile = caFile
			})
			require.True(t, h.configure(t, 1, repos(1, 1, 2, 2)).Accepted)
			conf := readFile(t, h.s.cfg.stateSub(fileClientConf))
			assert.NotContains(t, conf, "verify-tls")
			assert.Contains(t, conf, "repo1-s3-endpoint=127.0.0.1:9000")
			if caFile != "" {
				assert.Contains(t, conf, "repo1-storage-ca-file="+caFile)
				assert.Contains(t, conf, "repo2-storage-ca-file="+caFile)
			} else {
				assert.NotContains(t, conf, "storage-ca-file")
			}
		})
	}
}

// Status, hence health, never waits for the version probe, and a failing
// probe is not re-run on every call.
func TestStatus_NeverWaitsForVersion(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{versionBody: "sleep 3"})
	h := newControlHarness(t, nil)
	start := time.Now()
	for range 3 {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		assert.Empty(t, resp.Msg.PgbackrestVersion)
	}
	assert.Less(t, time.Since(start), 2*time.Second)
	require.Eventually(t, func() bool { return len(fake.lines(t)) == 1 }, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"version"}, fake.lines(t), "one probe, however many Status calls")
}

func TestStatus_FailedVersionIsCachedForATTL(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{versionBody: "exit 3"})
	h := newControlHarness(t, nil)
	old := versionRetryTTL
	versionRetryTTL = 300 * time.Millisecond
	t.Cleanup(func() { versionRetryTTL = old })

	status := func() {
		_, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
	}
	probes := func() int { return len(fake.lines(t)) }
	status()
	require.Eventually(t, func() bool { return probes() == 1 }, 5*time.Second, 5*time.Millisecond)
	for range 5 {
		status()
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, probes(), "no re-run inside the TTL")
	time.Sleep(350 * time.Millisecond)
	status()
	require.Eventually(t, func() bool { return probes() == 2 }, 5*time.Second, 5*time.Millisecond, "re-run after the TTL")
}

type blockedSender struct{ release chan struct{} }

func (b *blockedSender) Send(*pb.RunOutput) error { <-b.release; return nil }

// A client that stays connected but stops reading must not block the child's
// writes.
func TestStreamSender_DropsInsteadOfBlocking(t *testing.T) {
	b := &blockedSender{release: make(chan struct{})}
	out := newStreamSender(b)
	w := out.writer(false)
	done := make(chan struct{})
	go func() {
		for range 4 * sendQueueLen {
			n, err := w.Write([]byte("chunk"))
			assert.NoError(t, err)
			assert.Equal(t, 5, n)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a child write blocked on a client that stopped reading")
	}
	close(b.release)
	require.NoError(t, out.finish(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{}}))
}

func TestStreamSender_FinishWithoutFinalIsBounded(t *testing.T) {
	b := &blockedSender{release: make(chan struct{})}
	t.Cleanup(func() { close(b.release) })
	old := sendDrainTimeout
	sendDrainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { sendDrainTimeout = old })
	out := newStreamSender(b)
	_, err := out.writer(false).Write([]byte("x"))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- out.finish(nil) }()
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("finish(nil) waited on a pump stuck in Send")
	}
}

func TestRun_UnavailableDuringRestore(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	done := make(chan error, 1)
	go func() {
		_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
		done <- err
	}()
	fake.waitRestoreStarted(t)

	_, err := h.run(t.Context(), pb.Command_COMMAND_INFO)
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	_, ok := h.s.gate.TryEnter()
	assert.False(t, ok, "WAL requests are held off too")

	// A second restore does not queue behind the first, and runs nothing.
	_, err = h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	assert.Equal(t, []string{"start", "stop", "restore"}, fake.commands(t))

	fake.releaseRestore(t)
	require.NoError(t, <-done)
	res, err := h.run(t.Context(), pb.Command_COMMAND_INFO)
	require.NoError(t, err)
	assert.Equal(t, int32(0), res.exit)
}

// The client going away mid-restore must not leave the stop file behind or
// the gate shut.
func TestRestore_ClientDisconnectStillStarts(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.run(ctx, pb.Command_COMMAND_RESTORE)
		done <- err
	}()
	fake.waitRestoreStarted(t)
	cancel()
	require.Error(t, <-done)

	// Still running: the restore child was not killed with the request.
	assert.Equal(t, append(configurePause, "start", "stop", "restore"), shortened(fake.lines(t)))
	_, ok := h.s.gate.TryEnter()
	assert.False(t, ok)

	fake.releaseRestore(t)
	require.Eventually(t, func() bool {
		return strings.Join(shortened(fake.lines(t)), ",") == strings.Join(append(configurePause, "start", "stop", "restore", "start", "RESUME"), ",")
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		release, ok := h.s.gate.TryEnter()
		if ok {
			release()
		}
		return ok
	}, 10*time.Second, 10*time.Millisecond)
}

func TestRestore_DrainTimeoutLeavesEverythingRunning(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	old := restoreDrainWait
	restoreDrainWait = 100 * time.Millisecond
	t.Cleanup(func() { restoreDrainWait = old })

	stuck, ok := h.s.gate.TryEnter()
	require.True(t, ok)
	_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	assert.Equal(t, []string{"PAUSE", "RESUME"}, fake.commands(t), "nothing was stopped, and no pause beyond Configure's own")
	assert.Equal(t, []string{"PAUSE", "RESUME"}, shortened(fake.lines(t)), "only Configure's own pause, none for the refused restore")

	// The stuck request did not leave the gate shut.
	release, ok := h.s.gate.TryEnter()
	require.True(t, ok)
	release()
	stuck()
}

func TestRestore_ClearsPrefetchQueueOnly(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	archive := filepath.Join(h.s.cfg.stateSub(dirSpool), "archive", Stanza)
	require.NoError(t, os.MkdirAll(filepath.Join(archive, "in"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(archive, "out"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(archive, "in", "000000010000000000000001"), []byte("w"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(archive, "out", "000000010000000000000002.ok"), nil, 0o600))

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.NoError(t, err)
	assert.Equal(t, int32(0), res.exit)
	assert.Equal(t, []string{"start", "stop", "restore", "start"}, shortened(fake.lines(t)))
	assert.NoDirExists(t, filepath.Join(archive, "in"))
	assert.FileExists(t, filepath.Join(archive, "out", "000000010000000000000002.ok"))
}

// A child can plant links in spool/. PID 1 must not follow one while it
// clears the queue, and a failed clear must not restore into a stale queue.
func TestRestore_PlantedSpoolLinkIsNotFollowed(t *testing.T) {
	fake := newFakeBackrest(t, nil, false)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)

	victim := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(victim, Stanza, "in"), 0o700))
	precious := filepath.Join(victim, Stanza, "in", "precious")
	require.NoError(t, os.WriteFile(precious, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(victim, filepath.Join(h.s.cfg.stateSub(dirSpool), "archive")))

	res, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	_ = res
	assert.FileExists(t, precious)
	assert.Equal(t, []string{"start", "stop", "start"}, shortened(fake.lines(t)), "no restore into an unclearable queue, and start still ran")
	release, ok := h.s.gate.TryEnter()
	require.True(t, ok)
	release()
}

// health calls Status, so a missing pgbackrest, an unreadable key file and an
// unconfigured sidecar must all come back as fields, never as an error.
func TestStatus_NeverFails(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	keys := writeKeyFile(t, `{"1":"one"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
	require.NoError(t, os.Remove(keys))
	resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	assert.False(t, resp.Msg.Configured)
	assert.Empty(t, resp.Msg.PgbackrestVersion)
	assert.Empty(t, resp.Msg.GenerationsWithKeys)
	assert.NotEmpty(t, resp.Msg.LocationFingerprint)
}

func TestStatus_GenerationFingerprints(t *testing.T) {
	newFakeBackrest(t, nil, false)
	keys := writeKeyFile(t, `{"1":"one","2":"","3":"three"}`)
	h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })

	resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	assert.Equal(t, map[uint32]string{
		1: backup.CipherKeyFingerprint("one"),
		3: backup.CipherKeyFingerprint("three"),
	}, resp.Msg.GenerationFingerprints, "the same function pgbackrest_repos.key_fingerprint uses; a generation with no key has none")
	for _, fp := range resp.Msg.GenerationFingerprints {
		assert.NotContains(t, fp, "one")
		assert.NotContains(t, fp, "three")
	}
}

func TestStatus_RestoreInProgressSpansTheWholeSequence(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	status := func() *pb.StatusResponse {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg
	}
	assert.False(t, status().RestoreInProgress)
	assert.False(t, status().LastRestoreSucceeded)

	// The Run call is abandoned while the child keeps running; the flag is
	// what tells the caller not to touch the data directory.
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.run(ctx, pb.Command_COMMAND_RESTORE)
		done <- err
	}()
	fake.waitRestoreStarted(t)
	cancel()
	require.Error(t, <-done)
	st := status()
	assert.True(t, st.RestoreInProgress, "the restore outlives the call that began it")
	assert.False(t, st.LastRestoreSucceeded)

	// A second restore is refused at the gate and must not clear the mark.
	_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	assert.True(t, status().RestoreInProgress, "a refused restore must not clear the first one's mark")

	fake.releaseRestore(t)
	require.Eventually(t, func() bool { return !status().RestoreInProgress }, 10*time.Second, 10*time.Millisecond)
	assert.True(t, status().LastRestoreSucceeded)
}

func TestStatus_FailedRestoreIsNotSucceeded(t *testing.T) {
	fake := newFakeBackrest(t, nil, true)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	old := restoreTimeout
	restoreTimeout = 200 * time.Millisecond
	t.Cleanup(func() { restoreTimeout = old })

	_, err := h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	assert.False(t, resp.Msg.RestoreInProgress)
	assert.False(t, resp.Msg.LastRestoreSucceeded, "a restore killed by its timeout is not a success")
}

func TestStreamSender_RestoreDropsAndReportsTruncation(t *testing.T) {
	b := &blockedSender{release: make(chan struct{})}
	out := newStreamSender(b)
	w := out.writer(false)
	for range 4 * sendQueueLen {
		_, _ = w.Write([]byte("chunk"))
	}
	assert.True(t, out.truncated.Load())
	close(b.release)
	require.NoError(t, out.finish(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{}}))
}

func TestStreamSender_OtherCommandsBlockInsteadOfDropping(t *testing.T) {
	b := &blockedSender{release: make(chan struct{})}
	out := newStreamSender(b)
	ctx, cancel := context.WithCancel(t.Context())
	out.blockUntil = ctx
	w := out.writer(false)

	wrote := make(chan struct{})
	go func() {
		for range 3 * sendQueueLen {
			_, _ = w.Write([]byte("chunk"))
		}
		close(wrote)
	}()
	select {
	case <-wrote:
		t.Fatal("a write was dropped instead of waiting for a reader that fell behind")
	case <-time.After(300 * time.Millisecond):
	}
	assert.False(t, out.truncated.Load())

	// The reader catches up: everything gets through, nothing is lost.
	close(b.release)
	select {
	case <-wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("writer stayed blocked after the reader resumed")
	}
	assert.False(t, out.truncated.Load())
	require.NoError(t, out.finish(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{}}))

	// A request that ends while the reader is stalled releases the writer and
	// says so.
	b2 := &blockedSender{release: make(chan struct{})}
	out2 := newStreamSender(b2)
	ctx2, cancel2 := context.WithCancel(t.Context())
	out2.blockUntil = ctx2
	w2 := out2.writer(false)
	done := make(chan struct{})
	go func() {
		for range 3 * sendQueueLen {
			_, _ = w2.Write([]byte("chunk"))
		}
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel2()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceling the request did not release a blocked writer")
	}
	assert.True(t, out2.truncated.Load())
	close(b2.release)
	cancel()
}

func TestRun_FinalMessageCarriesTruncation(t *testing.T) {
	newFakeBackrest(t, []string{"check"}, false)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	res, err := h.run(t.Context(), pb.Command_COMMAND_CHECK)
	require.NoError(t, err)
	assert.False(t, res.truncated, "a normal run is not truncated")
}

// pauseCounter counts the 8432 server pauses a Configure takes.
func pauseCounter(h *controlHarness) *atomic.Int64 {
	var n atomic.Int64
	h.s.pauseFn = func() func() {
		n.Add(1)
		return func() {}
	}
	return &n
}

// multipooler configures on every start, and a Configure that changes nothing
// must not restart the 8432 server and kill a standby's backup through it.
func TestConfigure_IdenticalConfigureLeavesTheServerAlone(t *testing.T) {
	pw := filepath.Join(t.TempDir(), "pw")
	require.NoError(t, os.WriteFile(pw, []byte("secret\n"), 0o600))
	h := newControlHarness(t, func(c *Config) { c.PGPasswordFile = pw; c.PGUser = "pgbackrest" })
	pauses := pauseCounter(h)

	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	require.Equal(t, int64(1), pauses.Load(), "the first Configure writes pgpass")

	pgpassInode := func() uint64 { return inodeOf(t, h.s.cfg.stateSub(filePgpass)) }
	confInode := func() uint64 { return inodeOf(t, h.s.cfg.stateSub(fileClientConf)) }
	serverInode := func() uint64 { return inodeOf(t, h.s.cfg.stateSub(fileServerConf)) }
	p0, c0, s0 := pgpassInode(), confInode(), serverInode()

	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	assert.Equal(t, int64(1), pauses.Load(), "an identical Configure does not pause the server")
	assert.Equal(t, []uint64{p0, c0, s0}, []uint64{pgpassInode(), confInode(), serverInode()}, "and rewrites no file")

	require.True(t, h.configure(t, 2, repos(1, 1)).Accepted)
	assert.Equal(t, int64(1), pauses.Load(), "a newer revision with the same content does not pause it either")
	assert.Equal(t, []uint64{p0, c0, s0}, []uint64{pgpassInode(), confInode(), serverInode()})
	st, ok, err := h.s.readState()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(2), st.Revision, "but the state file records it")

	require.True(t, h.configure(t, 3, repos(1, 1, 2, 2)).Accepted)
	assert.Equal(t, int64(2), pauses.Load(), "a changed repository list pauses it")

	n := pauses.Load()
	resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 4, Repos: repos(1, 1, 2, 2), WalSegmentSizeBytes: 1 << 30}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Accepted)
	assert.Equal(t, n+1, pauses.Load(), "a segment size that changes the rendered queue pauses it")

	require.NoError(t, os.WriteFile(pw, []byte("rotated\n"), 0o600))
	require.True(t, h.configure(t, 5, repos(1, 1, 2, 2)).Accepted)
	assert.Equal(t, n+2, pauses.Load(), "a rotated postgres password pauses it")
	assert.Equal(t, "*:*:*:pgbackrest:rotated\n", readFile(t, h.s.cfg.stateSub(filePgpass)))
}

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Sys().(*syscall.Stat_t).Ino
}

// A command whose output the caller parses must arrive whole even when the
// caller reads slowly and the send queue fills: Run makes every command but
// restore wait for queue space (blockUntil) instead of dropping.
func TestRun_NonRestoreOutputSurvivesASlowReader(t *testing.T) {
	const size = 48 << 20
	newFakeBackrestOpts(t, fakeOpts{infoBytes: size})
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	old := sendQueueLen
	sendQueueLen = 1
	t.Cleanup(func() { sendQueueLen = old })

	stream, err := h.client.Run(t.Context(), connect.NewRequest(&pb.RunRequest{Command: pb.Command_COMMAND_INFO}))
	require.NoError(t, err)
	defer stream.Close()
	// Let the child fill every buffer between it and this reader.
	time.Sleep(500 * time.Millisecond)

	var got int
	var final *pb.RunOutput
	for stream.Receive() {
		m := stream.Msg()
		switch c := m.Chunk.(type) {
		case *pb.RunOutput_Stdout:
			got += len(c.Stdout)
		case *pb.RunOutput_ExitCode:
			final = m
		}
	}
	require.NoError(t, stream.Err())
	require.NotNil(t, final)
	assert.False(t, final.OutputTruncated, "back-pressure, not loss")
	assert.Equal(t, size, got, "every byte arrived")
}
