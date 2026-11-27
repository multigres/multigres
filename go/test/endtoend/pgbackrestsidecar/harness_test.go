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

package pgbackrestsidecar

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
	"github.com/multigres/multigres/go/test/utils"
)

const bucket = "multigres"

var clusterSlots = make(chan struct{}, 4)

// requireSidecarHost skips what cannot run in this invocation, and says why,
// then applies the sidecar's host requirement (see
// clustersetup.RequireSidecarHost).
func requireSidecarHost(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}
	clustersetup.RequireSidecarHost(t)
}

// requireLinuxSandbox skips a test whose point is confinement or /proc.
func requireLinuxSandbox(t *testing.T) {
	t.Helper()
	clustersetup.RequireEnforcedSandbox(t)
}

func pemOf(srv *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

// sidecarCluster is a shard whose repository is an s3mock that
// only accepts session credentials, behind an s3Front.
type sidecarCluster struct {
	setup *shardsetup.ShardSetup
	s3    *s3mock.Server
	front *s3Front
}

// newSidecarCluster boots poolers poolers. The caller owns nothing: cleanup is
// registered on t.
func newSidecarCluster(t *testing.T, poolers int, opts ...shardsetup.SetupOption) *sidecarCluster {
	t.Helper()
	requireSidecarHost(t)
	// Every test runs in parallel so the credential-expiry tests, which wait
	// out real wall-clock time, overlap with the rest. The slots keep the number
	// of live clusters (each has etcd, postgres and a sidecar per pooler) sane.
	t.Parallel()
	clusterSlots <- struct{}{}
	t.Cleanup(func() { <-clusterSlots })

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s3Server.Stop() })
	require.NoError(t, s3Server.CreateBucket(bucket))
	// Only a request signed by an issued, unexpired session passes, and the
	// sidecar mints those itself: no AWS_* variable is set for this process.
	s3Server.RequireSessions()
	front := newS3Front(t, s3Server)

	all := append([]shardsetup.SetupOption{
		shardsetup.WithMultipoolerCount(poolers),
		shardsetup.WithS3Backup(bucket, "us-east-1", front.URL()),
		shardsetup.WithS3CAPEM(front.CertPEM()),
	}, opts...)
	setup, cleanup := shardsetup.NewIsolated(t, all...)
	t.Cleanup(cleanup)
	return &sidecarCluster{setup: setup, s3: s3Server, front: front}
}

func openSocketDB(t *testing.T, inst *shardsetup.MultipoolerInstance) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable password=%s",
		filepath.Join(inst.Pgctld.PoolerDir, "pg_sockets"), inst.Pgctld.PgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil
	}, 60*time.Second, 200*time.Millisecond, "postgres on %s did not accept connections", inst.Name)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func unixClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives: true,
	}}
}

// controlClient talks to a pooler's sidecar over its control socket, the way
// multipooler does.
func controlClient(inst *shardsetup.MultipoolerInstance) pgbackrestdserviceconnect.PgBackRestdClient {
	return pgbackrestdserviceconnect.NewPgBackRestdClient(unixClient(inst.PgBackRestd.Paths.ControlSocket), "http://pgbackrestd")
}

func sidecarStatus(t *testing.T, inst *shardsetup.MultipoolerInstance) *pgbackrestdservice.StatusResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := controlClient(inst).Status(ctx, connect.NewRequest(&pgbackrestdservice.StatusRequest{}))
	require.NoError(t, err)
	return resp.Msg
}

// walRequest sends one request to a pooler's WAL socket and returns the status
// and body.
func walRequest(t *testing.T, inst *shardsetup.MultipoolerInstance, method, name string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "http://pgbackrest/wal/"+name, nil)
	require.NoError(t, err)
	resp, err := unixClient(inst.PgBackRestd.Paths.WALSocket).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

var segmentName = regexp.MustCompile(`^[0-9A-F]{24}$`)

// repoSegments returns the names of the WAL segments in the repository.
func repoSegments(s *s3mock.Server) map[string]bool {
	out := map[string]bool{}
	for _, key := range s.ListKeys(bucket, "") {
		if !strings.Contains(key, "/archive/") {
			continue
		}
		base := key[strings.LastIndex(key, "/")+1:]
		if len(base) >= 24 && segmentName.MatchString(base[:24]) {
			out[base[:24]] = true
		}
	}
	return out
}

// writeAndSwitch commits a row, then closes the segment holding it so it can be
// archived, and returns its name.
func writeAndSwitch(t *testing.T, db *sql.DB, note string) string {
	t.Helper()
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS sidecar_e2e (id serial PRIMARY KEY, note text NOT NULL)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO sidecar_e2e (note) VALUES ($1)", note)
	require.NoError(t, err)
	var seg string
	require.NoError(t, db.QueryRow("SELECT pg_walfile_name(pg_switch_wal())").Scan(&seg))
	return seg
}

// waitArchived waits for seg to be in the repository and no longer queued.
func waitArchived(t *testing.T, c *sidecarCluster, inst *shardsetup.MultipoolerInstance, seg string, within time.Duration) {
	t.Helper()
	// Postgres removes a segment's archive_status files when it recycles the
	// segment, so a finished one may have neither: archived means in the
	// repository with no .ready left.
	ready := filepath.Join(inst.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status", seg+".ready")
	require.Eventually(t, func() bool {
		if _, err := os.Stat(ready); err == nil {
			return false
		}
		return repoSegments(c.s3)[seg]
	}, within, time.Second, "segment %s should be in the repository and no longer queued", seg)
}

// sidecarPID is the pid of a pooler's pgbackrestd.
func sidecarPID(inst *shardsetup.MultipoolerInstance) int {
	return inst.PgBackRestd.Process.Process.Pid
}

// pgbackrestProc is one pgBackRest process as the watcher saw it.
type pgbackrestProc struct {
	pid int
	// start is the process's start time as the kernel reports it
	// (/proc/<pid>/stat field 22). With the pid it names one process: a later
	// process that reuses the pid has another start time.
	start string
	// role is the pgBackRest command and role, such as "archive-push:async".
	role string
	// first and last are the wall-clock times of the first and last samples
	// that saw the process.
	first, last time.Time
}

type pgbackrestProcs []*pgbackrestProc

func (ps pgbackrestProcs) String() string {
	var b strings.Builder
	for _, p := range ps {
		fmt.Fprintf(&b, "[%s pid %d seen %s to %s (%s)] ", p.role, p.pid,
			p.first.Format(time.TimeOnly), p.last.Format(time.TimeOnly), p.last.Sub(p.first).Round(time.Second))
	}
	return b.String()
}

// longest returns the process of this role that was seen for the longest time.
func (ps pgbackrestProcs) longest(role string) *pgbackrestProc {
	var best *pgbackrestProc
	for _, p := range ps {
		if p.role == role && (best == nil || p.last.Sub(p.first) > best.last.Sub(best.first)) {
			best = p
		}
	}
	return best
}

// during returns the processes of this role seen at any time from from to to.
func (ps pgbackrestProcs) during(role string, from, to time.Time) pgbackrestProcs {
	var out pgbackrestProcs
	for _, p := range ps {
		if p.role == role && !p.last.Before(from) && !p.first.After(to) {
			out = append(out, p)
		}
	}
	return out
}

type archivePushWatch struct {
	done  chan struct{}
	procs chan pgbackrestProcs
}

// stop ends the watch and returns every process it saw.
func (w *archivePushWatch) stop() pgbackrestProcs {
	close(w.done)
	return <-w.procs
}

// watchArchivePushWorkers samples /proc every two seconds for the pgBackRest
// archive-push processes of the sidecar whose state directory is stateDir,
// which is in their --config argument. A process doing a 40s PUT cannot fall
// between two samples. Linux only.
func watchArchivePushWorkers(t *testing.T, stateDir string) *archivePushWatch {
	t.Helper()
	w := &archivePushWatch{done: make(chan struct{}), procs: make(chan pgbackrestProcs, 1)}
	sample := func(seen map[string]*pgbackrestProc, order *pgbackrestProcs) {
		now := wallNow()
		entries, _ := os.ReadDir("/proc")
		for _, e := range entries {
			pid, err := strconv.Atoi(e.Name())
			if err != nil {
				continue
			}
			raw, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
			if err != nil || !strings.Contains(string(raw), stateDir) {
				continue
			}
			role := ""
			for arg := range strings.SplitSeq(string(raw), "\x00") {
				if strings.HasPrefix(arg, "archive-push") {
					role = arg
				}
			}
			if role == "" {
				continue
			}
			stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
			if err != nil {
				continue
			}
			// The fields after the command name, which is in parentheses and
			// may itself hold spaces; start time is field 22 of the line.
			fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
			if len(fields) < 20 {
				continue
			}
			key := e.Name() + "/" + fields[19]
			p := seen[key]
			if p == nil {
				p = &pgbackrestProc{pid: pid, start: fields[19], role: role, first: now}
				seen[key] = p
				*order = append(*order, p)
			}
			p.last = now
		}
	}
	go func() {
		seen := map[string]*pgbackrestProc{}
		var order pgbackrestProcs
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			sample(seen, &order)
			select {
			case <-w.done:
				w.procs <- order
				return
			case <-tick.C:
			}
		}
	}()
	t.Cleanup(func() {
		select {
		case <-w.done:
		default:
			close(w.done)
		}
	})
	return w
}

// sidecarLogs marks the current end of every pooler's sidecar log, so a test
// can read what is logged from here on.
type sidecarLogs map[string]int64

func sidecarLogMarks(c *sidecarCluster) sidecarLogs {
	marks := sidecarLogs{}
	for _, inst := range c.setup.Multipoolers {
		size := int64(0)
		if info, err := os.Stat(inst.PgBackRestd.LogFile); err == nil {
			size = info.Size()
		}
		marks[inst.PgBackRestd.LogFile] = size
	}
	return marks
}

// added returns what every sidecar logged since the marks were taken.
func (m sidecarLogs) added(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for path, from := range m {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		if int64(len(raw)) > from {
			b.Write(raw[from:])
		}
	}
	return b.String()
}
