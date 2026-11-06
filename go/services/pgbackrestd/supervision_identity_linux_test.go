//go:build linux

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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

func TestWorkerRuntimeDomainClassification(t *testing.T) {
	current := workerOwner{Platform: "linux", PID: 1, Token: uuid.NewString(), BootID: uuid.NewString(), StartTicks: 200, NamespaceDevice: 4, NamespaceInode: 100, Hostname: "private-container-slot"}
	for _, tc := range []struct {
		name   string
		change func(*workerOwner)
		want   bool
	}{
		{"same namespace", func(o *workerOwner) { o.Token = uuid.NewString() }, false},
		{"recycled namespace inode", func(o *workerOwner) { o.StartTicks = 100 }, true},
		{"prior dedicated container", func(o *workerOwner) { o.NamespaceInode = 99 }, true},
		{"local owner in another namespace", func(o *workerOwner) { o.PID = 42; o.NamespaceInode = 99 }, false},
		{"different runtime hostname", func(o *workerOwner) { o.NamespaceInode = 99; o.Hostname = "other-slot" }, false},
		{"unscoped namespace", func(o *workerOwner) { o.NamespaceInode = 0 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := current
			tc.change(&old)
			require.Equal(t, tc.want, priorRuntimeDomainEnded(old, current))
		})
	}
	local := current
	local.PID = 42
	old := current
	old.NamespaceInode = 99
	require.False(t, priorRuntimeDomainEnded(old, local))
}

// TestContainerNamespaceRestartRecovery is driven by the disposable Docker
// restart check. It MUST run as PID 1 in the same container across starts with
// its private state volume retained. First start holds a real restore until the
// host runtime SIGKILLs PID 1; the second start must recover without marker reset.
// A separate live-owner probe uses the same volume and hostname while the first
// namespace is still alive, and must fail its flock before touching markers.
func TestContainerNamespaceRestartRecovery(t *testing.T) {
	root := os.Getenv("PGBACKRESTD_NAMESPACE_RESTART_ROOT")
	if root == "" {
		t.Skip("requires the isolated runtime restart driver")
	}
	require.Equal(t, 1, os.Getpid(), "the runtime contract requires namespace init")
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	cfg.S3CredSource = ""
	cfg.CipherKeyFile = ""
	cfg.StateDir = filepath.Join(root, "state")
	cfg.DataVolume = filepath.Join(root, "data")
	cfg.PGData = filepath.Join(cfg.DataVolume, "pg_data")
	cfg.PGSocketDir = filepath.Join(cfg.DataVolume, "pg_sockets")
	cfg.RestorePIDFile = filepath.Join(cfg.DataVolume, "restore.pid")
	cfg.FSRepoPath = filepath.Join(root, "repo")
	cfg.TLSCertDir = makeCertDir(t)
	for _, dir := range []string{cfg.DataVolume, cfg.FSRepoPath} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
	}
	if os.Getenv("PGBACKRESTD_NAMESPACE_RESTART_PROBE") == "1" {
		before, err := os.ReadDir(cfg.StateDir)
		require.NoError(t, err)
		_, err = newForTest(cfg)
		require.ErrorContains(t, err, "still own")
		after, err := os.ReadDir(cfg.StateDir)
		require.NoError(t, err)
		require.Equal(t, len(before), len(after), "probe must not retire the live owner's markers")
		return
	}
	phase := filepath.Join(root, "restart-owner.json")
	previous, phaseErr := os.ReadFile(phase)
	require.True(t, phaseErr == nil || os.IsNotExist(phaseErr))
	s, err := newForTest(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	script := filepath.Join(root, "pgbackrest-probe")
	started, release := filepath.Join(root, "restore-started"), filepath.Join(root, "release")
	marker := filepath.Join(cfg.DataVolume, constants.RestoreSentinelFile)
	if os.IsNotExist(phaseErr) {
		require.NoError(t, os.WriteFile(script, fmt.Appendf(nil, `#!/bin/sh
for a in "$@"; do case "$a" in --*) ;; *) cmd=$a; break;; esac; done
case "$cmd" in
restore)
 mkdir -p %q
 echo 17 > %q
 if [ ! -f %q ]; then
  echo partial > %q
  touch %q
  while [ ! -f %q ]; do sleep 0.01; done
 fi
 echo complete > %q
 ;;
esac
exit 0
`, cfg.PGData, filepath.Join(cfg.PGData, "PG_VERSION"), release, filepath.Join(cfg.PGData, "partial"), started, release, filepath.Join(cfg.PGData, "complete")), 0o700))
		s.bin = script
		_, err = s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
		require.NoError(t, err)
		require.NoError(t, privateWrite(marker, []byte("restore pending\n")))
		out := newStreamSender(discardRestoreStream{})
		req := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation}}
		go func() { _, _ = s.runRestoreOperation(t.Context(), req, out); _ = out.finish(nil) }()
		require.Eventually(t, func() bool { _, err := os.Stat(started); return err == nil }, 10*time.Second, 5*time.Millisecond)
		data, err := json.Marshal(s.workerOwner)
		require.NoError(t, err)
		require.NoError(t, privateWrite(phase, data))
		fmt.Println("CONTAINER_RESTORE_READY")
		// The host driver now probes a competing namespace, then docker-kills this
		// namespace init and waits for runtime teardown before restarting the slot.
		select {}
	}
	var old workerOwner
	require.NoError(t, json.Unmarshal(previous, &old))
	require.True(t, priorRuntimeDomainEnded(old, s.workerOwner), "runtime replacement must have a different kernel domain identity")
	require.Equal(t, old.Hostname, s.workerOwner.Hostname)
	require.True(t, s.restoreRecord.Terminal)
	require.False(t, s.restoreRecord.Succeeded, "interrupted restore must not become success")
	require.FileExists(t, marker)
	require.FileExists(t, filepath.Join(cfg.PGData, "partial"), "startup retirement must not delete PGDATA")
	audit, err := os.ReadDir(s.cfg.stateSub("recovered-workers"))
	require.NoError(t, err)
	require.NotEmpty(t, audit)
	active, err := os.ReadDir(s.cfg.StateDir)
	require.NoError(t, err)
	for _, entry := range active {
		require.False(t, strings.HasPrefix(entry.Name(), "worker-") && strings.HasSuffix(entry.Name(), ".pending"))
	}
	require.NoError(t, privateWrite(release, nil))
	s.bin = script
	req := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: s.restoreRecord.Operation.Sequence + 1, Predecessor: s.restoreRecord.Operation.Id, Incarnation: s.incarnation, Cleanup: true}}
	out := newStreamSender(discardRestoreStream{})
	defer func() { require.NoError(t, out.finish(nil)) }()
	code, err := s.runRestoreOperation(t.Context(), req, out)
	require.NoError(t, err)
	require.Zero(t, code)
	require.True(t, s.restoreRecord.Succeeded)
	require.FileExists(t, filepath.Join(cfg.PGData, "complete"))
	require.NoFileExists(t, filepath.Join(cfg.PGData, "partial"))
	require.FileExists(t, marker, "only manager postprocessing may remove the restore sentinel")
	t.Logf("recovered PID namespace %d/start %d -> %d/start %d; retained %d worker records; fenced retry succeeded", old.NamespaceInode, old.StartTicks, s.workerOwner.NamespaceInode, s.workerOwner.StartTicks, len(audit))
}

func TestWorkerMarkerMixedDomainsStayIntact(t *testing.T) {
	s := newTestServer(t, nil)
	// Unit-test classification only; actual domain teardown is exercised by the
	// retained Docker restart test above, not inferred from these synthetic IDs.
	current := workerOwner{Platform: "linux", PID: 1, Token: uuid.NewString(), BootID: uuid.NewString(), StartTicks: 200, NamespaceDevice: 4, NamespaceInode: 100, Hostname: "slot"}
	s.workerOwner = current
	prior := current
	prior.NamespaceInode = 99
	local := current
	local.PID = 42
	for name, owner := range map[string]workerOwner{"worker-aaa.pending": prior, "worker-zzz.pending": local} {
		data, err := json.Marshal(workerMarker{Version: 1, Owner: owner})
		require.NoError(t, err)
		require.NoError(t, privateWrite(s.cfg.stateSub(name), data))
	}
	require.ErrorContains(t, s.recoverWorkerMarkers(), "not proven ended")
	require.FileExists(t, s.cfg.stateSub("worker-aaa.pending"), "later ambiguous marker must prevent earlier retirement too")
	require.FileExists(t, s.cfg.stateSub("worker-zzz.pending"))
	require.NoDirExists(t, s.cfg.stateSub("recovered-workers"))
}
