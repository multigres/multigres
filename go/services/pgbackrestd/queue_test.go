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
	"os/exec"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/tools/executil"
)

func TestArchiveGetQueueMax(t *testing.T) {
	for _, tc := range []struct {
		configured string
		segment    uint32
		want       string
	}{
		{"256MiB", 0, "256MiB"},
		{"256MiB", 16 << 20, "256MiB"},
		{"256MiB", 128 << 20, "256MiB"},
		{"256MiB", 256 << 20, "536870912"},
		{"256MiB", 1 << 30, "2147483648"},
		{"64mb", 64 << 20, "134217728"},
		{"1GiB", 1 << 30, "2147483648"},
		{"1g", 512 << 20, "1g"},
		{"1024", 16 << 20, "33554432"},
		{"2kb", 1 << 20, "2097152"},
	} {
		assert.Equal(t, tc.want, archiveGetQueueMax(tc.configured, tc.segment), "%s with a %d byte segment", tc.configured, tc.segment)
	}
}

// The WAL segment size from Configure sizes the prefetch queue (the design's
// "larger of archive-get-queue-max and two segments"), and 0 keeps the default.
func TestConfigure_SegmentSizeSizesTheGetQueue(t *testing.T) {
	h := newControlHarness(t, nil)
	send := func(rev int64, seg uint32) {
		t.Helper()
		resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: rev, Repos: repos(1, 1), WalSegmentSizeBytes: seg}))
		require.NoError(t, err)
		require.True(t, resp.Msg.Accepted)
	}
	conf := func() string { return readFile(t, h.s.cfg.stateSub(fileClientConf)) }

	send(1, 0)
	assert.Contains(t, conf(), "archive-get-queue-max=256MiB", "no size known: the flag stands")
	send(2, 1<<30)
	assert.Contains(t, conf(), "archive-get-queue-max=2147483648", "two 1 GiB segments exceed the flag")
	assert.NotContains(t, conf(), "archive-get-queue-max=256MiB")
	send(3, 0)
	assert.Contains(t, conf(), "archive-get-queue-max=2147483648", "0 keeps the size already held")
	send(4, 16<<20)
	assert.Contains(t, conf(), "archive-get-queue-max=256MiB")
	assert.NotRegexp(t, `(?m)^archive-push-queue-max=`, conf(), "a push queue limit would let pgbackrest drop WAL")
}

// The rendered size is accepted by the pinned pgBackRest, the one thing a
// string comparison cannot show. Skipped where pgbackrest is not installed.
func TestRenderedQueueMaxIsAcceptedByPgBackRest(t *testing.T) {
	bin, err := exec.LookPath("pgbackrest")
	if err != nil {
		t.Skip("pgbackrest is not installed")
	}
	for _, seg := range []uint32{0, 16 << 20, 1 << 30} {
		h := newControlHarness(t, nil)
		resp, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1), WalSegmentSizeBytes: seg}))
		require.NoError(t, err)
		require.True(t, resp.Msg.Accepted)
		conf := h.s.cfg.stateSub(fileClientConf)
		out, err := executil.Command(t.Context(), bin, "--config="+conf, "--stanza=multigres", "--output=json", "info").CombinedOutput()
		require.NoError(t, err, "segment %d: %s\n%s", seg, out, readFile(t, conf))
	}
}

// The restore_command the sidecar leaves in recovery config is the text
// multipooler writes, quoting included: both come from one renderer.
func TestRestoreCommand_MatchesTheSharedRendererForTrickyPaths(t *testing.T) {
	for _, dir := range []string{"/run/pgbackrest-wal", "/tmp/with space", "/tmp/it's here", "/tmp/a$b;`c`"} {
		s := newTestServer(t, func(c *Config) {
			c.WALSocket = dir + "/wal.sock"
			c.RestorePIDFile = dir + "/restore.pid"
		})
		want := backup.SidecarRestoreCommand(dir+"/wal.sock", dir+"/restore.pid")
		assert.Equal(t, want, s.restoreCommand(), dir)
		assert.Contains(t, s.withFixedArgs(pb.Command_COMMAND_RESTORE, nil)[0], "--recovery-option=restore_command=", dir)
	}
}
