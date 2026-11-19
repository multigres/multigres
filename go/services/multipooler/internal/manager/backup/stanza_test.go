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

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// TestStanzaCreate_ExecPaths drives StanzaCreate against a stubbed pgbackrest,
// covering the success (exit 0) and failure (non-zero) paths.
func TestStanzaCreate_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		exitCode    int
		wantErr     bool
		errContains string
	}{
		{name: "success on exit 0", exitCode: 0},
		{name: "failure on nonzero exit", exitCode: 1, wantErr: true, errContains: "pgbackrest stanza-create failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\nexit %d\n", tt.exitCode))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")

			err := e.StanzaCreate(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

// StanzaDelete is one sidecar command: the sidecar stops archiving, deletes the
// stanza and starts it again, so no stop or start comes from this side.
func TestStanzaDelete_IsOneSidecarCommand(t *testing.T) {
	fake := &fakeSidecar{}
	e := newSidecarTestEngine(t, fake)

	require.NoError(t, e.StanzaDelete(t.Context()))

	require.Len(t, fake.calls, 1)
	call := fake.calls[0]
	assert.Equal(t, pb.Command_COMMAND_STANZA_DELETE, call.cmd)
	assert.Empty(t, call.args, "the sidecar takes no options for a delete")
	assert.True(t, call.hasDeadline, "the call is bounded by the sidecar's whole sequence")
}

func TestStanzaDelete_Errors(t *testing.T) {
	tests := []struct {
		name  string
		reply func(pb.Command, []string, io.Writer, io.Writer) int
		err   error
		want  string
	}{
		{
			name: "stanza-delete failing",
			reply: func(_ pb.Command, _ []string, _, stderr io.Writer) int {
				_, _ = io.WriteString(stderr, "ERROR: [055]: boom")
				return 55
			},
			want: "pgbackrest stanza-delete failed",
		},
		{name: "start failing after the delete", err: errors.New("pgbackrest start failed; WAL archiving is stopped"), want: "WAL archiving is stopped"},
		{name: "sidecar unreachable", err: fmt.Errorf("%w: dial", ErrRunNotDelivered), want: "pgbackrest stanza-delete failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeSidecar{reply: tt.reply, err: tt.err}
			e := newSidecarTestEngine(t, fake)
			err := e.StanzaDelete(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			if tt.name == "stanza-delete failing" {
				assert.Contains(t, err.Error(), "[055]", "the sidecar's output reaches the caller")
			}
		})
	}
}

func TestStanzaDelete_InvalidatesTheListCache(t *testing.T) {
	fake := &fakeSidecar{}
	e := newSidecarTestEngine(t, fake)
	e.listCache.mu.Lock()
	e.listCache.value = []*multipoolermanagerdata.BackupMetadata{{BackupId: "stale"}}
	e.listCache.expires = time.Now().Add(time.Hour)
	e.listCache.mu.Unlock()

	require.NoError(t, e.StanzaDelete(t.Context()))

	e.listCache.mu.Lock()
	defer e.listCache.mu.Unlock()
	assert.Nil(t, e.listCache.value, "a delete drops what the cache holds")
}

func TestResetArchiveSpool_IsOneSidecarCommand(t *testing.T) {
	fake := &fakeSidecar{}
	e := newSidecarTestEngine(t, fake)

	require.NoError(t, e.ResetArchiveSpool(t.Context()))

	require.Len(t, fake.calls, 1)
	assert.Equal(t, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET, fake.calls[0].cmd)
	assert.Empty(t, fake.calls[0].args)
	assert.True(t, fake.calls[0].hasDeadline)
}

func TestResetArchiveSpool_ErrorOnFailure(t *testing.T) {
	fake := &fakeSidecar{reply: func(pb.Command, []string, io.Writer, io.Writer) int { return 1 }}
	e := newSidecarTestEngine(t, fake)
	err := e.ResetArchiveSpool(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "archive spool reset failed")
}

// The deadline is the sum of every wait the sidecar's supervised sequence is
// specified to make, term by term from the shared constants: dropping or
// changing one of them fails here.
func TestSidecarResetDeadlineSumsEveryWaitOfTheSequence(t *testing.T) {
	const (
		drain   = commonbackup.ResetDrainWait
		step    = commonbackup.ResetStepTimeout
		settle  = commonbackup.WorkerStopWait
		info    = commonbackup.InfoTimeout
		starts  = commonbackup.StartAttempts
		delays  = commonbackup.StartRetryDelay
		attempt = step + settle
	)
	want := drain +
		settle + // the pause of the 8432 server
		attempt + // start
		attempt + // stop
		settle + // worker barrier
		info + settle + // the preflight info and its settlement
		attempt + // stanza-delete
		settle + // worker barrier
		starts*attempt + (starts-1)*delays + // the unwind's starts
		resetMargin
	assert.Equal(t, want, sidecarResetDeadline())
	assert.Equal(t, 30*time.Second+15*time.Second+45*time.Second+45*time.Second+15*time.Second+45*time.Second+45*time.Second+15*time.Second+3*45*time.Second+2*time.Second+30*time.Second,
		sidecarResetDeadline(), "the figure, for a reader: 7m2s")
}

func TestIsStaleStanzaError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "info files do not match", err: errors.New("stanza-create failed\nERROR: [028]: backup and archive info files exist but do not match the database"), want: true},
		{name: "info file missing", err: errors.New("stanza-create failed\nERROR: [055]: unable to open missing file backup.info"), want: true},
		{name: "stop file present", err: errors.New("ERROR: [062]: stop file exists for stanza multigres"), want: false},
		{name: "cannot connect", err: errors.New("ERROR: [039]: unable to connect"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsStaleStanzaError(tt.err))
		})
	}
}
