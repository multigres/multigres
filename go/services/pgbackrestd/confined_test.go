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
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sandbox"
)

func enforcedDecision() sandbox.Decision {
	return sandbox.Decision{
		Requested: sandbox.ModeRequired, Probed: true,
		Capability: sandbox.Capability{Supported: true, ABI: 7},
		Effective:  sandbox.EffectiveEnforced,
	}
}

// Under an enforced decision every child is wrapped in sandbox-exec, so a child
// can only ever start confined or not at all. The decision is data: the host's
// own capability is not consulted again per child.
func TestCommandWrapsEveryChildWhenEnforced(t *testing.T) {
	cfg := testConfig(t)
	s := &Server{cfg: cfg, self: "/self/pgbackrestd", decision: enforcedDecision(), logger: slog.Default()}
	for _, command := range []string{cmdServer, cmdBackup, cmdRestore, cmdInfo} {
		cmd := s.command(context.Background(), command, []string{"/usr/bin/pgbackrest", command})
		require.GreaterOrEqual(t, len(cmd.Args), 3, command)
		assert.Equal(t, "/self/pgbackrestd", cmd.Path, command)
		assert.Equal(t, sandbox.SubcommandName, cmd.Args[1], command)
		assert.Contains(t, cmd.Args, "--policy", command)
	}
}

// An unconfined decision execs pgBackRest directly and says so on every spawn,
// whatever the host could have offered.
func TestCommandUnconfinedExecsDirectlyAndWarnsPerChild(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := testConfig(t)
	d := sandbox.Decision{Requested: sandbox.ModeAuto, Probed: true, Capability: sandbox.Capability{Reason: "x"}, Effective: sandbox.EffectiveUnconfined}
	s := &Server{cfg: cfg, self: "/self/pgbackrestd", decision: d, logger: logger}
	for _, command := range []string{cmdServer, cmdBackup, cmdInfo} {
		cmd := s.command(context.Background(), command, []string{"/usr/bin/pgbackrest", command})
		assert.Equal(t, []string{"/usr/bin/pgbackrest", command}, cmd.Args, command)
		assert.NotContains(t, cmd.Args, sandbox.SubcommandName)
	}
	assert.Equal(t, 3, strings.Count(buf.String(), "running pgBackRest child unconfined"), buf.String())
	assert.Contains(t, buf.String(), "level=WARN")
}

func TestEnforcedChildLogsNothingAboutBeingUnconfined(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	s := &Server{cfg: testConfig(t), self: "/self", decision: enforcedDecision(), logger: logger}
	s.command(context.Background(), cmdBackup, []string{"/usr/bin/pgbackrest", cmdBackup})
	assert.Empty(t, buf.String())
}

func TestDecisionLogLine(t *testing.T) {
	tests := []struct {
		name  string
		d     sandbox.Decision
		level string
		want  []string
	}{
		{"enforced", enforcedDecision(), "level=INFO", []string{"requested=required", `capability="Landlock ABI 7"`, "effective=enforced"}},
		{
			"degraded auto",
			sandbox.Decision{Requested: sandbox.ModeAuto, Probed: true, Capability: sandbox.Capability{Reason: "the kernel has no Landlock (ENOSYS)"}, Effective: sandbox.EffectiveUnconfined},
			"level=WARN",
			[]string{"requested=auto", "unsupported: the kernel has no Landlock (ENOSYS)", "effective=unconfined"},
		},
		{"off", sandbox.Decision{Requested: sandbox.ModeOff, Effective: sandbox.EffectiveUnconfined}, "level=WARN", []string{"requested=off", "not probed", "effective=unconfined"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logDecision(slog.New(slog.NewTextHandler(&buf, nil)), tt.d)
			out := buf.String()
			assert.Equal(t, 1, strings.Count(out, "\n"), "one line: %q", out)
			assert.Contains(t, out, tt.level)
			for _, w := range tt.want {
				assert.Contains(t, out, w)
			}
		})
	}
}

func filesystemConfig(t *testing.T, mode string) Config {
	cfg := testConfig(t)
	cfg.LocationType = LocationFilesystem
	cfg.FSRepoPath = t.TempDir()
	cfg.S3CredSource, cfg.CipherKeyFile = "", ""
	cfg.Sandbox = mode
	return cfg
}

func probeReturning(c sandbox.Capability, err error, calls *int) sandbox.ProbeFunc {
	return func() (sandbox.Capability, error) {
		*calls++
		return c, err
	}
}

func TestNewDecidesFromTheProbe(t *testing.T) {
	unsupported := sandbox.Capability{Reason: "the kernel has no Landlock (ENOSYS)"}
	eperm := sandbox.Capability{}
	epermErr := errors.New("probing Landlock failed with operation not permitted (EPERM); seccomp")

	t.Run("required fails closed on unsupported", func(t *testing.T) {
		var calls int
		_, err := newWithProbe(filesystemConfig(t, constants.SandboxRequired), probeReturning(unsupported, nil, &calls))
		require.ErrorIs(t, err, sandbox.ErrUnsupported)
		assert.ErrorContains(t, err, "MULTIGRES_PGBACKREST_SANDBOX")
	})
	t.Run("empty mode is required", func(t *testing.T) {
		var calls int
		_, err := newWithProbe(filesystemConfig(t, ""), probeReturning(unsupported, nil, &calls))
		require.ErrorIs(t, err, sandbox.ErrUnsupported)
	})
	for _, mode := range []string{constants.SandboxRequired, constants.SandboxAuto} {
		t.Run(mode+" fails on a probe that cannot tell", func(t *testing.T) {
			var calls int
			_, err := newWithProbe(filesystemConfig(t, mode), probeReturning(eperm, epermErr, &calls))
			require.Error(t, err)
			assert.NotErrorIs(t, err, sandbox.ErrUnsupported)
			assert.ErrorContains(t, err, "seccomp")
			assert.Equal(t, 1, calls)
		})
	}
	t.Run("auto degrades on unsupported", func(t *testing.T) {
		var calls int
		s, err := newWithProbe(filesystemConfig(t, constants.SandboxAuto), probeReturning(unsupported, nil, &calls))
		require.NoError(t, err)
		defer s.Close()
		assert.False(t, s.decision.Enforced())
		assert.Equal(t, sandbox.ModeAuto, s.decision.Requested)
		assert.Equal(t, 1, calls)
	})
	t.Run("off never probes", func(t *testing.T) {
		var calls int
		s, err := newWithProbe(filesystemConfig(t, constants.SandboxOff), probeReturning(eperm, epermErr, &calls))
		require.NoError(t, err)
		defer s.Close()
		assert.False(t, s.decision.Enforced())
		assert.Zero(t, calls)
	})
	t.Run("unknown mode is a startup error", func(t *testing.T) {
		var calls int
		_, err := newWithProbe(filesystemConfig(t, "sometimes"), probeReturning(unsupported, nil, &calls))
		require.ErrorContains(t, err, "--sandbox")
		assert.Zero(t, calls)
	})
	t.Run("required and auto enforce where the host supports it", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("an enforced server anchors the data volume, which needs Linux")
		}
		for _, mode := range []string{constants.SandboxRequired, constants.SandboxAuto} {
			var calls int
			s, err := newWithProbe(filesystemConfig(t, mode), probeReturning(sandbox.Capability{Supported: true, ABI: 7}, nil, &calls))
			require.NoError(t, err, mode)
			assert.True(t, s.decision.Enforced(), mode)
			s.Close()
		}
	})
}

// Without Landlock the real probe makes required refuse to start.
func TestNewFailsClosedWithoutLandlock(t *testing.T) {
	if err := sandbox.CheckAvailable(); err == nil {
		t.Skip("this host has Landlock; the refusal is exercised where it does not")
	}
	_, err := New(filesystemConfig(t, constants.SandboxRequired))
	require.ErrorIs(t, err, sandbox.ErrUnsupported)
	assert.ErrorContains(t, err, sandbox.RequirementMessage)
}
