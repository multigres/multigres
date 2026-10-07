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

package initproc

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// roleEnv selects what a re-executed test binary does instead of running tests.
// Supervise has to own every child of its process, which a `go test` process
// running other tests cannot promise, so it always runs in a re-executed copy.
const roleEnv = "INITPROC_TEST_ROLE"

// roles maps a roleEnv value to the code a re-executed test binary runs.
// Platform-specific test files add to it from init.
var roles = map[string]func() int{
	"supervisor":       runSupervisor,
	"child-exit-7":     func() int { return 7 },
	"child-self-kill":  childSelfKill,
	"child-await-term": childAwaitTerm,
}

func TestMain(m *testing.M) {
	if role := os.Getenv(roleEnv); role != "" {
		fn, ok := roles[role]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown %s %q\n", roleEnv, role)
			os.Exit(2) //nolint:forbidigo // TestMain is allowed to call os.Exit
		}
		os.Exit(fn()) //nolint:forbidigo // TestMain is allowed to call os.Exit
	}
	os.Exit(m.Run()) //nolint:forbidigo // TestMain is allowed to call os.Exit
}

// supervisorChildEnv names the role the supervisor gives the child it starts.
const supervisorChildEnv = "INITPROC_TEST_CHILD_ROLE"

// becomeSubreaper is set on Linux, where it lets the supervisor stand in for
// PID 1 by having orphans reparent to it.
var becomeSubreaper func() error

func runSupervisor() int {
	if becomeSubreaper != nil {
		if err := becomeSubreaper(); err != nil {
			fmt.Fprintln(os.Stderr, "become subreaper:", err)
			return 2
		}
	}
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "executable:", err)
		return 2
	}
	// Unlike os/exec, ForkExec does not dedupe env, and getenv returns the first
	// match, so the inherited role has to be removed rather than overridden or
	// the child becomes another supervisor.
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, roleEnv+"=") })
	env = append(env, roleEnv+"="+os.Getenv(supervisorChildEnv))
	return Supervise(slog.New(slog.NewTextHandler(os.Stderr, nil)), exe, []string{exe}, env)
}

func childSelfKill() int {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	time.Sleep(10 * time.Second)
	return 1
}

// childAwaitTerm reports readiness on stdout, then exits 42 only if SIGTERM
// arrives, so the exit code proves the supervisor forwarded it.
func childAwaitTerm() int {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	fmt.Println("ready")
	select {
	case <-ch:
		return 42
	case <-time.After(30 * time.Second):
		return 1
	}
}

// supervisorCommand returns a command running the supervisor, which starts a
// child playing childRole.
func supervisorCommand(t *testing.T, childRole string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), roleEnv+"=supervisor", supervisorChildEnv+"="+childRole)
	cmd.Stderr = os.Stderr
	return cmd
}

func exitCodeOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	require.True(t, errors.As(err, &exitErr), "unexpected error: %v", err)
	return exitErr.ExitCode()
}

func TestSupervise_PropagatesExitCode(t *testing.T) {
	cmd := supervisorCommand(t, "child-exit-7")
	require.Equal(t, 7, exitCodeOf(t, cmd.Run()))
}

func TestSupervise_ReportsSignalDeathAs128PlusSignal(t *testing.T) {
	cmd := supervisorCommand(t, "child-self-kill")
	require.Equal(t, 128+int(syscall.SIGKILL), exitCodeOf(t, cmd.Run()))
}

func TestSupervise_ForwardsSIGTERM(t *testing.T) {
	cmd := supervisorCommand(t, "child-await-term")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)

	// The raw signal is the thing under test, and cmd.Wait below reaps the process.
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM)) //nolint:gocritic // see above
	require.Equal(t, 42, exitCodeOf(t, cmd.Wait()))
}

func TestSupervise_StartFailureReturnsOne(t *testing.T) {
	code := Supervise(slog.New(slog.DiscardHandler), "/nonexistent/initproc-test", []string{"x"}, nil)
	require.Equal(t, 1, code)
}
