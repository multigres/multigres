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

package sandbox

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	helperOp   = "SANDBOX_TEST_OP"
	helperPath = "SANDBOX_TEST_PATH"
	helperABI  = "SANDBOX_TEST_ABI"
)

// TestMain doubles as the sandboxed helper: Landlock is irreversible for the
// calling process, so every confined action runs in a re-executed copy of the
// test binary.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == SubcommandName {
		if v := os.Getenv(helperABI); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				os.Stderr.WriteString(err.Error() + "\n")
				os.Exit(125) //nolint:forbidigo // TestMain
			}
			effectiveABI = func() int { return min(ABI(), n) }
		}
		if err := ExecMain(os.Args[2:]); err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(125) //nolint:forbidigo // TestMain
		}
		return
	}
	if op := os.Getenv(helperOp); op != "" {
		os.Exit(runHelper(op, os.Getenv(helperPath))) //nolint:forbidigo // TestMain
	}
	os.Exit(m.Run()) //nolint:forbidigo // TestMain
}

// runHelper prints "ok" or the errno name, so the parent can assert on the
// exact failure rather than on any non-zero exit.
func runHelper(op, path string) int {
	switch op {
	case "open-ro", "open-wo":
		flags := os.O_RDONLY
		if op == "open-wo" {
			flags = os.O_WRONLY
		}
		f, err := os.OpenFile(path, flags, 0)
		if err != nil {
			os.Stdout.WriteString(errnoName(err))
			return 0
		}
		f.Close()
		os.Stdout.WriteString("ok")
		return 0
	case "create-rename":
		// The temp-file-plus-rename write pattern, within one directory.
		tmp := filepath.Join(path, "f.tmp")
		if err := os.WriteFile(tmp, []byte("x"), 0o600); err != nil {
			os.Stdout.WriteString(errnoName(err))
			return 0
		}
		if err := os.Rename(tmp, filepath.Join(path, "f")); err != nil {
			os.Stdout.WriteString(errnoName(err))
			return 0
		}
		os.Stdout.WriteString("ok")
		return 0
	case "rename":
		src, dst, _ := strings.Cut(path, ":")
		if err := os.Rename(src, dst); err != nil {
			os.Stdout.WriteString(errnoName(err))
			return 0
		}
		os.Stdout.WriteString("ok")
		return 0
	case "exec":
		cmd := exec.Command(path)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			os.Stdout.WriteString(errnoName(err))
			return 0
		}
		os.Stdout.WriteString("ok")
		return 0
	case "connect-unix":
		c, err := net.Dial("unix", path)
		if err != nil {
			os.Stdout.WriteString(errnoName(err))
			return 0
		}
		c.Close()
		os.Stdout.WriteString("ok")
		return 0
	case "grandchild-open":
		self, err := os.Executable()
		if err != nil {
			return 2
		}
		cmd := exec.Command(self)
		cmd.Env = append(os.Environ(), helperOp+"=open-ro")
		// Inherit all three: a nil stream makes os/exec open /dev/null, which the policy does not grant.
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			os.Stderr.WriteString(err.Error())
			return 3
		}
		return 0
	}
	return 4
}

func errnoName(err error) string {
	switch {
	case errors.Is(err, syscall.EACCES):
		return "EACCES"
	case errors.Is(err, syscall.ENOENT):
		return "ENOENT"
	case errors.Is(err, syscall.EXDEV):
		return "EXDEV"
	}
	return err.Error()
}

func requireLandlock(t *testing.T) {
	t.Helper()
	if ABI() < 3 {
		t.Skipf("Landlock ABI %d < 3", ABI())
	}
}

// baseRules lets the helper binary itself start. The test binary is granted
// as a single file so that nothing around it, including the temp dir the
// test files live in, is covered by accident.
func baseRules(t *testing.T) []Rule {
	t.Helper()
	self, err := os.Executable()
	require.NoError(t, err)
	rules := []Rule{{Path: self, Access: Read | Exec}}
	for _, p := range []string{"/lib", "/lib64", "/usr", "/etc"} {
		if _, err := os.Stat(p); err == nil {
			rules = append(rules, Rule{Path: p, Access: Read | Exec})
		}
	}
	return rules
}

// runSandboxed runs the helper op under the policy and returns what it
// printed and how the sandbox-exec wrapper itself ended.
func runSandboxed(t *testing.T, policy Policy, op, path string, env ...string) (string, string, error) {
	t.Helper()
	self, err := os.Executable()
	require.NoError(t, err)
	cmd := Command(context.Background(), self, policy, []string{self})
	cmd.Env = append(os.Environ(), append([]string{helperOp + "=" + op, helperPath + "=" + path}, env...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	return stdout.String(), stderr.String(), err
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
}

func TestRestrictSelf_DeniesUnlistedRead(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	allowed := filepath.Join(dir, "allowed", "a")
	denied := filepath.Join(dir, "denied", "x")
	writeFile(t, allowed)
	writeFile(t, denied)
	policy := Policy{Rules: append(baseRules(t), Rule{Path: filepath.Dir(allowed), Access: Read})}

	out, stderr, err := runSandboxed(t, policy, "open-ro", allowed)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out, "control: a granted path must stay readable")

	out, stderr, err = runSandboxed(t, policy, "open-ro", denied)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out)
}

func TestRestrictSelf_DeniesSymlinkOut(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	denied := filepath.Join(dir, "secret", "token")
	writeFile(t, denied)
	allowedDir := filepath.Join(dir, "allowed")
	writeFile(t, filepath.Join(allowedDir, "plain"))
	link := filepath.Join(allowedDir, "link")
	require.NoError(t, os.Symlink(denied, link))
	policy := Policy{Rules: append(baseRules(t), Rule{Path: allowedDir, Access: Read})}

	out, stderr, err := runSandboxed(t, policy, "open-ro", filepath.Join(allowedDir, "plain"))
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out, "control: a real file in the same dir stays readable")

	out, stderr, err = runSandboxed(t, policy, "open-ro", link)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out)
}

func TestRestrictSelf_WriteNotGrantedByRead(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "f")
	writeFile(t, file)

	ro := Policy{Rules: append(baseRules(t), Rule{Path: dir, Access: Read})}
	out, stderr, err := runSandboxed(t, ro, "open-ro", file)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out)
	out, stderr, err = runSandboxed(t, ro, "open-wo", file)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out)

	rw := Policy{Rules: append(baseRules(t), Rule{Path: dir, Access: Read | Write})}
	out, stderr, err = runSandboxed(t, rw, "open-wo", file)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out, "control: Write must actually grant O_WRONLY")
}

func TestRestrictSelf_InheritedAcrossExec(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	allowed := filepath.Join(dir, "allowed", "a")
	denied := filepath.Join(dir, "denied", "x")
	writeFile(t, allowed)
	writeFile(t, denied)
	policy := Policy{Rules: append(baseRules(t), Rule{Path: filepath.Dir(allowed), Access: Read})}

	out, stderr, err := runSandboxed(t, policy, "grandchild-open", allowed)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out, "control: the grandchild can start and read a granted path")

	out, stderr, err = runSandboxed(t, policy, "grandchild-open", denied)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out)
}

func TestRestrictSelf_GrantsUnixSocketConnect(t *testing.T) {
	requireLandlock(t)
	if ABI() < resolveUnixABI {
		t.Skipf("Landlock ABI %d < %d: connect(2) is not governed", ABI(), resolveUnixABI)
	}
	dir := t.TempDir()
	sockDir := filepath.Join(dir, "sock")
	require.NoError(t, os.MkdirAll(sockDir, 0o755))
	sock := filepath.Join(sockDir, "s")
	l, err := net.Listen("unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	with := Policy{Rules: append(baseRules(t), Rule{Path: sockDir, Access: Read})}
	out, stderr, err := runSandboxed(t, with, "connect-unix", sock)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out)

	out, stderr, err = runSandboxed(t, Policy{Rules: baseRules(t)}, "connect-unix", sock)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out)
}

func TestRestrictSelf_MissingPathErrors(t *testing.T) {
	requireLandlock(t)
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	policy := Policy{Rules: append(baseRules(t), Rule{Path: missing, Access: Read})}

	out, stderr, err := runSandboxed(t, policy, "open-ro", missing)
	require.Error(t, err)
	assert.Empty(t, out, "the helper must never have run")
	assert.Contains(t, stderr, missing)
}

func TestOpenAnchor_RejectsNonMount(t *testing.T) {
	dir := t.TempDir()

	_, err := OpenAnchor(dir, AnchorMount)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a mount point")

	f, err := OpenAnchor(dir, AnchorDir)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// /dev is a mount root in every container and on every systemd host, so
	// it is the positive control for the statx check.
	f, err = OpenAnchor("/dev", AnchorMount)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestOpenAnchor_RejectsPseudoRoots(t *testing.T) {
	for _, p := range []string{"/", "/proc", "/sys", "/proc/../proc", "/proc/self/root"} {
		_, err := OpenAnchor(p, AnchorMount)
		require.Error(t, err, p)
	}
	// AnchorDir is for tests and the local provisioner and stays permissive.
	f, err := OpenAnchor("/proc", AnchorDir)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestOpenAnchor_RejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(dir, link))

	for _, mode := range []AnchorMode{AnchorMount, AnchorDir} {
		_, err := OpenAnchor(link, mode)
		require.Error(t, err, "mode %d", mode)
		assert.Contains(t, err.Error(), "not a directory")
	}

	// A symlink to a real mount point must not pass AnchorMount.
	procLink := filepath.Join(t.TempDir(), "dev")
	require.NoError(t, os.Symlink("/dev", procLink))
	_, err := OpenAnchor(procLink, AnchorMount)
	require.Error(t, err)
}

func TestCheckAvailable_FailsBelowFloor(t *testing.T) {
	old := ABI()
	if old < minABI {
		assert.ErrorIs(t, CheckAvailable(), ErrUnsupported)
		assert.ErrorIs(t, RestrictSelf(Policy{}), ErrUnsupported)
		return
	}
	assert.NoError(t, CheckAvailable())
}

func TestCommand_DeterministicArgv(t *testing.T) {
	a := Policy{Rules: []Rule{{Path: "/b", Access: Read}, {Path: "/a", Access: Write}}}
	b := Policy{Rules: []Rule{{Path: "/a", Access: Write}, {Path: "/b", Access: Read}}}

	ca := Command(context.Background(), "/self", a, []string{"x", "y"})
	cb := Command(context.Background(), "/self", b, []string{"x", "y"})
	assert.Equal(t, ca.Args, cb.Args)
	assert.Equal(t, []string{"sandbox-exec", "--policy"}, ca.Args[1:3])
	assert.Equal(t, "--", ca.Args[4])
	assert.Equal(t, "/b", a.Rules[0].Path, "encoding must not reorder the caller's policy")
}

func TestExecMain_FailsClosed(t *testing.T) {
	// Every one of these must return before anything is executed or
	// confined; reaching syscall.Exec would replace the test process.
	assert.Error(t, ExecMain(nil), "no policy and no argv")
	assert.Error(t, ExecMain([]string{"--", "true"}), "absent policy")
	assert.Error(t, ExecMain([]string{"--policy", "", "--", "true"}), "empty policy")
	assert.Error(t, ExecMain([]string{"--policy", "!!!", "--", "true"}), "bad base64")
	assert.Error(t, ExecMain([]string{"--policy", "bm90LWpzb24=", "--", "true"}), "bad JSON")
	assert.Error(t, ExecMain([]string{"--policy", Policy{}.encode()}), "no argv")
	assert.Error(t, ExecMain([]string{"--bogus", "x"}), "unknown flag")
	err := ExecMain([]string{"--policy", Policy{}.encode(), "--", "/nonexistent-binary"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent-binary")
}

func TestExecMain_NeverRunsUnconfined(t *testing.T) {
	requireLandlock(t)
	// An empty policy is a complete deny: the exec of the helper itself is
	// refused, so the helper never runs. A path that skipped confinement would run it.
	out, stderr, err := runSandboxed(t, Policy{}, "open-ro", "/etc/passwd")
	require.Error(t, err)
	assert.Empty(t, out)
	assert.Contains(t, stderr, "exec")
	assert.Contains(t, stderr, syscall.EACCES.Error(), "the denial must come from the exec, not from some other failure")
}

func TestExecMain_SilentOnParseError(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	old := os.Stderr
	os.Stderr = w
	_ = ExecMain([]string{"--bogus"})
	os.Stderr = old
	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	assert.Empty(t, buf.String())
}

func TestRestrictSelf_WriteGrantsCreateAndSameDirRename(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	policy := Policy{Rules: append(baseRules(t), Rule{Path: dir, Access: Read | Write})}
	out, stderr, err := runSandboxed(t, policy, "create-rename", dir)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out)
	_, err = os.Stat(filepath.Join(dir, "f"))
	require.NoError(t, err)

	ro := Policy{Rules: append(baseRules(t), Rule{Path: dir, Access: Read})}
	out, stderr, err = runSandboxed(t, ro, "create-rename", dir)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out, "control: Read alone must not allow creating")
}

func TestRestrictSelf_CrossDirRenameDenied(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	writeFile(t, filepath.Join(a, "f"))
	require.NoError(t, os.MkdirAll(b, 0o755))
	policy := Policy{Rules: append(baseRules(t), Rule{Path: dir, Access: Read | Write})}

	// Within one rule's hierarchy, the same dir still renames fine ...
	out, stderr, err := runSandboxed(t, policy, "rename", filepath.Join(a, "f")+":"+filepath.Join(a, "g"))
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out, "control: same-directory rename")

	// ... but moving between directories needs Refer, which is not granted.
	out, stderr, err = runSandboxed(t, policy, "rename", filepath.Join(a, "g")+":"+filepath.Join(b, "g"))
	require.NoError(t, err, stderr)
	assert.Equal(t, "EXDEV", out)
}

func TestRestrictSelf_ExecNeedsExec(t *testing.T) {
	requireLandlock(t)
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin", "true")
	require.NoError(t, os.MkdirAll(filepath.Dir(bin), 0o755))
	src, err := os.ReadFile(mustLookPath(t, "true"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(bin, src, 0o755))

	rx := Policy{Rules: append(baseRules(t), Rule{Path: filepath.Dir(bin), Access: Read | Exec})}
	out, stderr, err := runSandboxed(t, rx, "exec", bin)
	require.NoError(t, err, stderr)
	assert.Equal(t, "ok", out, "control: Exec grants execution")

	ro := Policy{Rules: append(baseRules(t), Rule{Path: filepath.Dir(bin), Access: Read})}
	out, stderr, err = runSandboxed(t, ro, "exec", bin)
	require.NoError(t, err, stderr)
	assert.Equal(t, "EACCES", out)
}

func mustLookPath(t *testing.T, name string) string {
	t.Helper()
	p, err := exec.LookPath(name)
	require.NoError(t, err)
	return p
}

// TestRestrictSelf_OlderABIRuleset enforces the ruleset production nodes
// (ABI 7 and 8) get, on whatever newer kernel runs CI: go-landlock's V-n
// config is strict, so the lower handled set is really applied.
func TestRestrictSelf_OlderABIRuleset(t *testing.T) {
	requireLandlock(t)
	for _, abi := range []int{3, 7, 8} {
		t.Run("abi"+strconv.Itoa(abi), func(t *testing.T) {
			if ABI() < abi {
				t.Skipf("kernel ABI %d < %d", ABI(), abi)
			}
			env := helperABI + "=" + strconv.Itoa(abi)
			dir := t.TempDir()
			allowed := filepath.Join(dir, "allowed", "a")
			denied := filepath.Join(dir, "denied", "x")
			writeFile(t, allowed)
			writeFile(t, denied)
			sockDir := filepath.Join(dir, "sock")
			require.NoError(t, os.MkdirAll(sockDir, 0o755))
			sock := filepath.Join(sockDir, "s")
			l, err := net.Listen("unix", sock)
			require.NoError(t, err)
			t.Cleanup(func() { l.Close() })
			policy := Policy{Rules: append(baseRules(t), Rule{Path: filepath.Dir(allowed), Access: Read | Write})}

			out, stderr, err := runSandboxed(t, policy, "open-ro", allowed, env)
			require.NoError(t, err, stderr)
			assert.Equal(t, "ok", out)
			out, stderr, err = runSandboxed(t, policy, "open-ro", denied, env)
			require.NoError(t, err, stderr)
			assert.Equal(t, "EACCES", out, "filesystem confinement holds at ABI %d", abi)
			out, stderr, err = runSandboxed(t, policy, "create-rename", filepath.Dir(allowed), env)
			require.NoError(t, err, stderr)
			assert.Equal(t, "ok", out)

			// Pins the known gap: below ABI 9 connect(2) to a pathname
			// socket is not governed, with or without a rule.
			out, stderr, err = runSandboxed(t, policy, "connect-unix", sock, env)
			require.NoError(t, err, stderr)
			assert.Equal(t, "ok", out)
		})
	}
}

func TestRestrictAt_RejectsBelowFloor(t *testing.T) {
	// Safe in-process: the guard returns before anything is confined. The
	// ruleset for each supported ABI is exercised in OlderABIRuleset, in a
	// child.
	for _, abi := range []int{-1, 0, 1, minABI - 1} {
		err := restrictAt(Policy{}, abi)
		require.ErrorIs(t, err, ErrUnsupported, "abi %d", abi)
	}
}
