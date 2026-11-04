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
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestSidecar_CredsRequireToken checks the two ways to reach credentials.
//
//  1. GET /creds on the creds socket without the token (or with a wrong one) is
//     401: loopback and the same UID are not an authentication boundary.
//  2. A pgBackRest TLS client holding the shard certificate connects to the 8432
//     server and configures itself with repo1-s3-key-type=process and a
//     process-cmd of its choosing (the server's remote runs it).
//     Whatever runs there holds no token and no AWS or PGBACKREST_REPO variable,
//     so the client gets no credentials and no repository access.
//
// It fails if the creds handler skips the token check (1 is 200), or if the
// 8432 server is started with the token or a credential in its environment
// (the server's environment and the probe's dump show it, and the client reads
// the repo). The same client first gets a normal answer for a repository that
// needs no credentials, so a broken remote path cannot pass for a refusal.
func TestSidecar_CredsRequireToken(t *testing.T) {
	c := newSidecarCluster(t, 2)
	inst := c.setup.GetPrimary(t)

	credsSock := filepath.Join(inst.PgBackRestd.Paths.StateDir, "creds", "creds.sock")
	client := unixClient(credsSock)
	for name, header := range map[string]string{"missing": "", "wrong": "not-the-token"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://creds/creds", nil)
		require.NoError(t, err)
		if header != "" {
			req.Header.Set("X-Pgbackrestd-Token", header)
		}
		resp, err := client.Do(req)
		require.NoError(t, err, "%s token", name)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "%s token must be refused", name)
	}

	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Fatalf("the pgBackRest client binary is needed for the 8432 probe: %v", err)
	}
	dir := t.TempDir()
	envDump := filepath.Join(dir, "env.out")
	probe := filepath.Join(dir, "probe.sh")
	// The remote runs this as the process-cmd: it dumps what it can see and
	// answers with no credentials.
	require.NoError(t, os.WriteFile(probe, []byte("#!/bin/sh\nenv > "+envDump+"\necho '{}'\n"), 0o755))
	conf := filepath.Join(dir, "client.conf")
	require.NoError(t, os.WriteFile(conf, []byte("[global]\n"), 0o600))

	certs := c.setup.PgBackRestCertPaths
	require.NotNil(t, certs)
	// info runs a pgBackRest client that reaches its repository through the
	// 8432 server, with the shard certificate and the repository options given.
	infoThrough8432 := func(repo ...string) string {
		args := append([]string{
			"--config=" + conf,
			"--stanza=multigres",
			"--repo1-host=127.0.0.1",
			fmt.Sprintf("--repo1-host-port=%d", inst.PgBackRestd.Config.ServerPort),
			"--repo1-host-type=tls",
			"--repo1-host-ca-file=" + certs.CACertFile,
			"--repo1-host-cert-file=" + certs.ServerCertFile,
			"--repo1-host-key-file=" + certs.ServerKeyFile,
		}, repo...)
		cmd := executil.Command(t.Context(), "pgbackrest", append(args, "--log-level-console=detail", "info")...)
		cmd.SetEnv([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir})
		// info exits 0 even when the stanza is unreachable, so the text is the result.
		out, runErr := cmd.CombinedOutput()
		t.Logf("pgbackrest info through 8432 %v (exit: %v):\n%s", repo, runErr, out)
		return string(out)
	}

	// Positive control: the same client, certificate and server answer for a
	// repository that needs no credentials. The remote the server starts lists
	// a directory on the data volume and reports that it holds no stanza. A
	// broken TLS setup, certificate or remote launch cannot produce this
	// answer, so the refusal below is about credentials and nothing else.
	noRepo := filepath.Join(inst.Pgctld.PoolerDir, "no-repository-here")
	control := infoThrough8432("--repo1-type=posix", "--repo1-path="+noRepo)
	require.Contains(t, control, "missing stanza path", "the 8432 remote path must work for a repository that needs no credentials")

	endpoint := strings.TrimPrefix(c.front.URL(), "https://")
	out := infoThrough8432(
		"--repo1-type=s3",
		"--repo1-s3-bucket="+bucket,
		"--repo1-s3-endpoint="+endpoint,
		"--repo1-s3-region=us-east-1",
		"--repo1-s3-key-type=process",
		"--repo1-s3-process-cmd="+probe,
		"--repo1-path=/",
	)
	assert.NotContains(t, out, "status: ok", "the client must not reach the repository")
	require.Contains(t, out, "raised from remote-0 tls protocol", "the refusal must come from the remote the server started")
	if inst.PgBackRestd.Config.Sandbox == "off" {
		// Unconfined, the remote runs the client's process-cmd, which has no
		// credentials to give it.
		assert.Contains(t, out, "access key missing")
		dump, err := os.ReadFile(envDump)
		require.NoError(t, err, "the remote ran the probe, so its environment dump must exist")
		for _, secret := range credentialNames {
			assert.NotContains(t, string(dump), secret, "the 8432 remote's environment must hold no %s", secret)
		}
	} else {
		// Confined, the remote may not execute a file the client names.
		assert.Contains(t, out, "probe.sh terminated unexpectedly")
		assert.NoFileExists(t, envDump, "the sandbox must not let the remote run the client's process-cmd")
	}

	// The environment of the running 8432 server itself, which every remote
	// inherits, read from the process table and not from anything the remote
	// chose to write.
	env := serverEnviron(t, inst)
	require.Contains(t, env, "PGBACKREST_CONFIG="+inst.PgBackRestd.Paths.StateDir, "this must be the 8432 server of this sidecar")
	for _, secret := range credentialNames {
		assert.NotContains(t, env, secret, "the 8432 server's environment must hold no %s", secret)
	}
}

// credentialNames are substrings of every environment variable that carries
// the creds token, an AWS key or a pgBackRest repository secret.
var credentialNames = []string{"PGBACKRESTD_CREDS_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "PGBACKREST_REPO"}

// serverEnviron returns the environment of the `pgbackrest server` process a
// pooler's sidecar runs, as the kernel reports it: /proc/<pid>/environ on
// Linux, and `ps eww` elsewhere.
func serverEnviron(t *testing.T, inst *shardsetup.MultipoolerInstance) string {
	t.Helper()
	marker := "PGBACKREST_CONFIG=" + inst.PgBackRestd.Paths.StateDir
	var found string
	require.Eventually(t, func() bool {
		list, err := executil.Command(t.Context(), "ps", "-axww", "-o", "pid=,command=").Output()
		if err != nil {
			return false
		}
		for line := range strings.SplitSeq(string(list), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 3 || !strings.Contains(fields[1], "pgbackrest") || fields[len(fields)-1] != "server" {
				continue
			}
			var env string
			if runtime.GOOS == "linux" {
				raw, err := os.ReadFile(filepath.Join("/proc", fields[0], "environ"))
				if err != nil {
					continue
				}
				env = strings.ReplaceAll(string(raw), "\x00", "\n")
			} else {
				raw, err := executil.Command(t.Context(), "ps", "eww", "-o", "command=", "-p", fields[0]).Output()
				if err != nil {
					continue
				}
				env = string(raw)
			}
			if strings.Contains(env, marker) {
				found = env
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "no pgbackrest server process with %s in its environment", marker)
	return found
}

// listeningTCPPorts returns the TCP ports pid has a socket in LISTEN state on,
// from /proc/<pid>/fd joined with /proc/<pid>/net/tcp and tcp6.
func listeningTCPPorts(t *testing.T, pid int) []int {
	t.Helper()
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return nil
	}
	inodes := map[string]bool{}
	for _, fd := range fds {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
		if err != nil {
			continue
		}
		if rest, ok := strings.CutPrefix(link, "socket:["); ok {
			inodes[strings.TrimSuffix(rest, "]")] = true
		}
	}
	var ports []int
	for _, table := range []string{"tcp", "tcp6"} {
		// A child can exit between listing and reading, and a container may
		// have no IPv6 table.
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/%s", pid, table))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n")[1:] {
			f := strings.Fields(line)
			// sl local_address rem_address st ... inode; st 0A is LISTEN.
			if len(f) < 10 || f[3] != "0A" || !inodes[f[9]] {
				continue
			}
			var port int
			_, err := fmt.Sscanf(f[1][strings.LastIndex(f[1], ":")+1:], "%X", &port)
			require.NoError(t, err)
			ports = append(ports, port)
		}
	}
	return ports
}

// descendants returns pid and every process below it, from /proc/<n>/stat.
func descendants(t *testing.T, pid int) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)
	children := map[int][]int{}
	for _, e := range entries {
		var n int
		if _, err := fmt.Sscanf(e.Name(), "%d", &n); err != nil {
			continue
		}
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", n))
		if err != nil {
			continue
		}
		// The command name is parenthesised and may hold spaces; ppid follows it.
		rest := string(data[strings.LastIndex(string(data), ")")+2:])
		var state string
		var ppid int
		if _, err := fmt.Sscanf(rest, "%s %d", &state, &ppid); err == nil {
			children[ppid] = append(children[ppid], n)
		}
	}
	all := []int{pid}
	for i := 0; i < len(all); i++ {
		all = append(all, children[all[i]]...)
	}
	return all
}

// TestSidecar_NoTCPExcept8432 lists the TCP sockets the sidecar process and
// its children listen on. The sidecar itself listens on none (the WAL, control
// and credentials APIs are unix sockets) and the only TCP listener below it is
// the pgBackRest server on the server port (8432 in a pod).
//
// It fails if any API is served on a TCP listener, loopback or not: Landlock
// cannot see who connects to a loopback port, and pg_net can reach it.
func TestSidecar_NoTCPExcept8432(t *testing.T) {
	requireLinuxSandbox(t)
	c := newSidecarCluster(t, 2)
	inst := c.setup.GetPrimary(t)
	pid := sidecarPID(inst)
	want := inst.PgBackRestd.Config.ServerPort

	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", want), time.Second)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, time.Minute, 500*time.Millisecond, "the pgBackRest server should be listening on %d", want)

	assert.Empty(t, listeningTCPPorts(t, pid), "the sidecar process itself (pid %d) must not listen on TCP", pid)
	var ports []int
	for _, p := range descendants(t, pid) {
		ports = append(ports, listeningTCPPorts(t, p)...)
	}
	assert.Equal(t, []int{want}, ports, "only the server port may be listening below the sidecar")
}
