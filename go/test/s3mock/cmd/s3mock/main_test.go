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

// go/test/s3mock/cmd/s3mock/main_test.go
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestCertOutFlag(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "s3mock")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("building s3mock: %v\n%s", err, out)
	}
	certPath := filepath.Join(t.TempDir(), "cert.pem")

	cmd := exec.Command(bin, "--port", "0", "--cert-out", certPath, "bkt")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Reap in the background: StopProcess waits for the pid to vanish, which a
	// zombie child never does.
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_, _ = executil.StopProcess(context.Background(), cmd.Process)
		<-exited
	})

	endpoint := ""
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "Endpoint: "); ok {
			endpoint = v
			break
		}
	}
	if endpoint == "" {
		t.Fatal("s3mock never printed its endpoint")
	}

	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("--cert-out file missing at startup: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("--cert-out file is not a PEM certificate")
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
	}
	resp, err := client.Get(endpoint + "/")
	if err != nil {
		t.Fatalf("client trusting the exported cert failed: %v", err)
	}
	resp.Body.Close()
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cert.pem")
	want := []byte(strings.Repeat("x", 1<<20))

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			got, err := os.ReadFile(path)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				done <- err
				return
			}
			if len(got) != len(want) {
				done <- fmt.Errorf("observed partial file: %d of %d bytes", len(got), len(want))
				return
			}
		}
	}()
	for range 50 {
		if err := writeFileAtomic(path, want, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("mode = %v, want 0644", info.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}
}

func TestWriteFileAtomic_FailureLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	if err := writeFileAtomic(filepath.Join(dir, "missing", "cert.pem"), []byte("x"), 0o644); err == nil {
		t.Fatal("expected an error for a missing directory")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("unexpected entries: %v", entries)
	}
}
