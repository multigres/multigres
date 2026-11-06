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

package pgbackrestd

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

// writeSelfSignedCertDir fills dir with a certificate pgBackRest's TLS server
// will load; the same certificate is its own CA.
func writeSelfSignedCertDir(t *testing.T, dir string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pgbackrest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, keyPath, ca := constants.PgBackRestCertFiles(dir)
	require.NoError(t, os.WriteFile(cert, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(ca, certPEM, 0o600))
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The state dir has no log/ directory and the sandbox grants no write on one,
// so a server conf that leaves file logging on makes pgBackRest warn about an
// unopenable log file on every start.
func TestSandbox_RealServerAttemptsNoLogFile(t *testing.T) {
	if _, err := exec.LookPath("pgbackrest"); err != nil {
		t.Skip("pgbackrest is not installed")
	}
	if os.Geteuid() == 0 {
		t.Skip("pgBackRest refuses to run as root; run this test as an unprivileged user")
	}
	s := sandboxedServerWithProbe(t, false, func(c *Config) {
		c.TLSCertDir = t.TempDir()
		writeSelfSignedCertDir(t, c.TLSCertDir)
		c.ServerPort = 18432
	})
	require.NoError(t, s.renderServerConf())

	ctx, cancel := context.WithCancel(context.Background())
	var stderr lockedBuffer
	done := make(chan error, 1)
	go func() {
		_, err := s.runChild(ctx, KindServer, cmdServer, nil, &stderr, &stderr)
		done <- err
	}()
	// A server that fails to start exits at once; one that started stays up.
	select {
	case err := <-done:
		cancel()
		t.Fatalf("the server exited early: err=%v stderr=%s", err, stderr.String())
	case <-time.After(3 * time.Second):
	}
	cancel()
	<-done

	out := stderr.String()
	assert.NotContains(t, out, "unable to open log file", "stderr=%s", out)
	assert.NotContains(t, strings.ToLower(out), "warn", "stderr=%s", out)
}
