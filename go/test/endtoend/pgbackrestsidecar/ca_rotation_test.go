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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
)

// testCA is a certificate authority that signs the certificates an S3 front
// presents.
type testCA struct {
	pem  []byte
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, commonName string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), cert: cert, key: key}
}

// serverCert is a certificate for 127.0.0.1 signed by the CA.
func (ca *testCA) serverCert(t *testing.T) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// projectedCA is a CA file laid out the way the kubelet lays out a projected
// volume: the directory holds a timestamped directory with the real file, a
// ..data symlink to that directory, and ca.crt as a symlink through ..data.
// A new version is a new timestamped directory and an atomic replacement of
// ..data; the old directory is then removed.
type projectedCA struct {
	dir     string
	version int
	current string
}

func newProjectedCA(t *testing.T, content []byte) *projectedCA {
	t.Helper()
	// Outside the cluster directory: the sidecar refuses a CA file on the data
	// volume.
	dir, err := os.MkdirTemp("/tmp", "s3-ca-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	require.NoError(t, os.Chmod(dir, 0o755))
	p := &projectedCA{dir: dir}
	p.swap(t, content)
	require.NoError(t, os.Symlink(filepath.Join("..data", "ca.crt"), p.path()))
	return p
}

// path is the path a pod would name: <mount>/ca.crt.
func (p *projectedCA) path() string { return filepath.Join(p.dir, "ca.crt") }

// swap publishes content as the new version of the file.
func (p *projectedCA) swap(t *testing.T, content []byte) {
	t.Helper()
	p.version++
	next := fmt.Sprintf("..2026_11_26_00_00_%02d.%d", p.version, time.Now().UnixNano())
	require.NoError(t, os.Mkdir(filepath.Join(p.dir, next), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(p.dir, next, "ca.crt"), content, 0o644))
	tmp := filepath.Join(p.dir, "..data_tmp")
	require.NoError(t, os.Symlink(next, tmp))
	require.NoError(t, os.Rename(tmp, filepath.Join(p.dir, "..data")))
	if p.current != "" {
		require.NoError(t, os.RemoveAll(filepath.Join(p.dir, p.current)))
	}
	p.current = next
}

// TestSidecar_S3CASwapReachesNewCommands replaces the CA file under a running
// sidecar, with the file mounted the way a Kubernetes projected volume mounts
// it, and under Landlock. The endpoint switches to a certificate signed by
// the new CA at the same time. pgBackRest loads the file when a process
// creates its S3 client, and the sidecar builds the Landlock rule for the
// file when it starts each pgBackRest process, so the next archive-push is
// granted, and verifies with, the new file. The sidecar is not restarted.
//
// It fails if the Landlock rule stays bound to the file that existed when the
// sidecar started: the kubelet removes that file, and every later command
// would be denied the new one.
func TestSidecar_S3CASwapReachesNewCommands(t *testing.T) {
	requireSidecarHost(t)
	requireLinuxSandbox(t)
	t.Parallel()
	clusterSlots <- struct{}{}
	t.Cleanup(func() { <-clusterSlots })

	caA, caB := newTestCA(t, "rotation test CA A"), newTestCA(t, "rotation test CA B")
	var endpointCert atomic.Pointer[tls.Certificate]
	endpointCert.Store(caA.serverCert(t))

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s3Server.Stop() })
	require.NoError(t, s3Server.CreateBucket(bucket))
	s3Server.RequireSessions()
	front := newS3FrontWithCert(t, s3Server, &endpointCert)
	// Static keys: the sidecar then makes no S3 request of its own, so
	// pgBackRest is the only reader of the CA file after startup.
	keys := s3Server.IssueSession(2 * time.Hour)
	mount := newProjectedCA(t, caA.pem)

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithS3Backup(bucket, "us-east-1", front.URL()),
		shardsetup.WithS3CAFile(mount.path()),
		shardsetup.WithS3StaticKey(keys.AccessKeyID, keys.SecretAccessKey, keys.SessionToken),
	)
	t.Cleanup(cleanup)
	c := &sidecarCluster{setup: setup, s3: s3Server, front: front}
	primary := setup.GetPrimary(t)
	db := openSocketDB(t, primary)
	pid := sidecarPID(primary)
	require.Contains(t, clientConf(t, primary), "repo1-storage-ca-file="+mount.path())

	waitArchived(t, c, primary, writeAndSwitch(t, db, "ca-a"), 3*time.Minute)

	mount.swap(t, caB.pem)
	endpointCert.Store(caB.serverCert(t))
	waitArchived(t, c, primary, writeAndSwitch(t, db, "ca-b"), 3*time.Minute)

	require.True(t, primary.PgBackRestd.IsRunning(), "the sidecar should still be running")
	assert.Equal(t, pid, sidecarPID(primary), "the sidecar must not have been restarted")
}
