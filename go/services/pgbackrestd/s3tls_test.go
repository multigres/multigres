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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/sandbox"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// selfSignedPEM is a self-signed certificate that is not marked as a CA, the
// kind a self-hosted S3 service generates for itself.
func selfSignedPEM(t *testing.T, name string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{name},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeCA(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s3-ca.pem")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	return path
}

// s3Location makes a test config an S3 one with no credential source.
func s3Location(c *Config) {
	c.LocationType, c.FSRepoPath = LocationS3, ""
	c.S3Bucket, c.S3Region, c.S3Endpoint = "b", "r", "s3.example.internal:9000"
	c.S3CredSource = CredNone
}

// captureDefaultLog routes the default logger, which a Server takes when it is
// built, into a buffer for the test.
func captureDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func TestConfigValidate_S3CAFile(t *testing.T) {
	good := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	bundle := writeCA(t, append(selfSignedPEM(t, "other-ca"), selfSignedPEM(t, "s3.example.internal")...))
	notPEM := writeCA(t, []byte("not a certificate"))
	for name, tc := range map[string]struct {
		ca      string
		skip    bool
		wantErr string
	}{
		"a self-signed certificate":  {ca: good},
		"a bundle":                   {ca: bundle},
		"verification off":           {skip: true},
		"neither":                    {},
		"missing file":               {ca: filepath.Join(t.TempDir(), "absent.pem"), wantErr: "--s3-ca-file"},
		"no certificate in the file": {ca: notPEM, wantErr: "holds no PEM certificate"},
		"relative path":              {ca: "ca.pem", wantErr: "must be an absolute path"},
		"a CA with verification off": {ca: good, skip: true, wantErr: "cannot be combined"},
		"on the data volume":         {ca: "@datavolume", wantErr: "is on the data volume"},
		"a link to the data volume":  {ca: "@link", wantErr: "on the data volume"},
		"below a link to the volume": {ca: "@linkdir", wantErr: "on the data volume"},
		"a projected volume":         {ca: "@projected"},
		"a directory":                {ca: "@dir", wantErr: "not a regular file"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			s3Location(&cfg)
			cfg.S3CAFile, cfg.S3SkipTLSVerify = tc.ca, tc.skip
			if tc.ca == "@datavolume" {
				cfg.S3CAFile = filepath.Join(cfg.DataVolume, "s3-ca.pem")
				require.NoError(t, os.WriteFile(cfg.S3CAFile, selfSignedPEM(t, "s3.example.internal"), 0o600))
			}
			outside := t.TempDir()
			onVolume := func() string {
				dir := filepath.Join(cfg.DataVolume, "planted")
				require.NoError(t, os.MkdirAll(dir, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), selfSignedPEM(t, "attacker"), 0o600))
				return dir
			}
			switch tc.ca {
			case "@link":
				// The path is outside the volume; the file it leads to is not.
				cfg.S3CAFile = filepath.Join(outside, "ca.crt")
				require.NoError(t, os.Symlink(filepath.Join(onVolume(), "ca.crt"), cfg.S3CAFile))
			case "@linkdir":
				require.NoError(t, os.Symlink(onVolume(), filepath.Join(outside, "s3-ca")))
				cfg.S3CAFile = filepath.Join(outside, "s3-ca", "ca.crt")
			case "@projected":
				// The kubelet's layout: links, all of them outside the volume.
				real := filepath.Join(outside, "..2026_11_26")
				require.NoError(t, os.Mkdir(real, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(real, "ca.crt"), selfSignedPEM(t, "s3.example.internal"), 0o644))
				require.NoError(t, os.Symlink("..2026_11_26", filepath.Join(outside, "..data")))
				cfg.S3CAFile = filepath.Join(outside, "ca.crt")
				require.NoError(t, os.Symlink(filepath.Join("..data", "ca.crt"), cfg.S3CAFile))
			case "@dir":
				cfg.S3CAFile = outside
			}
			err := cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// The file behind the CA path can change after startup. A child started when
// the path leads onto the data volume gets no rule for it, so it cannot read a
// CA that postgres could have written, and the next child gets the rule again
// once the path leads elsewhere.
func TestPolicy_S3CAFileOnTheDataVolumeIsNotGranted(t *testing.T) {
	cfg := testConfig(t)
	s3Location(&cfg)
	dir := t.TempDir()
	good := filepath.Join(dir, "good.crt")
	require.NoError(t, os.WriteFile(good, selfSignedPEM(t, "s3.example.internal"), 0o644))
	planted := filepath.Join(cfg.DataVolume, "planted.crt")
	require.NoError(t, os.WriteFile(planted, selfSignedPEM(t, "attacker"), 0o600))
	cfg.S3CAFile = filepath.Join(dir, "ca.crt")
	hasRule := func() bool {
		for _, r := range policyFor(cmdArchivePush, cfg, "/self").Rules {
			if r.Path == cfg.S3CAFile {
				return true
			}
		}
		return false
	}

	require.NoError(t, os.Symlink(good, cfg.S3CAFile))
	require.True(t, hasRule())
	require.NoError(t, os.Remove(cfg.S3CAFile))
	require.NoError(t, os.Symlink(planted, cfg.S3CAFile))
	require.False(t, hasRule(), "a child must not be granted a CA that lies on the data volume")
	require.NoError(t, os.Remove(cfg.S3CAFile))
	require.NoError(t, os.Symlink(good, cfg.S3CAFile))
	require.True(t, hasRule())
}

// The rendered conf is what pgBackRest verifies the endpoint with.
func TestRender_S3CAFileAndVerifyOff(t *testing.T) {
	ca := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	for name, tc := range map[string]struct {
		mutate      func(*Config)
		wantCA      string
		wantVerify  bool
		wantWarning bool
	}{
		"default":          {mutate: func(*Config) {}, wantVerify: true},
		"CA file":          {mutate: func(c *Config) { c.S3CAFile = ca }, wantCA: ca, wantVerify: true},
		"verification off": {mutate: func(c *Config) { c.S3SkipTLSVerify = true }, wantWarning: true},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureDefaultLog(t)
			h := newControlHarness(t, func(c *Config) {
				s3Location(c)
				tc.mutate(c)
			})
			const warning = "TLS certificate verification of the S3 endpoint is OFF"
			assert.Equal(t, tc.wantWarning, strings.Contains(logs.String(), warning+` (--s3-skip-tls-verify)`) && strings.Contains(logs.String(), "when=start"),
				"the warning at start: %s", logs)

			logs.Reset()
			require.True(t, h.configure(t, 1, repos(1, 1, 2, 2)).Accepted)
			conf := readFile(t, h.s.cfg.stateSub(fileClientConf))
			for _, repo := range []string{"repo1", "repo2"} {
				if tc.wantCA != "" {
					assert.Contains(t, conf, repo+"-storage-ca-file="+tc.wantCA)
				} else {
					assert.NotContains(t, conf, repo+"-storage-ca-file")
				}
				if tc.wantVerify {
					assert.NotContains(t, conf, "verify-tls")
				} else {
					assert.Contains(t, conf, repo+"-storage-verify-tls=n")
				}
			}
			assert.Equal(t, tc.wantWarning, strings.Contains(logs.String(), warning) && strings.Contains(logs.String(), "when=configure") && strings.Contains(logs.String(), "level=WARN"),
				"the warning at Configure: %s", logs)

			// Every Configure that applies it warns again.
			logs.Reset()
			require.True(t, h.configure(t, 2, repos(1, 1, 2, 2)).Accepted)
			assert.Equal(t, tc.wantWarning, strings.Contains(logs.String(), warning))

			st, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
			require.NoError(t, err)
			assert.Equal(t, tc.wantWarning, st.Msg.GetS3TlsVerifyDisabled(), "Status must say whether verification is off")
		})
	}
}

// A filesystem repository has no endpoint to verify: the switch is not
// reported there, and nothing is rendered.
func TestRender_VerifyOffMeansNothingWithoutS3(t *testing.T) {
	logs := captureDefaultLog(t)
	h := newControlHarness(t, func(c *Config) { c.S3SkipTLSVerify = true })
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	assert.NotContains(t, readFile(t, h.s.cfg.stateSub(fileClientConf)), "verify-tls")
	assert.NotContains(t, logs.String(), "verification of the S3 endpoint is OFF")
	st, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	assert.False(t, st.Msg.GetS3TlsVerifyDisabled())
}

// The CA file is outside everything else the policy grants, so each command
// that talks to the repository needs a read rule on exactly that file, and
// the 8432 server, which does not, gets none.
func TestPolicy_S3CAFileIsReadableByRepositoryCommandsOnly(t *testing.T) {
	ca := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	cfg := testConfig(t)
	s3Location(&cfg)
	cfg.S3CAFile = ca
	for _, cmd := range s3Commands {
		p := policyFor(cmd, cfg, "/self")
		if cmd == cmdStart || cmd == cmdStop || cmd == cmdVersion {
			// They do not open the repository.
			assert.False(t, grants(p, ca, sandbox.Read), "%s must not be given the CA file", cmd)
			continue
		}
		assert.True(t, grants(p, ca, sandbox.Read), "%s must read the CA file", cmd)
		assert.False(t, grants(p, ca, sandbox.Write), "%s must not write the CA file", cmd)
		assert.False(t, grants(p, filepath.Join(filepath.Dir(ca), "sibling"), sandbox.Read), "%s: the rule is on the file, not its directory", cmd)
	}
	assert.False(t, grants(policyFor(cmdServer, cfg, "/self"), ca, sandbox.Read))

	off := cfg
	off.S3SkipTLSVerify = true
	assert.False(t, grants(policyFor(cmdBackup, off, "/self"), ca, sandbox.Read), "no CA is read when verification is off")
}

// The CA file and the verification switch are part of what multipooler
// compares, and a location without them hashes as it did before they existed.
func TestLocationFingerprint_CoversTLSSettings(t *testing.T) {
	ca := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	base := testConfig(t)
	s3Location(&base)
	withCA, off := base, base
	withCA.S3CAFile = ca
	off.S3SkipTLSVerify = true

	assert.NotEqual(t, base.locationFingerprint(), withCA.locationFingerprint())
	assert.NotEqual(t, base.locationFingerprint(), off.locationFingerprint())
	assert.NotEqual(t, withCA.locationFingerprint(), off.locationFingerprint())
	assert.Equal(t, backup.LocationFingerprint{
		LocationType: LocationS3, S3Bucket: "b", S3Region: "r", S3Endpoint: "s3.example.internal:9000", S3KeyPrefix: base.S3KeyPrefix,
		S3CAFile: ca,
	}.Hash(), withCA.locationFingerprint())

	// The repository itself is the same one however its endpoint is verified.
	scope, err := base.repositoryScope()
	require.NoError(t, err)
	scopeCA, err := withCA.repositoryScope()
	require.NoError(t, err)
	assert.Equal(t, scope, scopeCA)
}

func TestTLSVerifyDisabledGauge(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*Config)
		want   int64
	}{
		"verifying":        {mutate: s3Location, want: 0},
		"verification off": {mutate: func(c *Config) { s3Location(c); c.S3SkipTLSVerify = true }, want: 1},
	} {
		t.Run(name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			prev := otel.GetMeterProvider()
			otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
			t.Cleanup(func() { otel.SetMeterProvider(prev) })

			s := newTestServer(t, tc.mutate)
			s.registerTLSVerifyMetric()
			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))
			found := false
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "pgbackrest.s3.tls_verify_disabled" {
						continue
					}
					g, ok := m.Data.(metricdata.Gauge[int64])
					require.True(t, ok)
					require.Len(t, g.DataPoints, 1)
					assert.Equal(t, tc.want, g.DataPoints[0].Value)
					found = true
				}
			}
			assert.True(t, found, "the gauge must be exported")
		})
	}
}
