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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Credentials is one set of temporary S3 credentials.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// credSource produces fresh credentials on every call. Caching is the
// caller's business.
type credSource interface {
	Get(ctx context.Context) (Credentials, error)
}

// maxCredResponse bounds what a credential endpoint can make PID 1 read.
const maxCredResponse = 1 << 20

// stsRegion is the region STS is called in: the pod's own, which is where the
// web identity token is valid, and only failing that the bucket's. An
// opt-in STS region would otherwise reject a cross-region bucket's call.
func stsRegion(bucketRegion string, getenv func(string) string) string {
	for _, k := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
		if v := getenv(k); v != "" {
			return v
		}
	}
	return bucketRegion
}

// readCapped reads at most maxCredResponse bytes and fails, rather than
// truncates, when there is more.
func readCapped(r io.Reader, what string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxCredResponse+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", what, err)
	}
	if len(b) > maxCredResponse {
		return nil, fmt.Errorf("%s exceeds %d bytes", what, maxCredResponse)
	}
	return b, nil
}

// readCappedFile is readCapped on a file, so a token path pointed at
// something huge cannot exhaust PID 1's memory.
func readCappedFile(path, what string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", what, err)
	}
	defer f.Close()
	return readCapped(f, what)
}

// hostIsLocal reports whether host is loopback (and, when linkLocal is set,
// also a link-local or private address such as the Pod Identity agent's).
func hostIsLocal(host string, linkLocal bool) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	// The IPv6 Pod Identity agent address, fd00:ec2::23, is unique-local
	// rather than link-local, hence IsPrivate.
	return linkLocal && (ip.IsLinkLocalUnicast() || ip.IsPrivate())
}

// validateCredURL accepts https anywhere, and plain http only for a host a
// network peer cannot sit in front of.
func validateCredURL(raw, flag string, linkLocal bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s is not a URL: %w", flag, err)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !hostIsLocal(u.Hostname(), linkLocal) {
			return fmt.Errorf("%s %q: plain http is allowed only for a local host, use https", flag, raw)
		}
	default:
		return fmt.Errorf("%s %q must be an http(s) URL", flag, raw)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("%s %q has no host", flag, raw)
	}
	return nil
}

// newCredSource builds the source named by cfg.S3CredSource. Environment
// reads happen here, in PID 1, and nowhere else: no child ever sees them.
func newCredSource(cfg Config) (credSource, error) {
	switch cfg.S3CredSource {
	case CredWebIdentity:
		return newWebIdentitySource(stsRegion(cfg.S3Region, os.Getenv), os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"), os.Getenv("AWS_ROLE_ARN"), nil)
	case CredPodIdentity:
		return newPodIdentitySource(os.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"), os.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"))
	case CredEndpoint:
		return newEndpointSource(cfg.S3CredEndpoint, cfg.S3CredCAFile)
	case CredStatic:
		return unservedSource{CredStatic}, nil
	default:
		return unservedSource{cfg.S3CredSource}, nil
	}
}

// unservedSource is for credential sources that are not served over
// creds.sock. Static keys reach pgBackRest through its environment with
// key-type=shared, so a request here is a configuration mistake.
type unservedSource struct{ name string }

func (u unservedSource) Get(context.Context) (Credentials, error) {
	return Credentials{}, fmt.Errorf("credential source %q is not served over the credential socket", u.name)
}

// credJSON is the credential document in both directions. Container
// credential endpoints name the session token field Token; the
// pgBackRest-facing protocol names it SessionToken.
type credJSON struct {
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string
	SessionToken    string
	Token           string
	Expiration      string
}

func (j credJSON) credentials() (Credentials, error) {
	if j.AccessKeyID == "" || j.SecretAccessKey == "" {
		return Credentials{}, errors.New("credential response lacks AccessKeyId or SecretAccessKey")
	}
	exp, err := time.Parse(time.RFC3339, j.Expiration)
	if err != nil {
		return Credentials{}, fmt.Errorf("credential response Expiration: %w", err)
	}
	tok := j.SessionToken
	if tok == "" {
		tok = j.Token
	}
	return Credentials{
		AccessKeyID:     j.AccessKeyID,
		SecretAccessKey: j.SecretAccessKey,
		SessionToken:    tok,
		Expiration:      exp.UTC(),
	}, nil
}

func decodeCredResponse(resp *http.Response) (Credentials, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// The body is not read or echoed: it may carry token material.
		return Credentials{}, fmt.Errorf("credential endpoint answered %s", resp.Status)
	}
	body, err := readCapped(resp.Body, "credential response")
	if err != nil {
		return Credentials{}, err
	}
	var j credJSON
	if err := json.Unmarshal(body, &j); err != nil {
		return Credentials{}, fmt.Errorf("decode credential response: %w", err)
	}
	return j.credentials()
}

// webIdentitySource exchanges the projected service account token for role
// credentials. The token file is re-read on every call because the kubelet
// rotates it.
type webIdentitySource struct {
	tokenFile string
	roleARN   string
	client    *sts.Client
}

// newWebIdentitySource builds the source. A nil client gets a regional STS
// client; AssumeRoleWithWebIdentity is unsigned, so it carries no credentials.
func newWebIdentitySource(region, tokenFile, roleARN string, client *sts.Client) (*webIdentitySource, error) {
	if tokenFile == "" || roleARN == "" {
		return nil, errors.New("web-identity needs AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN in the environment")
	}
	if client == nil {
		client = sts.New(sts.Options{Region: region, Credentials: aws.AnonymousCredentials{}})
	}
	return &webIdentitySource{tokenFile: tokenFile, roleARN: roleARN, client: client}, nil
}

func (w *webIdentitySource) Get(ctx context.Context) (Credentials, error) {
	token, err := readCappedFile(w.tokenFile, "web identity token")
	if err != nil {
		return Credentials{}, err
	}
	out, err := w.client.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(w.roleARN),
		RoleSessionName:  aws.String("pgbackrestd"),
		WebIdentityToken: aws.String(strings.TrimSpace(string(token))),
	})
	if err != nil {
		return Credentials{}, fmt.Errorf("assume role with web identity: %w", err)
	}
	c := out.Credentials
	if c == nil || c.AccessKeyId == nil || c.SecretAccessKey == nil || c.SessionToken == nil || c.Expiration == nil {
		return Credentials{}, errors.New("STS returned incomplete credentials")
	}
	return Credentials{
		AccessKeyID:     *c.AccessKeyId,
		SecretAccessKey: *c.SecretAccessKey,
		SessionToken:    *c.SessionToken,
		Expiration:      c.Expiration.UTC(),
	}, nil
}

// podIdentitySource reads EKS Pod Identity credentials from the agent.
type podIdentitySource struct {
	uri       string
	tokenFile string
	client    *http.Client
}

func newPodIdentitySource(uri, tokenFile string) (*podIdentitySource, error) {
	if uri == "" || tokenFile == "" {
		return nil, errors.New("pod-identity needs AWS_CONTAINER_CREDENTIALS_FULL_URI and AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE in the environment")
	}
	if err := validateCredURL(uri, "AWS_CONTAINER_CREDENTIALS_FULL_URI", true); err != nil {
		return nil, err
	}
	return &podIdentitySource{uri: uri, tokenFile: tokenFile, client: &http.Client{Timeout: 15 * time.Second}}, nil
}

func (p *podIdentitySource) Get(ctx context.Context) (Credentials, error) {
	token, err := readCappedFile(p.tokenFile, "pod identity token")
	if err != nil {
		return Credentials{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.uri, nil)
	if err != nil {
		return Credentials{}, fmt.Errorf("build pod identity request: %w", err)
	}
	req.Header.Set("Authorization", strings.TrimSpace(string(token)))
	resp, err := p.client.Do(req)
	if err != nil {
		return Credentials{}, fmt.Errorf("pod identity request: %w", err)
	}
	return decodeCredResponse(resp)
}

// endpointSource POSTs to a URL that answers in the creds protocol. It exists
// for end-to-end tests against s3mock's session endpoint, which is TLS with a
// self-signed certificate: the CA file is that certificate, and verification
// is never skipped.
type endpointSource struct {
	url    string
	client *http.Client
}

func newEndpointSource(endpoint, caFile string) (*endpointSource, error) {
	if err := validateCredURL(endpoint, "--s3-cred-endpoint", false); err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	if caFile != "" {
		pem, err := readCappedFile(caFile, "credential endpoint CA")
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("credential endpoint CA %q holds no certificate", caFile)
		}
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	}
	return &endpointSource{url: endpoint, client: client}, nil
}

func (e *endpointSource) Get(ctx context.Context) (Credentials, error) {
	// The session admin endpoint is exempt from the mock's session check, so
	// the request needs no signature.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, nil)
	if err != nil {
		return Credentials{}, fmt.Errorf("build credential endpoint request: %w", err)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return Credentials{}, fmt.Errorf("credential endpoint request: %w", err)
	}
	return decodeCredResponse(resp)
}
