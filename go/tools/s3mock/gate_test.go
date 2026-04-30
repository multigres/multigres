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

package s3mock

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// newTestS3Client returns an S3 SDK client configured for srv.
func newTestS3Client(srv *Server) *s3.Client {
	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			"test-key", "test-secret", "",
		),
		BaseEndpoint: aws.String(srv.Endpoint()),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // #nosec G402 - test code
				},
			},
		},
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

// putObject uploads body to bucket/key on srv.
func putObject(srv *Server, bucket, key string, body []byte) error {
	return putObjectWithContext(srv, context.Background(), bucket, key, body)
}

// putObjectWithContext uploads body to bucket/key on srv using ctx.
func putObjectWithContext(srv *Server, ctx context.Context, bucket, key string, body []byte) error {
	client := newTestS3Client(srv)
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	return err
}

// getObject downloads bucket/key from srv.
func getObject(srv *Server, bucket, key string) ([]byte, error) {
	client := newTestS3Client(srv)
	resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestGate_WaitReturnsHitOnFirstMatchingPut(t *testing.T) {
	gate := NewGate(MatchPut("data/"))
	srv, err := NewServer(0, WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()
	require.NoError(t, srv.CreateBucket("b"))

	gate.Arm()

	done := make(chan error, 1)
	go func() {
		done <- putObject(srv, "b", "data/file1", []byte("hello"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hit, err := gate.Wait(ctx)
	require.NoError(t, err)
	require.Equal(t, "PUT", hit.Method)
	require.Equal(t, "b", hit.Bucket)
	require.Equal(t, "data/file1", hit.Key)

	gate.Release()
	require.NoError(t, <-done)
}

func TestGate_ReleaseAllowsPutToComplete(t *testing.T) {
	gate := NewGate(MatchPut("data/"))
	srv, err := NewServer(0, WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()
	require.NoError(t, srv.CreateBucket("b"))

	gate.Arm()
	done := make(chan error, 1)
	go func() { done <- putObject(srv, "b", "data/file", []byte("x")) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = gate.Wait(ctx)
	require.NoError(t, err)

	select {
	case err := <-done:
		t.Fatalf("PUT completed before Release: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	gate.Release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not complete after Release")
	}

	body, err := getObject(srv, "b", "data/file")
	require.NoError(t, err)
	require.Equal(t, []byte("x"), body)
}

func TestGate_ContextCancellationUnblocksPut(t *testing.T) {
	gate := NewGate(MatchPut("data/"))
	srv, err := NewServer(0, WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()
	require.NoError(t, srv.CreateBucket("b"))

	gate.Arm()

	putCtx, cancelPut := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- putObjectWithContext(srv, putCtx, "b", "data/x", []byte("x"))
	}()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	_, err = gate.Wait(waitCtx)
	require.NoError(t, err)

	cancelPut() // simulates pgBackRest process being killed mid-upload

	select {
	case err := <-done:
		require.Error(t, err) // SDK reports context cancelled
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not unblock on context cancellation")
	}
}

func TestGate_WaitReturnsCtxErrWhenNoMatch(t *testing.T) {
	gate := NewGate(MatchPut("never/"))
	srv, err := NewServer(0, WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()
	require.NoError(t, srv.CreateBucket("b"))

	gate.Arm()
	// Issue a non-matching PUT — should pass through.
	require.NoError(t, putObject(srv, "b", "other/x", []byte("y")))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = gate.Wait(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMatchDataUpload(t *testing.T) {
	// Loose files in pg_data/ (backup_label.zst, postgresql.auto.conf.zst, etc.)
	require.True(t, MatchDataUpload("b", "stanza/backup/20260428-120000F/pg_data/base/16384/1"))
	require.True(t, MatchDataUpload("b", "stanza/backup/20260428-120000F/pg_data/backup_label.zst"))
	// Bundled small files — the typical pgBackRest data path.
	require.True(t, MatchDataUpload("b", "stanza/backup/20260428-120000F/bundle/1"))
	require.True(t, MatchDataUpload("b", "stanza/backup/20260428-120000F/bundle/42"))
	// Manifests and archive WAL must not match.
	require.False(t, MatchDataUpload("b", "stanza/backup/20260428-120000F/backup.manifest.copy"))
	require.False(t, MatchDataUpload("b", "stanza/backup/20260428-120000F/backup.manifest"))
	require.False(t, MatchDataUpload("b", "stanza/archive/13-1/000000010000000000000001-abc.gz"))
}

func TestPhaseMatcher(t *testing.T) {
	var phase Phase
	m := PhaseMatcher(&phase, "first", MatchPut("data/"))

	require.False(t, m("b", "data/x"), "unset phase must not match")
	phase.Set("first")
	require.True(t, m("b", "data/x"), "phase=first must delegate to base")
	require.False(t, m("b", "other/x"), "phase=first but base mismatch must not match")
	phase.Set("second")
	require.False(t, m("b", "data/x"), "phase=second must not match")
}

func TestGate_RearmEnablesSecondMatch(t *testing.T) {
	gate := NewGate(MatchPut("data/"))
	srv, err := NewServer(0, WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = srv.Stop() }()
	require.NoError(t, srv.CreateBucket("b"))

	gate.Arm()
	done1 := make(chan error, 1)
	go func() { done1 <- putObject(srv, "b", "data/1", []byte("a")) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h1, err := gate.Wait(ctx)
	require.NoError(t, err)
	require.Equal(t, "data/1", h1.Key)
	gate.Release()
	require.NoError(t, <-done1)

	gate.Rearm()
	done2 := make(chan error, 1)
	go func() { done2 <- putObject(srv, "b", "data/2", []byte("b")) }()
	h2, err := gate.Wait(ctx)
	require.NoError(t, err)
	require.Equal(t, "data/2", h2.Key)
	gate.Release()
	require.NoError(t, <-done2)
}
