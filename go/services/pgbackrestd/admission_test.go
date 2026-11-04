//go:build !linux

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
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// Status with after_configure waits for a Configure that is between reading
// the key files and publishing, and then reports what that Configure
// published. A plain Status answers at once with the previous revision. A
// caller that replaced the key files uses the first form: once it has an
// answer, every Configure that could have read the old files is finished.
func TestStatus_AfterConfigureWaitsForTheConfigureInProgress(t *testing.T) {
	s := newTestServer(t, nil)
	_, err := s.configure(&pb.ConfigureRequest{LocationFingerprint: s.cfg.locationFingerprint(), Revision: 1, Repos: repos(1, 1)})
	require.NoError(t, err)

	entered, release := make(chan struct{}), make(chan struct{})
	testHookConfigureSnapshotBuilt = func() { close(entered); <-release }
	t.Cleanup(func() { testHookConfigureSnapshotBuilt = nil })
	configured := make(chan error, 1)
	go func() {
		_, err := s.configure(&pb.ConfigureRequest{LocationFingerprint: s.cfg.locationFingerprint(), Revision: 2, Repos: repos(1, 1)})
		configured <- err
	}()
	<-entered

	svc := &controlService{s: s}
	plain, err := svc.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	require.Equal(t, int64(1), plain.Msg.GetRevision(), "the Configure in progress has published nothing yet")

	answered := make(chan *pb.StatusResponse, 1)
	go func() {
		resp, err := svc.Status(context.WithoutCancel(t.Context()), connect.NewRequest(&pb.StatusRequest{AfterConfigure: true}))
		if err == nil {
			answered <- resp.Msg
		}
	}()
	select {
	case <-answered:
		t.Fatal("Status with after_configure answered while a Configure was in progress")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-configured)
	select {
	case st := <-answered:
		assert.Equal(t, int64(2), st.GetRevision())
	case <-time.After(10 * time.Second):
		t.Fatal("Status with after_configure never answered")
	}
}

// A Configure names the location fingerprint its caller expects. A sidecar
// started for another location, CA file or TLS verification setting refuses
// it and stays as it was.
func TestConfigure_RefusesAnotherLocationFingerprint(t *testing.T) {
	s := newTestServer(t, nil)
	own := s.cfg.locationFingerprint()

	_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1), LocationFingerprint: "not-" + own})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.False(t, s.configured(), "a refused Configure must configure nothing")
	_, statErr := os.Stat(s.cfg.stateSub(fileGenerations))
	assert.True(t, errors.Is(statErr, os.ErrNotExist), "a refused Configure must persist nothing")

	resp, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1), LocationFingerprint: own})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
}

// A Configure that names no fingerprint comes from a caller that compared
// nothing: a multipooler build that does not send one sees a restarted
// sidecar as unconfigured and re-sends its list. It is refused whether or not
// the sidecar is configured, and changes nothing.
func TestConfigure_RefusesNoLocationFingerprint(t *testing.T) {
	s := newTestServer(t, nil)

	_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.ErrorContains(t, err, "names no location fingerprint")
	assert.False(t, s.configured(), "a Configure without a fingerprint must not establish a configuration")
	_, statErr := os.Stat(s.cfg.stateSub(fileGenerations))
	assert.True(t, errors.Is(statErr, os.ErrNotExist), "a refused Configure must persist nothing")

	_, err = s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1), LocationFingerprint: s.cfg.locationFingerprint()})
	require.NoError(t, err)
	_, err = s.configure(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1)})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	rev, _ := s.held()
	assert.Equal(t, int64(1), rev, "a Configure without a fingerprint must not change a configured sidecar")
}

// The wait of Status with after_configure ends with its request. A Configure
// holds its lock across file writes that a stalled volume can hold up without
// bound, and a handler that outlived every cancelled request would pile up.
func TestStatus_AfterConfigureWaitEndsWithTheRequest(t *testing.T) {
	s := newTestServer(t, nil)
	own := s.cfg.locationFingerprint()
	_, err := s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1), LocationFingerprint: own})
	require.NoError(t, err)

	entered, release := make(chan struct{}), make(chan struct{})
	testHookConfigureSnapshotBuilt = func() { close(entered); <-release }
	t.Cleanup(func() { testHookConfigureSnapshotBuilt = nil })
	configured := make(chan error, 1)
	go func() {
		_, err := s.configure(&pb.ConfigureRequest{Revision: 2, Repos: repos(1, 1), LocationFingerprint: own})
		configured <- err
	}()
	<-entered

	svc := &controlService{s: s}
	for name, code := range map[string]connect.Code{"cancelled": connect.CodeCanceled, "deadline": connect.CodeDeadlineExceeded} {
		ctx, cancel := context.WithCancel(t.Context())
		if code == connect.CodeDeadlineExceeded {
			ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
		}
		returned := make(chan error, 1)
		go func() {
			_, err := svc.Status(ctx, connect.NewRequest(&pb.StatusRequest{AfterConfigure: true}))
			returned <- err
		}()
		if code == connect.CodeCanceled {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}
		select {
		case err := <-returned:
			require.Error(t, err, name)
			assert.Equal(t, code, connect.CodeOf(err), name)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: the Status handler outlived its request while the Configure was still paused", name)
		}
		cancel()
	}
	select {
	case err := <-configured:
		t.Fatalf("the Configure finished before it was released: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-configured)
	rev, _ := s.held()
	assert.Equal(t, int64(2), rev, "a Status that gave up must not disturb the Configure")
}

// A sidecar that set its saved state aside refuses every revision below the
// one that state held. A standby whose own copy of the list is older, and
// whose postgres needs WAL from the archive before it can be asked for a
// newer one, would never get its sidecar configured. So the sidecar reports
// the list it set aside, and applies it when a caller that expects this
// sidecar's fingerprint sends it back at the same revision.
func TestState_SetAsideListIsOfferedAndAppliedAtItsRevision(t *testing.T) {
	ca := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	held := repos(1, 1)
	for name, prepare := range map[string]func(t *testing.T, first *Server) Config{
		"saved under other TLS settings": func(t *testing.T, first *Server) Config {
			cfg := first.cfg
			cfg.S3CAFile, cfg.S3SkipTLSVerify = "", true
			return cfg
		},
		"saved by a build that recorded no fingerprint": func(t *testing.T, first *Server) Config {
			path := first.cfg.stateSub(fileGenerations)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(raw, &fields))
			delete(fields, "location_fingerprint")
			raw, err = json.Marshal(fields)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, raw, 0o600))
			return first.cfg
		},
	} {
		t.Run(name, func(t *testing.T) {
			first := newTestServer(t, func(c *Config) { s3Location(c); c.S3CAFile = ca })
			_, err := first.configure(&pb.ConfigureRequest{Revision: 4, Repos: held, LocationFingerprint: first.cfg.locationFingerprint()})
			require.NoError(t, err)
			require.NoError(t, first.Close())

			// Twice: the offer is rebuilt from the kept file at every start.
			cfg := prepare(t, first)
			var s *Server
			for range 2 {
				if s != nil {
					require.NoError(t, s.Close())
				}
				s, err = newForTest(cfg)
				require.NoError(t, err)
				require.NoError(t, s.loadState())
			}
			defer func() { require.NoError(t, s.Close()) }()
			own := s.cfg.locationFingerprint()
			st := s.status(t.Context())
			require.False(t, st.GetConfigured())
			require.Equal(t, int64(4), st.GetRevision())
			require.Empty(t, st.GetRepos(), "a list that was not loaded is not reported as held")
			require.True(t, proto.Equal(&pb.ConfigureRequest{Repos: held}, &pb.ConfigureRequest{Repos: st.GetSetAsideRepos()}),
				"set aside: %v", st.GetSetAsideRepos())

			// The caller's older copy is still refused.
			resp, err := s.configure(&pb.ConfigureRequest{Revision: 3, Repos: repos(1, 1), LocationFingerprint: own})
			require.NoError(t, err)
			require.False(t, resp.GetAccepted())
			require.False(t, s.configured())

			resp, err = s.configure(&pb.ConfigureRequest{Revision: st.GetRevision(), Repos: st.GetSetAsideRepos(), LocationFingerprint: own})
			require.NoError(t, err)
			require.True(t, resp.GetAccepted())
			after := s.status(t.Context())
			assert.True(t, after.GetConfigured())
			assert.Empty(t, after.GetSetAsideRepos())
			assert.Empty(t, after.GetStateError())
		})
	}
}

// A saved state that does not decode offers no list: nothing shows what the
// sidecar held at the revision the floor comes from.
func TestState_UnreadableSetAsideStateOffersNoList(t *testing.T) {
	first := newTestServer(t, nil)
	_, err := first.configure(&pb.ConfigureRequest{Revision: 4, Repos: repos(1, 1), LocationFingerprint: first.cfg.locationFingerprint()})
	require.NoError(t, err)
	require.NoError(t, first.Close())
	require.NoError(t, os.WriteFile(first.cfg.stateSub(fileGenerations), []byte(`{"revision":4,"repos":[{"generation":1,"repo_number":1}],"from_a_newer_build":true}`), 0o600))

	s, err := newForTest(first.cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.NoError(t, s.loadState())
	st := s.status(t.Context())
	assert.False(t, st.GetConfigured())
	assert.Equal(t, int64(4), st.GetRevision(), "the floor survives")
	assert.Empty(t, st.GetSetAsideRepos())
}

// A sidecar restarted on the same state directory with another CA file or
// with TLS verification switched off does not load the configuration the
// previous one persisted: it comes up unconfigured, so it serves no WAL
// request, and a Configure from a caller that expects the earlier settings is
// refused. Restarted with the original settings it accepts that Configure.
func TestState_NotLoadedBySidecarWithOtherTLSSettings(t *testing.T) {
	ca := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	otherCA := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	for name, mutate := range map[string]func(*Config){
		"verification off": func(c *Config) { c.S3CAFile, c.S3SkipTLSVerify = "", true },
		"another CA file":  func(c *Config) { c.S3CAFile = otherCA },
		"no CA file":       func(c *Config) { c.S3CAFile = "" },
	} {
		t.Run(name, func(t *testing.T) {
			first := newTestServer(t, func(c *Config) { s3Location(c); c.S3CAFile = ca })
			expected := first.cfg.locationFingerprint()
			_, err := first.configure(&pb.ConfigureRequest{Revision: 4, Repos: repos(1, 1), LocationFingerprint: expected})
			require.NoError(t, err)
			require.NoError(t, first.Close())

			cfg := first.cfg
			mutate(&cfg)
			s, err := newForTest(cfg)
			require.NoError(t, err)
			require.NoError(t, s.loadState())
			assert.False(t, s.configured(), "the persisted configuration was loaded under other TLS settings")
			assert.Contains(t, s.stateError(), "location fingerprint")
			rev, _ := s.held()
			assert.Equal(t, int64(4), rev, "the revision stays as the floor")

			_, err = s.configure(&pb.ConfigureRequest{Revision: 4, Repos: repos(1, 1), LocationFingerprint: expected})
			require.Error(t, err, "the pooler that checked the earlier settings must be refused")
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.False(t, s.configured())
			require.NoError(t, s.Close())

			// The original settings again: the file was set aside, so there is
			// nothing to load, and the Configure of the pooler that expects
			// them is accepted.
			s, err = newForTest(first.cfg)
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			require.NoError(t, s.loadState())
			assert.False(t, s.configured())
			resp, err := s.configure(&pb.ConfigureRequest{Revision: 4, Repos: repos(1, 1), LocationFingerprint: expected})
			require.NoError(t, err)
			require.True(t, resp.GetAccepted())
		})
	}
}

// And with unchanged settings a restart loads what was persisted.
func TestState_LoadedBySidecarWithTheSameSettings(t *testing.T) {
	ca := writeCA(t, selfSignedPEM(t, "s3.example.internal"))
	first := newTestServer(t, func(c *Config) { s3Location(c); c.S3CAFile = ca })
	_, err := first.configure(&pb.ConfigureRequest{LocationFingerprint: first.cfg.locationFingerprint(), Revision: 4, Repos: repos(1, 1)})
	require.NoError(t, err)
	require.NoError(t, first.Close())
	s, err := newForTest(first.cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.NoError(t, s.loadState())
	require.True(t, s.configured())
}

// The compensating removal of a first Configure's state file has to be made
// durable. When the directory sync fails the rollback is unproven, and the
// sidecar stops admitting repository work until the next Configure.
func TestConfigure_FirstRollbackSyncsTheStateDirectory(t *testing.T) {
	s := newTestServer(t, nil)
	var synced []string
	syncErr := error(nil)
	real := syncDirectory
	syncDirectory = func(dir string) error {
		synced = append(synced, dir)
		if syncErr != nil {
			return syncErr
		}
		return real(dir)
	}
	t.Cleanup(func() { syncDirectory = real })

	require.NoError(t, s.rollbackConfigure(false, 0, nil, RepoSnapshot{}))
	assert.Contains(t, synced, s.cfg.stateSub(dirState), "the directory that held the state file must be synced")

	syncErr = errors.New("injected sync failure")
	require.ErrorContains(t, s.rollbackConfigure(false, 0, nil, RepoSnapshot{}), "injected sync failure")
}
