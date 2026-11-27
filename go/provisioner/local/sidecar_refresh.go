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

package local

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

// S3KeyRefresh is what RefreshSidecarS3Keys did for one pooler's sidecar.
type S3KeyRefresh int

const (
	// S3KeysReloaded means a running, configured sidecar read the new keys:
	// every pgBackRest command it starts from here on uses them.
	S3KeysReloaded S3KeyRefresh = iota + 1
	// S3KeysStaged means the keys are on disk and no sidecar holds a
	// repository configuration to reload: it is stopped, or it is waiting for
	// its first Configure. Either way it reads the keys the next time it is
	// configured.
	S3KeysStaged
)

// ErrNoSidecar reports a pooler directory that has no pgbackrestd directory
// beside it, so there is nothing to refresh.
var ErrNoSidecar = errors.New("pooler has no pgbackrestd sidecar directory")

// sidecarReloadTimeout bounds one Configure. A Configure rewrites the conf
// files and restarts the sidecar's pgBackRest TLS server.
const sidecarReloadTimeout = 30 * time.Second

// sidecarReloadAttempts bounds how often a reload is sent again because the
// sidecar moved to a newer revision between Status and Configure.
const sidecarReloadAttempts = 5

// RefreshSidecarS3Keys replaces the S3 keys of one pooler's pgbackrestd with
// the ones in this process's environment (AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) and makes a running sidecar use
// them.
//
// pgbackrestd reads its static key directory only while handling a Configure,
// and multipooler sends none while the repository mapping is unchanged, so
// writing the key file alone changes nothing for a running sidecar. This sends
// the Configure the sidecar already holds (same revision, same repositories)
// to make it read the directory again. A command that is already running keeps
// the keys it started with.
func RefreshSidecarS3Keys(ctx context.Context, poolerDir string) (S3KeyRefresh, error) {
	paths := NewPgBackRestdPaths(poolerDir)
	if _, err := os.Stat(paths.Dir); err != nil {
		if os.IsNotExist(err) {
			return 0, ErrNoSidecar
		}
		return 0, err
	}
	// The ownership lock keeps a concurrent start or stop from changing which
	// process owns the sockets between the identity check and the Configure.
	lock, err := paths.lockOwner()
	if err != nil {
		return 0, err
	}
	defer lock.Close()

	if _, err := paths.writeS3KeysFromEnv(); err != nil {
		return 0, err
	}

	owner, err := paths.loadOwner()
	if err != nil {
		return 0, fmt.Errorf("read sidecar ownership: %w", err)
	}
	if owner == nil || owner.RetiredStateDir != "" {
		return S3KeysStaged, nil
	}
	alive, err := owner.alive()
	if err != nil {
		return 0, fmt.Errorf("inspect sidecar PID %d: %w", owner.PID, err)
	}
	if !alive {
		return S3KeysStaged, nil
	}
	if err := paths.verifyInstance(ctx, owner); err != nil {
		return 0, fmt.Errorf("sidecar PID %d: %w", owner.PID, err)
	}
	return ReloadSidecarS3Keys(ctx, paths, owner.PID)
}

// ReloadSidecarS3Keys makes the pgbackrestd running as pid read its static key
// directory again, by re-sending the Configure it holds. The connection is
// refused unless the process behind the control socket is pid. It returns
// S3KeysStaged, and sends nothing, when the sidecar holds no configuration.
func ReloadSidecarS3Keys(ctx context.Context, p PgBackRestdPaths, pid int) (S3KeyRefresh, error) {
	ctx, cancel := context.WithTimeout(ctx, sidecarReloadTimeout)
	defer cancel()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialOwnedSidecar(ctx, p.ControlSocket, pid)
	}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: tr}, "http://pgbackrestd")

	// Only a Configure that the sidecar accepts after the key file was
	// written proves it read the new keys: Configure calls are serialized, and
	// each reads the key directory afresh. A Configure that was already in
	// progress read the directory before, so Status is asked to wait for it,
	// and a refusal (a newer revision was applied meanwhile, possibly by that
	// very Configure) is answered by sending the newer list, never taken as
	// proof.
	for range sidecarReloadAttempts {
		status, err := client.Status(ctx, connect.NewRequest(&pb.StatusRequest{AfterConfigure: true}))
		if err != nil {
			return 0, fmt.Errorf("sidecar status: %w", err)
		}
		if !status.Msg.GetConfigured() {
			// No Configure is in progress, and one that starts from here on
			// reads the new key file.
			return S3KeysStaged, nil
		}
		// The WAL segment size is left unset, which keeps the one the sidecar
		// holds. The sidecar applies no Configure without a location
		// fingerprint. This one repeats the list of a configured sidecar, which
		// multipooler established under that fingerprint, so the sidecar's own
		// is sent back: the request cannot establish a configuration (an
		// unconfigured sidecar returned above) or change the list.
		resp, err := client.Configure(ctx, connect.NewRequest(&pb.ConfigureRequest{
			Revision:            status.Msg.GetRevision(),
			Repos:               status.Msg.GetRepos(),
			LocationFingerprint: status.Msg.GetLocationFingerprint(),
		}))
		if err != nil {
			return 0, fmt.Errorf("sidecar did not reload its S3 keys: %w", err)
		}
		if resp.Msg.GetAccepted() {
			return S3KeysReloaded, nil
		}
		if resp.Msg.GetHeldRevision() <= status.Msg.GetRevision() {
			return 0, fmt.Errorf("sidecar refused revision %d while holding revision %d", status.Msg.GetRevision(), resp.Msg.GetHeldRevision())
		}
	}
	return 0, fmt.Errorf("sidecar moved to a newer revision %d times in a row; it did not confirm that it read the new S3 keys", sidecarReloadAttempts)
}
