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

package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/safefs"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/tools/repocreds"
)

// sidecarBaseURL is only a placeholder: the transport dials the socket and
// ignores the host.
const sidecarBaseURL = "http://pgbackrestd"

// sidecarClient talks to the pgbackrestd control API over a unix socket.
type sidecarClient struct {
	client pgbackrestdserviceconnect.PgBackRestdClient
	http   *http.Client

	// expectLocation is the location fingerprint of the topology's backup
	// location, set once the manager has compared it with the sidecar's at
	// startup. Every Configure carries it, and the sidecar refuses one that
	// differs from its own, so a sidecar restarted later with another backup
	// location, CA file or TLS verification setting is never configured.
	expectLocation atomic.Pointer[string]

	// Timing is immutable after client setup. Tests shorten their own client's
	// bounds, never globals read by other managers' background loops.
	callTimeout      time.Duration
	stopTimeout      time.Duration
	stopPoll         time.Duration
	syncInterval     time.Duration
	configureTimeout time.Duration
	configureRetry   time.Duration
	// settleWarnEvery is how often an unbounded settlement wait logs that it is
	// still waiting.
	settleWarnEvery time.Duration
	logger          *slog.Logger

	// shutdown, when set, ends a wait for a cancelled command to stop. It must
	// be cancelled at the top of GracefulShutdown: the waiting run holds the
	// action lock shutdown is about to take.
	shutdown context.Context

	mu sync.Mutex
	// inflight counts this client's own Runs per command, so a wait for a
	// cancelled command is not held up by a legitimate one still running.
	inflight map[pb.Command]int
}

func newSidecarClient(socketPath string) *sidecarClient {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socketPath)
		},
	}
	httpClient := &http.Client{Transport: transport}
	return &sidecarClient{
		client:           pgbackrestdserviceconnect.NewPgBackRestdClient(httpClient, sidecarBaseURL),
		http:             httpClient,
		callTimeout:      sidecarCallTimeout,
		stopTimeout:      sidecarStopTimeout,
		stopPoll:         sidecarStopPoll,
		syncInterval:     sidecarSyncInterval,
		configureTimeout: sidecarConfigureTimeout,
		configureRetry:   sidecarConfigureRetry,
		settleWarnEvery:  sidecarSettleWarnEvery,
		logger:           slog.Default(),
	}
}

// Close drops idle connections to the socket.
func (c *sidecarClient) Close() {
	c.http.CloseIdleConnections()
}

// Configure sets the sidecar's generation list. accepted=false means the
// sidecar holds a higher revision, which it reports as heldRevision.
func (c *sidecarClient) Configure(ctx context.Context, rev int64, repos []RepoGeneration, walSegBytes uint32) (*pb.ConfigureResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	req := &pb.ConfigureRequest{Revision: rev, WalSegmentSizeBytes: walSegBytes}
	if want := c.expectLocation.Load(); want != nil {
		req.LocationFingerprint = *want
	}
	for _, r := range repos {
		req.Repos = append(req.Repos, &pb.RepoGeneration{Generation: r.Generation, RepoNumber: r.RepoNumber})
	}
	resp, err := c.client.Configure(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, fmt.Errorf("pgbackrestd configure: %w", err)
	}
	return resp.Msg, nil
}

// Run executes one pgBackRest command and copies its output to stdout and
// stderr. The exit code comes from the final stream message, so a stream that
// ends without one is an error, never an implied success.
//
// A Run that ends early (the caller's context was cancelled, or the stream
// broke) does not return until the sidecar reports the command gone: the
// sidecar finishes the cancel after this client has already left, and the
// caller's next command (a second backup after a lost lease) would otherwise
// find pgBackRest's lock still held. This is the kill-and-wait that running
// pgBackRest as a child gave. A restore is exempt: it is deliberately not
// cancelled by its client. So are the commands that take no backup lock (info,
// check, verify): a cancelled caller has nothing to protect by waiting.
//
// Every Run carries a run_id. A stanza delete also carries the sidecar instance
// id read from Status, which the sidecar checks, and waits for its own run_id to
// leave Status's active_runs; the other lock-taking commands are counted
// instead, because a same-command Run started during the wait is legitimate
// there and nothing destructive depends on which run it is.
func (c *sidecarClient) Run(ctx context.Context, req *pb.RunRequest, stdout, stderr io.Writer) (int, error) {
	cmd := req.GetCommand()
	msg := proto.Clone(req).(*pb.RunRequest)
	msg.RunId = uuid.NewString()
	msg.Restore = backupengine.RestoreOperationFromContext(ctx)
	// A delete's settlement wait ends when the sidecar instance that was asked to
	// run it is gone, and the sidecar refuses the delete when it is not that
	// instance, so the instance has to be known before the delete is sent.
	instance := ""
	if cmd == pb.Command_COMMAND_STANZA_DELETE {
		st, err := c.Status(ctx)
		if err != nil {
			return 0, fmt.Errorf("%w: cannot identify the sidecar instance: %w", backupengine.ErrRunNotDelivered, err)
		}
		instance = st.GetInstanceId()
		if instance == "" {
			return 0, fmt.Errorf("%w: the sidecar reported no instance id", backupengine.ErrRunNotDelivered)
		}
		msg.ExpectedInstanceId = instance
	}
	c.addInflight(cmd, 1)
	defer c.addInflight(cmd, -1)
	var spawned string
	exit, err := c.runMsg(ctx, msg, stdout, stderr, &spawned)
	if err == nil || !takesBackupLock(cmd) ||
		errors.Is(err, backupengine.ErrRunNotDelivered) || errors.Is(err, backupengine.ErrOutputTruncated) {
		return exit, err
	}
	// The sidecar says whether it ever started a delete child for this run. A
	// lost response carries no answer, which must be read as "it may have".
	if cmd == pb.Command_COMMAND_STANZA_DELETE && spawned == backup.ChildSpawnedNone {
		return exit, err
	}
	var werr error
	if cmd == pb.Command_COMMAND_STANZA_DELETE {
		werr = c.waitDeleteSettled(ctx, msg.RunId, instance)
	} else {
		werr = c.waitCountedStopped(ctx, cmd)
	}
	if werr != nil {
		return 0, fmt.Errorf("%w (and %w)", err, werr)
	}
	return exit, err
}

// takesBackupLock reports whether cmd can hold the lock a following backup
// would collide with. Only those are worth waiting for after a cancel.
func takesBackupLock(cmd pb.Command) bool {
	switch cmd {
	case pb.Command_COMMAND_BACKUP, pb.Command_COMMAND_EXPIRE, pb.Command_COMMAND_STANZA_CREATE,
		// A delete a cancelled caller leaves running could remove a stanza the
		// next backup lease holder just created, so its caller waits for the
		// sidecar to report the delete gone before it returns (and so, if the
		// lease is still held, before the lease is released).
		pb.Command_COMMAND_STANZA_DELETE:
		return true
	}
	return false
}

func (c *sidecarClient) addInflight(cmd pb.Command, d int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight == nil {
		c.inflight = map[pb.Command]int{}
	}
	c.inflight[cmd] += d
}

// pollStatus polls Status every stopPoll until step reports the wait over,
// returning nil then. A Status that fails never ends the wait; it is the reason
// reported while it lasts. The wait ends with onShutdown's error at manager
// shutdown and, when onTimeout is not nil, with onTimeout's error when wctx ends;
// a nil onTimeout means the wait has no timeout. warn, when set, is called every
// settleWarnEvery with the last reason.
func (c *sidecarClient) pollStatus(wctx context.Context, step func(*pb.StatusResponse) (bool, error),
	warn func(waited time.Duration, reason error), onShutdown, onTimeout func(reason error) error,
) error {
	var shutdown <-chan struct{}
	if c.shutdown != nil {
		shutdown = c.shutdown.Done()
	}
	var timeout <-chan struct{}
	if onTimeout != nil {
		timeout = wctx.Done()
	}
	began := time.Now()
	lastWarn := began
	var lastErr error
	for {
		if st, err := c.Status(wctx); err == nil {
			done, pending := step(st)
			if done {
				return nil
			}
			lastErr = pending
		} else {
			lastErr = err
		}
		if warn != nil && time.Since(lastWarn) >= c.settleWarnEvery {
			lastWarn = time.Now()
			warn(time.Since(began), lastErr)
		}
		select {
		case <-time.After(c.stopPoll):
		case <-shutdown:
			return onShutdown(lastErr)
		case <-timeout:
			return onTimeout(lastErr)
		}
	}
}

// waitCountedStopped polls Status until the sidecar lists no more runs of cmd
// than this client still has in flight itself. The wait is detached from ctx,
// which is already cancelled, and ends at sidecarStopTimeout or manager
// shutdown; either is an error, never a silent proceed. Runs are counted, not
// identified: a same-command Run started during the wait is legitimate, so the
// count is read on every poll (this Run is counted in inflight but has ended).
func (c *sidecarClient) waitCountedStopped(ctx context.Context, cmd pb.Command) error {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.stopTimeout)
	defer cancel()
	return c.pollStatus(wctx,
		func(st *pb.StatusResponse) (bool, error) {
			c.mu.Lock()
			others := c.inflight[cmd] - 1
			c.mu.Unlock()
			n := 0
			for _, a := range st.GetActiveCommands() {
				if a == cmd {
					n++
				}
			}
			if n <= others {
				return true, nil
			}
			return false, fmt.Errorf("%d %s run(s) still active", n-others, cmd)
		},
		nil,
		func(reason error) error {
			return fmt.Errorf("manager shutting down while waiting for pgbackrestd to stop %s: %w", cmd, reason)
		},
		func(reason error) error {
			return fmt.Errorf("pgbackrestd did not stop %s within %s of the cancel: %w", cmd, c.stopTimeout, reason)
		})
}

// waitDeleteSettled polls Status until the sidecar no longer lists the stanza
// delete's own run_id. The wait is detached from ctx, which is already
// cancelled, and has no timeout. The caller holds the backup lease while it
// waits, and a delete that is still running when the lease moves to another
// pooler can remove the stanza and archive that pooler is building, or a
// completed backup of a live shard. The wait ends when Status no longer lists
// the run, when Status reports a different instance_id than the one the delete
// was sent to, or at manager shutdown.
//
// A different instance ends it because the sidecar refuses a delete bound to
// another instance, and the old instance's delete is dead: in a container the
// restart killed every process in it, and outside one a new instance cannot take
// the state directory (instance lock, worker markers) while an old supervisor
// or its marker remains. A Status that fails does not end it.
//
// Manager shutdown ends it with an error that abandons the lease
// (topoclient.AbandonLease): the callback returns without unlocking, renewal
// stops, and the lease expires on its own after its TTL, the same as if this
// process had died. Shutdown does not prove the delete is gone, so the lease is
// not released while it is still valid.
//
// What this does and does not give: the wait keeps the lease while the lease is
// still held, including after the caller's context is cancelled. If the lease is
// lost while a delete's settlement is unproven (it expired, it was revoked by a
// steal, no renewal succeeded within the lease monitor's loss window, or it was
// abandoned at shutdown and ran out), nothing fences that delete against the
// next holder. A delete whose supervisor stalls is the same class of limit as a
// stanza-delete stuck uninterruptibly after SIGKILL.
func (c *sidecarClient) waitDeleteSettled(ctx context.Context, runID, instance string) error {
	latch := ""
	return c.pollStatus(context.WithoutCancel(ctx),
		func(st *pb.StatusResponse) (bool, error) {
			if st.GetInstanceId() != instance {
				c.logger.WarnContext(ctx, "pgbackrestd restarted while a stanza delete was outstanding; the delete is bound to the instance that died",
					"run_id", runID, "instance_before", instance, "instance_now", st.GetInstanceId())
				return true, nil
			}
			if st.GetDeleteUnproven() {
				latch = st.GetDeleteUnprovenReason()
			}
			if !slices.ContainsFunc(st.GetActiveRuns(), func(a *pb.ActiveRun) bool { return a.GetRunId() == runID }) {
				return true, nil
			}
			return false, fmt.Errorf("run %s of %s still active", runID, pb.Command_COMMAND_STANZA_DELETE)
		},
		func(waited time.Duration, reason error) {
			c.logger.WarnContext(ctx, "still waiting for pgbackrestd to settle a cancelled stanza delete; the caller's backup lease is retained only while it is still held",
				"run_id", runID, "waited", waited.Round(time.Second), "reason", reason, "sidecar_latch", latch)
		},
		func(reason error) error {
			c.logger.WarnContext(ctx, "manager shutting down with a stanza delete unsettled; abandoning the backup lease so it expires on its own",
				"run_id", runID, "reason", reason, "sidecar_latch", latch)
			return topoclient.AbandonLease(fmt.Errorf("manager shutting down while waiting for pgbackrestd to settle the stanza delete: %w", reason))
		},
		nil)
}

// runMsg sends msg and streams the output to stdout and stderr. When spawned is
// not nil it stores the sidecar's child-spawned trailer there ("" when the
// response carried none).
func (c *sidecarClient) runMsg(ctx context.Context, msg *pb.RunRequest, stdout, stderr io.Writer, spawned *string) (int, error) {
	stream, err := c.client.Run(ctx, connect.NewRequest(msg))
	if err != nil {
		return 0, notDelivered(err)
	}
	defer stream.Close()
	if spawned != nil {
		defer func() { *spawned = stream.ResponseTrailer().Get(backup.ChildSpawnedTrailer) }()
	}

	exit := -1
	gotExit, truncated, received := false, false, false
	for stream.Receive() {
		received = true
		switch chunk := stream.Msg().GetChunk().(type) {
		case *pb.RunOutput_Stdout:
			if _, err := stdout.Write(chunk.Stdout); err != nil {
				return 0, err
			}
		case *pb.RunOutput_Stderr:
			if _, err := stderr.Write(chunk.Stderr); err != nil {
				return 0, err
			}
		case *pb.RunOutput_ExitCode:
			exit = int(chunk.ExitCode)
			gotExit = true
			truncated = stream.Msg().GetOutputTruncated()
		}
	}
	if err := stream.Err(); err != nil {
		if !received {
			return 0, notDelivered(err)
		}
		return 0, err
	}
	if !gotExit {
		return 0, errors.New("pgbackrestd stream ended without an exit code")
	}
	if truncated {
		return exit, backupengine.ErrOutputTruncated
	}
	return exit, nil
}

// notDelivered marks an error from dialing the socket: nothing reached the
// sidecar, so it cannot have started anything.
func notDelivered(err error) error {
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return fmt.Errorf("%w: %w", backupengine.ErrRunNotDelivered, err)
	}
	return err
}

// Status reports the sidecar's state. It never fails on a sidecar that is up,
// whatever state that sidecar is in.
func (c *sidecarClient) Status(ctx context.Context) (*pb.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	resp, err := c.client.Status(ctx, connect.NewRequest(&pb.StatusRequest{}))
	if err != nil {
		return nil, fmt.Errorf("pgbackrestd status: %w", err)
	}
	return resp.Msg, nil
}

// SidecarServerPort asks the sidecar on socketPath which port its pgBackRest
// TLS server listens on. It is what a replica's peers dial.
func SidecarServerPort(ctx context.Context, socketPath string) (int, error) {
	c := newSidecarClient(socketPath)
	defer c.Close()
	for {
		st, err := c.Status(ctx)
		if err == nil {
			if st.GetServerPort() == 0 {
				return 0, errors.New("pgbackrestd reports no server port")
			}
			return int(st.GetServerPort()), nil
		}
		// The sidecar is an init container and normally answers at once; a
		// bounded wait covers one that is still starting.
		select {
		case <-ctx.Done():
			return 0, err
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Default timing; clients copy these values at construction.
const (
	// sidecarCallTimeout bounds each Configure and Status call, so a sidecar
	// that accepts a connection and never answers cannot hold a caller. Run is
	// not bounded here: its callers carry their own deadlines.
	sidecarCallTimeout = 10 * time.Second
	// sidecarStopTimeout bounds the wait for a cancelled command to leave
	// Status.active_commands; sidecarStopPoll is how often it looks.
	sidecarStopTimeout = 30 * time.Second
	sidecarStopPoll    = 75 * time.Millisecond
	// sidecarSyncInterval is how often the sync loop compares the sidecar with
	// the generation list.
	sidecarSyncInterval = 10 * time.Second
	// sidecarConfigureTimeout bounds the Configure made before postgres starts.
	sidecarConfigureTimeout = 15 * time.Second
	sidecarConfigureRetry   = time.Second
	// sidecarSettleWarnEvery is how often the wait for a stanza delete to settle,
	// which has no bound, says that it is still waiting.
	sidecarSettleWarnEvery = 30 * time.Second
)

// checkCredentialExposure reports whether multipooler's own environment
// carries a backup repository credential (see repocreds.EnvVars). It logs but never refuses to start: the pod is
// misconfigured, not unsafe to run.
func (pm *MultipoolerManager) checkCredentialExposure() bool {
	found := repocreds.EnvVars(os.Environ())
	if len(found) == 0 {
		return false
	}
	pm.logger.Error("multipooler environment holds a backup repository credential that only the pgbackrestd sidecar should have",
		"variables", found)
	pm.credentialExposed.Store(1)
	return true
}

// backupLocationFingerprint is the value the sidecar reports for the location
// it was started with.
func backupLocationFingerprint(loc *clustermetadatapb.BackupLocation) string {
	return backup.LocationFingerprintOf(loc).Hash()
}

// openSidecarVolumeRoot returns the opener for the PVC generation copy: the
// data volume mount is anchored first, then the pooler dir below it is opened
// without following links.
func (pm *MultipoolerManager) openSidecarVolumeRoot() (*safefs.Root, error) {
	volume := pm.dataVolumeRoot()
	poolerDir := pm.record.PoolerDir()
	if poolerDir == "" {
		return nil, errors.New("pooler dir is not set")
	}
	rel, err := filepath.Rel(volume, poolerDir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return nil, fmt.Errorf("pooler dir %q is not under the data volume %q", poolerDir, volume)
	}
	root, err := backupengine.OpenVolumeRoot(volume, pm.dataVolumeAnchor())
	if err != nil {
		return nil, err
	}
	defer root.Close()
	// Resolve every component beneath the anchored volume. Opening poolerDir
	// by absolute path would follow untrusted intermediate symlinks.
	return root.OpenRoot(rel)
}

// sidecarCipherKeys stands in for the key file: the sidecar owns the keys, and
// what multipooler may know is which generations have one.
func (pm *MultipoolerManager) sidecarCipherKeys(st *pb.StatusResponse) backup.CipherKeys {
	keys := backup.CipherKeys{}
	for _, g := range st.GetGenerationsWithKeys() {
		keys[int(g)] = "held-by-sidecar"
	}
	return keys
}

// cipherKeys returns the keys encryption checks consult: the sidecar's report
// of which generations it holds a key for.
func (pm *MultipoolerManager) cipherKeys(ctx context.Context) (backup.CipherKeys, error) {
	st, err := pm.sidecar.Status(ctx)
	if err != nil {
		return nil, err
	}
	return pm.sidecarCipherKeys(st), nil
}

// errSidecarLocationMismatch means the sidecar was started for another backup
// location, CA file or TLS verification setting than the topology's database
// record names. It is not final: either side can be brought in line (the
// sidecar restarted with the record's settings, or the record changed), so
// startup retries it and the sync loop re-reads the record.
var errSidecarLocationMismatch = errors.New("pgbackrest sidecar location does not match topology")

// errSidecarHoldsNoList means the sidecar is not configured and refused this
// pooler's copy of the generation list, because the copy's revision is below
// one the sidecar held before it set its saved state aside, and the sidecar
// kept no list from that state to apply instead.
var errSidecarHoldsNoList = errors.New("pgbackrest sidecar is not configured and refused this pooler's copy of the generation list")

// setupSidecar prepares the sidecar at startup, before postgres is started:
// it refuses a sidecar that serves another location, removes the pgbackrest
// directory that pods from before the sidecar left on the data volume, and configures the sidecar from the PVC copy so
// archiving works without a database. The generation list in the table is
// reconciled by the sync loop once postgres is up.
func (pm *MultipoolerManager) setupSidecar(ctx context.Context, loc *clustermetadatapb.BackupLocation, scope string) error {
	st, err := pm.statusWithRetry(ctx)
	if err != nil {
		return fmt.Errorf("pgbackrest sidecar unreachable: %w", err)
	}
	want := backupLocationFingerprint(loc)
	if st.GetLocationFingerprint() != want {
		return errSidecarLocationMismatch
	}
	pm.sidecar.expectLocation.Store(&want)
	if err := checkSidecarScope(st.GetRepositoryScope(), scope); err != nil {
		return err
	}

	keys := pm.sidecarCipherKeys(st)
	if _, declared := keys[backup.InitialRepoGeneration]; len(keys) > 0 && !declared {
		return fmt.Errorf("pgbackrest sidecar holds no key for generation %d (the initial repository)", backup.InitialRepoGeneration)
	}
	if err := requireInitialRepoEncryptionError(loc, keys); err != nil {
		return err
	}
	if gen := loc.GetAuthoritativeGeneration(); gen != 0 && gen != backup.InitialRepoGeneration {
		return fmt.Errorf("authoritative_generation %d is not supported: only the initial generation %d exists", gen, backup.InitialRepoGeneration)
	}

	// The passfile primary_conninfo names is written before the pgbackrest
	// directory, which a data volume written by a pod that ran pgBackRest
	// in-process may have kept it in, is removed, so a standby that restarts in
	// between never finds the passfile it names missing.
	if err := pm.writeReplicationPassfile(); err != nil {
		return err
	}

	// A pgbackrest.conf on the data volume holds the cipher passphrase, which
	// must not stay there: the sidecar owns it.
	root, err := pm.openGenerationsRoot()
	if err != nil {
		return fmt.Errorf("open pooler dir to remove the pgbackrest directory: %w", err)
	}
	if err := checkLegacyScopePin(root, st.GetRepositoryScope()); err != nil {
		root.Close()
		return err
	}
	err = root.RemoveAll("pgbackrest")
	root.Close()
	if err != nil {
		return fmt.Errorf("remove pgbackrest directory: %w", err)
	}

	if err := pm.configureFromCopy(ctx, st); err != nil {
		// The sync loop keeps trying; postgres can start meanwhile, and the
		// sidecar answers the WAL API 503 until it is configured.
		pm.logger.ErrorContext(ctx, "could not configure the pgbackrest sidecar before start; will keep retrying", "error", err)
	}
	return nil
}

// statusWithRetry reads the sidecar's Status, waiting a bounded time for a
// sidecar that is still starting.
func (pm *MultipoolerManager) statusWithRetry(ctx context.Context) (*pb.StatusResponse, error) {
	deadline := time.Now().Add(pm.sidecar.configureTimeout)
	for {
		st, err := pm.sidecar.Status(ctx)
		if err == nil {
			return st, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(pm.sidecar.configureRetry):
		}
	}
}

// configureFromCopy configures a sidecar that reported itself unconfigured in
// st, so that it can serve WAL before postgres is up. It sends the PVC copy
// (or generation 1 at repo 1, revision 0 when there is none).
//
// A sidecar that set its saved state aside refuses a copy below the revision
// that state held. The table may hold a newer list, but a standby that needs
// WAL from the archive before postgres answers cannot read it. If the sidecar
// reports the list it set aside (set_aside_repos), that list is sent back at
// the sidecar's revision: it is what this sidecar held at that revision, and
// the caller has compared the sidecar's location fingerprint with the
// topology. Otherwise the sidecar stays unconfigured and the error says so
// (errSidecarHoldsNoList): the copy is never relabelled with the higher
// revision.
func (pm *MultipoolerManager) configureFromCopy(ctx context.Context, st *pb.StatusResponse) error {
	rev, repos, ok := pm.readGenerationsCopy()
	if !ok {
		rev, repos = 0, []RepoGeneration{{Generation: backup.InitialRepoGeneration, RepoNumber: 1}}
	}
	seg := pm.walSegBytes.Load()
	resp, err := pm.sidecar.Configure(ctx, rev, repos, seg)
	if err != nil {
		return err
	}
	if resp.GetAccepted() {
		pm.sidecarWalSegBytes.Store(seg)
		return nil
	}
	held := resp.GetHeldRevision()
	if !st.GetConfigured() && st.GetRevision() == held && len(st.GetSetAsideRepos()) > 0 {
		kept := make([]RepoGeneration, 0, len(st.GetSetAsideRepos()))
		for _, r := range st.GetSetAsideRepos() {
			kept = append(kept, RepoGeneration{Generation: r.GetGeneration(), RepoNumber: r.GetRepoNumber()})
		}
		resp, err = pm.sidecar.Configure(ctx, held, kept, seg)
		if err != nil {
			return fmt.Errorf("apply the generation list the sidecar set aside at revision %d: %w", held, err)
		}
		if resp.GetAccepted() {
			pm.logger.InfoContext(ctx, "configured the pgbackrest sidecar with the generation list it had set aside; the PVC copy is older",
				"copy_revision", rev, "revision", held)
			pm.sidecarWalSegBytes.Store(seg)
			return nil
		}
		held = resp.GetHeldRevision()
	}
	return fmt.Errorf("%w: the copy is at revision %d and the sidecar once held revision %d, of which it kept no usable list. "+
		"It archives and fetches no WAL until this pooler reads revision %d or later from the generation table, which needs postgres here to answer queries. "+
		"If postgres cannot start without WAL from the archive, remove generations.json.bad* from the sidecar's state directory and restart the sidecar, "+
		"which then accepts the copy; that is safe only while the copy's list is current",
		errSidecarHoldsNoList, rev, held, held)
}

// walSegmentSize returns postgres's wal_segment_size in bytes, reading it once
// postgres answers and caching it: it is fixed at initdb, so one read is
// enough. 0 means not known yet, and the sidecar keeps the size it holds.
func (pm *MultipoolerManager) walSegmentSize(ctx context.Context) uint32 {
	if seg := pm.walSegBytes.Load(); seg != 0 {
		return seg
	}
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	res, err := pm.adminQuery(queryCtx, "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size' AND unit = 'B'")
	if err != nil {
		pm.logger.DebugContext(ctx, "cannot read wal_segment_size", "error", err)
		return 0
	}
	var setting string
	if err := executor.ScanSingleRow(res, &setting); err != nil {
		pm.logger.DebugContext(ctx, "cannot scan wal_segment_size", "error", err)
		return 0
	}
	n, err := strconv.ParseUint(setting, 10, 32)
	if err != nil || n == 0 {
		pm.logger.WarnContext(ctx, "ignoring an unusable wal_segment_size", "setting", setting, "error", err)
		return 0
	}
	pm.walSegBytes.Store(uint32(n))
	return uint32(n)
}

// noteTableBehind records whether the table trails the floor, as a gauge, and
// logs it once per distinct (revision, floor) pair rather than on every pass.
func (pm *MultipoolerManager) noteTableBehind(ctx context.Context, rev, floor, sidecarRev, copyRev int64) {
	pm.behindMu.Lock()
	defer pm.behindMu.Unlock()
	if rev >= floor {
		pm.tableBehind.Store(0)
		pm.behindWarned = false
		return
	}
	pm.tableBehind.Store(1)
	if pm.behindWarned && pm.behindRev == rev && pm.behindFloor == floor {
		return
	}
	pm.behindWarned, pm.behindRev, pm.behindFloor = true, rev, floor
	pm.logger.WarnContext(ctx, "pgbackrest generation table trails the sidecar or the PVC copy",
		"table_revision", rev, "sidecar_revision", sidecarRev, "copy_revision", copyRev)
}

// logSyncFailure warns the first time a sync pass fails with a given error and
// drops repeats to debug until it succeeds or the error changes. A shard that
// has not bootstrapped yet fails every pass the same way until the seed lands.
func (pm *MultipoolerManager) logSyncFailure(err error) {
	pm.behindMu.Lock()
	defer pm.behindMu.Unlock()
	if err == nil {
		pm.lastSyncErr = ""
		return
	}
	if msg := err.Error(); msg != pm.lastSyncErr {
		pm.lastSyncErr = msg
		pm.logger.Warn("pgbackrest sidecar sync failed", "error", err)
		return
	}
	pm.logger.Debug("pgbackrest sidecar sync still failing", "error", err)
}

func sameGenerations(a []RepoGeneration, b []*pb.RepoGeneration) bool {
	x := slices.Clone(a)
	slices.SortFunc(x, func(p, q RepoGeneration) int { return int(p.Generation) - int(q.Generation) })
	y := make([]RepoGeneration, 0, len(b))
	for _, r := range b {
		y = append(y, RepoGeneration{Generation: r.GetGeneration(), RepoNumber: r.GetRepoNumber()})
	}
	slices.SortFunc(y, func(p, q RepoGeneration) int { return int(p.Generation) - int(q.Generation) })
	return slices.Equal(x, y)
}

// syncSidecar brings the sidecar in line with the generation list. It is safe
// to run at any time and on any role.
//
// Order matters: the table is read before anything is sent, and a table that
// trails the sidecar or the PVC copy is first raised past both on a primary
// (reconcileRevisionFloor), never lowered on the sidecar. A standby with a
// trailing table waits: it only sees the primary's fix through replication.
func (pm *MultipoolerManager) syncSidecar(ctx context.Context) error {
	// The gauge reports what a pass learns; one that cannot compare (no database,
	// no table, sidecar down) must not leave the last verdict standing.
	compared := false
	defer func() {
		if !compared {
			pm.tableBehind.Store(0)
		}
	}()
	st, err := pm.sidecar.Status(ctx)
	if err != nil {
		return err
	}
	// The sidecar may be another process than the one compared at startup. One
	// restarted with other settings is left alone: it is not configured, and
	// it does not serve the configuration it found on disk either (it sets a
	// state written under another fingerprint aside).
	//
	// The topology's record may have changed since startup, with the sidecar
	// restarted to match it. So the record is read again, and its location is
	// adopted when the sidecar serves exactly that. What the sidecar reports is
	// only ever compared with the record, never taken as the expectation.
	if want := pm.sidecar.expectLocation.Load(); want != nil && st.GetLocationFingerprint() != *want {
		if err := pm.adoptTopologyLocation(ctx, st); err != nil {
			return err
		}
		if st, err = pm.sidecar.Status(ctx); err != nil {
			return err
		}
	}
	// A sidecar that came up without a usable state answers 503 on the WAL API
	// until configured, with or without a database to read. One that refuses
	// the copy and kept no list may still be configured from the table below,
	// so that error is returned only if this pass ends without doing so.
	var holdsNoList error
	if !st.GetConfigured() {
		if err := pm.configureFromCopy(ctx, st); err != nil {
			if !errors.Is(err, errSidecarHoldsNoList) {
				return err
			}
			holdsNoList = err
		}
		if st, err = pm.sidecar.Status(ctx); err != nil {
			return err
		}
	}

	mode, err := pm.postgresMode(ctx)
	if err != nil {
		// No database to read yet; the sidecar already has the copy.
		return holdsNoList
	}
	seg := pm.walSegmentSize(ctx)
	rev, repos, err := pm.readGenerations(ctx)
	if err != nil {
		return err
	}
	if len(repos) == 0 {
		return holdsNoList
	}

	copyRev, _, _ := pm.readGenerationsCopy()
	floor := max(st.GetRevision(), copyRev)
	pm.noteTableBehind(ctx, rev, floor, st.GetRevision(), copyRev)
	compared = true
	if rev < floor {
		// Known limitation: a standby cannot raise the table, and the primary's
		// floor counts only its own sidecar and copy. A standby whose floor is
		// above the primary's table therefore waits until something else raises
		// the table. Rare, and harmless while only generation 1 exists; the
		// rotation flow must bump the table to the maximum floor over every
		// pod's sidecar and copy before it relies on a revision order.
		if !mode.OutOfRecovery() {
			return holdsNoList
		}
		if rev, err = pm.reconcileRevisionFloor(ctx, st.GetRevision()); err != nil {
			return err
		}
	}
	if err := pm.refreshGenerationsCopy(ctx); err != nil {
		return err
	}

	segChanged := seg != 0 && seg != pm.sidecarWalSegBytes.Load()
	if st.GetConfigured() && rev == st.GetRevision() && sameGenerations(repos, st.GetRepos()) && !segChanged {
		return nil
	}
	resp, err := pm.sidecar.Configure(ctx, rev, repos, seg)
	if err != nil {
		return err
	}
	if !resp.GetAccepted() {
		// The sidecar moved past this revision since Status: raise the table
		// past it and send again on the next pass.
		if !mode.OutOfRecovery() {
			return holdsNoList
		}
		raised, err := pm.reconcileRevisionFloor(ctx, resp.GetHeldRevision())
		if err != nil {
			return err
		}
		if err := pm.refreshGenerationsCopy(ctx); err != nil {
			return err
		}
		resp, err = pm.sidecar.Configure(ctx, raised, repos, seg)
		if err != nil {
			return err
		}
		if !resp.GetAccepted() {
			return fmt.Errorf("pgbackrest sidecar refused revision %d, holding %d", raised, resp.GetHeldRevision())
		}
	}
	pm.sidecarWalSegBytes.Store(seg)
	return nil
}

// adoptTopologyLocation handles a sidecar that reports another location
// fingerprint than the one this pooler expects. It reads the database record
// from the topology again (not the static leader's copy, which is what this
// pooler already holds). If the record now names the location the sidecar
// serves, the sidecar is set up for it as at startup and the pooler expects
// that location from then on. Otherwise the sidecar is left unconfigured.
func (pm *MultipoolerManager) adoptTopologyLocation(ctx context.Context, st *pb.StatusResponse) error {
	database := pm.record.ShardRef().GetDatabase()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	db, err := pm.topoClient.GetDatabase(readCtx, database)
	cancel()
	if err != nil {
		return fmt.Errorf("%w, and the database record could not be read again to see whether it changed: %w", errSidecarLocationMismatch, err)
	}
	if st.GetLocationFingerprint() != backupLocationFingerprint(db.BackupLocation) {
		return fmt.Errorf("%w: the sidecar was started with another backup location, CA file or TLS verification setting than the database record names; "+
			"it is not configured until it is restarted with the record's settings or the record is changed to match", errSidecarLocationMismatch)
	}
	backupConfig, err := pm.backupConfigFor(db)
	if err != nil {
		return fmt.Errorf("the database record changed its backup location to the one the sidecar serves, but it cannot be used: %w", err)
	}
	if err := pm.setupSidecar(ctx, db.BackupLocation, backupConfig.RepositoryScope()); err != nil {
		return err
	}
	pm.saveDatabaseCache(db)
	pm.backup.SetBackupConfig(backupConfig)
	pm.logger.InfoContext(ctx, "the database record's backup location changed and the pgbackrest sidecar serves the new one; using it")
	return nil
}

// runSidecarSync is the loop that keeps the sidecar configured for the life
// of the manager. Every pass is bounded by the call timeouts underneath it,
// and a failed pass only logs: the next one retries, so a sidecar that
// restarts unconfigured is repaired within one interval.
func (pm *MultipoolerManager) runSidecarSync() {
	ctx := pm.shutdownCtx
	ticker := time.NewTicker(pm.sidecar.syncInterval)
	defer ticker.Stop()
	for {
		passCtx, cancel := context.WithTimeout(ctx, 2*pm.sidecar.syncInterval)
		if err := pm.syncSidecar(passCtx); ctx.Err() == nil {
			pm.logSyncFailure(err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// initialRepo is the generation-1 row to seed. Multipooler never sees the key,
// so the fingerprint comes from the sidecar.
func (pm *MultipoolerManager) initialRepo(ctx context.Context) (backup.PgBackRestRepo, error) {
	st, err := pm.sidecar.Status(ctx)
	if err != nil {
		return backup.PgBackRestRepo{}, err
	}
	repo := backup.InitialPgBackRestRepo(nil)
	if slices.Contains(st.GetGenerationsWithKeys(), backup.InitialRepoGeneration) {
		fp := st.GetGenerationFingerprints()[backup.InitialRepoGeneration]
		if fp == "" {
			return backup.PgBackRestRepo{}, errors.New("pgbackrest sidecar holds a key for generation 1 but reports no fingerprint for it")
		}
		repo.Encrypted = true
		repo.KeyFingerprint = fp
	}
	return repo, nil
}

// replicationPassfileName sits beside, not inside, the pgbackrest directory
// that setupSidecar deletes, so the deletion never takes the passfile with it.
const replicationPassfileName = "replication.pgpass" //nolint:gosec // a file name, not a credential

// writeReplicationPassfile writes the passfile postgres's primary_conninfo
// points at. The sidecar keeps its own pgpass off the data volume; what is
// written here is the project's own postgres credential, which postgres already
// holds, and only its user entry.
func (pm *MultipoolerManager) writeReplicationPassfile() error {
	user := constants.DefaultPostgresUser
	password := os.Getenv(constants.PgPasswordEnvVar)
	if pm.connPoolMgr != nil {
		user = pm.connPoolMgr.PgUser()
		pw, ok := pm.connPoolMgr.PgPassword()
		if !ok {
			return errors.New("replication passfile: postgres password not resolved (ResolvePgPassword must run before sidecar setup)")
		}
		password = pw
	}
	root, err := pm.openGenerationsRoot()
	if err != nil {
		return fmt.Errorf("open pooler dir for the replication passfile: %w", err)
	}
	defer root.Close()
	if err := root.WriteFile(replicationPassfileName, fmt.Appendf(nil, "*:*:*:%s:%s\n", user, password), 0o600); err != nil {
		return fmt.Errorf("write replication passfile: %w", err)
	}
	pm.mu.Lock()
	pm.pgpassPath = filepath.Join(pm.record.PoolerDir(), replicationPassfileName)
	pm.mu.Unlock()
	return nil
}

// validateSidecarSockets refuses a configuration whose sockets are missing or
// would render nonsense: an empty WAL socket becomes --unix-socket ” in
// archive_command and restore_command, and both must be absolute paths.
func validateSidecarSockets(config *Config) error {
	if config == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "a manager config is required")
	}
	for _, f := range []struct{ flag, path string }{
		{"--pgbackrest-control-socket", config.PgBackRestControlSocket},
		{"--pgbackrest-wal-socket", config.PgBackRestWALSocket},
	} {
		flag, path := f.flag, f.path
		if path == "" {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "%s is required: --pgbackrest-control-socket and --pgbackrest-wal-socket name the pgbackrestd sidecar and must both be set", flag)
		}
		if !filepath.IsAbs(path) {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "%s must be an absolute path, got %q", flag, path)
		}
	}
	return nil
}
