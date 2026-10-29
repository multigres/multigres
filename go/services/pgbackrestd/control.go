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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/safefs"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/tools/fileutil"
)

const (
	maxControlRequestBytes = 1 << 20
	versionTimeout         = 10 * time.Second

	// startAttempts bounds retries of the final "pgbackrest start": the stop
	// file it removes blocks archiving until something does.
	startAttempts = 3
)

// restoreDrainWait is how long a restore waits for in-flight commands and WAL
// requests before giving up and leaving everything running. A variable only so
// a test need not wait it out.
var restoreDrainWait = 60 * time.Second

// Timing a test shortens.
var (
	// restoreTimeout bounds the restore child, matching the local restore.
	restoreTimeout = backup.RestoreTimeout
	// restoreStepTimeout bounds each "stop" and "start" child of the restore
	// sequence, so a wedged one cannot hold the gate, the paused 8432 server
	// and restore_in_progress forever.
	restoreStepTimeout = 60 * time.Second
	startRetryDelay    = time.Second
	versionRetryTTL    = 30 * time.Second
)

// registerControl mounts the control API. Only multipooler and the sidecar
// itself can reach its socket.
func (s *Server) registerControl(mux *http.ServeMux) {
	path, h := pgbackrestdserviceconnect.NewPgBackRestdHandler(&controlService{s: s},
		connect.WithReadMaxBytes(maxControlRequestBytes))
	mux.Handle(path, h)
}

type controlService struct{ s *Server }

func (c *controlService) Configure(_ context.Context, req *connect.Request[pb.ConfigureRequest]) (*connect.Response[pb.ConfigureResponse], error) {
	resp, err := c.s.configure(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (c *controlService) Status(ctx context.Context, _ *connect.Request[pb.StatusRequest]) (*connect.Response[pb.StatusResponse], error) {
	return connect.NewResponse(c.s.status(ctx)), nil
}

func (c *controlService) Run(ctx context.Context, req *connect.Request[pb.RunRequest], stream *connect.ServerStream[pb.RunOutput]) error {
	out := newStreamSender(stream)
	if req.Msg.GetCommand() != pb.Command_COMMAND_RESTORE {
		// A caller that parses this output needs all of it, and nothing here is
		// stuck behind a closed gate, so back-pressure beats loss.
		out.blockUntil = ctx
	}
	exit, err := c.s.run(ctx, req.Msg.GetCommand(), req.Msg.GetArgs(), out)
	final := &pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: int32(exit)}, OutputTruncated: out.truncated.Load()}
	if err != nil {
		// Flush what the command printed, then fail the call.
		_ = out.finish(nil)
		return err
	}
	return out.finish(final)
}

// configure replaces the repository list. It is serialized as a whole, and
// either takes effect completely (snapshot, conf files, persisted state) or
// not at all: a failed Configure leaves the previous generation list in
// place, on disk and in memory.
//
// Configure is refused while a restore holds the gate, and a restore waits for
// a running Configure: a repository remap under restore's helper processes
// would change the conf they were started with. A Configure that remaps a
// repository still does not reach pgBackRest workers already running (async
// archive-push and archive-get): they drain with the conf and environment they
// were forked with, which is accepted because multipooler serializes its own
// actions and the backlog they hold is bounded.
func (s *Server) configure(req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	release, ok := s.gate.TryEnter()
	if !ok {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("a restore is in progress"))
	}
	defer release()
	s.configureMu.Lock()
	defer s.configureMu.Unlock()

	heldRev, oldRepos := s.held()
	if req.GetRevision() < heldRev {
		return &pb.ConfigureResponse{Accepted: false, HeldRevision: heldRev}, nil
	}

	st := persistedState{Revision: req.GetRevision(), WALSegmentSizeBytes: req.GetWalSegmentSizeBytes()}
	if st.WALSegmentSizeBytes == 0 {
		st.WALSegmentSizeBytes = s.walSegmentSize()
	}
	for _, r := range req.GetRepos() {
		st.Repos = append(st.Repos, repoGen{Generation: r.GetGeneration(), RepoNumber: r.GetRepoNumber()})
	}
	sortRepos(st.Repos)
	if err := validateState(st); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The key file is read afresh so a rotated Secret takes effect here. A
	// generation missing from it rejects the whole request.
	snap, err := s.buildSnapshot(st.repoMap())
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}

	oldSnap := s.snapshot()
	wasConfigured := s.configured()
	// The snapshot is published before the conf. A child that starts between
	// the two reads the old conf with the new environment: for a repository
	// that stays as it was, nothing differs; for a repository remapped to
	// another generation it gets the new passphrase with the old path and
	// cipher-type, so it fails to decrypt or to start. That is transient and
	// loud, and never a downgrade because cipher-type comes from the conf. The
	// opposite order could start a child that reads a conf naming a repository
	// whose passphrase it was not given.
	s.setRepoSnapshot(snap)
	if err := s.applyConfigure(st, snap); err != nil {
		s.setRepoSnapshot(*oldSnap)
		s.restoreClientConf(wasConfigured, heldRev, oldRepos, *oldSnap)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.mu.Lock()
	s.revision, s.repos = st.Revision, st.Repos
	s.mu.Unlock()
	s.walSegBytes.Store(st.WALSegmentSizeBytes)
	s.configuredFlag.Store(true)
	s.setStateError("")
	return &pb.ConfigureResponse{Accepted: true, HeldRevision: st.Revision}, nil
}

// restoreClientConf puts back the conf a failed Configure replaced: the
// previous one when there was one, otherwise the minimal conf that start and
// stop need before any Configure.
func (s *Server) restoreClientConf(wasConfigured bool, rev int64, repos []repoGen, snap RepoSnapshot) {
	if wasConfigured {
		prev := persistedState{Revision: rev, Repos: repos, WALSegmentSizeBytes: s.walSegmentSize()}
		if err := s.renderClientConf(prev, snap); err != nil {
			s.logger.Error("restore previous pgbackrest.conf after failed configure", "error", err)
		}
		return
	}
	if err := os.Remove(s.cfg.stateSub(fileClientConf)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error("remove pgbackrest.conf after failed first configure", "error", err)
		return
	}
	if err := writeMinimalConf(s.cfg); err != nil {
		s.logger.Error("restore minimal pgbackrest.conf after failed first configure", "error", err)
	}
}

// applyConfigure writes everything a Configure changes on disk, and only what
// differs: multipooler configures on every start, and a Configure that changes
// nothing must not restart the 8432 server (that kills a standby's in-flight
// remote backup through this primary). The state file goes last: it is the
// record of what was accepted, so a failure before it leaves a restart on the
// previous list.
//
// The 8432 server runs sandboxed with a file rule on pgpass, and Landlock pins
// a file rule to the inode open when the ruleset was built. pgpass is replaced
// by temp file plus rename, so a server that kept running would lose read
// access to it (libpq then skips it silently and a standby backup fails with
// "no password supplied"). The server is paused around any write of the three
// files and restarts on the new inodes; a Configure that leaves every file as
// it is (and so the repository mapping, cipher-type and segment-sized queue
// they render) does not touch it, and a changed revision alone only rewrites
// the state file. Pauses nest, so this composes with a restore's own pause.
// pgpass is the only file-level rule on a file the sidecar replaces: the rest
// are directories, the binary, device nodes, and the CA file the operator
// mounts, which the sidecar never rewrites. The secrets snapshot is already
// swapped by the caller and read by S3-command children at spawn.
func (s *Server) applyConfigure(st persistedState, snap RepoSnapshot) error {
	pgpass, havePgpass, err := s.pgpassBody()
	if err != nil {
		return err
	}
	serverConf, err := s.serverConfBody()
	if err != nil {
		return err
	}
	clientConf, err := s.clientConfBody(st, snap)
	if err != nil {
		return err
	}
	writePgpass := havePgpass && !fileHolds(s.cfg.stateSub(filePgpass), pgpass)
	writeServer := !fileHolds(s.cfg.stateSub(fileServerConf), serverConf)
	writeClient := !fileHolds(s.cfg.stateSub(fileClientConf), clientConf)
	if writePgpass || writeServer || writeClient {
		resume := s.pause()
		defer resume()
	}
	if writePgpass {
		if err := fileutil.AtomicWriteFile(s.cfg.stateSub(filePgpass), pgpass, 0o600); err != nil {
			return err
		}
	}
	if writeServer {
		if err := fileutil.AtomicWriteFile(s.cfg.stateSub(fileServerConf), serverConf, 0o600); err != nil {
			return err
		}
	}
	if writeClient {
		if err := fileutil.AtomicWriteFile(s.cfg.stateSub(fileClientConf), clientConf, 0o600); err != nil {
			return err
		}
	}
	return s.writeState(st)
}

// status reports the sidecar's state. It has no failure path: every problem
// it could hit shows in a field instead. generations_with_keys comes from the
// key file as it is now, not as of the last Configure, so a caller can see a
// rotated Secret before sending the Configure that needs it.
func (s *Server) status(ctx context.Context) *pb.StatusResponse {
	rev, repos := s.held()
	resp := &pb.StatusResponse{
		Configured:           s.configured(),
		Revision:             rev,
		ServerUp:             s.serverUp(),
		ServerPort:           uint32(s.cfg.ServerPort),
		PgbackrestVersion:    s.pgbackrestVersion(),
		LocationFingerprint:  s.cfg.locationFingerprint(),
		StateError:           s.stateError(),
		RestoreStartPending:  s.startPending.Load() > 0,
		RestoreInProgress:    s.restoreActive.Load() > 0,
		LastRestoreSucceeded: s.lastRestoreOK.Load(),
		ActiveCommands:       s.activeCommands(),
	}
	for _, r := range repos {
		resp.Repos = append(resp.Repos, &pb.RepoGeneration{Generation: r.Generation, RepoNumber: r.RepoNumber})
	}
	if s.cfg.CipherKeyFile != "" {
		// An unreadable key file shows as no keys: health calls Status, and a
		// Status that fails would hide the very condition it reports.
		keys, err := backup.LoadCipherKeys(s.cfg.CipherKeyFile)
		if err != nil {
			s.logger.WarnContext(ctx, "status: cipher key file unreadable", "error", err)
		}
		for gen, pass := range keys {
			if pass != "" {
				resp.GenerationsWithKeys = append(resp.GenerationsWithKeys, uint32(gen))
				// The same function the pgbackrest_repos rows store, so a row
				// seeded from here matches. The key itself is never returned or
				// logged.
				if resp.GenerationFingerprints == nil {
					resp.GenerationFingerprints = map[uint32]string{}
				}
				resp.GenerationFingerprints[uint32(gen)] = backup.CipherKeyFingerprint(pass)
			}
		}
		slices.Sort(resp.GenerationsWithKeys)
	}
	return resp
}

// pgbackrestVersion returns the version learned so far and never waits for a
// child: health calls Status, and a slow or broken pgbackrest must not stall
// it. When the version is unknown and no attempt is running or recent, a
// background probe starts; a failure is remembered for versionRetryTTL so a
// broken binary is not re-run on every Status.
func (s *Server) pgbackrestVersion() string {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	if s.version != "" {
		return s.version
	}
	if !s.versionBusy && (s.versionFailedAt.IsZero() || time.Since(s.versionFailedAt) >= versionRetryTTL) {
		s.versionBusy = true
		s.goBackground(s.probeVersion)
	}
	return ""
}

func (s *Server) probeVersion() {
	ctx, cancel := context.WithTimeout(s.backgroundCtx(), versionTimeout)
	defer cancel()
	var buf bytes.Buffer
	code, err := s.runChild(ctx, KindS3Command, cmdVersion, nil, &buf, io.Discard)
	version := ""
	if err == nil && code == 0 {
		if f := strings.Fields(buf.String()); len(f) > 0 {
			version = f[len(f)-1]
		}
	}
	if version == "" {
		s.logger.WarnContext(ctx, "pgbackrest version", "exit_code", code, "error", err)
	}
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	s.versionBusy = false
	if version == "" {
		s.versionFailedAt = time.Now()
		return
	}
	s.version = version
}

// goBackground runs f in a goroutine that stopBackground waits for. Once
// stopBackground has begun it refuses (false) rather than add to a WaitGroup
// being waited on.
func (s *Server) goBackground(f func()) bool {
	s.bgMu.Lock()
	defer s.bgMu.Unlock()
	if s.bgStopped {
		return false
	}
	s.pg.bg.Go(f)
	return true
}

// backgroundCtx is the context of the serving process, so work that outlives
// a request ends with Run. It is Background only for a Server that is not
// running.
func (s *Server) backgroundCtx() context.Context {
	if c := s.serveCtx.Load(); c != nil {
		return *c
	}
	return context.Background() //nolint:gocritic // a Server that is not running has no serving context
}

// run executes one allowlisted command and returns its exit code.
func (s *Server) run(ctx context.Context, cmd pb.Command, args []string, out *streamSender) (int, error) {
	if err := checkArgs(cmd, args); err != nil {
		return 0, err
	}
	name, _ := pgbackrestCommand(cmd)
	if !s.configured() {
		return 0, connect.NewError(connect.CodeFailedPrecondition, errors.New("not configured: no Configure has been accepted"))
	}
	// Registered before the gate, so Status shows the run from the moment it is
	// accepted until its child is reaped.
	lc := s.trackRun(ctx, cmd)
	defer s.untrackChild(lc)
	if cmd == pb.Command_COMMAND_RESTORE {
		return s.runRestore(ctx, args, out)
	}
	release, ok := s.gate.TryEnter()
	if !ok {
		return 0, connect.NewError(connect.CodeUnavailable, errors.New("a restore is in progress"))
	}
	defer release()
	code, err := s.runChild(ctx, KindS3Command, name, s.withFixedArgs(cmd, args), out.writer(false), out.writer(true))
	return code, runError(ctx, err)
}

func runError(ctx context.Context, err error) error {
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return connect.NewError(connect.CodeCanceled, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

// withFixedArgs appends the flags the sidecar owns. The allowlist already
// refuses each of these from a caller, so none can appear twice.
func (s *Server) withFixedArgs(cmd pb.Command, args []string) []string {
	argv := slices.Clone(args)
	switch cmd {
	case pb.Command_COMMAND_BACKUP:
		if slices.ContainsFunc(args, func(a string) bool { return strings.HasPrefix(a, "--pg2-host=") }) {
			cert, key, ca := constants.PgBackRestCertFiles(s.cfg.TLSCertDir)
			argv = append(argv,
				"--pg2-host-type=tls",
				"--pg2-host-ca-file="+ca,
				"--pg2-host-cert-file="+cert,
				"--pg2-host-key-file="+key,
			)
		}
	case pb.Command_COMMAND_RESTORE:
		argv = append(argv, "--recovery-option=restore_command="+strings.ReplaceAll(s.restoreCommand(), "'", "''"))
	}
	return argv
}

// restoreCommand is the restore_command recovery fetches WAL with, rendered by
// the function multipooler writes its own with.
func (s *Server) restoreCommand() string {
	return backup.SidecarRestoreCommand(s.cfg.WALSocket, s.cfg.RestorePIDFile)
}

func (s *Server) pause() func() {
	if s.pauseFn != nil {
		return s.pauseFn()
	}
	return s.pauseServer()
}

// runRestore stops everything that uses the repository, restores, and starts
// everything again. The gate keeps new commands and WAL requests out for the
// whole time; "pgbackrest stop" keeps archive-push out (it does not stop
// archive-get, which the gate covers).
//
// Once the gate is closed nothing here may be cut short by the client: a
// multipooler that disconnects retries, and returning with the stop file in
// place would wedge archiving. The deferred start, resume and reopen run on
// every path, in that order. Every child is bounded (the restore by
// restoreTimeout, each stop and start by restoreStepTimeout) and a timeout
// ends only its own step, so a stalled repository or a wedged pgbackrest
// reopens the gate instead of holding it until the sidecar is restarted.
func (s *Server) runRestore(ctx context.Context, args []string, out *streamSender) (exit int, err error) {
	// Visible to Status for the whole sequence, so a caller whose Run call
	// failed can tell the restore is still writing the data directory.
	s.restoreActive.Add(1)
	defer s.restoreActive.Add(-1)
	reopen, gerr := s.gate.Close(ctx, restoreDrainWait)
	if gerr != nil {
		return 0, connect.NewError(connect.CodeUnavailable, fmt.Errorf("could not quiesce for restore: %w", gerr))
	}
	defer reopen()
	resume := s.pause()
	defer resume()

	rctx := context.WithoutCancel(ctx)
	defer func() {
		code, serr := s.startRepository(rctx, out)
		if serr == nil && code == 0 {
			return
		}
		// A restore that succeeded is not repeated because start failed, so
		// its result stands; the stop file is cleared by the retry below.
		s.logger.ErrorContext(rctx, "pgbackrest start after restore failed; retrying in the background", "exit_code", code, "error", serr)
		s.startPending.Add(1)
		if !s.goBackground(func() {
			defer s.startPending.Add(-1)
			s.retryStart(s.backgroundCtx())
		}) {
			s.startPending.Add(-1) // shutting down: the boot start clears the file next run
		}
	}()

	// A stop file left by an earlier restore or a failed start makes
	// "stop --force" a no-op that signals nobody, so a live async worker would
	// survive the barrier. The gate is closed, so clearing it first cannot let
	// a push in.
	if code, serr := s.runStep(rctx, out, cmdStart, nil); serr != nil || code != 0 {
		s.logger.WarnContext(rctx, "pgbackrest start before stop failed", "exit_code", code, "error", serr)
	}
	code, rerr := s.runStep(rctx, out, cmdStop, []string{"--force"})
	if rerr != nil || code != 0 {
		// Without the stop barrier a restore could race an archive worker. A
		// stop that timed out counts as failed: the unwinding still runs.
		return code, rerr
	}
	if cerr := s.clearPrefetchQueue(); cerr != nil {
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("clear archive-get queue: %w", cerr))
	}
	restoreCtx, cancel := context.WithTimeout(rctx, restoreTimeout)
	defer cancel()
	s.lastRestoreOK.Store(false)
	code, rerr = s.runChild(restoreCtx, KindS3Command, cmdRestore, s.withFixedArgs(pb.Command_COMMAND_RESTORE, args), out.writer(false), out.writer(true))
	s.lastRestoreOK.Store(rerr == nil && code == 0)
	if rerr != nil && restoreCtx.Err() != nil && rctx.Err() == nil {
		return -1, connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("restore exceeded %s: %w", restoreTimeout, rerr))
	}
	return code, runError(rctx, rerr)
}

// startRepository lifts the stop file, retrying a few times in place; the
// caller hands a persistent failure to retryStart.
func (s *Server) startRepository(ctx context.Context, out *streamSender) (code int, err error) {
	for attempt := 1; attempt <= startAttempts; attempt++ {
		code, err = s.runStep(ctx, out, cmdStart, nil)
		if err == nil && code == 0 {
			return 0, nil
		}
		s.logger.ErrorContext(ctx, "pgbackrest start after restore failed", "attempt", attempt, "exit_code", code, "error", err)
		if attempt < startAttempts {
			time.Sleep(startRetryDelay)
		}
	}
	return code, err
}

// runStep runs one stop or start child of the restore sequence under
// restoreStepTimeout. A timeout returns DeadlineExceeded, which callers treat
// as that step failing; any other failure is reported as runChild's would be.
func (s *Server) runStep(ctx context.Context, out *streamSender, command string, args []string) (int, error) {
	stepCtx, cancel := context.WithTimeout(ctx, restoreStepTimeout)
	defer cancel()
	code, err := s.runChild(stepCtx, KindS3Command, command, args, out.writer(false), out.writer(true))
	if err != nil && stepCtx.Err() != nil && ctx.Err() == nil {
		return -1, connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("pgbackrest %s exceeded %s: %w", command, restoreStepTimeout, err))
	}
	return code, runError(ctx, err)
}

// clearPrefetchQueue empties archive-get's prefetch directory. Restore may not
// write spool/archive, and WAL prefetched before the restore belongs to the
// timeline being replaced. PID 1 does this itself, through safefs because
// children can plant links in spool/.
func (s *Server) clearPrefetchQueue() error {
	root, err := safefs.OpenRoot(s.cfg.stateSub(dirSpool))
	if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveAll("archive/" + Stanza + "/in")
}

// streamSender carries a child's output to the response stream. A child's
// stdout and stderr are copied by separate goroutines, and a stream is not
// safe for concurrent Send, so one pump goroutine sends from a bounded queue.
// For a restore, a write that finds the queue full is dropped (and reported as
// truncation on the final message): a client that stays connected but stops
// reading must not fill the child's pipe and hang it with the gate closed. Any
// other command blocks, ending only with its request. After the first failed Send (the client went away)
// output is likewise dropped without failing the child, which must not die on
// a write error halfway through a restore.
type streamSender struct {
	stream interface{ Send(*pb.RunOutput) error }
	queue  chan *pb.RunOutput
	done   chan struct{}
	mu     sync.Mutex
	err    error
	// blockUntil, when set, makes a write wait for queue space until it is
	// done instead of dropping; it is the request context.
	blockUntil context.Context
	// truncated records that a chunk was dropped.
	truncated atomic.Bool
}

// sendQueueLen is a variable only so a test can fill the queue cheaply.
var sendQueueLen = 256

// sendDrainTimeout is a variable only so a test need not wait it out.
var sendDrainTimeout = 5 * time.Second

func newStreamSender(stream interface{ Send(*pb.RunOutput) error }) *streamSender {
	o := &streamSender{stream: stream, queue: make(chan *pb.RunOutput, sendQueueLen), done: make(chan struct{})}
	go func() {
		defer close(o.done)
		for m := range o.queue {
			_ = o.sendNow(m) // a failed Send latches in o.err
		}
	}()
	return o
}

func (o *streamSender) sendNow(m *pb.RunOutput) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	o.err = o.stream.Send(m)
	return o.err
}

// finish flushes queued output and then sends the final message. A pump stuck
// in Send on a client that stopped reading is abandoned after a bound.
func (o *streamSender) finish(final *pb.RunOutput) error {
	close(o.queue)
	select {
	case <-o.done:
	case <-time.After(sendDrainTimeout):
		return errors.New("client stopped reading the output stream")
	}
	if final == nil {
		return nil
	}
	return o.sendNow(final)
}

type chunkWriter struct {
	o      *streamSender
	stderr bool
}

func (o *streamSender) writer(stderr bool) io.Writer { return &chunkWriter{o: o, stderr: stderr} }

func (w *chunkWriter) Write(p []byte) (int, error) {
	b := bytes.Clone(p)
	m := &pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: b}}
	if w.stderr {
		m = &pb.RunOutput{Chunk: &pb.RunOutput_Stderr{Stderr: b}}
	}
	if w.o.blockUntil != nil {
		select {
		case w.o.queue <- m:
		case <-w.o.blockUntil.Done():
			w.o.truncated.Store(true)
		}
		return len(p), nil
	}
	select {
	case w.o.queue <- m:
	default:
		w.o.truncated.Store(true)
	}
	return len(p), nil
}
