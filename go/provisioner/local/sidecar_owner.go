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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/shirou/gopsutil/v4/process"
	"golang.org/x/sys/unix"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// Direct child handles are safe to stop through executil while this launcher
// owns them; recovery in a later process uses instance-matched self-shutdown.
var localSidecarChildren sync.Map // owner-file path -> *executil.Cmd

var errSidecarIdentityChanged = errors.New("sidecar process identity changed")

// sidecarOwner is independent of pgctld's state and liveness. Never signal a
// recovered PID without checking both its birth time and exact command line.
type sidecarOwner struct {
	PID        int      `json:"pid"`
	Created    int64    `json:"created"`
	Argv       []string `json:"argv"`
	InstanceID string   `json:"instance_id,omitempty"`
	// Only a launch durably recorded by this protocol may finish readiness
	// after a launcher crash. Legacy records never acquire this permission.
	InstancePending bool `json:"instance_pending,omitempty"`
	// Set atomically only after v1 process exit and authoritative worker
	// quiescence. Retain the incarnation and canonical installation so a
	// non-clean stop remains distinguishable from unknown PID-only legacy state.
	RetiredStateDir string `json:"retired_state_dir,omitempty"`
}

func (p PgBackRestdPaths) ownerFile() string { return filepath.Join(p.Dir, "owner.json") }

// The provisioner lock serializes ownership changes, not the server lifetime.
// The server's own instance lock must protect socket replacement even if the
// provisioner crashes between spawning the process and recording its identity.
func (p PgBackRestdPaths) lockOwner() (*os.File, error) {
	return p.lockFile("provisioner.lock")
}

func (p PgBackRestdPaths) lockFile(name string) (*os.File, error) {
	if err := os.MkdirAll(p.Dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(p.Dir, name), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sidecar ownership is busy: %w", err)
	}
	return f, nil
}

func readSidecarIdentity(pid int) (*sidecarOwner, error) {
	owner, err := observeSidecarIdentity(pid)
	if err != nil {
		return nil, err
	}
	if len(owner.Argv) < 2 {
		return nil, errors.New("incomplete sidecar process identity")
	}
	return owner, nil
}

// observeSidecarIdentity permits an empty argv only for launch observation.
// It is not sufficient evidence for persisting ownership, adoption or signaling.
func observeSidecarIdentity(pid int) (*sidecarOwner, error) {
	if pid <= 0 {
		return nil, errors.New("invalid sidecar PID")
	}
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}
	created, err := proc.CreateTime()
	if err != nil {
		return nil, err
	}
	argv, err := proc.CmdlineSlice()
	if err != nil {
		return nil, err
	}
	if created <= 0 {
		return nil, errors.New("incomplete sidecar process identity")
	}
	return &sidecarOwner{PID: pid, Created: created, Argv: argv}, nil
}

// waitForLaunchedSidecarIdentity is only for a child we just started. initial
// must be captured before Wait can reap that child; it anchors every subsequent
// observation to the same birth identity. Recovery always uses the strict reader.
// No inspection errors are retried, nor can a different command become an owner.
func waitForLaunchedSidecarIdentity(ctx context.Context, initial *sidecarOwner, expected, parentArgv []string, exited <-chan struct{}, observe func(int) (*sidecarOwner, error)) (*sidecarOwner, error) {
	if initial == nil || initial.PID <= 0 || initial.Created <= 0 || len(expected) < 2 {
		return nil, errors.New("incomplete launched sidecar identity")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	pid, created := initial.PID, initial.Created
	owner := initial
	for {
		// Check again after every observation, including a matching one. Exit
		// wins over readiness and never authorizes a recycled PID.
		select {
		case <-exited:
			return nil, errors.New("sidecar exited during launch identity observation")
		default:
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("sidecar launch identity observation: %w", err)
		}
		if owner == nil || owner.PID != pid || owner.Created != created {
			return nil, errSidecarIdentityChanged
		}
		if slices.Equal(owner.Argv, expected) {
			return owner, nil
		}
		// Only the two observed pre-exec states may settle: no argv, or the
		// exact parent command line. An unrelated nonempty argv is a veto.
		if len(owner.Argv) != 0 && !slices.Equal(owner.Argv, parentArgv) {
			return nil, errors.New("launched sidecar identity does not match command")
		}
		select {
		case <-exited:
			return nil, errors.New("sidecar exited during launch identity observation")
		case <-ctx.Done():
			return nil, fmt.Errorf("sidecar launch identity observation: %w", ctx.Err())
		case <-ticker.C:
		}
		var err error
		owner, err = observe(pid)
		if err != nil {
			return nil, err
		}
	}
}

func (o *sidecarOwner) alive() (alive bool, err error) {
	// Process inspection races normal exit: argv can disappear after a live
	// status sample, before the child is reaped (and macOS ps can fail).
	// Recheck for positive exit evidence; an inspection error alone is not exit.
	defer func() {
		if err != nil {
			if running, checkErr := sidecarProcessRunning(o.PID); checkErr == nil && !running {
				alive, err = false, nil
			}
		}
	}()
	running, err := sidecarProcessRunning(o.PID)
	if err != nil || !running {
		return false, err
	}
	now, err := readSidecarIdentity(o.PID)
	if err != nil {
		return false, err
	}
	if o.Created != now.Created || !slices.Equal(o.Argv, now.Argv) {
		return false, fmt.Errorf("sidecar PID %d identity changed; refusing adoption or signal: %w", o.PID, errSidecarIdentityChanged)
	}
	return true, nil
}

// sidecarProcessRunning is only a liveness observation, not identity or
// ownership validation. Only absence or zombie status proves process exit;
// neither proves that descendants are quiescent.
func sidecarProcessRunning(pid int) (bool, error) {
	exists, err := process.PidExists(int32(pid))
	if err != nil || !exists {
		return false, err
	}
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return false, err
	}
	status, err := proc.Status()
	if err != nil {
		return false, err
	}
	return !slices.Contains(status, process.Zombie), nil
}

func (p PgBackRestdPaths) loadOwner() (*sidecarOwner, error) {
	raw, err := os.ReadFile(p.ownerFile())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var o sidecarOwner
	if err := json.Unmarshal(raw, &o); err != nil {
		return nil, err
	}
	if o.PID <= 0 || o.Created <= 0 || len(o.Argv) < 2 {
		return nil, errors.New("invalid sidecar ownership record")
	}
	return &o, nil
}

// rejectLegacyOwnership refuses PID-only experimental state. Even an exact
// argv match cannot establish that pre-protocol descendants are quiescent.
func (p PgBackRestdPaths) rejectLegacyOwnership(pid int) error {
	owner, err := p.loadOwner()
	if err != nil {
		return err
	}
	if owner != nil {
		if owner.RetiredStateDir != "" && pid > 0 && owner.PID != pid {
			return errors.New("retired sidecar does not match saved legacy PID")
		}
		return nil
	}
	if pid <= 0 {
		return nil
	}
	return errors.New("legacy sidecar ownership is unsupported; verified full teardown or namespace reset required")
}

func (p PgBackRestdPaths) saveOwner(o *sidecarOwner) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	if err := fileutil.AtomicWriteFile(p.ownerFile(), raw, 0o600); err != nil {
		return err
	}
	// AtomicWriteFile syncs the file, but not the renamed directory entry.
	return p.syncOwnerDirectory()
}

func (p PgBackRestdPaths) syncOwnerDirectory() error {
	dir, err := os.Open(p.Dir)
	if err != nil {
		return fmt.Errorf("open sidecar ownership directory for sync: %w", err)
	}
	err = errors.Join(dir.Sync(), dir.Close())
	if err != nil {
		return fmt.Errorf("sync sidecar ownership directory: %w", err)
	}
	return nil
}

func (p PgBackRestdPaths) removeOwner() error {
	if err := os.Remove(p.ownerFile()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Sync even when already absent: a prior unlink may have succeeded but
	// failed its directory sync, and retry must complete that transition.
	return p.syncOwnerDirectory()
}

func dialOwnedSidecar(ctx context.Context, socket string, pid int) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	var peer int
	var peerErr error
	err = raw.Control(func(fd uintptr) { peer, peerErr = sidecarPeerPID(int(fd)) })
	if err != nil || peerErr != nil || peer != pid {
		_ = conn.Close()
		return nil, fmt.Errorf("sidecar socket peer does not match PID %d: %w", pid, errors.Join(err, peerErr, errors.New("peer identity mismatch")))
	}
	return conn, nil
}

func sidecarStatus(ctx context.Context, paths PgBackRestdPaths, pid int) (*pb.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialOwnedSidecar(ctx, paths.ControlSocket, pid)
	}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: tr}, "http://pgbackrestd")
	status, err := client.Status(ctx, connect.NewRequest(&pb.StatusRequest{}))
	if err != nil {
		return nil, err
	}
	wal, err := dialOwnedSidecar(ctx, paths.WALSocket, pid)
	if err != nil {
		return nil, err
	}
	return status.Msg, wal.Close()
}

// verifyInstance only mutates the in-memory record. Callers publish it after
// all read-only adoption checks succeed, before credentials or shutdown change.
func (p PgBackRestdPaths) verifyInstance(ctx context.Context, owner *sidecarOwner) error {
	status, err := sidecarStatus(ctx, p, owner.PID)
	if err != nil {
		return err
	}
	if status.GetLifecycleProtocolVersion() != 1 || status.GetInstanceId() == "" {
		return errors.New("sidecar lifecycle/credential-bundle capability is unverified")
	}
	if owner.InstanceID == "" && owner.InstancePending {
		owner.InstanceID = status.GetInstanceId()
		owner.InstancePending = false
	}
	if owner.InstanceID == "" || owner.InstanceID != status.GetInstanceId() {
		return errors.New("sidecar instance identity mismatch")
	}
	if alive, err := owner.alive(); err != nil || !alive {
		return fmt.Errorf("sidecar identity unavailable after Status: %w", errors.Join(err, errors.New("process unavailable")))
	}
	return nil
}

// shutdownInstance never signals a recovered numeric PID. The kernel-verified
// control peer must accept its own recorded incarnation, then actually exit.
// RPC failure (including EOF) is ambiguous and never enables signal fallback.
func (p PgBackRestdPaths) shutdownInstance(ctx context.Context, owner *sidecarOwner) error {
	alive, err := owner.alive()
	if err != nil || !alive {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialOwnedSidecar(ctx, p.ControlSocket, owner.PID)
	}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: tr}, "http://pgbackrestd")
	response, err := client.Shutdown(ctx, connect.NewRequest(&pb.ShutdownRequest{InstanceId: owner.InstanceID}))
	if err != nil {
		return fmt.Errorf("sidecar self-shutdown unconfirmed: %w", err)
	}
	if !response.Msg.GetAccepted() {
		return errors.New("sidecar self-shutdown was not accepted")
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		alive, err := owner.alive()
		if errors.Is(err, errSidecarIdentityChanged) {
			return err
		}
		if err == nil && !alive {
			return nil
		}
		// Darwin process inspection may transiently fail during exit. Retry
		// observation; never interpret an inspection error as proof of exit.
		select {
		case <-ctx.Done():
			return fmt.Errorf("sidecar self-shutdown exit unconfirmed: %w", errors.Join(ctx.Err(), err))
		case <-tick.C:
		}
	}
}

// stop uses a process handle (pidfd on Linux), checking identity before each
// signal. A mismatched/reused PID is an error, never a reason to kill it.
func (o *sidecarOwner) stop(ctx context.Context) (resultErr error) {
	alive, err := o.alive()
	if err != nil || !alive {
		return err
	}
	signal, closeHandle, err := openRecoveredSidecar(o.PID)
	if err != nil {
		return fmt.Errorf("open stable sidecar process handle: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, closeHandle()) }()
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		alive, err := o.alive()
		if err != nil || !alive {
			return err
		}
		if err := signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		timer := time.NewTimer(2 * time.Second)
		tick := time.NewTicker(20 * time.Millisecond)
		done, waitErr := func() (bool, error) {
			for {
				alive, err := o.alive()
				if errors.Is(err, errSidecarIdentityChanged) {
					return true, err
				}
				if err == nil && !alive {
					return true, nil
				}
				// Exit can clear argv before zombie status becomes visible.
				// Retry observation, not signaling: only positive exit evidence
				// completes the stop, and an unresolved error vetoes escalation.
				select {
				case <-ctx.Done():
					return true, ctx.Err()
				case <-timer.C:
					return err != nil, err
				case <-tick.C:
				}
			}
		}()
		timer.Stop()
		tick.Stop()
		if done {
			return waitErr
		}
	}
	return errors.New("sidecar did not exit")
}

func (p PgBackRestdPaths) stopOwned(ctx context.Context, legacy ...*LocalProvisionedService) error {
	lock, err := p.lockOwner()
	if err != nil {
		return err
	}
	defer lock.Close()
	owner, err := p.loadOwner()
	if err != nil {
		return err
	}
	if owner == nil && len(legacy) == 1 {
		owner, err = p.legacyOwnerForStop(legacy[0])
		if err != nil {
			return err
		}
		if owner != nil {
			if err := p.saveOwner(owner); err != nil {
				return err
			}
		}
	}
	if owner == nil {
		if socketAccepts(p.ControlSocket) || socketAccepts(p.WALSocket) {
			return errors.New("unrecorded sidecar is still listening; refusing unverified stop")
		}
		// A previous stop may have unlinked owner.json but failed its sync.
		return p.syncOwnerDirectory()
	}
	if owner.RetiredStateDir != "" {
		return p.verifyRetiredOwner(owner)
	}
	if alive, err := owner.alive(); err != nil {
		return err
	} else if alive {
		if err := p.verifyInstance(ctx, owner); err != nil {
			return err
		}
		if err := p.saveOwner(owner); err != nil {
			return err
		}
	}
	if child, ok := localSidecarChildren.Load(p.ownerFile()); ok {
		cmd := child.(*executil.Cmd)
		if cmd.Process.Pid != owner.PID {
			return errors.New("local sidecar child differs from saved owner")
		}
		if alive, err := owner.alive(); err != nil {
			return err
		} else if alive {
			if err, stopped := cmd.Stop(ctx); !stopped {
				return fmt.Errorf("local sidecar failed to stop: %w", errors.Join(err, ctx.Err(), errors.New("process still running")))
			}
		}
	} else if err := p.shutdownInstance(ctx, owner); err != nil {
		return err
	}
	// Parent exit alone cannot prove its workers are gone. Retire this exact
	// owner under the guard, using one atomic file transition instead of unlink
	// plus a second service-record write (which would create a crash gap).
	if owner.InstanceID == "" || owner.InstancePending {
		return errors.New("cannot retire unverified sidecar instance")
	}
	stateDir, err := cleanupComparisonPath(p.StateDir, true)
	if err != nil {
		return err
	}
	guard, err := pgbackreststate.AcquireCleanupGuard(p.StateDir)
	if err != nil {
		return err
	}
	owner.RetiredStateDir = stateDir
	return errors.Join(p.saveOwner(owner), guard.Close())
}

// A retired record is proof of a past successful stop, not permission to
// bypass current worker evidence. Revalidate the same installation on every
// retry/start; missing state, pending markers, or a new lifetime still veto.
func (p PgBackRestdPaths) verifyRetiredOwner(owner *sidecarOwner) error {
	stateDir, err := cleanupComparisonPath(p.StateDir, true)
	if err != nil {
		return err
	}
	if owner.InstanceID == "" || owner.InstancePending || owner.RetiredStateDir != stateDir {
		return errors.New("invalid retired sidecar installation identity")
	}
	if socketAccepts(p.ControlSocket) || socketAccepts(p.WALSocket) {
		return errors.New("sidecar is listening despite retired ownership")
	}
	guard, err := pgbackreststate.AcquireCleanupGuard(p.StateDir)
	if err != nil {
		return err
	}
	return guard.Close()
}

// A PID-only record cannot authorize termination or prove descendant exit.
func (p PgBackRestdPaths) legacyOwnerForStop(service *LocalProvisionedService) (*sidecarOwner, error) {
	if pid, ok := service.Metadata["pgbackrestd_pid"].(float64); ok && pid > 0 {
		return nil, errors.New("legacy sidecar ownership is unsupported; verified full teardown or namespace reset required")
	}
	return nil, nil
}

// ensureSidecar must run under the ownership lock. It adopts only an identical,
// healthy process; an unhealthy or differently configured live owner fails
// closed, leaving its sockets and PostgreSQL data untouched.
func (p PgBackRestdPaths) ensureSidecar(ctx context.Context, binary string, args []string) (resultPID int, resultErr error) {
	owner, err := p.validateAdoption(ctx, binary, args)
	if err != nil {
		return 0, err
	}
	if owner != nil {
		if err := p.saveOwner(owner); err != nil {
			return 0, err
		}
		return owner.PID, nil
	}
	expected := append([]string{binary}, args...)
	cmd := executil.Command(context.WithoutCancel(ctx), binary, args...).SetEnv([]string{"PATH=" + os.Getenv("PATH"), "LANG=C"})
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	// Capture birth identity before starting Wait: even an exited child cannot
	// have its PID recycled until it is reaped. Some Linux launch observations
	// still expose empty argv or the parent's pre-exec argv at this point.
	initial, identityErr := observeSidecarIdentity(cmd.Process.Pid)
	exited := make(chan struct{})
	localSidecarChildren.Store(p.ownerFile(), cmd)
	go func() {
		_ = cmd.Wait()
		localSidecarChildren.CompareAndDelete(p.ownerFile(), cmd)
		close(exited)
	}()
	ready := false
	defer func() {
		if !ready {
			// Failed-start cleanup must outlive request cancellation and stay bounded.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err, stopped := cmd.Stop(cleanup); !stopped {
				resultErr = errors.Join(resultErr, fmt.Errorf("sidecar startup rollback failed: %w", errors.Join(err, errors.New("process still running"))))
			}
		}
	}()
	if identityErr != nil {
		return 0, identityErr
	}
	owner, err = waitForLaunchedSidecarIdentity(ctx, initial, expected, os.Args, exited, observeSidecarIdentity)
	if err != nil {
		return 0, err
	}
	owner.InstancePending = true
	// Persist before readiness, and before pgctld starts. Failed starts leave a
	// dead record, so a later call can safely retry without forgetting a live PID.
	if err := p.saveOwner(owner); err != nil {
		return 0, err
	}
	if err := WaitForPgBackRestdSockets(ctx, p, exited, 30*time.Second); err != nil {
		return 0, err
	}
	if err := p.verifyInstance(ctx, owner); err != nil {
		return 0, err
	}
	if err := p.saveOwner(owner); err != nil {
		return 0, err
	}
	if alive, err := owner.alive(); err != nil || !alive {
		return 0, fmt.Errorf("sidecar exited during readiness: %w", errors.Join(err, errors.New("process unavailable")))
	}
	ready = true
	return owner.PID, nil
}

// validateAdoption is read-only and runs before any live configuration writes.
func (p PgBackRestdPaths) validateAdoption(ctx context.Context, binary string, args []string) (*sidecarOwner, error) {
	owner, err := p.loadOwner()
	if err != nil {
		return nil, err
	}
	expected := append([]string{binary}, args...)
	if owner != nil && owner.RetiredStateDir != "" {
		if err := p.verifyRetiredOwner(owner); err != nil {
			return nil, err
		}
		// Do not inspect or signal the retired numeric PID: it may now belong
		// to somebody else. The server will acquire the same lifetime lock.
		return nil, nil
	}
	if owner != nil {
		alive, err := owner.alive()
		if err != nil {
			return nil, err
		}
		if alive {
			if !slices.Equal(owner.Argv, expected) {
				return nil, errors.New("live sidecar configuration differs; stop it before reprovisioning")
			}
			if err := p.verifyInstance(ctx, owner); err != nil {
				return nil, fmt.Errorf("existing sidecar unhealthy: %w", err)
			}
			return owner, nil
		}
	}
	// Protect legacy or partially recorded processes, even before the server's
	// lifetime lock gets a chance to reject the new launch.
	if socketAccepts(p.WALSocket) || socketAccepts(p.ControlSocket) {
		return nil, errors.New("unrecorded sidecar is listening; refusing duplicate launch")
	}

	return nil, nil
}
