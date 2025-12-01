// Copyright 2019 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications Copyright 2025 Supabase, Inc.

package topo

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/user"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// This file contains utility methods and definitions to lock resources using topology server.

// Lock describes a long-running lock on a database or a shard.
// It needs to be public as we JSON-serialize it.
type Lock struct {
	// Action and the following fields are set at construction time.
	Action   string
	HostName string
	UserName string
	Time     string
	Options  lockOptions

	// Status is the current status of the Lock.
	Status string
}

func init() {
	servenv.OnParse(registerTopoLockFlags)
}

func registerTopoLockFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&RemoteOperationTimeout, "remote-operation-timeout", RemoteOperationTimeout, "time to wait for a remote operation")
	fs.DurationVar(&LockTimeout, "lock-timeout", LockTimeout, "Maximum time to wait when attempting to acquire a lock from the topo server")
}

// newLock creates a new Lock.
func newLock(action string) *Lock {
	l := &Lock{
		Action:   action,
		HostName: "unknown",
		UserName: "unknown",
		Time:     time.Now().Format(time.RFC3339),
		Status:   "Running",
	}
	if h, err := os.Hostname(); err == nil {
		l.HostName = h
	}
	if u, err := user.Current(); err == nil {
		l.UserName = u.Username
	}
	return l
}

// ToJSON returns a JSON representation of the object.
func (l *Lock) ToJSON() (string, error) {
	data, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return "", mterrors.Wrapf(err, "cannot JSON-marshal node")
	}
	return string(data), nil
}

// lockInfo is an individual info structure for a lock
type lockInfo struct {
	lockDescriptor LockDescriptor
	actionNode     *Lock
}

// locksInfo is the structure used to remember which locks we took
type locksInfo struct {
	// mu protects the following members of the structure.
	// Safer to be thread safe here, in case multiple go routines
	// lock different things.
	mu sync.Mutex

	// info contains all the locks we took. It is indexed by
	// database (for databases) or database/shard (for shards).
	info map[string]*lockInfo
}

// Context glue
type locksKeyType int

var locksKey locksKeyType

// Support different lock types.
type LockType int

const (
	// Blocking is the default lock type when no other valid type
	// is specified.
	Blocking         LockType = iota
	NonBlocking               // Uses TryLock
	Named                     // Uses LockName
	NamedNonBlocking          // Uses LockName semantics with TryLock fail-fast behavior
)

func (lt LockType) String() string {
	switch lt {
	case NonBlocking:
		return "non blocking"
	case Named:
		return "named"
	case NamedNonBlocking:
		return "named non blocking"
	default:
		return "blocking"
	}
}

// iTopoLock is the interface for knowing the resource that is being locked.
// It allows for better controlling nuances for different lock types and log messages.
type iTopoLock interface {
	Type() string
	ResourceName() string
	Path() string
}

// perform the topo lock operation
func (l *Lock) lock(ctx context.Context, ts *store, lt iTopoLock, opts ...LockOption) (LockDescriptor, error) {
	for _, o := range opts {
		o.apply(&l.Options)
	}
	slog.InfoContext(ctx, "Locking resource", "type", lt.Type(), "resource", lt.ResourceName(), "action", l.Action, "options", l.Options)

	ctx, cancel := context.WithTimeout(ctx, LockTimeout)
	defer cancel()

	ctx, span := telemetry.Tracer().Start(ctx, "TopoServer.Lock",
		trace.WithAttributes(
			attribute.String("action", l.Action),
			attribute.String("path", lt.Path()),
		))
	defer span.End()

	j, err := l.ToJSON()
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ts.globalTopo == nil {
		return nil, errors.New("no global cell connection on the topo server")
	}

	start := time.Now()
	var lockDescriptor LockDescriptor
	var lockOp LockOperation

	switch l.Options.lockType {
	case NonBlocking:
		lockOp = LockOpTryLock
		lockDescriptor, err = ts.globalTopo.TryLock(ctx, lt.Path(), j)
	case Named:
		lockOp = LockOpLockNameWithTTL
		lockDescriptor, err = ts.globalTopo.LockNameWithTTL(ctx, lt.Path(), j, l.Options.ttl)
	case NamedNonBlocking:
		lockOp = LockOpTryLockName
		lockDescriptor, err = ts.globalTopo.TryLockName(ctx, lt.Path(), j)
	default:
		if l.Options.ttl != 0 {
			lockOp = LockOpLockWithTTL
			lockDescriptor, err = ts.globalTopo.LockWithTTL(ctx, lt.Path(), j, l.Options.ttl)
		} else {
			lockOp = LockOpLock
			lockDescriptor, err = ts.globalTopo.Lock(ctx, lt.Path(), j)
		}
	}

	// Record metrics
	result := LockResultSuccess
	if err != nil {
		if ctx.Err() != nil {
			result = LockResultTimeout
		} else {
			result = LockResultError
		}
	}
	RecordLockOperation(ctx, lockOp, lt.Type(), lt.ResourceName(), result, time.Since(start))

	return lockDescriptor, err
}

// unlock unlocks a previously locked key.
func (l *Lock) unlock(ctx context.Context, lt iTopoLock, lockDescriptor LockDescriptor, actionError error) error {
	// Detach from the parent timeout, but preserve the trace span.
	// We need to still release the lock even if the parent
	// context timed out.
	span := trace.SpanFromContext(ctx)
	ctx = trace.ContextWithSpan(context.Background(), span)
	ctx, cancel := context.WithTimeout(ctx, RemoteOperationTimeout)
	defer cancel()

	ctx, unlockSpan := telemetry.Tracer().Start(ctx, "TopoServer.Unlock",
		trace.WithAttributes(
			attribute.String("action", l.Action),
			attribute.String("path", lt.Path()),
		))
	defer unlockSpan.End()

	// first update the actionNode
	if actionError != nil {
		slog.InfoContext(ctx, "Unlocking resource with error", "type", lt.Type(), "resource", lt.ResourceName(), "action", l.Action, "error", actionError)
		l.Status = "Error: " + actionError.Error()
	} else {
		slog.InfoContext(ctx, "Unlocking resource successfully", "type", lt.Type(), "resource", lt.ResourceName(), "action", l.Action)
		l.Status = "Done"
	}

	start := time.Now()
	err := lockDescriptor.Unlock(ctx)

	// Record metrics
	result := LockResultSuccess
	if err != nil {
		result = LockResultError
	}
	RecordLockOperation(ctx, LockOpUnlock, lt.Type(), lt.ResourceName(), result, time.Since(start))

	return err
}

func (ts *store) internalLock(ctx context.Context, lt iTopoLock, action string, opts ...LockOption) (context.Context, func(*error), error) {
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		i = &locksInfo{
			info: make(map[string]*lockInfo),
		}
		ctx = context.WithValue(ctx, locksKey, i)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	// check that we are not already locked
	if _, ok := i.info[lt.ResourceName()]; ok {
		return nil, nil, mterrors.Errorf(mtrpc.Code_INTERNAL, "lock for %v %v is already held", lt.Type(), lt.ResourceName())
	}

	// lock it
	l := newLock(action)
	lockDescriptor, err := l.lock(ctx, ts, lt, opts...)
	if err != nil {
		return nil, nil, err
	}
	// and update our structure
	i.info[lt.ResourceName()] = &lockInfo{
		lockDescriptor: lockDescriptor,
		actionNode:     l,
	}
	return ctx, func(finalErr *error) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if _, ok := i.info[lt.ResourceName()]; !ok {
			if *finalErr != nil {
				slog.ErrorContext(ctx, "trying to unlock multiple times", "type", lt.Type(), "resource", lt.ResourceName())
			} else {
				*finalErr = mterrors.Errorf(mtrpc.Code_INTERNAL, "trying to unlock %v %v multiple times", lt.Type(), lt.ResourceName())
			}
			return
		}

		err := l.unlock(ctx, lt, lockDescriptor, *finalErr)
		// if we have an error, we log it, but we still want to delete the lock
		if *finalErr != nil {
			if err != nil {
				// both error are set, just log the unlock error
				slog.WarnContext(ctx, "unlock failed", "type", lt.Type(), "resource", lt.ResourceName(), "error", err)
			}
		} else {
			*finalErr = err
		}
		delete(i.info, lt.ResourceName())
	}, nil
}

// checkLocked checks that the given resource is locked.
func checkLocked(ctx context.Context, lt iTopoLock) error {
	// extract the locksInfo pointer
	i, ok := ctx.Value(locksKey).(*locksInfo)
	if !ok {
		return mterrors.Errorf(mtrpc.Code_INTERNAL, "%v %v is not locked (no locksInfo)", lt.Type(), lt.ResourceName())
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	// find the individual entry
	li, ok := i.info[lt.ResourceName()]
	if !ok {
		return mterrors.Errorf(mtrpc.Code_INTERNAL, "%v %v is not locked (no lockInfo in map)", lt.Type(), lt.ResourceName())
	}

	// Check the lock server implementation still holds the lock.
	return li.lockDescriptor.Check(ctx)
}

// lockOptions configure a Lock call. lockOptions are set by the LockOption
// values passed to the lock functions.
type lockOptions struct {
	lockType LockType
	ttl      time.Duration
}

// LockOption configures how we perform the locking operation.
type LockOption interface {
	apply(*lockOptions)
}

// funcLockOption wraps a function that modifies lockOptions into an
// implementation of the LockOption interface.
type funcLockOption struct {
	f func(*lockOptions)
}

func (flo *funcLockOption) apply(lo *lockOptions) {
	flo.f(lo)
}

func newFuncLockOption(f func(*lockOptions)) *funcLockOption {
	return &funcLockOption{
		f: f,
	}
}

// WithTTL allows you to specify how long the underlying topo server
// implementation should hold the lock before releasing it — even if the caller
// has not explicitly released it. This provides a way to override the global
// ttl values that are set via --topo_consul_lock_session_ttl and
// --topo_etcd_lease_ttl.
// Note: This option is ignored by the ZooKeeper implementation as it does not
// support TTLs.
func WithTTL(ttl time.Duration) LockOption {
	return newFuncLockOption(func(o *lockOptions) {
		o.ttl = ttl
	})
}

// WithType determines the type of lock we take. The options are defined
// by the LockType type.
func WithType(lt LockType) LockOption {
	return newFuncLockOption(func(o *lockOptions) {
		o.lockType = lt
	})
}
