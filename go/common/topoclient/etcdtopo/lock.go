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

package etcdtopo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"time"

	"github.com/spf13/pflag"

	"go.etcd.io/etcd/api/v3/mvccpb"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/pb/mtrpc"
)

var leaseTTL = 30 // This is the default used for all non-named locks

func init() {
	servenv.OnParse(registerEtcd2TopoLockFlags)
}

func registerEtcd2TopoLockFlags(fs *pflag.FlagSet) {
	fs.IntVar(&leaseTTL, "topo-etcd-lease-ttl", leaseTTL, "Lease TTL for locks and leader election. The client will use KeepAlive to keep the lease going.")
}

// newUniqueEphemeralKV creates a new file in the provided directory.
// It is linked to the Lease.
// Errors returned are converted to topo errors.
func (s *Server) newUniqueEphemeralKV(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, nodePath string, contents string) (string, int64, error) {
	// Use the lease ID as the file name, so it's guaranteed unique.
	newKey := fmt.Sprintf("%v/%v", nodePath, leaseID)

	// Only create a new file if it doesn't exist already
	// (version = 0), to avoid two processes using the
	// same file name. Since we use the lease ID, this should never happen.
	txnresp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(newKey), "=", 0)).
		Then(clientv3.OpPut(newKey, contents, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Our context was canceled as we were sending
			// a creation request. We don't know if it
			// succeeded or not. In any case, let's try to
			// delete the node, so we don't leave an orphan
			// node behind for *leaseTTL time.

			if _, err := cli.Delete(context.TODO(), newKey); err != nil {
				slog.InfoContext(ctx, "cli.Delete(context.TODO(), newKey) failed", "error", err)
			}
		}
		return "", 0, convertError(err, newKey)
	}
	if !txnresp.Succeeded {
		// The key already exists, that should not happen.
		return "", 0, ErrBadResponse
	}
	// The key was created.
	return newKey, txnresp.Header.Revision, nil
}

// waitOnLastRev waits on all revisions of the files in the provided
// directory that have revisions smaller than the provided revision.
// It returns true only if there is no more other older files.
func (s *Server) waitOnLastRev(ctx context.Context, cli *clientv3.Client, nodePath string, revision int64) (bool, error) {
	// Get the keys that are blocking us, if any.
	opts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(revision-1))
	lastKey, err := cli.Get(ctx, nodePath+"/", opts...)
	if err != nil {
		return false, convertError(err, nodePath)
	}
	if len(lastKey.Kvs) == 0 {
		// No older key, we're done waiting.
		return true, nil
	}

	// Wait for release on blocking key. Cancel the watch when we
	// exit this function.
	key := string(lastKey.Kvs[0].Key)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := cli.Watch(ctx, key, clientv3.WithRev(revision))
	if wc == nil {
		return false, mterrors.Errorf(mtrpc.Code_INTERNAL, "Watch failed")
	}

	select {
	case <-ctx.Done():
		return false, convertError(ctx.Err(), nodePath)
	case wresp := <-wc:
		for _, ev := range wresp.Events {
			if ev.Type == mvccpb.DELETE {
				// There might still be older keys,
				// but not this one.
				return false, nil
			}
		}
	}

	// The Watch stopped, we're not sure if there are more items.
	return false, nil
}

// etcdLockDescriptor implements topoclient.LockDescriptor.
type etcdLockDescriptor struct {
	s       *Server
	leaseID clientv3.LeaseID
}

// TryLock is part of the topoclient.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	// We list all the entries under dirPath
	entries, err := s.ListDir(ctx, dirPath, true)
	if err != nil {
		// We need to return the right error codes, like
		// topoclient.ErrNoNode and topoclient.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	// If there is a folder '/locks' with some entries in it then we can assume that someone else already has a lock.
	// Throw error in this case
	for _, e := range entries {
		if e.Name == locksPath && e.Type == topoclient.TypeDirectory && e.Ephemeral {
			return nil, topoclient.NewError(topoclient.NodeExists, fmt.Sprintf("lock already exists at path %s", dirPath))
		}
	}

	// everything is good let's acquire the lock.
	return s.lock(ctx, dirPath, contents, leaseTTL)
}

// Lock is part of the topoclient.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topoclient.ErrNoNode and topoclient.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents, leaseTTL)
}

// LockWithTTL is part of the topoclient.Conn interface.
func (s *Server) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topoclient.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topoclient.ErrNoNode and topoclient.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents, int(ttl.Seconds()))
}

// LockName is part of the topoclient.Conn interface.
func (s *Server) LockName(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	return s.lock(ctx, dirPath, contents, int(topoclient.NamedLockTTL.Seconds()))
}

// LockNameWithTTL is part of the topoclient.Conn interface.
func (s *Server) LockNameWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topoclient.LockDescriptor, error) {
	ttlSeconds := int(topoclient.NamedLockTTL.Seconds())
	if ttl > 0 {
		ttlSeconds = int(ttl.Seconds())
	}
	return s.lock(ctx, dirPath, contents, ttlSeconds)
}

// TryLockName is part of the topoclient.Conn interface.
// It combines the fail-fast semantics of TryLock with LockName's ability to
// lock paths that don't exist. It checks if a lock already exists at the
// named lock path, and if so returns an error immediately.
func (s *Server) TryLockName(ctx context.Context, dirPath, contents string) (topoclient.LockDescriptor, error) {
	// Check if a lock already exists by listing the locks directory
	lockPath := path.Join(s.root, dirPath, locksPath)
	resp, err := s.cli.Get(ctx, lockPath, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return nil, convertError(err, dirPath)
	}
	if len(resp.Kvs) > 0 {
		return nil, topoclient.NewError(topoclient.NodeExists, fmt.Sprintf("lock already exists at path %s", dirPath))
	}
	// No lock exists, proceed with acquiring the named lock
	return s.lock(ctx, dirPath, contents, int(topoclient.NamedLockTTL.Seconds()))
}

// lock is used by both Lock() and primary election.
func (s *Server) lock(ctx context.Context, nodePath, contents string, ttl int) (topoclient.LockDescriptor, error) {
	nodePath = path.Join(s.root, nodePath, locksPath)

	// Get a lease, set its KeepAlive.
	lease, err := s.cli.Grant(ctx, int64(ttl))
	if err != nil {
		return nil, convertError(err, nodePath)
	}
	leaseKA, err := s.cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return nil, convertError(err, nodePath)
	}
	go func() {
		// Drain the lease keepAlive channel, we're not
		// interested in its contents.
		for range leaseKA {
		}
	}()

	// Create an ephemeral node in the locks directory.
	key, revision, err := s.newUniqueEphemeralKV(ctx, s.cli, lease.ID, nodePath, contents)
	if err != nil {
		return nil, err
	}

	// Wait until all older nodes in the locks directory are gone.
	for {
		done, err := s.waitOnLastRev(ctx, s.cli, nodePath, revision)
		if err != nil {
			// We had an error waiting on the last node.
			// Revoke our lease, this will delete the file.
			if _, rerr := s.cli.Revoke(context.TODO(), lease.ID); rerr != nil {
				slog.InfoContext(ctx, fmt.Sprintf("Revoke(%d) failed, may have left %v behind: %v", lease.ID, key, rerr))
			}
			return nil, err
		}
		if done {
			// No more older nodes, we're it!
			return &etcdLockDescriptor{
				s:       s,
				leaseID: lease.ID,
			}, nil
		}
	}
}

// Check is part of the topoclient.LockDescriptor interface.
// We use KeepAliveOnce to make sure the lease is still active and well.
func (ld *etcdLockDescriptor) Check(ctx context.Context) error {
	_, err := ld.s.cli.KeepAliveOnce(ctx, ld.leaseID)
	if err != nil {
		return convertError(err, "lease")
	}
	return nil
}

// Unlock is part of the topoclient.LockDescriptor interface.
func (ld *etcdLockDescriptor) Unlock(ctx context.Context) error {
	_, err := ld.s.cli.Revoke(ctx, ld.leaseID)
	if err != nil {
		return convertError(err, "lease")
	}
	return nil
}
