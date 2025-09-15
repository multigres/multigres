// Copyright 2025 Supabase, Inc.
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

// Package memorytopo contains an implementation of the topo.Factory /
// topo.Conn interfaces based on an in-memory tree of data.
// It is constructed with an immutable set of cells.
package memorytopo

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const (
	// Path components
	electionsPath = "elections"
)

var ErrConnectionClosed = errors.New("connection closed")

const (
	// UnreachableServerAddr is a sentinel value for CellInfo.ServerAddr.
	// If a memorytopo topo.Conn is created with this serverAddr then every
	// method on that conn which takes a context will simply block until the
	// context finishes, and return ctx.Err(), in order to simulate an
	// unreachable local cell for testing.
	UnreachableServerAddr = "unreachable"
)

// Operation is one of the operations defined by topo.Conn
type Operation int

// The following is the list of topo.Conn operations
const (
	ListDir = Operation(iota)
	Create
	Update
	Get
	List
	Delete
	Lock
	TryLock
	Watch
	WatchRecursive
	NewLeaderParticipation
	Close
)

// Factory is a memory-based implementation of topo.Factory.  It
// takes a file-system like approach, with directories at each level
// being an actual directory node. This is meant to be closer to
// file-system like servers, like ZooKeeper or Chubby. etcd or Consul
// implementations would be closer to a node-based implementation.
//
// It contains a single tree of nodes. Each cell topo.Conn will use
// a subdirectory in that tree.
type Factory struct {
	// mu protects the following fields.
	mu sync.Mutex
	// cells is the toplevel map that has one entry per cell.
	cells map[string]*node
	// generation is used to generate unique incrementing version
	// numbers.  We want a global counter so when creating a file,
	// then deleting it, then re-creating it, we don't restart the
	// version at 1. It is initialized with a random number,
	// so if we have two implementations, the numbers won't match.
	generation uint64
	// err is used for testing purposes to force queries / watches
	// to return the given error
	err error
	// operationErrors is used for testing purposes to fake errors from
	// operations and paths matching the spec
	operationErrors map[Operation][]errorSpec
	// callstats allows us to keep track of how many topo.conn calls
	// we make (Create, Get, Update, Delete, List, ListDir, etc).
	// TODO: Implement stats
	// callstats *stats.CountersWithMultiLabels
}

type errorSpec struct {
	op          Operation
	pathPattern *regexp.Regexp
	err         error
	callCount   int
	maxCalls    int // 0 means unlimited, >0 means fail only this many times
}

// Create is part of the topo.Factory interface.
func (f *Factory) Create(cell, root string, serverAddrs []string) (topo.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cells[cell]; !ok {
		return nil, topo.NewError(topo.NoNode, cell)
	}
	//  note (root, doesn't matter for the in memory topo, hence we don't use it).
	return &conn{
		factory:     f,
		cell:        cell,
		serverAddrs: serverAddrs,
	}, nil
}

// SetError forces the given error to be returned from all calls and propagates
// the error to all active watches.
func (f *Factory) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.err = err
	if err != nil {
		for _, node := range f.cells {
			node.PropagateWatchError(err)
		}
	}
}

// func (f *Factory) GetCallStats() *stats.CountersWithMultiLabels {
//	return f.callstats
//}

// Lock blocks all requests to the topo and is exposed to allow tests to
// simulate an unresponsive topo server
func (f *Factory) Lock() {
	f.mu.Lock()
}

// Unlock unblocks all requests to the topo and is exposed to allow tests to
// simulate an unresponsive topo server
func (f *Factory) Unlock() {
	f.mu.Unlock()
}

// conn implements the topo.Conn interface. It remembers the cell and serverAddr,
// and points at the Factory that has all the data.
type conn struct {
	factory     *Factory
	cell        string
	serverAddrs []string
	closed      atomic.Bool
}

var _ topo.Conn = (*conn)(nil)

// dial returns immediately, unless the conn points to the sentinel
// UnreachableServerAddr, in which case it will block until the context expires.
func (c *conn) dial(ctx context.Context) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}
	for _, addr := range c.serverAddrs {
		if addr == UnreachableServerAddr {
			<-ctx.Done()
		}
	}

	return ctx.Err()
}

// Close is part of the topo.Conn interface.
func (c *conn) Close() error {
	// c.factory.callstats.Add([]string{"Close"}, 1)
	c.closed.Store(true)
	return nil
}

type watch struct {
	contents  chan *topo.WatchData
	recursive chan *topo.WatchDataRecursive
	lock      chan string
}

// node contains a directory or a file entry.
// Exactly one of contents or children is not nil.
type node struct {
	name     string
	version  uint64
	contents []byte
	children map[string]*node

	// parent is a pointer to the parent node.
	// It is set to nil in toplevel and cell node.
	parent *node

	// watches is a map of all watches for this node.
	watches map[int]watch

	// lock is nil when the node is not locked.
	// otherwise it has a channel that is closed by unlock.
	lock chan struct{}

	// lockContents is the contents of the locks.
	// For regular locks, it has the contents that was passed in.
	// For primary election, it has the id of the election leader.
	lockContents string
}

func (n *node) isDirectory() bool {
	return n.children != nil
}

func (n *node) recurseContents(callback func(n *node)) {
	if n.isDirectory() {
		for _, child := range n.children {
			child.recurseContents(callback)
		}
	} else {
		callback(n)
	}
}

func (n *node) propagateRecursiveWatch(ev *topo.WatchDataRecursive) {
	for parent := n.parent; parent != nil; parent = parent.parent {
		for _, w := range parent.watches {
			if w.recursive != nil {
				w.recursive <- ev
			}
		}
	}
}

var (
	nextWatchIndex   = 0
	nextWatchIndexMu sync.Mutex
)

func (n *node) addWatch(w watch) int {
	nextWatchIndexMu.Lock()
	defer nextWatchIndexMu.Unlock()
	watchIndex := nextWatchIndex
	nextWatchIndex++
	n.watches[watchIndex] = w
	return watchIndex
}

// PropagateWatchError propagates the given error to all watches on this node
// and recursively applies to all children
func (n *node) PropagateWatchError(err error) {
	for _, ch := range n.watches {
		if ch.contents == nil {
			continue
		}
		ch.contents <- &topo.WatchData{
			Err: err,
		}
	}

	for _, c := range n.children {
		c.PropagateWatchError(err)
	}
}

// NewServerAndFactory returns a new MemoryTopo and the backing factory for all
// the cells. It will create one cell for each parameter passed in.  It will log.Exit out
// in case of a problem.
func NewServerAndFactory(ctx context.Context, cells ...string) (topo.Store, *Factory) {
	f := &Factory{
		cells:      make(map[string]*node),
		generation: uint64(rand.Int64N(1 << 60)),
		// callstats:       stats.NewCountersWithMultiLabels("", "", []string{"Call"}),
		operationErrors: make(map[Operation][]errorSpec),
	}
	f.cells[topo.GlobalCell] = f.newDirectory(topo.GlobalCell, nil)

	ts, err := topo.NewWithFactory(f, "" /*root*/, []string{""} /*serverAddrs*/)
	if err != nil {
		// CHECK THIS BEFORE MERGING
		slog.Error("topo.NewWithFactory() failed", "error", err)
		os.Exit(1)
	}
	for _, cell := range cells {
		f.cells[cell] = f.newDirectory(cell, nil)
		if err := ts.CreateCell(ctx, cell, &clustermetadatapb.Cell{}); err != nil {
			slog.Error("ts.CreateCellInfo failed", "cell", cell, "error", err)
		}
	}
	return ts, f
}

// NewServer returns the new server
func NewServer(ctx context.Context, cells ...string) topo.Store {
	store, _ := NewServerAndFactory(ctx, cells...)
	return store
}

func (f *Factory) getNextVersion() uint64 {
	f.generation++
	return f.generation
}

func (f *Factory) newFile(name string, contents []byte, parent *node) *node {
	return &node{
		name:     name,
		version:  f.getNextVersion(),
		contents: contents,
		parent:   parent,
		watches:  make(map[int]watch),
	}
}

func (f *Factory) newDirectory(name string, parent *node) *node {
	return &node{
		name:     name,
		version:  f.getNextVersion(),
		children: make(map[string]*node),
		parent:   parent,
		watches:  make(map[int]watch),
	}
}

func (f *Factory) nodeByPath(cell, filePath string) *node {
	n, ok := f.cells[cell]
	if !ok {
		return nil
	}

	parts := strings.Split(filePath, "/")
	for _, part := range parts {
		if part == "" {
			// Skip empty parts, usually happens at the end.
			continue
		}
		if n.children == nil {
			// This is a file.
			return nil
		}
		child, ok := n.children[part]
		if !ok {
			// Path doesn't exist.
			return nil
		}
		n = child
	}
	return n
}

func (f *Factory) getOrCreatePath(cell, filePath string) *node {
	n, ok := f.cells[cell]
	if !ok {
		return nil
	}

	parts := strings.Split(filePath, "/")
	for _, part := range parts {
		if part == "" {
			// Skip empty parts, usually happens at the end.
			continue
		}
		if n.children == nil {
			// This is a file.
			return nil
		}
		child, ok := n.children[part]
		if !ok {
			// Path doesn't exist, create it.
			child = f.newDirectory(part, n)
			n.children[part] = child
		}
		n = child
	}
	return n
}

// recursiveDelete deletes a node and its parent directory if empty.
func (f *Factory) recursiveDelete(n *node) {
	parent := n.parent
	if parent == nil {
		return
	}
	delete(parent.children, n.name)
	if len(parent.children) == 0 {
		f.recursiveDelete(parent)
	}
}

func (f *Factory) AddOperationError(op Operation, pathPattern string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.operationErrors[op] = append(f.operationErrors[op], errorSpec{
		op:          op,
		pathPattern: regexp.MustCompile(pathPattern),
		err:         err,
		maxCalls:    0, // unlimited
	})
}

// AddOneTimeOperationError adds an error that will only be returned once
func (f *Factory) AddOneTimeOperationError(op Operation, pathPattern string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.operationErrors[op] = append(f.operationErrors[op], errorSpec{
		op:          op,
		pathPattern: regexp.MustCompile(pathPattern),
		err:         err,
		maxCalls:    1, // only fail once
	})
}

// ClearOperationErrors clears all operation errors for testing purposes.
func (f *Factory) ClearOperationErrors() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.operationErrors = make(map[Operation][]errorSpec)
}

func (f *Factory) getOperationError(op Operation, path string) error {
	specs := f.operationErrors[op]
	for i, spec := range specs {
		if spec.pathPattern.MatchString(path) {
			// Check if this error should still be returned
			if spec.maxCalls > 0 && spec.callCount >= spec.maxCalls {
				continue
			}
			// Increment call count
			f.operationErrors[op][i].callCount++
			return spec.err
		}
	}
	return nil
}

func init() {
	// TODO: @rafael
	// This is short lived, I will remove this enterilely once we have a real topo server.
	// Adding it to have all the wiring set up and get the tests to pass with an in memory topo.
	_, factory := NewServerAndFactory(context.Background(), "global")
	topo.RegisterFactory("memory", factory)
}
