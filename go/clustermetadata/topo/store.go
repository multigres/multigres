// Copyright 2025 The Multigres Authors
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

/*
Package topo provides the API to read and write topology data for a Multigres
cluster. It maintains one Conn to the global topology service and one Conn to
each cell topology service.

The package defines the plug-in interfaces Conn, Factory, and Version that
topology backends implement. Etcd is currently supported as a real backend.

The TopoStore exposes the full API for interacting with the topology. Data is
split into two logical locations, each managed through its own connection:

 1. Global topology: cluster-level static metadata. This includes the minimal
    information required for components to discover databases and their
    locations.

 2. Cell topology: cell-level catalogs that store dynamic metadata about local
    components (gateways, poolers, orchestrators, etc). Each cell is logically
    distinct and accessed through a separate connection. In practice, a
    deployment may choose to run the global and cell topologies on the same
    etcd cluster, but they remain separate in terms of naming and client
    management.

Below a diagram representing the architecture:

	     +----------------------+
	     |    Global Topology   |
	     |  (static metadata)   |
	     |----------------------|
	     | - Databases          |
	     | - Cell locations     |
	     +----------+-----------+
	                |
	----------------+-----------------
	|                                |

+-------v-------+                +-------v-------+
|  Cell Topo A  |                |  Cell Topo B  |
| (dynamic data)|                | (dynamic data)|
|---------------|                |---------------|
| - Gateways    |                | - Gateways    |
| - Poolers     |                | - Poolers     |
| - Orch state  |                | - Orch state  |
+---------------+                +---------------+
*/
package topo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const (
	// GlobalTopo is the name of the global topology.  It is special
	// connection where we store the minimum pieces of information
	// to connect to a multigres cluster: database information
	// and cell locations.
	GlobalTopo = "global"
)

// Filenames for all object types.
const (
	CellLocationFile = "CellLocation"
)

// Path for all object types.
const (
	DatabasesPath = "databases"
	CellsPath     = "cells"
	MultiGateways = "multi_gateways"
)

// Factory is a factory interface to create Conn objects.
// Topo implementations will provide an implementation for this.
type Factory interface {
	Create(topoName, root string, serverAddrs []string) (Conn, error)
}

// GlobalStore defines APIs for cluster-level static metadata.
// These methods are backed by the global topology service.
type GlobalStore interface {
	// GetCellLocationNames returns the names of the existing cells,
	// sorted by name.
	GetCellLocationNames(ctx context.Context) ([]string, error)

	// GetCellLocation retrieves the CellLocation for a given cell.
	GetCellLocation(ctx context.Context, cell string) (*clustermetadatapb.CellLocation, error)

	// CreateCellLocation creates a new CellLocation for a cell.
	CreateCellLocation(ctx context.Context, cell string, ci *clustermetadatapb.CellLocation) error

	// UpdateCellLocationFields reads a CellLocation, applies an update function,
	// and writes it back. Retries transparently on version mismatches.
	UpdateCellLocationFields(ctx context.Context, cell string, update func(*clustermetadatapb.CellLocation) error) error

	// DeleteCellLocation deletes the specified CellLocation. If 'force' is true,
	// it will proceed even if references exist.
	DeleteCellLocation(ctx context.Context, cell string, force bool) error
}

// CellStore defines APIs for cell-level dynamic metadata.
// These methods are backed by the cell topology services.
type CellStore interface {
	// Later: add APIs for gateways, poolers, orchestrator state, etc.
}

// Store is the full topology API. Implementations must satisfy both
// GlobalStore and CellStore. Consumers can depend on the narrower
// interfaces if they only need one.
type Store interface {
	GlobalStore
	CellStore
}

type ConnProvider interface {
	ConnForCell(ctx context.Context, cell string) (Conn, error)
}

// store is the main topo.store object. We support two ways of creating one:
//  1. From an implementation, server addresses, and root path.
//     This uses a plugin mechanism, and we have implementations for
//     etcd and memory.
//  2. Specific implementations may have higher level creation methods
//     (in which case they may provide a more complex Factory).
//     We support memory (for tests and processes that only need an
//     in-memory server).
type store struct {
	// globalTopo is the main connection to the global topo service.
	// It is created once at construction time.
	globalTopo Conn

	// factory allows the creation of connections to various backends.
	// It is set at construction time.
	factory Factory

	// mu protects the following fields.
	mu sync.Mutex
	// cellConns contains clients configured to talk to a list of
	// topo instances representing local topo clusters. These
	// should be accessed with the ConnForCell() method, which
	// will read the list of addresses for that cell from the
	// global cluster and create clients as needed.
	cellConns map[string]cellConn
}

var _ Store = (*store)(nil)
var _ ConnProvider = (*store)(nil)
var _ io.Closer = (*store)(nil)

type cellConn struct {
	CellLocation *clustermetadatapb.CellLocation
	conn         Conn
}

var (
	// topoImplementation is the flag for which implementation to use.
	topoImplementation string

	// topoGlobalServerAddress is the address of the global topology
	// server.
	topoGlobalServerAddresses []string

	// topoGlobalRoot is the root path to use for the global topology
	// server.
	topoGlobalRoot string

	// factories has the factories for the Conn objects.
	factories = make(map[string]Factory)

	FlagBinaries = []string{"multigateway", "multiorch", "multipooler", "pgctld"}

	// DefaultReadConcurrency Default read concurrency to use in order to avoid overwhelming the topo server.
	DefaultReadConcurrency int64 = 32
)

func init() {
	// TODO: Follow up PR perform this hooking
	//for _, cmd := range FlagBinaries {
	//	//	servenv.OnParseFor(cmd, registerTopoFlags)
	//}
}

// func registerTopoFlags(fs *pflag.FlagSet) {
// 	fs.StringVar(&topoImplementation, "topo_implementation", topoImplementation, "the topology implementation to use")
// 	fs.StringSliceVar(&topoGlobalServerAddresses, "topo_global_server_addresses", topoGlobalServerAddresses, "the addresses of the global topology servers")
// 	fs.StringVar(&topoGlobalRoot, "topo_global_root", topoGlobalRoot, "the path of the global topology data in the global topology server")
// 	fs.Int64Var(&DefaultReadConcurrency, "topo_read_concurrency", DefaultReadConcurrency, "Maximum concurrency of topo reads per global or local cell.")
// }

// RegisterFactory registers a Factory for an implementation for a store.
// If an implementation with that name already exists, it log.Fatals out.
// Call this in the 'init' function in your topology implementation module.
func RegisterFactory(name string, factory Factory) {
	if factories[name] != nil {
		log.Fatalf("Duplicate topo.Factory registration for %v", name)
	}
	factories[name] = factory
}

// NewWithFactory creates a new store based on the given Factory.
// It also opens the global cell connection.
func NewWithFactory(factory Factory, root string, serverAddrs []string) (Store, error) {
	conn, err := factory.Create(GlobalTopo, root, serverAddrs)
	if err != nil {
		return nil, err
	}
	// TODO: Follow up and add stats module
	// conn = NewStatsConn(GlobalTopo, conn, globalReadSem)

	return &store{
		globalTopo: conn,
		factory:    factory,
		cellConns:  make(map[string]cellConn),
	}, nil
}

// OpenServer returns a store using the provided implementation,
// address and root for the global server.
func OpenServer(implementation, root string, serverAddrs []string) (Store, error) {
	factory, ok := factories[implementation]
	if !ok {
		return nil, NewError(NoImplementation, implementation)
	}
	return NewWithFactory(factory, root, serverAddrs)
}

// Open returns a store using the command line parameter flags
// for implementation, address and root. It log.Exits out if an error occurs.
func Open() Store {
	if len(topoGlobalServerAddresses) == 0 {
		// TODO: Should we just bring the vitess logger from the get go?
		// CHECK THIS BEFORE MERGING
		slog.Error("topo_global_server_addresses must be configured")
		os.Exit(1)
	}
	if topoGlobalRoot == "" {
		slog.Error("topo_global_root must be non-empty")
		os.Exit(1)
	}
	ts, err := OpenServer(topoImplementation, topoGlobalRoot, topoGlobalServerAddresses)
	if err != nil {
		slog.Error("Failed to open topo server", "error", err, "implementation", topoImplementation, "addresses", topoGlobalServerAddresses, "root", topoGlobalRoot)
		os.Exit(1)
	}
	return ts
}

// ConnForCell returns a Conn object for the given cell.
// It caches Conn objects from previously requested cells.
func (ts *store) ConnForCell(ctx context.Context, cell string) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Global cell is the easy case.
	if cell == GlobalTopo {
		return ts.globalTopo, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// We can use the GlobalReadOnlyCell for this call.
	ci, err := ts.GetCellLocation(ctx, cell)
	if err != nil {
		return nil, err
	}

	serverAddrsStr := strings.Join(ci.ServerAddresses, ",")

	// Return a cached client if present.
	ts.mu.Lock()
	defer ts.mu.Unlock()
	cc, ok := ts.cellConns[cell]
	if ok {
		// Client exists in cache.
		// Let's verify that it is the same cell as we are looking for.
		// The cell name can be re-used with a different ServerAddress and/or Root
		// in which case we should get a new connection and update the cache
		cellLocationAddrs := strings.Join(cc.CellLocation.ServerAddresses, ",")
		if serverAddrsStr == cellLocationAddrs && ci.Root == cc.CellLocation.Root {
			return cc.conn, nil
		}
		// Close the cached connection, we don't need it anymore
		if cc.conn != nil {
			cc.conn.Close()
		}
	}

	// Connect to the cell topo server, while holding the lock.
	// This ensures only one connection is established at any given time.
	// Create the connection and cache it
	conn, err := ts.factory.Create(cell, ci.Root, ci.ServerAddresses)
	switch {
	case err == nil:
		// TODO: Follow up and add stats module
		// cellReadSem := semaphore.NewWeighted(DefaultReadConcurrency)
		// conn = NewStatsConn(cell, conn, cellReadSem)
		ts.cellConns[cell] = cellConn{ci, conn}
		return conn, nil
	case errors.Is(err, &TopoError{code: NoNode}):
		err = mterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", serverAddrsStr, ci.Root))
		return nil, NewError(NoNode, err.Error())
	default:
		return nil, mterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", serverAddrsStr, ci.Root))
	}
}

// Close will close all connections to underlying topo store.
// It will nil all member variables, so any further access will panic.
func (ts *store) Close() error {
	var errs []error

	// Close global topo connection
	if ts.globalTopo != nil {
		if err := ts.globalTopo.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close global topo: %w", err))
		}
		ts.globalTopo = nil
	}

	// Close all cell connections
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for cell, cc := range ts.cellConns {
		if cc.conn != nil {
			if err := cc.conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close cell connection %s: %w", cell, err))
			}
		}
	}

	// Clear the map
	ts.cellConns = make(map[string]cellConn)

	// Return combined error if any occurred
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing connections: %v", errs)
	}

	return nil
}
