// Copyright 2025 The Multigres Authors.
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
	// GlobalCell is the name of the global topology. It is a special
	// connection where we store the minimum pieces of information
	// to connect to a multigres cluster: database information
	// and cell locations.
	GlobalCell = "global"
)

// Filenames for all object types.
const (
	CellFile     = "Cell"
	DatabaseFile = "Database"
	PoolerFile   = "Pooler"
)

// Paths for all object types in the topology hierarchy.
const (
	DatabasesPath = "databases"
	CellsPath     = "cells"
	GatewaysPath  = "gateways"
	PoolersPath   = "poolers"
)

// Factory is a factory interface to create Conn objects.
// Topology implementations must provide an implementation for this interface.
type Factory interface {
	Create(topoName, root string, serverAddrs []string) (Conn, error)
}

// GlobalStore defines APIs for cluster-level static metadata.
// These methods are backed by the global topology service and provide
// access to cluster-wide configuration and discovery information.
type GlobalStore interface {
	// GetCellNames returns the names of all existing cells,
	// sorted alphabetically by name.
	GetCellNames(ctx context.Context) ([]string, error)

	// GetCell retrieves the Cell configuration for a given cell.
	GetCell(ctx context.Context, cell string) (*clustermetadatapb.Cell, error)

	// CreateCell creates a new Cell configuration for a cell.
	CreateCell(ctx context.Context, cell string, ci *clustermetadatapb.Cell) error

	// UpdateCellFields reads a Cell, applies an update function,
	// and writes it back atomically.
	UpdateCellFields(ctx context.Context, cell string, update func(*clustermetadatapb.Cell) error) error

	// DeleteCell deletes the specified Cell. If 'force' is true,
	// it will proceed even if references exist, potentially leaving the system
	// in an inconsistent state.
	DeleteCell(ctx context.Context, cell string, force bool) error

	// GetDatabaseNames returns the names of all existing databases, sorted
	// alphabetically by name.
	GetDatabaseNames(ctx context.Context) ([]string, error)

	// GetDatabase retrieves the Database configuration for a given database name.
	GetDatabase(ctx context.Context, database string) (*clustermetadatapb.Database, error)

	// CreateDatabase creates a new Database with the provided configuration.
	CreateDatabase(ctx context.Context, database string, db *clustermetadatapb.Database) error

	// UpdateDatabaseFields reads a Database, applies an update function,
	// and writes it back atomically. Retries transparently on version mismatches.
	UpdateDatabaseFields(ctx context.Context, database string, update func(*clustermetadatapb.Database) error) error

	// DeleteDatabase deletes the specified Database. If 'force' is true,
	// it will proceed even if references exist, potentially leaving the system
	// in an inconsistent state.
	DeleteDatabase(ctx context.Context, database string, force bool) error
}

// CellStore defines APIs for cell-level dynamic metadata.
// These methods are backed by the cell topology services and provide
// access to runtime state information for local components.
type CellStore interface {
	// MultiPooler CRUD operations
	GetMultiPooler(ctx context.Context, id *clustermetadatapb.ID) (*MultiPoolerInfo, error)
	GetMultiPoolerIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error)
	GetMultiPoolersByCell(ctx context.Context, cellName string, opt *GetMultiPoolersByCellOptions) ([]*MultiPoolerInfo, error)
	CreateMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler) error
	UpdateMultiPooler(ctx context.Context, mpi *MultiPoolerInfo) error
	UpdateMultiPoolerFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiPooler) error) (*clustermetadatapb.MultiPooler, error)
	DeleteMultiPooler(ctx context.Context, id *clustermetadatapb.ID) error
	InitMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler, allowPrimaryOverride, allowUpdate bool) error
}

// Store is the full topology API that combines both global and cell operations.
// Implementations must satisfy both GlobalStore and CellStore interfaces.
// Consumers can depend on the narrower interfaces if they only need one type
// of functionality.
type Store interface {
	// Core APIs for global and cell topology operations
	GlobalStore
	CellStore

	// Connection provider for accessing cell-specific connections
	ConnProvider

	// Resource cleanup
	io.Closer
}

// ConnProvider defines the interface for obtaining connections to specific cells.
type ConnProvider interface {
	// ConnForCell returns a connection to the topology service for the specified cell.
	// The connection is cached and reused for subsequent requests to the same cell.
	ConnForCell(ctx context.Context, cell string) (Conn, error)
}

// store is the main topology store implementation. It supports two ways of creation:
//  1. From an implementation, server addresses, and root path using a plugin mechanism.
//     Currently supports etcd and memory backends.
//  2. Specific implementations may provide higher-level creation methods
//     (e.g., memory store for tests and processes that only need in-memory storage).
type store struct {
	// globalTopo is the main connection to the global topology service.
	// It is created once at construction time and handles all cluster-level operations.
	globalTopo Conn

	// factory allows the creation of connections to various topology backends.
	// It is set at construction time and used to create cell-specific connections.
	factory Factory

	// mu protects the following fields from concurrent access.
	mu sync.Mutex
	// cellConns contains cached connections to cell-specific topology services.
	// These connections should be accessed through the ConnForCell() method, which
	// will read the cell configuration from the global cluster and create clients
	// as needed.
	cellConns map[string]cellConn
}

// Ensure store implements the Store interface at compile time.
var _ Store = (*store)(nil)

// cellConn represents a cached connection to a cell's topology service
// along with its associated configuration.
type cellConn struct {
	Cell *clustermetadatapb.Cell
	conn Conn
}

var (
	// topoImplementation specifies which topology implementation to use.
	topoImplementation string

	// topoGlobalServerAddresses contains the addresses of the global topology servers.
	topoGlobalServerAddresses []string

	// topoGlobalRoot is the root path to use for the global topology server.
	topoGlobalRoot string

	// factories contains the registered factories for creating topology connections.
	// Each implementation (e.g., etcd, memory) registers its factory here.
	factories = make(map[string]Factory)

	// FlagBinaries lists the binary names that should register topology flags.
	FlagBinaries = []string{"multigateway", "multiorch", "multipooler", "pgctld"}

	// DefaultReadConcurrency is the default read concurrency limit to avoid
	// overwhelming the topology server with too many concurrent requests.
	DefaultReadConcurrency int64 = 32
)

// RegisterFactory registers a Factory for a specific topology implementation.
// If an implementation with that name already exists, it will log.Fatal and exit.
// Call this function in the 'init' function of your topology implementation module.
func RegisterFactory(name string, factory Factory) {
	if factories[name] != nil {
		log.Fatalf("Duplicate topo.Factory registration for %v", name)
	}
	factories[name] = factory
}

// NewWithFactory creates a new topology store based on the given Factory.
// It also opens the global topology connection and initializes the store.
func NewWithFactory(factory Factory, root string, serverAddrs []string) (Store, error) {
	conn, err := factory.Create(GlobalCell, root, serverAddrs)
	if err != nil {
		return nil, err
	}
	// TODO: Add statistics and monitoring module for topology operations
	// conn = NewStatsConn(GlobalTopo, conn, globalReadSem)

	return &store{
		globalTopo: conn,
		factory:    factory,
		cellConns:  make(map[string]cellConn),
	}, nil
}

// OpenServer returns a topology store using the specified implementation,
// root path, and server addresses for the global topology server.
func OpenServer(implementation, root string, serverAddrs []string) (Store, error) {
	factory, ok := factories[implementation]
	if !ok {
		return nil, NewError(NoImplementation, implementation)
	}
	return NewWithFactory(factory, root, serverAddrs)
}

// Open returns a topology store using the command-line parameter flags
// for implementation, address, and root. It will log.Error and exit if
// required configuration is missing or if an error occurs.
func Open() Store {
	if len(topoGlobalServerAddresses) == 0 {
		// TODO: Consider using a proper logger from the start instead of slog
		// This should be reviewed before merging
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

// ConnForCell returns a connection object for the given cell.
// It caches connection objects from previously requested cells and reuses them
// when the cell configuration hasn't changed.
func (ts *store) ConnForCell(ctx context.Context, cell string) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Global cell is the easy case - return the existing connection.
	if cell == GlobalCell {
		return ts.globalTopo, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// We can use the GlobalReadOnlyCell for this call.
	ci, err := ts.GetCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	serverAddrsStr := strings.Join(ci.ServerAddresses, ",")

	// Return a cached client if present and configuration hasn't changed.
	ts.mu.Lock()
	defer ts.mu.Unlock()
	cc, ok := ts.cellConns[cell]
	if ok {
		// Client exists in cache. Verify that it's for the same cell configuration.
		// The cell name can be reused with different ServerAddresses and/or Root,
		// in which case we should get a new connection and update the cache.
		cellAddrs := strings.Join(cc.Cell.ServerAddresses, ",")
		if serverAddrsStr == cellAddrs && ci.Root == cc.Cell.Root {
			return cc.conn, nil
		}
		// Close the cached connection as it's no longer valid.
		if cc.conn != nil {
			cc.conn.Close()
		}
	}

	// Connect to the cell topology server while holding the lock.
	// This ensures only one connection is established at any given time.
	// Create the connection and cache it for future use.
	conn, err := ts.factory.Create(cell, ci.Root, ci.ServerAddresses)
	switch {
	case err == nil:
		// TODO: Add statistics and monitoring module for cell operations
		// cellReadSem := semaphore.NewWeighted(DefaultReadConcurrency)
		// conn = NewStatsConn(cell, conn, cellReadSem)
		ts.cellConns[cell] = cellConn{ci, conn}
		return conn, nil
	case errors.Is(err, &TopoError{Code: NoNode}):
		err = mterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", serverAddrsStr, ci.Root))
		return nil, NewError(NoNode, err.Error())
	default:
		return nil, mterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", serverAddrsStr, ci.Root))
	}
}

// Close will close all connections to underlying topology stores.
// It will nil all member variables, so any further access will panic.
// Returns a combined error if any errors occurred during cleanup.
func (ts *store) Close() error {
	var errs []error

	// Close global topology connection
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

	// Clear the map to release references
	ts.cellConns = make(map[string]cellConn)

	// Return combined error if any occurred during cleanup
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing connections: %v", errs)
	}

	return nil
}
