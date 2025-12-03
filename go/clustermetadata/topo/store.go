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
	"fmt"
	"io"
	"log"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/viperutil"
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
	GatewayFile  = "Gateway"
	PoolerFile   = "Pooler"
	OrchFile     = "Orch"
)

// Paths for all object types in the topology hierarchy.
const (
	DatabasesPath = "databases"
	CellsPath     = "cells"
	GatewaysPath  = "gateways"
	PoolersPath   = "poolers"
	OrchsPath     = "orchs"
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
	UnregisterMultiPooler(ctx context.Context, id *clustermetadatapb.ID) error
	RegisterMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler, allowUpdate bool) error

	// MultiGateway CRUD operations
	GetMultiGateway(ctx context.Context, id *clustermetadatapb.ID) (*MultiGatewayInfo, error)
	GetMultiGatewayIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error)
	GetMultiGatewaysByCell(ctx context.Context, cellName string) ([]*MultiGatewayInfo, error)
	CreateMultiGateway(ctx context.Context, multigateway *clustermetadatapb.MultiGateway) error
	UpdateMultiGateway(ctx context.Context, mgi *MultiGatewayInfo) error
	UpdateMultiGatewayFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiGateway) error) (*clustermetadatapb.MultiGateway, error)
	UnregisterMultiGateway(ctx context.Context, id *clustermetadatapb.ID) error
	RegisterMultiGateway(ctx context.Context, multigateway *clustermetadatapb.MultiGateway, allowUpdate bool) error

	// MultiOrch CRUD operations
	GetMultiOrch(ctx context.Context, id *clustermetadatapb.ID) (*MultiOrchInfo, error)
	GetMultiOrchIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error)
	GetMultiOrchsByCell(ctx context.Context, cellName string) ([]*MultiOrchInfo, error)
	CreateMultiOrch(ctx context.Context, multiorch *clustermetadatapb.MultiOrch) error
	UpdateMultiOrch(ctx context.Context, moi *MultiOrchInfo) error
	UpdateMultiOrchFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiOrch) error) (*clustermetadatapb.MultiOrch, error)
	UnregisterMultiOrch(ctx context.Context, id *clustermetadatapb.ID) error
	RegisterMultiOrch(ctx context.Context, multiorch *clustermetadatapb.MultiOrch, allowUpdate bool) error
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

	// LockShard acquires a lock on the specified shard.
	// See shard_lock.go for full documentation.
	LockShard(ctx context.Context, shardKey types.ShardKey, action string, opts ...LockOption) (context.Context, func(*error), error)

	// TryLockShard attempts to acquire a lock on the specified shard without blocking.
	// See shard_lock.go for full documentation.
	TryLockShard(ctx context.Context, shardKey types.ShardKey, action string) (context.Context, func(*error), error)

	// Resource cleanup
	io.Closer
}

// ConnProvider defines the interface for obtaining connections to specific cells.
type ConnProvider interface {
	// ConnForCell returns a connection to the topology service for the specified cell.
	// The connection is cached and reused for subsequent requests to the same cell.
	ConnForCell(ctx context.Context, cell string) (Conn, error)

	// Status returns the connection status for all cells
	Status() map[string]string
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

	// cellConns contains cached connections to cell-specific topology services.
	// These connections should be accessed through the ConnForCell() method, which
	// will read the cell configuration from the global cluster and create clients
	// as needed.
	cellConnsMu sync.Mutex
	cellConns   map[string]cellConn

	// status contains information about each connection.
	// If the string for a cell is empty, the connection
	// is healthy. Otherwise, it contains an error message.
	statusMu sync.Mutex
	status   map[string]string
}

// Ensure store implements the Store interface at compile time.
var _ Store = (*store)(nil)

// cellConn represents a cached connection to a cell's topology service
// along with its associated configuration.
type cellConn struct {
	Cell *clustermetadatapb.Cell
	conn Conn
}

// TopoConfig holds topology configuration using viperutil values
type TopoConfig struct {
	implementation        viperutil.Value[string]
	globalServerAddresses viperutil.Value[[]string]
	globalRoot            viperutil.Value[string]
	readConcurrency       viperutil.Value[int64]
}

// NewTopoConfig creates a new TopoConfig with default values
func NewTopoConfig(reg *viperutil.Registry) *TopoConfig {
	return &TopoConfig{
		implementation: viperutil.Configure(reg, "topo-implementation", viperutil.Options[string]{
			Default:  "",
			FlagName: "topo-implementation",
			Dynamic:  false,
		}),
		globalServerAddresses: viperutil.Configure(reg, "topo-global-server-addresses", viperutil.Options[[]string]{
			Default:  []string{},
			FlagName: "topo-global-server-addresses",
			Dynamic:  false,
		}),
		globalRoot: viperutil.Configure(reg, "topo-global-root", viperutil.Options[string]{
			Default:  "",
			FlagName: "topo-global-root",
			Dynamic:  false,
		}),
		readConcurrency: viperutil.Configure(reg, "topo-read-concurrency", viperutil.Options[int64]{
			Default:  32,
			FlagName: "topo-read-concurrency",
			Dynamic:  false,
		}),
	}
}

// RegisterFlags registers all topo flags with the given FlagSet
func (tc *TopoConfig) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("topo-implementation", tc.implementation.Default(), "the topology implementation to use")
	fs.StringSlice("topo-global-server-addresses", tc.globalServerAddresses.Default(), "the address of the global topology server")
	fs.String("topo-global-root", tc.globalRoot.Default(), "the path of the global topology data in the global topology server")
	fs.Int64("topo-read-concurrency", tc.readConcurrency.Default(), "Maximum concurrency of topo reads per global or local cell.")

	viperutil.BindFlags(fs,
		tc.implementation,
		tc.globalServerAddresses,
		tc.globalRoot,
		tc.readConcurrency,
	)
}

// factories contains the registered factories for creating topology connections.
// Each implementation (e.g., etcd, memory) registers its factory here.
var factories = make(map[string]Factory)

// RegisterFactory registers a Factory for a specific topology implementation.
// If an implementation with that name already exists, it will log.Fatal and exit.
// Call this function in the 'init' function of your topology implementation module.
func RegisterFactory(name string, factory Factory) {
	if factories[name] != nil {
		log.Fatalf("Duplicate topo.Factory registration for %v", name)
	}
	factories[name] = factory
}

// GetAvailableImplementations returns a sorted list of all registered topology implementations.
func GetAvailableImplementations() []string {
	implementations := make([]string, 0, len(factories))
	for name := range factories {
		implementations = append(implementations, name)
	}
	// Sort for consistent output
	slices.Sort(implementations)
	return implementations
}

// NewWithFactory creates a new topology store based on the given Factory.
// It also opens the global topology connection and initializes the store.
func NewWithFactory(factory Factory, root string, serverAddrs []string) Store {
	ts := &store{
		factory:   factory,
		cellConns: make(map[string]cellConn),
		status:    make(map[string]string),
	}
	ts.status[GlobalCell] = ""
	conn := NewWrapperConn(
		func() (Conn, error) {
			return factory.Create(GlobalCell, root, serverAddrs)
		},
		func(s string) {
			ts.setStatus(GlobalCell, s)
		},
	)
	ts.globalTopo = conn
	return ts
}

// OpenServer returns a topology store using the specified implementation,
// root path, and server addresses for the global topology server.
func OpenServer(implementation, root string, serverAddrs []string) (Store, error) {
	factory, ok := factories[implementation]
	if !ok {
		// Build a helpful error message showing available implementations
		var available []string
		for name := range factories {
			available = append(available, name)
		}

		if len(available) == 0 {
			return nil, fmt.Errorf("no topology implementations registered. This may indicate a build or import issue")
		}

		return nil, fmt.Errorf("topology implementation '%s' not found. Available implementations: %s", implementation, strings.Join(available, ", "))
	}
	return NewWithFactory(factory, root, serverAddrs), nil
}

// Open returns a topology store using the command-line parameter flags
// for implementation, address, and root. It will log.Error and exit if
// required configuration is missing or if an error occurs.
func (config *TopoConfig) Open() Store {
	addresses := config.globalServerAddresses.Get()
	root := config.globalRoot.Get()
	implementation := config.implementation.Get()

	if len(addresses) == 0 {
		slog.Error("topo-global-server-addresses must be configured")
		os.Exit(1)
	}
	if root == "" {
		slog.Error("topo-global-root must be non-empty")
		os.Exit(1)
	}

	if implementation == "" {
		// Build a helpful message showing available implementations
		var available []string
		for name := range factories {
			available = append(available, name)
		}

		if len(available) == 0 {
			slog.Error("topo-implementation must be configured. Available: none (no implementations registered)")
		} else {
			slog.Error("topo-implementation must be configured. Available: " + strings.Join(available, ", "))
		}
		os.Exit(1)
	}

	ts, err := OpenServer(implementation, root, addresses)
	if err != nil {
		slog.Error("Failed to open topo server", "error", err, "implementation", implementation, "addresses", addresses, "root", root)
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

	ts.cellConnsMu.Lock()
	defer ts.cellConnsMu.Unlock()
	cc, ok := ts.cellConns[cell]
	if ok {
		// Verify that the connection parameters match.
		if cellsEqual(ci, cc.Cell) {
			return cc.conn, nil
		}
		// Connections parameters have changed,
		// close the cached connection and create a new one.
		if cc.conn != nil {
			cc.conn.Close()
		}
	}

	// Connect to the cell topology server while holding the lock.
	// This ensures only one connection is established at any given time.
	// Create the connection and cache it for future use.

	ts.setStatus(cell, "")
	conn := NewWrapperConn(
		func() (Conn, error) {
			return ts.factory.Create(cell, ci.Root, ci.ServerAddresses)
		},
		func(s string) {
			ts.setStatus(cell, s)
		},
	)
	ts.cellConns[cell] = cellConn{
		Cell: &clustermetadatapb.Cell{
			Name:            ci.Name,
			ServerAddresses: slices.Clone(ci.ServerAddresses),
			Root:            ci.Root,
		},
		conn: conn,
	}
	return conn, nil
}

// cellsEqual compares two Cell protos for equality.
// This is needed because gogo/protobuf's proto.Equal doesn't work
// with protobuf v1 generated messages.
func cellsEqual(a, b *clustermetadatapb.Cell) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Name == b.Name &&
		a.Root == b.Root &&
		slices.Equal(a.ServerAddresses, b.ServerAddresses)
}

func (ts *store) setStatus(cell string, status string) {
	ts.statusMu.Lock()
	defer ts.statusMu.Unlock()
	ts.status[cell] = status
}

// Status returns the status of all the connections in the store.
func (ts *store) Status() map[string]string {
	ts.statusMu.Lock()
	defer ts.statusMu.Unlock()
	return maps.Clone(ts.status)
}

// Close will close all connections to underlying topology stores.
// It will nil all member variables, so any further access will panic.
// Returns a combined error if any errors occurred during cleanup.
func (ts *store) Close() error {
	g, _ := errgroup.WithContext(context.Background())

	// Close global topology connection
	g.Go(func() error {
		if ts.globalTopo != nil {
			if err := ts.globalTopo.Close(); err != nil {
				return fmt.Errorf("failed to close global topo: %w", err)
			}
			ts.globalTopo = nil
		}
		return nil
	})

	// Close all cell connections

	g.Go(func() error {
		ts.cellConnsMu.Lock()
		defer ts.cellConnsMu.Unlock()

		for cell, cc := range ts.cellConns {
			if cc.conn != nil {
				if err := cc.conn.Close(); err != nil {
					return fmt.Errorf("failed to close cell connection %s: %w", cell, err)
				}
			}
		}
		return nil
	})
	err := g.Wait()

	func() {
		ts.cellConnsMu.Lock()
		defer ts.cellConnsMu.Unlock()
		ts.cellConns = make(map[string]cellConn)
	}()

	func() {
		ts.statusMu.Lock()
		defer ts.statusMu.Unlock()
		ts.status = make(map[string]string)
	}()

	return err
}
