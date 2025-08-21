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

package topo

import (
	"context"
	"errors"
	"path"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

// This file provides the utility methods to save / retrieve Database
// in the topology server.
//

// pathForDatabase returns the path for a database in the topology.
func pathForDatabase(database string) string {
	return path.Join(DatabasesPath, database, DatabaseFile)
}

// GetDatabaseNames returns the names of the existing databases. They are
// sorted by name.
func (ts *store) GetDatabaseNames(ctx context.Context) ([]string, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	entries, err := ts.globalTopo.ListDir(ctx, DatabasesPath, false /*full*/)
	switch {
	case errors.Is(err, &TopoError{Code: NoNode}):
		return nil, nil
	case err == nil:
		return DirEntriesToStringArray(entries), nil
	default:
		return nil, err
	}
}

// GetDatabase reads a Database from the global Conn.
func (ts *store) GetDatabase(ctx context.Context, database string) (*clustermetadatapb.Database, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	conn := ts.globalTopo
	// Read the file.
	filePath := pathForDatabase(database)
	contents, _, err := conn.Get(ctx, filePath)
	if err != nil {
		return nil, err
	}

	// Unpack the contents.
	db := &clustermetadatapb.Database{}
	if err := proto.Unmarshal(contents, db); err != nil {
		return nil, err
	}
	return db, nil
}

// CreateDatabase creates a new Database with the provided content.
func (ts *store) CreateDatabase(ctx context.Context, database string, db *clustermetadatapb.Database) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Pack the content.
	contents, err := proto.Marshal(db)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForDatabase(database)
	_, err = ts.globalTopo.Create(ctx, filePath, contents)
	return err
}

// UpdateDatabaseFields is a high level helper method to read a Database
// object, update its fields, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil is returned.
func (ts *store) UpdateDatabaseFields(ctx context.Context, database string, update func(*clustermetadatapb.Database) error) error {
	filePath := pathForDatabase(database)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		db := &clustermetadatapb.Database{}

		// Read the file, unpack the contents.
		contents, version, err := ts.globalTopo.Get(ctx, filePath)
		switch {
		case err == nil:
			if err := proto.Unmarshal(contents, db); err != nil {
				return err
			}
		case errors.Is(err, &TopoError{Code: NoNode}):
			// Nothing to do.
		default:
			return err
		}

		// Call update method.
		if err = update(db); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil
			}
			return err
		}

		// Pack and save.
		contents, err = proto.Marshal(db)
		if err != nil {
			return err
		}
		if _, err = ts.globalTopo.Update(ctx, filePath, contents, version); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return err
		}
	}
}

// DeleteDatabase deletes the specified Database.
// We first try to make sure no other records reference this database,
// but we'll continue regardless if 'force' is true.
func (ts *store) DeleteDatabase(ctx context.Context, database string, force bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// TODO: Check if this database is being used by any MultiPooler records before deleting it.

	filePath := pathForDatabase(database)
	return ts.globalTopo.Delete(ctx, filePath, nil)
}
