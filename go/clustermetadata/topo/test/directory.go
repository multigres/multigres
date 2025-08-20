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

package test

import (
	"context"
	"errors"
	"testing"

	"github.com/multigres/multigres/go/clustermetadata/topo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkDirectory tests the directory part of the topo.Conn API.
func checkDirectory(t *testing.T, ctx context.Context, ts topo.Store) {
	// global topo
	t.Logf("===   checkDirectoryInCell global")
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")
	checkDirectoryInCell(t, conn, true /*hasCells*/)

	// local topo
	t.Logf("===   checkDirectoryInCell test")
	conn, err = ts.ConnForCell(ctx, LocalCellName)
	require.NoError(t, err, "ConnForCell(test) failed")
	checkDirectoryInCell(t, conn, false /*hasCells*/)
}

func checkListDir(ctx context.Context, t *testing.T, conn topo.Conn, dirPath string, expected []topo.DirEntry) {
	t.Helper()

	// Build the shallow expected list, when full=false.
	se := make([]topo.DirEntry, len(expected))
	for i, e := range expected {
		se[i].Name = e.Name
	}

	// Test with full=false.
	entries, err := conn.ListDir(ctx, dirPath, false /*full*/)
	switch {
	case errors.Is(err, &topo.TopoError{Code: topo.NoNode}):
		if len(se) != 0 {
			assert.Fail(t, "ListDir(%v, false) returned ErrNoNode but was expecting %v", dirPath, se)
		}
	case err == nil:
		if len(se) != 0 || len(entries) != 0 {
			assert.Equal(t, se, entries, "ListDir(%v, false) returned unexpected entries", dirPath)
		}
	default:
		assert.Fail(t, "ListDir(%v, false) returned unexpected error: %v", dirPath, err)
	}

	// Test with full=true.
	entries, err = conn.ListDir(ctx, dirPath, true /*full*/)
	switch {
	case errors.Is(err, &topo.TopoError{Code: topo.NoNode}):
		if len(expected) != 0 {
			assert.Fail(t, "ListDir(%v, true) returned ErrNoNode but was expecting %v", dirPath, expected)
		}
	case err == nil:
		if len(expected) != 0 || len(entries) != 0 {
			assert.Equal(t, expected, entries, "ListDir(%v, true) returned unexpected entries", dirPath)
		}
	default:
		assert.Fail(t, "ListDir(%v, true) returned unexpected error: %v", dirPath, err)
	}
}

func checkDirectoryInCell(t *testing.T, conn topo.Conn, hasCells bool) {
	ctx := context.Background()

	// ListDir root: nothing
	var expected []topo.DirEntry
	if hasCells {
		expected = append(expected, topo.DirEntry{
			Name: "cells",
			Type: topo.TypeDirectory,
		})
	}
	checkListDir(ctx, t, conn, "/", expected)

	// Create a topolevel entry
	version, err := conn.Create(ctx, "/MyFile", []byte{'a'})
	require.NoError(t, err, "cannot create toplevel file")

	// ListDir should return it.
	expected = append([]topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	}, expected...)
	checkListDir(ctx, t, conn, "/", expected)

	// Delete it, it should be gone.
	err = conn.Delete(ctx, "/MyFile", version)
	require.NoError(t, err, "cannot delete toplevel file")
	expected = expected[1:]
	checkListDir(ctx, t, conn, "/", expected)

	// Create a file 3 layers down.
	version, err = conn.Create(ctx, "/types/name/MyFile", []byte{'a'})
	require.NoError(t, err, "cannot create deep file")
	expected = append(expected, topo.DirEntry{
		Name: "types",
		Type: topo.TypeDirectory,
	})

	// Check listing at all levels.
	checkListDir(ctx, t, conn, "/", expected)
	checkListDir(ctx, t, conn, "/types/", []topo.DirEntry{
		{
			Name: "name",
			Type: topo.TypeDirectory,
		},
	})
	checkListDir(ctx, t, conn, "/types/name/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})

	// Add a second file
	version2, err := conn.Create(ctx, "/types/othername/MyFile", []byte{'a'})
	require.NoError(t, err, "cannot create deep file2")

	// Check entries at all levels
	checkListDir(ctx, t, conn, "/", expected)
	checkListDir(ctx, t, conn, "/types/", []topo.DirEntry{
		{
			Name: "name",
			Type: topo.TypeDirectory,
		},
		{
			Name: "othername",
			Type: topo.TypeDirectory,
		},
	})
	checkListDir(ctx, t, conn, "/types/name/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})
	checkListDir(ctx, t, conn, "/types/othername/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})

	// Delete the first file, expect all lists to return the second one.
	err = conn.Delete(ctx, "/types/name/MyFile", version)
	require.NoError(t, err, "cannot delete deep file")
	checkListDir(ctx, t, conn, "/", expected)
	checkListDir(ctx, t, conn, "/types/", []topo.DirEntry{
		{
			Name: "othername",
			Type: topo.TypeDirectory,
		},
	})
	checkListDir(ctx, t, conn, "/types/name/", nil)
	checkListDir(ctx, t, conn, "/types/othername/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})

	// Delete the second file, expect all lists to return nothing.
	err = conn.Delete(ctx, "/types/othername/MyFile", version2)
	require.NoError(t, err, "cannot delete second deep file")
	for _, dir := range []string{"/types/", "/types/name/", "/types/othername/"} {
		checkListDir(ctx, t, conn, dir, nil)
	}
	expected = expected[:len(expected)-1]
	checkListDir(ctx, t, conn, "/", expected)
}
