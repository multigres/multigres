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
	"strings"
	"testing"

	"github.com/multigres/multigres/go/clustermetadata/topo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkFile tests the file part of the Conn API.
func checkFile(t *testing.T, ctx context.Context, ts topo.Store) {
	// global cell
	t.Logf("===   checkFileInCell global")
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")
	checkFileInCell(t, conn, true /*hasCells*/)

	// local cell
	t.Logf("===   checkFileInCell global")
	conn, err = ts.ConnForCell(ctx, LocalCellName)
	require.NoError(t, err, "ConnForCell(test) failed")
	checkFileInCell(t, conn, false /*hasCells*/)
}

func checkFileInCell(t *testing.T, conn topo.Conn, hasCells bool) {
	ctx := context.Background()

	// ListDir root: nothing.
	var expected []topo.DirEntry
	if hasCells {
		expected = append(expected, topo.DirEntry{
			Name: "cells",
			Type: topo.TypeDirectory,
		})
	}
	checkListDir(ctx, t, conn, "/", expected)

	// Get with no file -> ErrNoNode.
	_, _, err := conn.Get(ctx, "/myfile")
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}), "Get(non-existent) didn't return ErrNoNode but: %v", err)

	// Create a file.
	version, err := conn.Create(ctx, "/myfile", []byte{'a'})
	require.NoError(t, err, "Create('/myfile') failed")

	// See it in the listing now.
	expected = append(expected, topo.DirEntry{
		Name: "myfile",
		Type: topo.TypeFile,
	})
	checkListDir(ctx, t, conn, "/", expected)

	// Get should work, get the right contents and version.
	contents, getVersion, err := conn.Get(ctx, "/myfile")
	require.NoError(t, err, "Get('/myfile') returned an error")
	assert.Equal(t, []byte{'a'}, contents, "Get('/myfile') returned bad content")
	assert.Equal(t, version, getVersion, "Get('/myfile') returned bad version")

	// Update it, make sure version changes.
	newVersion, err := conn.Update(ctx, "/myfile", []byte{'b'}, version)
	require.NoError(t, err, "Update('/myfile') failed")
	assert.NotEqual(t, version, newVersion, "Version didn't change")

	// Get should work, get the right contents and version.
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	require.NoError(t, err, "Get('/myfile') returned an error")
	assert.Equal(t, []byte{'b'}, contents, "Get('/myfile') returned bad content")
	assert.Equal(t, newVersion, getVersion, "Get('/myfile') returned bad version")

	// Try to update again with wrong version, should fail.
	_, err = conn.Update(ctx, "/myfile", []byte{'b'}, version)
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.BadVersion}), "Update(bad version) didn't return ErrBadVersion but: %v", err)

	// Try to update again with nil version, should work.
	newVersion, err = conn.Update(ctx, "/myfile", []byte{'c'}, nil)
	require.NoError(t, err, "Update(nil version) should have worked")

	// Get should work, get the right contents and version.
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	require.NoError(t, err, "Get('/myfile') returned an error")
	assert.Equal(t, []byte{'c'}, contents, "Get('/myfile') returned bad content")
	assert.Equal(t, newVersion, getVersion, "Get('/myfile') returned bad version")

	// Try to update again with empty content, should work.
	newVersion, err = conn.Update(ctx, "/myfile", nil, newVersion)
	require.NoError(t, err, "Update(empty content) should have worked")
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	require.NoError(t, err, "Get('/myfile') expecting no error")
	assert.Equal(t, 0, len(contents), "Get('/myfile') expecting empty content")
	assert.Equal(t, newVersion, getVersion, "Get('/myfile') expecting correct version")

	// Try to delete with wrong version, should fail.
	err = conn.Delete(ctx, "/myfile", version)
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.BadVersion}), "Delete('/myfile', wrong version) returned bad error: %v", err)

	// Now delete it.
	err = conn.Delete(ctx, "/myfile", newVersion)
	require.NoError(t, err, "Delete('/myfile') failed")

	// ListDir root: nothing.
	expected = expected[:len(expected)-1]
	checkListDir(ctx, t, conn, "/", expected)

	// Try to delete again, should fail.
	err = conn.Delete(ctx, "/myfile", newVersion)
	assert.True(t, errors.Is(err, &topo.TopoError{Code: topo.NoNode}), "Delete(already gone) returned bad error: %v", err)

	// Create again, with unconditional update.
	version, err = conn.Update(ctx, "/myfile", []byte{'d'}, nil)
	require.NoError(t, err, "Update('/myfile', nil) failed")

	// Check contents.
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	require.NoError(t, err, "Get('/myfile') returned an error")
	assert.Equal(t, []byte{'d'}, contents, "Get('/myfile') returned bad content")
	assert.Equal(t, version, getVersion, "Get('/myfile') returned bad version")

	// See it in the listing now.
	expected = append(expected, topo.DirEntry{
		Name: "myfile",
		Type: topo.TypeFile,
	})
	checkListDir(ctx, t, conn, "/", expected)

	// Unconditional delete.
	err = conn.Delete(ctx, "/myfile", nil)
	require.NoError(t, err, "Delete('/myfile', nil) failed")

	// ListDir root: nothing.
	expected = expected[:len(expected)-1]
	checkListDir(ctx, t, conn, "/", expected)
}

// checkList tests the file part of the Conn API.
func checkList(t *testing.T, ctx context.Context, ts topo.Store) {
	// global topo
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell(LocalCellName) failed")

	_, err = conn.Create(ctx, "/some/arbitrary/file", []byte{'a'})
	require.NoError(t, err, "Create('/myfile') failed")

	_, err = conn.List(ctx, "/")
	if errors.Is(err, &topo.TopoError{Code: topo.NoImplementation}) {
		// If this is not supported, skip the test
		t.Skipf("%T does not support List()", conn)
		return
	}
	require.NoError(t, err, "List(test) failed")

	_, err = conn.Create(ctx, "/toplevel/nested/myfile", []byte{'a'})
	require.NoError(t, err, "Create('/myfile') failed")

	for _, path := range []string{"/top", "/toplevel", "/toplevel/", "/toplevel/nes", "/toplevel/nested/myfile"} {
		entries, err := conn.List(ctx, path)
		require.NoError(t, err, "List failed(path: %q)", path)

		assert.Equal(t, 1, len(entries), "List(test) returned incorrect number of elements for path %q. Expected 1, got %d: %v", path, len(entries), entries)

		assert.True(t, strings.HasSuffix(string(entries[0].Key), "/toplevel/nested/myfile"), "found entry doesn't end with /toplevel/nested/myfile for path %q: %s", path, string(entries[0].Key))

		assert.Equal(t, "a", string(entries[0].Value), "found entry doesn't have value \"a\" for path %q: %s", path, string(entries[0].Value))
	}
}
