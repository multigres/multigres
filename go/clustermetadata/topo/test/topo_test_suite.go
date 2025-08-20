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
	"testing"

	"github.com/multigres/multigres/go/clustermetadata/topo"
)

// LocalCellName is the cell name used by this test suite.
const LocalCellName = "test"

// TopoServerTestSuite runs the full topo.Server/Conn test suite.
// The factory method should return a topo.Server that has a single cell
// called LocalCellName.
func TopoServerTestSuite(t *testing.T, ctx context.Context, factory func() topo.Store) {
	var ts topo.Store

	// Lock and TryLock are part of the Lock API.
	t.Log("=== (Lock) checkLock")
	ts = factory()
	checkLock(t, ctx, ts)
	_ = ts.Close()

	t.Log("=== (Lock) checkTryLock")
	ts = factory()
	checkTryLock(t, ctx, ts)
	_ = ts.Close()

	t.Log("=== (Lock) checkLockName")
	ts = factory()
	checkLockName(t, ctx, ts)
	_ = ts.Close()

	// Directory is part of the Directory API.
	t.Log("=== (Directory) checkDirectory")
	ts = factory()
	checkDirectory(t, ctx, ts)
	_ = ts.Close()

	// Watch and WatchRecursive are part of the Watch API.
	t.Log("=== (Watch) checkWatch")
	ts = factory()
	checkWatch(t, ctx, ts)
	_ = ts.Close()

	t.Log("=== (Watch) checkWatchInterrupt")
	ts = factory()
	checkWatchInterrupt(t, ctx, ts)
	_ = ts.Close()

	ts = factory()
	t.Log("=== (Watch) checkWatchRecursive")
	checkWatchRecursive(t, ctx, ts)
	_ = ts.Close()

	// File is part of the File API.

	t.Log("=== (File) checkFile")
	ts = factory()
	checkFile(t, ctx, ts)
	_ = ts.Close()

	ts = factory()
	t.Log("=== checkList")
	checkList(t, ctx, ts)
	_ = ts.Close()

}
