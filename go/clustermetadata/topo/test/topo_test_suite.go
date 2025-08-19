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

func executeTestSuite(f func(*testing.T, context.Context, topo.Store), t *testing.T, ctx context.Context, ts topo.Store, ignoreList []string, name string) {
	// some test does not apply everywhere therefore we ignore them
	for _, n := range ignoreList {
		if n == name {
			t.Logf("=== ignoring test %s", name)
			return
		}
	}
	f(t, ctx, ts)
}

// TopoServerTestSuite runs the full topo.Server/Conn test suite.
// The factory method should return a topo.Server that has a single cell
// called LocalCellName.
// Not all tests are applicable for each Topo server, therefore we provide ignoreList in order to
// avoid them for given Topo server tests. For example `TryLock` implementation is same as `Lock` for some Topo servers.
// Hence, for these Topo servers we ignore executing TryLock Tests.
func TopoServerTestSuite(t *testing.T, ctx context.Context, factory func() topo.Store, ignoreList []string) {
	var ts topo.Store

	t.Log("=== checkLock")
	ts = factory()
	executeTestSuite(checkLock, t, ctx, ts, ignoreList, "checkLock")
	_ = ts.Close()

	// t.Log("=== checkTryLock")
	// ts = factory()
	// executeTestSuite(checkTryLock, t, ctx, ts, ignoreList, "checkTryLock")
	// _ = ts.Close()

	// t.Log("=== checkDirectory")
	// ts = factory()
	// executeTestSuite(checkDirectory, t, ctx, ts, ignoreList, "checkDirectory")
	// ts.Close()

	// t.Log("=== checkFile")
	// ts = factory()
	// executeTestSuite(checkFile, t, ctx, ts, ignoreList, "checkFile")
	// ts.Close()

	// t.Log("=== checkWatch")
	// ts = factory()
	// executeTestSuite(checkWatch, t, ctx, ts, ignoreList, "checkWatch")
	// ts.Close()

	// ts = factory()
	// t.Log("=== checkWatchInterrupt")
	// executeTestSuite(checkWatchInterrupt, t, ctx, ts, ignoreList, "checkWatchInterrupt")
	// ts.Close()

	// ts = factory()
	// t.Log("=== checkList")
	// executeTestSuite(checkList, t, ctx, ts, ignoreList, "checkList")
	// ts.Close()

	// ts = factory()
	// t.Log("=== checkWatchRecursive")
	// executeTestSuite(checkWatchRecursive, t, ctx, ts, ignoreList, "checkWatchRecursive")
	// ts.Close()
}
