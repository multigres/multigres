// Copyright 2026 Supabase, Inc.
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

package plpgsql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// A push opens a block level; a var added after it resolves; pop discards it.
func TestNamespacePushLookupPop(t *testing.T) {
	var ns namespace
	ns.init()
	assert.Nil(t, ns.topItem())

	ns.push("", labelBlock)
	ns.additem(nsTypeVar, 0, "x")
	ns.additem(nsTypeVar, 1, "y")

	item, used := ns.lookup(ns.topItem(), false, "x", "", "")
	assert.NotNil(t, item)
	assert.Equal(t, 0, item.itemNo)
	assert.Equal(t, 1, used)

	item, used = ns.lookup(ns.topItem(), false, "missing", "", "")
	assert.Nil(t, item)
	assert.Equal(t, 0, used)

	ns.pop()
	// After popping the only block, nothing resolves.
	item, _ = ns.lookup(ns.topItem(), false, "x", "", "")
	assert.Nil(t, item)
}

// A nested block sees outer variables unless localmode limits the search to the
// innermost level.
func TestNamespaceNestedAndLocalmode(t *testing.T) {
	var ns namespace
	ns.init()
	ns.push("outer", labelBlock)
	ns.additem(nsTypeVar, 0, "outervar")
	ns.push("inner", labelBlock)
	ns.additem(nsTypeVar, 1, "innervar")

	// Non-local lookup climbs to the outer block.
	item, used := ns.lookup(ns.topItem(), false, "outervar", "", "")
	assert.NotNil(t, item)
	assert.Equal(t, 0, item.itemNo)
	assert.Equal(t, 1, used)

	// Local lookup only sees the inner block.
	item, _ = ns.lookup(ns.topItem(), true, "outervar", "", "")
	assert.Nil(t, item)

	item, _ = ns.lookup(ns.topItem(), true, "innervar", "", "")
	assert.NotNil(t, item)
	assert.Equal(t, 1, item.itemNo)
}

// A qualified name (label.var) resolves with names_used == 2.
func TestNamespaceQualifiedLookup(t *testing.T) {
	var ns namespace
	ns.init()
	ns.push("blk", labelBlock)
	ns.additem(nsTypeVar, 5, "v")

	item, used := ns.lookup(ns.topItem(), false, "blk", "v", "")
	assert.NotNil(t, item)
	assert.Equal(t, 5, item.itemNo)
	assert.Equal(t, 2, used)

	// A three-part name disregards a qualified match to a scalar var.
	item, _ = ns.lookup(ns.topItem(), false, "blk", "v", "field")
	assert.Nil(t, item)
}

// Labels are found by lookupLabel; findNearestLoop skips non-loop labels.
func TestNamespaceLabels(t *testing.T) {
	var ns namespace
	ns.init()
	ns.push("outer", labelBlock)
	ns.push("myloop", labelLoop)
	ns.push("", labelBlock)

	assert.NotNil(t, ns.lookupLabel(ns.topItem(), "myloop"))
	assert.NotNil(t, ns.lookupLabel(ns.topItem(), "outer"))
	assert.Nil(t, ns.lookupLabel(ns.topItem(), "nope"))

	loop := ns.findNearestLoop(ns.topItem())
	assert.NotNil(t, loop)
	assert.Equal(t, "myloop", loop.name)
}

// findNearestLoop returns nil when no enclosing loop exists.
func TestNamespaceNoLoop(t *testing.T) {
	var ns namespace
	ns.init()
	ns.push("blk", labelBlock)
	assert.Nil(t, ns.findNearestLoop(ns.topItem()))
}

// addDatum assigns each datum its dno (its index) and appends it to the list.
func TestAddDatum(t *testing.T) {
	l := newLexer("")
	a := plpgsqlast.NewPLpgSQL_var("a")
	b := plpgsqlast.NewPLpgSQL_alias("b")
	l.addDatum(a)
	l.addDatum(b)
	assert.Equal(t, 0, a.DatumNo())
	assert.Equal(t, 1, b.DatumNo())
	assert.Len(t, l.datums, 2)
}
