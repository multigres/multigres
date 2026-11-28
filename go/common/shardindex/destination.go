// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardindex

import (
	"encoding/hex"
	"strings"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// DestinationKind says what a Destination names.
type DestinationKind uint8

const (
	// KindNone is no shard: the value cannot be in any.
	KindNone DestinationKind = iota
	// KindRoutingID is the shard owning one routing identifier.
	KindRoutingID
	// KindRoutingIDs is the shards owning several routing identifiers.
	KindRoutingIDs
	// KindKeyRange is every shard overlapping a key range.
	KindKeyRange
	// KindAllShards is every shard.
	KindAllShards
)

// Destination is where a shard index sends a value, independent of any shard
// layout. It is a closed value type rather than an interface so that Map can
// return a slice of them without allocating one object per value, and so that
// new kinds can be added without breaking implementations. The zero value is
// DestinationNone.
type Destination struct {
	kind DestinationKind
	// id is the routing identifier of a KindRoutingID.
	id []byte
	// ids is the routing identifiers of a KindRoutingIDs.
	ids [][]byte
	// keyRange is the range of a KindKeyRange; nil is the full keyspace.
	keyRange *clustermetadatapb.KeyRange
}

// DestinationNone is the destination that resolves to no shard.
func DestinationNone() Destination { return Destination{} }

// DestinationRoutingID is the shard that owns the routing identifier id. id
// is retained and must not be modified afterward.
func DestinationRoutingID(id []byte) Destination {
	return Destination{kind: KindRoutingID, id: id}
}

// DestinationRoutingIDs is the shards that own the routing identifiers ids,
// which a non-unique index returns. ids is retained.
func DestinationRoutingIDs(ids [][]byte) Destination {
	return Destination{kind: KindRoutingIDs, ids: ids}
}

// DestinationKeyRange is every shard whose key range overlaps kr. A nil kr is
// the full keyspace. kr is retained.
func DestinationKeyRange(kr *clustermetadatapb.KeyRange) Destination {
	return Destination{kind: KindKeyRange, keyRange: kr}
}

// DestinationAllShards is every shard.
func DestinationAllShards() Destination { return Destination{kind: KindAllShards} }

// Kind says what the destination names.
func (d Destination) Kind() DestinationKind { return d.kind }

// RoutingID is the routing identifier of a KindRoutingID, nil otherwise.
func (d Destination) RoutingID() []byte { return d.id }

// RoutingIDs is the routing identifiers of a KindRoutingIDs, nil otherwise.
func (d Destination) RoutingIDs() [][]byte { return d.ids }

// KeyRange is the key range of a KindKeyRange, nil otherwise (and for the
// full keyspace).
func (d Destination) KeyRange() *clustermetadatapb.KeyRange { return d.keyRange }

// Resolve calls add for each active shard of tg the destination names: none
// for KindNone, the owning shard of each routing identifier, the shards
// overlapping a key range in range order, or every active shard. A shard that
// owns several of a KindRoutingIDs' identifiers is added once per identifier,
// so callers that need a set deduplicate. Resolve stops at, and returns, the
// first error from add. Resolving anything but KindNone against a tablegroup
// with no active shards is an error.
//
// It reads only the tablegroup's own active shards. Routing rules and fences,
// which redirect or hold traffic while an operation moves data, are the
// caller's to apply.
func (d Destination) Resolve(tg *shardingcatalog.TableGroup, add func(*shardingcatalog.Shard) error) error {
	if d.kind == KindNone {
		return nil
	}
	shards := tg.ActiveShards()
	if len(shards) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q has no active shards", tg.Name())
	}
	switch d.kind {
	case KindRoutingID:
		// Active shards cover the keyspace, so the shard always exists.
		sh, _ := tg.ShardFor(d.id)
		return add(sh)
	case KindRoutingIDs:
		for _, id := range d.ids {
			sh, _ := tg.ShardFor(id)
			if err := add(sh); err != nil {
				return err
			}
		}
		return nil
	case KindKeyRange:
		for _, sh := range shards {
			if !keyrange.Overlaps(sh.KeyRange(), d.keyRange) {
				continue
			}
			if err := add(sh); err != nil {
				return err
			}
		}
		return nil
	case KindAllShards:
		for _, sh := range shards {
			if err := add(sh); err != nil {
				return err
			}
		}
		return nil
	default:
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown shard index destination kind %d", d.kind)
	}
}

// String describes the destination for diagnostics, with routing identifiers
// in hex and key ranges as shard names.
func (d Destination) String() string {
	switch d.kind {
	case KindNone:
		return "None()"
	case KindRoutingID:
		return "RoutingID(" + hex.EncodeToString(d.id) + ")"
	case KindRoutingIDs:
		var b strings.Builder
		b.WriteString("RoutingIDs(")
		for i, id := range d.ids {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(hex.EncodeToString(id))
		}
		b.WriteByte(')')
		return b.String()
	case KindKeyRange:
		return "KeyRange(" + keyrange.ShardName(d.keyRange) + ")"
	case KindAllShards:
		return "AllShards()"
	default:
		return "Unknown()"
	}
}
