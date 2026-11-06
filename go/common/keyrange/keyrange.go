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

// Package keyrange converts between shard names and the key ranges they cover.
//
// A shard name is "<start>-<end>". start is "0" (the start of the keyspace) or
// a hex bound; end is "inf" (the end of the keyspace) or a hex bound. A hex
// bound is non-empty, lowercase, of even length, and has no trailing zero
// byte, since trailing zero bytes don't change its position in the keyspace.
// With those rules every key range has exactly one name, so
// ShardName(ParseShardName(n)) == n for every accepted name.
//
// Examples: "0-inf" (the full keyspace), "0-80", "80-inf", "40-80", "8040-c0".
package keyrange

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ParseShardName parses a canonical shard name into the key range it covers.
// An unbounded start or end is returned as nil bytes. Non-canonical names
// (e.g. "00-80", "0-8", "0-8A") are rejected.
func ParseShardName(name string) (*clustermetadatapb.KeyRange, error) {
	startStr, endStr, ok := strings.Cut(name, "-")
	if !ok {
		return nil, fmt.Errorf("invalid shard name %q: expected <start>-<end>", name)
	}

	var start, end []byte
	if startStr != constants.ShardNameMinBound {
		b, err := parseBound(startStr)
		if err != nil {
			return nil, fmt.Errorf("invalid shard name %q: start: %w", name, err)
		}
		start = b
	}
	if endStr != constants.ShardNameMaxBound {
		b, err := parseBound(endStr)
		if err != nil {
			return nil, fmt.Errorf("invalid shard name %q: end: %w", name, err)
		}
		end = b
	}

	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil, fmt.Errorf("invalid shard name %q: start must be less than end", name)
	}
	return &clustermetadatapb.KeyRange{Start: start, End: end}, nil
}

// ShardName returns the canonical shard name for a key range. A nil key range
// is the full keyspace, "0-inf".
func ShardName(kr *clustermetadatapb.KeyRange) string {
	start, end := constants.ShardNameMinBound, constants.ShardNameMaxBound
	if len(kr.GetStart()) > 0 {
		start = hex.EncodeToString(kr.GetStart())
	}
	if len(kr.GetEnd()) > 0 {
		end = hex.EncodeToString(kr.GetEnd())
	}
	return start + "-" + end
}

// ValidateTableGroupAndShard validates a multipooler's tablegroup and shard.
//
// The tablegroup must be non-empty. The default tablegroup always has the single
// shard constants.DefaultShard, since it hosts the catalog. Any other
// tablegroup's shard must be a canonical shard name (see ParseShardName).
func ValidateTableGroupAndShard(tableGroup, shard string) error {
	if tableGroup == "" {
		return errors.New("tablegroup is required")
	}
	if tableGroup == constants.DefaultTableGroup {
		if shard != constants.DefaultShard {
			return fmt.Errorf("tablegroup %s only supports shard %s, got: %s", constants.DefaultTableGroup, constants.DefaultShard, shard)
		}
		return nil
	}
	if _, err := ParseShardName(shard); err != nil {
		return err
	}
	return nil
}

// parseBound decodes a canonical hex bound.
func parseBound(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("empty bound")
	}
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("bound %q must have an even number of hex digits", s)
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return nil, fmt.Errorf("bound %q must be lowercase hex", s)
		}
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("bound %q: %w", s, err)
	}
	// An all-zero bound always ends in a zero byte, so this also rejects it;
	// the start of the keyspace is written "0".
	if b[len(b)-1] == 0 {
		return nil, fmt.Errorf("bound %q must not end in a zero byte", s)
	}
	return b, nil
}

// ValidateShardLayout checks that shards partition the whole keyspace: every
// name parses, no name repeats, and when the ranges are sorted by start the
// first begins at the start of the keyspace, the last ends at the end, and
// each range ends exactly where the next begins. It returns the ranges in
// that sorted order, which is the canonical order for storing a layout. The
// input may be in any order.
func ValidateShardLayout(shards []string) ([]*clustermetadatapb.KeyRange, error) {
	if len(shards) == 0 {
		return nil, errors.New("shard layout must have at least one shard")
	}
	ranges := make([]*clustermetadatapb.KeyRange, 0, len(shards))
	seen := make(map[string]struct{}, len(shards))
	for _, name := range shards {
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate shard %q", name)
		}
		seen[name] = struct{}{}
		kr, err := ParseShardName(name)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, kr)
	}
	// An empty start is the start of the keyspace, and bytes.Compare already
	// sorts the empty slice first.
	slices.SortStableFunc(ranges, func(a, b *clustermetadatapb.KeyRange) int {
		return bytes.Compare(a.GetStart(), b.GetStart())
	})
	if len(ranges[0].GetStart()) != 0 {
		return nil, fmt.Errorf("shard layout does not cover the start of the keyspace: first shard is %q", ShardName(ranges[0]))
	}
	for i := 1; i < len(ranges); i++ {
		prev, cur := ranges[i-1], ranges[i]
		switch c := compareEndToStart(prev.GetEnd(), cur.GetStart()); {
		case c < 0:
			return nil, fmt.Errorf("gap in shard layout between %q and %q", ShardName(prev), ShardName(cur))
		case c > 0:
			return nil, fmt.Errorf("shards %q and %q overlap", ShardName(prev), ShardName(cur))
		}
	}
	if len(ranges[len(ranges)-1].GetEnd()) != 0 {
		return nil, fmt.Errorf("shard layout does not cover the end of the keyspace: last shard is %q", ShardName(ranges[len(ranges)-1]))
	}
	return ranges, nil
}

// compareEndToStart compares the end bound of one range with the start bound
// of the range that follows it in sorted order. An empty end is the end of
// the keyspace, so it is past every start; an empty start is the start of the
// keyspace, so every end is past it.
func compareEndToStart(end, start []byte) int {
	if len(end) == 0 || len(start) == 0 {
		return 1
	}
	return bytes.Compare(end, start)
}
