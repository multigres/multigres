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

// Equal reports whether two key ranges cover the same keys. A nil key range
// is the full keyspace.
func Equal(a, b *clustermetadatapb.KeyRange) bool {
	return bytes.Equal(a.GetStart(), b.GetStart()) && bytes.Equal(a.GetEnd(), b.GetEnd())
}

// Overlaps reports whether two key ranges share at least one key. Adjacent
// ranges, where one ends where the other starts, do not overlap. A nil key
// range is the full keyspace.
func Overlaps(a, b *clustermetadatapb.KeyRange) bool {
	return startsBeforeEnd(a.GetStart(), b.GetEnd()) && startsBeforeEnd(b.GetStart(), a.GetEnd())
}

// Contiguous reports whether ranges are contiguous: ordered by start, each
// range ends exactly where the next one starts, with no gap and no overlap.
// If they are, it returns the single key range they cover together. It
// reports false if ranges is empty. A nil key range is the full keyspace.
func Contiguous(ranges []*clustermetadatapb.KeyRange) (*clustermetadatapb.KeyRange, bool) {
	if len(ranges) == 0 {
		return nil, false
	}
	sorted := slices.Clone(ranges)
	slices.SortFunc(sorted, func(a, b *clustermetadatapb.KeyRange) int {
		return bytes.Compare(a.GetStart(), b.GetStart())
	})
	for i := 0; i+1 < len(sorted); i++ {
		end := sorted[i].GetEnd()
		if len(end) == 0 || !bytes.Equal(end, sorted[i+1].GetStart()) {
			return nil, false
		}
	}
	return &clustermetadatapb.KeyRange{
		Start: sorted[0].GetStart(),
		End:   sorted[len(sorted)-1].GetEnd(),
	}, true
}

// startsBeforeEnd reports whether start is before end, where an empty start
// is the start of the keyspace and an empty end its end.
func startsBeforeEnd(start, end []byte) bool {
	return len(end) == 0 || bytes.Compare(start, end) < 0
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
