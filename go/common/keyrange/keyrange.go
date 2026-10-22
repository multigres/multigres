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
