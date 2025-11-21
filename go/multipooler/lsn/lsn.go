// Copyright 2025 Supabase, Inc.
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

// Package lsn provides utilities for parsing and comparing PostgreSQL Log Sequence Numbers (LSNs).
//
// PostgreSQL LSNs are represented as X/XXXXXXXX where:
//   - X is the high 32 bits (segment number) in hexadecimal
//   - XXXXXXXX is the low 32 bits (offset within segment) in hexadecimal
//
// Examples: 0/0, 0/3000028, 1/A0000000, FFFFFFFF/FFFFFFFF
package lsn

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN represents a PostgreSQL Log Sequence Number as a 64-bit value
// split into high and low 32-bit parts.
type LSN struct {
	high uint32 // Segment number (high 32 bits)
	low  uint32 // Offset within segment (low 32 bits)
}

// Parse parses a PostgreSQL LSN string in the format X/XXXXXXXX.
//
// Examples:
//   - Parse("0/0") -> LSN{0, 0}
//   - Parse("0/3000028") -> LSN{0, 50331688}
//   - Parse("1/A0000000") -> LSN{1, 2684354560}
//
// Returns an error if the format is invalid or contains non-hexadecimal characters.
func Parse(s string) (LSN, error) {
	if s == "" {
		return LSN{}, fmt.Errorf("empty LSN string")
	}

	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return LSN{}, fmt.Errorf("invalid LSN format: %s (expected X/XXXXXXXX)", s)
	}

	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return LSN{}, fmt.Errorf("invalid LSN high part %q: %w", parts[0], err)
	}

	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return LSN{}, fmt.Errorf("invalid LSN low part %q: %w", parts[1], err)
	}

	return LSN{high: uint32(high), low: uint32(low)}, nil
}

// Compare compares two LSNs numerically.
//
// Returns:
//   - -1 if a < b
//   - 0 if a == b
//   - 1 if a > b
//
// Comparison is done by first comparing the high 32 bits (segment number),
// then comparing the low 32 bits (offset) if the segments are equal.
func (a LSN) Compare(b LSN) int {
	if a.high < b.high {
		return -1
	}
	if a.high > b.high {
		return 1
	}
	// High parts are equal, compare low parts
	if a.low < b.low {
		return -1
	}
	if a.low > b.low {
		return 1
	}
	return 0
}

// CompareStrings compares two LSN strings numerically.
//
// Returns:
//   - -1 if a < b
//   - 0 if a == b
//   - 1 if a > b
//   - error if either string cannot be parsed
//
// This is a convenience function that parses both strings and compares them.
func CompareStrings(a, b string) (int, error) {
	lsnA, err := Parse(a)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN %q: %w", a, err)
	}

	lsnB, err := Parse(b)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN %q: %w", b, err)
	}

	return lsnA.Compare(lsnB), nil
}

// String returns the LSN in PostgreSQL format (X/XXXXXXXX).
func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", l.high, l.low)
}

// Greater returns true if a > b.
// Returns error if either string cannot be parsed.
func Greater(a, b string) (bool, error) {
	cmp, err := CompareStrings(a, b)
	if err != nil {
		return false, err
	}
	return cmp > 0, nil
}

// GreaterOrEqual returns true if a >= b.
// Returns error if either string cannot be parsed.
func GreaterOrEqual(a, b string) (bool, error) {
	cmp, err := CompareStrings(a, b)
	if err != nil {
		return false, err
	}
	return cmp >= 0, nil
}
