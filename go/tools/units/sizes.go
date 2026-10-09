// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package units parses human-readable byte quantities such as "8 MiB" into a
// number of bytes. It is a generic helper with no multigres-specific behavior.
package units

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// unitMultipliers maps a lower-cased unit suffix to its byte multiplier. Binary
// units (KiB/MiB/GiB/TiB) are powers of 1024; decimal units (KB/MB/GB/TB) and a
// bare "b" are powers of 1000.
var unitMultipliers = map[string]uint64{
	"":    1,
	"b":   1,
	"kb":  1000,
	"mb":  1000 * 1000,
	"gb":  1000 * 1000 * 1000,
	"tb":  1000 * 1000 * 1000 * 1000,
	"kib": 1 << 10,
	"mib": 1 << 20,
	"gib": 1 << 30,
	"tib": 1 << 40,
}

// ParseBytes parses a byte quantity that is either a bare unsigned integer number
// of bytes ("1048576") or a number followed by a unit ("1 MiB", "8MiB", "512KB",
// "1.5 GiB"). Whitespace between the number and unit is optional and unit matching
// is case-insensitive. A bare integer is treated as exact bytes; a value with a
// unit is multiplied and rounded to the nearest byte. It rejects negative values,
// unknown units, and results that do not fit in a uint64.
func ParseBytes(s string) (uint64, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, errors.New("empty byte quantity")
	}
	// A bare unsigned integer is exact bytes (no float rounding).
	if n, err := strconv.ParseUint(trimmed, 10, 64); err == nil {
		return n, nil
	}
	// Split the leading numeric part (digits and at most one '.') from the unit.
	i := 0
	for i < len(trimmed) && (trimmed[i] == '.' || (trimmed[i] >= '0' && trimmed[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid byte quantity %q: expected a number optionally followed by a unit", s)
	}
	num, err := strconv.ParseFloat(trimmed[:i], 64)
	if err != nil || num < 0 || math.IsInf(num, 0) || math.IsNaN(num) {
		return 0, fmt.Errorf("invalid byte quantity %q: not a valid non-negative number", s)
	}
	unit := strings.ToLower(strings.TrimSpace(trimmed[i:]))
	if unit == "" {
		// The bare-integer fast path above already handles every valid
		// no-unit input; reaching here with no unit means the number itself
		// was not a valid unsigned integer (e.g. "1.2", "1e3") — a bare
		// quantity with no unit must be a whole number of bytes, not a
		// fraction silently rounded to one.
		return 0, fmt.Errorf("invalid byte quantity %q: a bare number with no unit must be a whole number of bytes", s)
	}
	mult, ok := unitMultipliers[unit]
	if !ok {
		return 0, fmt.Errorf("invalid byte quantity %q: unknown unit %q", s, strings.TrimSpace(trimmed[i:]))
	}
	bytes := num * float64(mult)
	if bytes >= math.MaxUint64 {
		return 0, fmt.Errorf("byte quantity %q is out of range", s)
	}
	return uint64(math.Round(bytes)), nil
}
