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

// Package pid provides encoding and decoding of PostgreSQL process IDs (PIDs)
// for cross-gateway query cancellation. The 32-bit PID is split into a gateway prefix
// and a local connection ID, allowing any gateway to route cancel requests to the
// correct origin gateway.
package pid

const (
	// PrefixBits is the number of bits used for the gateway PID prefix.
	// Limited to 11 bits (max 2047) to keep bit 31 clear, ensuring PIDs
	// are always positive when interpreted as PostgreSQL's signed Int32.
	// This avoids negative PIDs in pg_stat_activity, client libraries,
	// and monitoring tools.
	PrefixBits = 11

	// LocalConnBits is the number of bits used for the local connection ID.
	LocalConnBits = 20

	// MaxPrefix is the maximum valid PID prefix value (2047).
	MaxPrefix = (1 << PrefixBits) - 1

	// MaxLocalConnID is the maximum valid local connection ID (~1M).
	MaxLocalConnID = (1 << LocalConnBits) - 1

	// localConnMask masks the lower LocalConnBits bits.
	localConnMask = MaxLocalConnID
)

// EncodePID combines a gateway prefix and local connection ID into a 32-bit PID.
// The prefix occupies the upper PrefixBits bits and the local connection ID
// occupies the lower LocalConnBits bits.
func EncodePID(prefix uint32, localConnID uint32) uint32 {
	return (prefix << LocalConnBits) | (localConnID & localConnMask)
}

// DecodePID extracts the gateway prefix and local connection ID from a 32-bit PID.
func DecodePID(pid uint32) (prefix uint32, localConnID uint32) {
	prefix = pid >> LocalConnBits
	localConnID = pid & localConnMask
	return prefix, localConnID
}
