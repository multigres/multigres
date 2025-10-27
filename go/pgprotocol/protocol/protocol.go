// Copyright 2025 Supabase, Inc.
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

package protocol

import "fmt"

// ProtocolVersion represents a frontend/backend protocol version number.
type ProtocolVersion uint32

// NewProtocolVersion creates a protocol version from major and minor numbers.
func NewProtocolVersion(major, minor uint16) ProtocolVersion {
	return ProtocolVersion((uint32(major) << 16) | uint32(minor))
}

// Major returns the major version number.
func (v ProtocolVersion) Major() uint16 {
	return uint16(v >> 16)
}

// Minor returns the minor version number.
func (v ProtocolVersion) Minor() uint16 {
	return uint16(v & 0xFFFF)
}

// String returns a string representation of the protocol version.
func (v ProtocolVersion) String() string {
	return fmt.Sprintf("%d.%d", v.Major(), v.Minor())
}

// IsSupported returns true if this protocol version is supported.
// Currently only protocol 3.0 is supported.
func (v ProtocolVersion) IsSupported() bool {
	return v == ProtocolVersionNumber
}
