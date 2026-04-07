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

package poolserver

// Protocol commands (client → server).
const (
	cmdAlloc  = "ALLOC"
	cmdReturn = "RETURN"
	cmdPing   = "PING"
)

// Protocol responses (server → client).
const (
	respOK   = "OK"
	respPong = "PONG"

	// respPrefixPort is the prefix of a successful ALLOC response ("PORT <n>").
	respPrefixPort = "PORT"

	// respPrefixErr is the prefix of all error responses.
	respPrefixErr = "ERR"

	// respErrCollision is the error response returned when allocPort detects
	// that the OS handed back a port already in the registry.
	respErrCollision = respPrefixErr + " port collision"
)
