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

package handler

import (
	"encoding/binary"
	"fmt"
)

// parsePgMessageHeader interprets a 5-byte PostgreSQL wire message header —
// a 1-byte type plus a 4-byte big-endian length that includes itself but
// excludes the type byte — returning the message type and body length.
// label distinguishes "backend"/"frontend" in the returned error so callers
// don't lose that context. Shared by pgMsgReader, copyModeDownstreamReader,
// and copyModeUpstreamReader: all three frame the same wire format, just
// from different sources (a gRPC recv func vs. a detached client socket).
func parsePgMessageHeader(header []byte, label string) (msgType byte, bodyLen int, err error) {
	length := binary.BigEndian.Uint32(header[1:5])
	if length < 4 {
		return 0, 0, fmt.Errorf("invalid %s message length: %d", label, length)
	}
	return header[0], int(length) - 4, nil
}

// ensureBuffered pulls chunks from recv into *buf until it holds at least n
// bytes, or a terminal error occurs. Bytes returned alongside a non-nil
// error are appended before the error is surfaced, matching io.Reader's
// documented contract for the underlying stream (a final read can both
// deliver data and signal EOF) — shared so every reader in this package
// honors that contract the same way instead of each hand-rolling its own
// accumulation loop.
func ensureBuffered(buf *[]byte, recv func() ([]byte, error), n int) error {
	for len(*buf) < n {
		chunk, err := recv()
		if len(chunk) > 0 {
			*buf = append(*buf, chunk...)
		}
		if len(*buf) >= n {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}
