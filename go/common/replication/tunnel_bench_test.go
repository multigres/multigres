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

package replication

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
)

// benchBackend serves a fixed payload out of a bytes.Reader (downstream:
// postgres -> client) and discards writes (upstream is not exercised here).
type benchBackend struct {
	io.Reader
}

func (benchBackend) Write(p []byte) (int, error) { return len(p), nil }
func (benchBackend) Close() error                { return nil }

// BenchmarkTunnelDownstream measures the cost of Tunnel.Run's downstream copy
// loop (postgres -> client), the direction that carries the bulk of logical
// replication traffic (WAL data). send copies its chunk before returning,
// mirroring the real callers in stream_replication.go and
// replication_stream.go, which must copy before handing the buffer to gRPC
// since Tunnel reuses its read buffer across iterations.
//
// Compare chunk-count scaling with:
//
//	go test -bench=BenchmarkTunnelDownstream -benchmem ./go/common/replication/
func BenchmarkTunnelDownstream(b *testing.B) {
	for _, size := range []int{64 * 1024, 1 << 20, 16 << 20} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			payload := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				backend := benchBackend{Reader: bytes.NewReader(payload)}

				done := make(chan struct{})
				sent := 0
				send := func(chunk []byte) error {
					cp := append([]byte(nil), chunk...)
					sent += len(cp)
					if sent >= size {
						close(done) // unblocks recv below once the payload is fully drained
					}
					return nil
				}
				recv := func() ([]byte, error) {
					<-done
					return nil, io.EOF
				}

				tun := NewTunnel(backend, nil, send, recv)
				if err := tun.Run(context.Background()); err != nil {
					b.Fatalf("Run: %v", err)
				}
			}
		})
	}
}
