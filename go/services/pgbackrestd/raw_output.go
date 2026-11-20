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

package pgbackrestd

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// Process-wide, not per invocation: even permanently blocked process stdio
// retains at most two writers and two bounded queues across server restarts.
// These are exclusively best-effort TLS server logs, NEVER INFO/VERIFY output.
var (
	serverStdout = newRawOutput(os.Stdout)
	serverStderr = newRawOutput(os.Stderr)
)

const (
	rawOutputChunk = 32 << 10
	rawOutputQueue = 8
)

type rawOutput struct {
	sink    io.Writer
	once    sync.Once
	queue   chan []byte
	done    chan struct{}
	dropped atomic.Uint64
}

func newRawOutput(sink io.Writer) *rawOutput {
	return &rawOutput{sink: sink, queue: make(chan []byte, rawOutputQueue), done: make(chan struct{})}
}

func (w *rawOutput) Write(p []byte) (int, error) {
	w.once.Do(func() { go w.forward() })
	n := len(p)
	for len(p) > 0 {
		size := min(len(p), rawOutputChunk)
		chunk := append([]byte(nil), p[:size]...)
		select {
		case w.queue <- chunk:
		default:
			w.dropped.Add(uint64(size))
		}
		p = p[size:]
	}
	return n, nil
}

func (w *rawOutput) forward() {
	defer close(w.done)
	for chunk := range w.queue {
		// Report loss on the same sink when it can accept output again. Neither
		// this notice nor a permanently blocked Write participates in completion.
		if n := w.dropped.Swap(0); n > 0 {
			if _, err := fmt.Fprintf(w.sink, "\npgbackrestd: raw server log truncated (%d bytes dropped)\n", n); err != nil {
				w.dropped.Add(n)
			}
		}
		n, _ := w.sink.Write(chunk)
		if n < len(chunk) {
			w.dropped.Add(uint64(len(chunk) - n))
		}
	}
}
