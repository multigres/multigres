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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

type signalledChunkWriter struct {
	*chunkWriter
	entered chan struct{}
}

func (w *signalledChunkWriter) Write(b []byte) (int, error) {
	close(w.entered)
	return w.chunkWriter.Write(b)
}

func TestWorkerOutputDetachesBackpressuredStream(t *testing.T) {
	// No queue receiver: this is real chunkWriter backpressure, not a sleeping
	// imitation of it. The entered event proves relay.Write owns the shared lock.
	stream := &streamSender{queue: make(chan *pb.RunOutput), blockUntil: t.Context()}
	entered := make(chan struct{})
	sink := &signalledChunkWriter{chunkWriter: stream.writer(false).(*chunkWriter), entered: entered}
	var mu sync.Mutex
	out := newWorkerOutput(&mu, sink)
	stderr := newWorkerOutput(&mu, stream.writer(true))
	written := make(chan struct{})
	go func() { defer close(written); _, _ = out.Write([]byte("held output")) }()
	<-entered
	detached := make(chan struct{})
	go func() { defer close(detached); detachWorkerOutputs(true, out, stderr) }()
	select {
	case <-detached:
	case <-time.After(5 * time.Second):
		t.Fatal("backpressured response prevented detachment")
	}
	<-written
	require.True(t, stream.truncated.Load())
	require.Nil(t, out.w)
	require.Nil(t, out.cancel)
	require.Nil(t, stderr.w)
	require.Nil(t, stderr.cancel)
	// Response finished; a late child write must not send on its closed queue.
	close(stream.queue)
	n, err := out.Write([]byte("late output"))
	require.NoError(t, err)
	require.Equal(t, len("late output"), n)
}

type barrierEOFReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r *barrierEOFReader) Read([]byte) (int, error) { close(r.entered); <-r.release; return 0, io.EOF }

func TestWorkerLifetimeWhitespaceIsNotEOF(t *testing.T) {
	eof := &barrierEOFReader{entered: make(chan struct{}), release: make(chan struct{})}
	ended := watchWorkerLifetime(io.MultiReader(strings.NewReader("\n \t\n"), eof))
	select {
	case <-eof.entered:
	case <-ended:
		t.Fatal("framing bytes were mistaken for owner death")
	case <-time.After(5 * time.Second):
		t.Fatal("lifetime reader did not consume framing bytes")
	}
	select {
	case <-ended:
		t.Fatal("ownership ended before EOF")
	default:
	}
	close(eof.release)
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("EOF did not end ownership")
	}
}
