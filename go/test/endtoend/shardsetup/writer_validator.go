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

package shardsetup

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/test/endtoend"
)

// WriterValidator continuously writes to a test table and tracks successful/failed writes.
// Useful for validating data durability during failover scenarios.
type WriterValidator struct {
	tableName     string
	workerCount   int
	writeInterval time.Duration

	pooler *endtoend.MultiPoolerTestClient

	nextID atomic.Int64

	mu         sync.Mutex
	successful []int64
	failed     []int64

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// WriterValidatorOption configures a WriterValidator.
type WriterValidatorOption func(*WriterValidator)

// WithWorkerCount sets the number of concurrent writer goroutines (default: 4).
func WithWorkerCount(count int) WriterValidatorOption {
	return func(w *WriterValidator) {
		w.workerCount = count
	}
}

// WithWriteInterval sets the interval between writes per worker (default: 10ms).
func WithWriteInterval(interval time.Duration) WriterValidatorOption {
	return func(w *WriterValidator) {
		w.writeInterval = interval
	}
}

// NewWriterValidator creates a new WriterValidator for the given pooler.
// It creates the test table immediately and returns a cleanup function that drops it.
func NewWriterValidator(ctx context.Context, pooler *endtoend.MultiPoolerTestClient, opts ...WriterValidatorOption) (*WriterValidator, func(), error) {
	w := &WriterValidator{
		tableName:     fmt.Sprintf("writer_validator_%d", time.Now().UnixNano()),
		workerCount:   4,
		writeInterval: 10 * time.Millisecond,
		pooler:        pooler,
	}

	for _, opt := range opts {
		opt(w)
	}

	if err := w.createTable(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to create table: %w", err)
	}

	cleanup := func() {
		// Stop workers if running
		w.Stop()
		// Drop table (best effort, ignore errors)
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.dropTable(dropCtx)
	}

	return w, cleanup, nil
}

// createTable creates the test table.
func (w *WriterValidator) createTable(ctx context.Context) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY)", w.tableName)
	_, err := w.pooler.ExecuteQuery(ctx, query, 0)
	return err
}

// dropTable drops the test table.
func (w *WriterValidator) dropTable(ctx context.Context) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", w.tableName)
	_, err := w.pooler.ExecuteQuery(ctx, query, 0)
	return err
}

// Start spawns worker goroutines that continuously write to the table.
func (w *WriterValidator) Start() {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return
	}
	w.started = true
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.mu.Unlock()

	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.worker()
	}
}

// Stop signals all worker goroutines to stop and waits for them to complete.
func (w *WriterValidator) Stop() {
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()

	w.cancel()
	w.wg.Wait()

	w.mu.Lock()
	w.started = false
	w.mu.Unlock()
}

// worker runs the write loop using a ticker.
func (w *WriterValidator) worker() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.writeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			id := w.nextID.Add(1)
			ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
			query := fmt.Sprintf("INSERT INTO %s (id) VALUES (%d)", w.tableName, id)
			_, err := w.pooler.ExecuteQuery(ctx, query, 0)
			cancel()
			w.recordResult(id, err)
		}
	}
}

// recordResult records a write attempt result.
func (w *WriterValidator) recordResult(id int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err == nil {
		w.successful = append(w.successful, id)
	} else {
		w.failed = append(w.failed, id)
	}
}

// SuccessfulWrites returns a copy of all successfully written IDs.
func (w *WriterValidator) SuccessfulWrites() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make([]int64, len(w.successful))
	copy(result, w.successful)
	return result
}

// FailedWrites returns a copy of all failed write IDs.
func (w *WriterValidator) FailedWrites() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make([]int64, len(w.failed))
	copy(result, w.failed)
	return result
}

// Stats returns the count of successful and failed writes.
func (w *WriterValidator) Stats() (successful, failed int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.successful), len(w.failed)
}

// Verify checks that all successful writes are present in at least one of the provided poolers.
func (w *WriterValidator) Verify(ctx context.Context, poolers []*endtoend.MultiPoolerTestClient) error {
	w.mu.Lock()
	successfulIDs := make([]int64, len(w.successful))
	copy(successfulIDs, w.successful)
	w.mu.Unlock()

	if len(successfulIDs) == 0 {
		return fmt.Errorf("no successful writes to verify")
	}

	// Build set of all IDs found across all poolers
	foundIDs := make(map[int64]bool)

	for _, pooler := range poolers {
		query := fmt.Sprintf("SELECT id FROM %s", w.tableName)
		resp, err := pooler.ExecuteQuery(ctx, query, 0)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		for _, row := range resp.Rows {
			if len(row.Values) > 0 {
				idStr := string(row.Values[0])
				id, err := strconv.ParseInt(idStr, 10, 64)
				if err == nil {
					foundIDs[id] = true
				}
			}
		}
	}

	// Check all successful writes are present
	var missing []int64
	for _, id := range successfulIDs {
		if !foundIDs[id] {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		showCount := min(len(missing), 10)
		return fmt.Errorf("missing %d successful writes, first %d: %v", len(missing), showCount, missing[:showCount])
	}

	return nil
}
