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
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// WriterValidator continuously writes to a test table and tracks successful/failed writes.
// Useful for validating data durability during failover scenarios.
type WriterValidator struct {
	tableName     string
	workerCount   int
	writeInterval time.Duration
	queryTimeout  time.Duration

	db *sql.DB

	nextID atomic.Int64

	mu         sync.Mutex
	successful []int64
	failed     []int64

	ctx     context.Context
	cancel  context.CancelFunc
	stop    chan struct{} // signals workers to stop issuing new writes
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

// WithQueryTimeout sets the per-query timeout (default: 5s). When buffering is
// enabled on the gateway, this should be at least as long as the buffer window
// so that the buffer can drain before the client gives up.
func WithQueryTimeout(timeout time.Duration) WriterValidatorOption {
	return func(w *WriterValidator) {
		w.queryTimeout = timeout
	}
}

// NewWriterValidator creates a new WriterValidator for the given sql.DB connection.
// It creates the test table immediately and returns a cleanup function that drops it.
func NewWriterValidator(t *testing.T, db *sql.DB, opts ...WriterValidatorOption) (*WriterValidator, func(), error) {
	t.Helper()
	w := &WriterValidator{
		tableName:     fmt.Sprintf("writer_validator_%d", time.Now().UnixNano()),
		workerCount:   4,
		writeInterval: 10 * time.Millisecond,
		queryTimeout:  5 * time.Second,
		db:            db,
	}

	for _, opt := range opts {
		opt(w)
	}

	if err := w.createTable(t.Context()); err != nil {
		return nil, nil, fmt.Errorf("failed to create table: %w", err)
	}

	cleanup := func() {
		// Stop workers if running
		w.Stop()
		// Drop table (best effort, use background context since test context may be done)
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.dropTable(dropCtx)
	}

	return w, cleanup, nil
}

// TableName returns the name of the test table.
func (w *WriterValidator) TableName() string {
	return w.tableName
}

// createTable creates the test table.
func (w *WriterValidator) createTable(ctx context.Context) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY)", w.tableName)
	_, err := w.db.ExecContext(ctx, query)
	return err
}

// dropTable drops the test table.
func (w *WriterValidator) dropTable(ctx context.Context) error {
	query := "DROP TABLE IF EXISTS " + w.tableName
	_, err := w.db.ExecContext(ctx, query)
	return err
}

// Start spawns worker goroutines that continuously write to the table.
func (w *WriterValidator) Start(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return
	}
	w.started = true
	w.ctx, w.cancel = context.WithCancel(t.Context())
	w.stop = make(chan struct{})

	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.worker()
	}
}

// Stop gracefully stops all worker goroutines. It signals workers to stop
// issuing new writes, then waits for any in-flight writes to complete.
// This avoids context cancellation errors on in-flight queries.
func (w *WriterValidator) Stop() {
	stop := func() chan struct{} {
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.started {
			return nil
		}
		return w.stop
	}()

	if stop == nil {
		return
	}

	// Signal workers to stop, then wait for in-flight writes to finish.
	close(stop)
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	w.started = false
}

// worker runs the write loop using a ticker.
func (w *WriterValidator) worker() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.writeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			id := w.nextID.Add(1)
			ctx, cancel := context.WithTimeout(w.ctx, w.queryTimeout)
			// #nosec G202 -- tableName is a constant from test setup, not user-controlled.
			query := "INSERT INTO " + w.tableName + " (id) VALUES ($1)"
			_, err := w.db.ExecContext(ctx, query, id)
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
func (w *WriterValidator) Verify(t *testing.T, poolers []*MultiPoolerTestClient) error {
	t.Helper()

	w.mu.Lock()
	successfulIDs := make([]int64, len(w.successful))
	copy(successfulIDs, w.successful)
	w.mu.Unlock()

	if len(successfulIDs) == 0 {
		return errors.New("no successful writes to verify")
	}

	// Build set of all IDs found across all pooler connections
	foundIDs := make(map[int64]bool)

	for _, pooler := range poolers {
		query := "SELECT id FROM " + w.tableName
		result, err := pooler.ExecuteQuery(t.Context(), query, 0)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		for _, row := range result.Rows {
			if len(row.Values) > 0 && !row.Values[0].IsNull() {
				id, err := strconv.ParseInt(string(row.Values[0]), 10, 64)
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
