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

package benchmarking

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// fetchProfile fetches a pprof endpoint at addr and writes the bytes to
// outputPath. Endpoints that block for a window (e.g. CPU profile) are
// supported via the timeout — pass it large enough to cover the
// server-side window plus a small margin.
func fetchProfile(ctx context.Context, addr, urlPath string, timeout time.Duration, outputPath string) error {
	url := fmt.Sprintf("http://%s%s", addr, urlPath)

	httpCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build pprof request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s returned %s: %s", url, resp.Status, string(body))
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("mkdir for profile: %w", err)
	}

	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outputPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("write profile body: %w", err)
	}
	return nil
}

// captureCPUProfile fetches a CPU profile of `seconds` duration from the
// given servenv HTTP address. Blocks for ~seconds.
func captureCPUProfile(ctx context.Context, addr string, seconds int, outputPath string) error {
	urlPath := fmt.Sprintf("/debug/pprof/profile?seconds=%d", seconds)
	return fetchProfile(ctx, addr, urlPath, time.Duration(seconds+30)*time.Second, outputPath)
}

// captureHeapProfile fetches a current heap (in-use space) profile snapshot.
// Returns immediately — this is a sample of live allocations.
func captureHeapProfile(ctx context.Context, addr, outputPath string) error {
	return fetchProfile(ctx, addr, "/debug/pprof/heap", 30*time.Second, outputPath)
}

// captureAllocsProfile fetches a cumulative allocation profile (all
// allocations since process start). Useful for spotting alloc hotspots.
func captureAllocsProfile(ctx context.Context, addr, outputPath string) error {
	return fetchProfile(ctx, addr, "/debug/pprof/allocs", 30*time.Second, outputPath)
}

// captureGoroutineProfile fetches the current goroutine stack dump.
// Useful to see what's parked / contending.
func captureGoroutineProfile(ctx context.Context, addr, outputPath string) error {
	return fetchProfile(ctx, addr, "/debug/pprof/goroutine", 30*time.Second, outputPath)
}
