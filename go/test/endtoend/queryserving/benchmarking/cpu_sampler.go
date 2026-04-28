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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// CPUStats summarizes CPU usage samples for a single process tree
// across a scenario run. Values are in "percent of one core" — i.e.
// 100.0 means 1 core fully busy. With many backends it can exceed 100.
type CPUStats struct {
	Label       string    `json:"label"`
	Samples     int       `json:"samples"`
	MeanPercent float64   `json:"mean_percent"`
	MinPercent  float64   `json:"min_percent"`
	MaxPercent  float64   `json:"max_percent"`
	Series      []float64 `json:"series_percent"`
}

// pidEnumerator returns the set of PIDs to sample at a given moment.
type pidEnumerator func() ([]int, error)

// staticPidEnumerator returns a constant list of PIDs.
func staticPidEnumerator(pids ...int) pidEnumerator {
	return func() ([]int, error) { return pids, nil }
}

// postgresPidEnumerator returns the postmaster pid plus its direct children
// (backends, bgworker, walwriter, etc.) on each call. The postmaster pid is
// read from postmaster.pid in the data dir, so it's resilient to postgres
// restarts mid-scenario.
func postgresPidEnumerator(dataDir string) pidEnumerator {
	pidFile := dataDir + "/postmaster.pid"
	return func() ([]int, error) {
		raw, err := os.ReadFile(pidFile)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", pidFile, err)
		}
		// First line is the postmaster PID.
		firstLine := raw
		if i := bytes.IndexByte(raw, '\n'); i > 0 {
			firstLine = raw[:i]
		}
		pmPid, err := strconv.Atoi(strings.TrimSpace(string(firstLine)))
		if err != nil {
			return nil, fmt.Errorf("parse postmaster pid: %w", err)
		}

		// Children (one level is enough for postgres backends).
		out, err := exec.Command("pgrep", "-P", strconv.Itoa(pmPid)).Output()
		if err != nil {
			// pgrep returns exit 1 when no children; treat as just the postmaster.
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				return []int{pmPid}, nil
			}
			return nil, fmt.Errorf("pgrep -P %d: %w", pmPid, err)
		}
		pids := []int{pmPid}
		for s := range strings.FieldsSeq(string(out)) {
			if pid, err := strconv.Atoi(s); err == nil {
				pids = append(pids, pid)
			}
		}
		return pids, nil
	}
}

// readPSCPU returns the sum of %cpu across the given pids using a single
// `ps` invocation. macOS ps reports "current" CPU% (decayed average).
// On Linux ps reports cumulative-since-start which is less useful for
// short windows, but we sample frequently enough that the per-sample
// granularity matters more than the absolute scale.
func readPSCPU(pids []int) (float64, error) {
	if len(pids) == 0 {
		return 0, nil
	}
	args := []string{"-o", "%cpu="}
	for _, p := range pids {
		args = append(args, "-p", strconv.Itoa(p))
	}
	// Errors are intentionally swallowed: pids may exit between
	// enumeration and sampling, in which case `ps` returns non-zero but
	// still emits %cpu lines for the survivors. Sum what we got.
	out, _ := exec.Command("ps", args...).Output()
	var total float64
	for ln := range strings.SplitSeq(string(out), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		v, perr := strconv.ParseFloat(ln, 64)
		if perr != nil {
			continue
		}
		total += v
	}
	return total, nil
}

// cpuSampler runs a goroutine sampling the given pidEnumerator at the
// configured interval. Stop() returns the aggregated stats once the
// goroutine has finished collecting.
type cpuSampler struct {
	label    string
	enum     pidEnumerator
	interval time.Duration

	wg     sync.WaitGroup
	cancel context.CancelFunc
	mu     sync.Mutex
	series []float64
}

func newCPUSampler(label string, enum pidEnumerator, interval time.Duration) *cpuSampler {
	return &cpuSampler{label: label, enum: enum, interval: interval}
}

// Start begins sampling. Cancel ctx (or call Stop) to finish.
func (s *cpuSampler) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel

	s.wg.Go(func() {
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pids, err := s.enum()
				if err != nil || len(pids) == 0 {
					continue
				}
				v, err := readPSCPU(pids)
				if err != nil {
					continue
				}
				s.mu.Lock()
				s.series = append(s.series, v)
				s.mu.Unlock()
			}
		}
	})
}

// Stop finalizes the sampler and returns aggregated stats.
func (s *cpuSampler) Stop() CPUStats {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	stats := CPUStats{Label: s.label, Samples: len(s.series), Series: s.series}
	if len(s.series) == 0 {
		return stats
	}
	var sum float64
	stats.MinPercent = s.series[0]
	stats.MaxPercent = s.series[0]
	for _, v := range s.series {
		sum += v
		if v < stats.MinPercent {
			stats.MinPercent = v
		}
		if v > stats.MaxPercent {
			stats.MaxPercent = v
		}
	}
	stats.MeanPercent = sum / float64(len(s.series))
	return stats
}

// writeCPUStatsFile persists per-process CPU stats for one (scenario,target)
// run as JSON under <outputDir>/cpu/<scenario>/<target>.json.
func writeCPUStatsFile(t *testing.T, outputDir, scenario, target string, stats map[string]CPUStats) {
	t.Helper()

	dir := filepath.Join(outputDir, "cpu", scenario)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Logf("cpu stats: mkdir %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, target+".json")
	body, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		t.Logf("cpu stats: marshal: %v", err)
		return
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Logf("cpu stats: write %s: %v", path, err)
	}
}
