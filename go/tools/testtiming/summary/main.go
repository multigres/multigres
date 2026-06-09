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

// Command summary reads the integration test JSONL output (gotestsum
// --jsonfile), extracts the timeout-bounded timing measurements that the
// end-to-end tests record via testtiming.Record (startup, failover, etc.), and
// writes a mean±99%CI / min / p50 / p95 / p99 / max markdown table to
// $GITHUB_STEP_SUMMARY (or stdout when that variable is unset).
//
// Measurements arrive as testtiming.Measurement payloads inside go test -json
// "attr" events; see the testtiming package for the wire contract.
package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/multigres/multigres/go/tools/testtiming"

	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
)

const (
	jsonlPath = "integration-test-results.jsonl"

	// status thresholds, as a percentage of the timeout limit.
	warnPct = 70.0
	critPct = 90.0
)

// poolerSuffix matches the per-instance suffix that varies between otherwise
// identical operations (e.g. "manager ready: pooler-3"), so they aggregate into
// a single row.
var poolerSuffix = regexp.MustCompile(`: pooler-\d+$`)

// meanCI99 returns the mean and the half-width of a 99% CI on the mean, assuming
// normality. For n<2 the half-width is 0 (a CI cannot be computed from a single
// sample). The t critical value comes from the exact Student's t quantile; a
// two-sided 99% interval uses the 0.995 quantile. 99% (rather than 95%) keeps
// the upper bound from reading as anomalous on roughly 1 run in 20.
func meanCI99(values []float64) (mean, halfWidth float64) {
	n := len(values)
	mean = stat.Mean(values, nil)
	if n < 2 {
		return mean, 0.0
	}
	tCrit := distuv.StudentsT{Mu: 0, Sigma: 1, Nu: float64(n - 1)}.Quantile(0.995)
	return mean, tCrit * stat.StdDev(values, nil) / math.Sqrt(float64(n))
}

// cleanLabel normalises labels that vary by pooler instance:
// "manager ready: pooler-N" -> "manager ready".
func cleanLabel(label string) string {
	return poolerSuffix.ReplaceAllString(label, "")
}

// formatElapsed renders a duration in seconds as a compact string.
func formatElapsed(seconds float64) string {
	if seconds < 1.0 {
		return fmt.Sprintf("%.0fms", seconds*1000)
	}
	return fmt.Sprintf("%.1fs", seconds)
}

// formatCell renders a percentile cell as "Xs (P%)" showing elapsed time and
// percent of the timeout.
func formatCell(elapsed, limit float64) string {
	return fmt.Sprintf("%s (%.0f%%)", formatElapsed(elapsed), elapsed/limit*100)
}

// formatCICell renders a mean±CI cell as "Xs ±Ys (P%, n=N)".
func formatCICell(mean, margin, limit float64, n int) string {
	return fmt.Sprintf("%s ±%s (%.0f%%, n=%d)",
		formatElapsed(mean), formatElapsed(margin), mean/limit*100, n)
}

// statusCircle returns a colored circle reflecting the worst percentile bucket,
// given p95/p99 as percentages of the timeout.
func statusCircle(p95, p99 float64) string {
	switch {
	case p99 >= critPct:
		return "🔴"
	case p95 >= warnPct:
		return "🟡"
	default:
		return "🟢"
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1) //nolint:forbidigo // os.Exit is fine in main()
	}
}

func run() error {
	f, err := os.Open(jsonlPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "No %s found — skipping timing summary\n", jsonlPath)
			return nil
		}
		return err
	}
	defer f.Close()

	measurements, err := testtiming.Parse(f)
	if err != nil {
		return err
	}
	data, limits := aggregate(measurements)
	if len(data) == 0 {
		fmt.Fprintln(os.Stderr, "No timing rows found — skipping timing summary")
		return nil
	}

	out := os.Stdout
	if summaryPath := os.Getenv("GITHUB_STEP_SUMMARY"); summaryPath != "" {
		sf, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer sf.Close()
		out = sf
	} else {
		fmt.Fprintln(os.Stderr, "GITHUB_STEP_SUMMARY not set — printing to stdout instead")
	}

	return writeTable(out, data, limits)
}

// aggregate groups measurements by cleaned operation label, returning the
// per-label elapsed samples (in seconds) and the per-label timeout limit.
func aggregate(measurements []testtiming.Measurement) (data map[string][]float64, limits map[string]float64) {
	data = map[string][]float64{}
	limits = map[string]float64{}
	for _, m := range measurements {
		label := cleanLabel(m.Op)
		data[label] = append(data[label], m.Elapsed.Seconds())
		limits[label] = m.Limit.Seconds()
	}
	return data, limits
}

func writeTable(out io.Writer, data map[string][]float64, limits map[string]float64) error {
	w := bufio.NewWriter(out)
	fmt.Fprint(w, "## Test Timing\n\n")
	fmt.Fprint(w, "| Operation | Timeout | mean ±99%CI | min | p50 | p95 | p99 | max |\n")
	fmt.Fprint(w, "|---|---|---|---|---|---|---|---|\n")

	labels := make([]string, 0, len(data))
	for label := range data {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	for _, label := range labels {
		vals := data[label]
		limit := limits[label]
		mean, margin := meanCI99(vals)

		// stat.Quantile requires sorted input; sort once and reuse for min/max.
		sorted := append([]float64(nil), vals...)
		sort.Float64s(sorted)
		minV, maxV := sorted[0], sorted[len(sorted)-1]
		p50 := stat.Quantile(0.50, stat.LinInterp, sorted, nil)
		p95 := stat.Quantile(0.95, stat.LinInterp, sorted, nil)
		p99 := stat.Quantile(0.99, stat.LinInterp, sorted, nil)

		circle := statusCircle(p95/limit*100, p99/limit*100)
		fmt.Fprintf(w, "| %s %s | %s | %s | %s | %s | %s | %s | %s |\n",
			circle, label, formatLimit(limit),
			formatCICell(mean, margin, limit, len(vals)),
			formatCell(minV, limit),
			formatCell(p50, limit),
			formatCell(p95, limit),
			formatCell(p99, limit),
			formatCell(maxV, limit),
		)
	}
	return w.Flush()
}

// formatLimit renders the timeout limit for the Timeout column, using Go's
// duration formatting (e.g. "30s", "1m0s").
func formatLimit(seconds float64) string {
	return time.Duration(seconds * float64(time.Second)).String()
}
