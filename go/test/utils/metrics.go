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

package utils

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ScrapeMetrics fetches the Prometheus text format from the given localhost
// port's /metrics endpoint and returns it as a string.
func ScrapeMetrics(t *testing.T, port int) string {
	t.Helper()

	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	resp, err := http.Get(url) //nolint:gosec // test-only code with localhost URL
	require.NoError(t, err, "failed to scrape metrics from %s", url)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "metrics endpoint returned non-200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read metrics response body")

	return string(body)
}

// MetricSample represents a single Prometheus metric sample.
type MetricSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// ParseMetrics parses Prometheus text format into a slice of MetricSamples.
func ParseMetrics(text string) []MetricSample {
	var samples []MetricSample
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		s := MetricSample{Labels: make(map[string]string)}

		// Split "metric_name{labels} value" or "metric_name value"
		nameEnd := strings.IndexAny(line, "{ ")
		if nameEnd == -1 {
			continue
		}
		s.Name = line[:nameEnd]

		rest := line[nameEnd:]
		if strings.HasPrefix(rest, "{") {
			labelEnd := strings.Index(rest, "}")
			if labelEnd == -1 {
				continue
			}
			labelStr := rest[1:labelEnd]
			for pair := range strings.SplitSeq(labelStr, ",") {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					s.Labels[kv[0]] = strings.Trim(kv[1], "\"")
				}
			}
			rest = rest[labelEnd+1:]
		}

		valStr := strings.TrimSpace(rest)
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		s.Value = val

		samples = append(samples, s)
	}
	return samples
}

// FindMetric returns the value of a metric matching name, where every key in
// labels must be present on the sample with that value (extra labels on the
// sample not mentioned in labels are ignored). Returns (value, true) if
// found, (0, false) if not.
func FindMetric(samples []MetricSample, name string, labels map[string]string) (float64, bool) {
	for _, s := range samples {
		if s.Name != name {
			continue
		}
		match := true
		for k, v := range labels {
			if s.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return s.Value, true
		}
	}
	return 0, false
}

// AssertMetricValue asserts that a metric has exactly the expected value.
func AssertMetricValue(t *testing.T, samples []MetricSample, name string, labels map[string]string, expected float64) {
	t.Helper()
	val, ok := FindMetric(samples, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found", name, labels)
		return
	}
	assert.Equal(t, expected, val, "metric %s%v", name, labels)
}

// AssertMetricGE asserts that a metric value is >= the expected minimum.
func AssertMetricGE(t *testing.T, samples []MetricSample, name string, labels map[string]string, minExpected float64) {
	t.Helper()
	val, ok := FindMetric(samples, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found", name, labels)
		return
	}
	assert.GreaterOrEqual(t, val, minExpected, "metric %s%v", name, labels)
}
