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

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRun(id int64, url string, t time.Time) *github.WorkflowRun {
	return &github.WorkflowRun{
		ID:        github.Ptr(id),
		HTMLURL:   github.Ptr(url),
		CreatedAt: &github.Timestamp{Time: t},
	}
}

func TestIsTestArtifact(t *testing.T) {
	cases := map[string]bool{
		"short-test-logs":       true,
		"race-test-logs":        true,
		"integration-test-logs": true,
		// coverage workflows omit --rerun-fails so they cannot produce flake
		// signal; their artifacts are correctly excluded.
		"coverage-direct":     false,
		"coverage-subprocess": false,
		"coverage-merged":     false,
		"some-other-artifact": false,
		"":                    false,
	}
	for name, want := range cases {
		assert.Equal(t, want, isTestArtifact(name), "isTestArtifact(%q)", name)
	}
}

// flakeFixture is a small gotestsum --jsonfile-style stream where TestFlake
// fails once and recovers, while TestSolid passes on the first attempt.
const flakeFixture = `{"Action":"run","Package":"pkg/a","Test":"TestSolid"}
{"Action":"pass","Package":"pkg/a","Test":"TestSolid","Elapsed":0.01}
{"Action":"run","Package":"pkg/a","Test":"TestFlake"}
{"Action":"fail","Package":"pkg/a","Test":"TestFlake","Elapsed":0.02}
{"Action":"fail","Package":"pkg/a","Elapsed":0.02}
{"Action":"run","Package":"pkg/a","Test":"TestFlake"}
{"Action":"pass","Package":"pkg/a","Test":"TestFlake","Elapsed":0.01}
{"Action":"pass","Package":"pkg/a","Elapsed":0.03}
`

func TestParseFlakesFromJSONL_FailThenPass(t *testing.T) {
	got, err := parseFlakesFromJSONL(strings.NewReader(flakeFixture))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "pkg/a", got[0].pkg)
	assert.Equal(t, "TestFlake", got[0].name)
}

func TestParseFlakesFromJSONL_SolidPassOnly(t *testing.T) {
	stream := `{"Action":"run","Package":"pkg/a","Test":"TestSolid"}
{"Action":"pass","Package":"pkg/a","Test":"TestSolid"}
`
	got, err := parseFlakesFromJSONL(strings.NewReader(stream))
	require.NoError(t, err)
	assert.Empty(t, got, "tests that only pass are not flakes")
}

func TestParseFlakesFromJSONL_FailWithoutRecovery(t *testing.T) {
	// A test that fails and never passes shouldn't be reported. In
	// practice the run conclusion would be failure and the API filter
	// would exclude it, but we want the parser itself to be safe even
	// if a stream is misclassified.
	stream := `{"Action":"run","Package":"pkg/a","Test":"TestBroken"}
{"Action":"fail","Package":"pkg/a","Test":"TestBroken"}
{"Action":"run","Package":"pkg/a","Test":"TestBroken"}
{"Action":"fail","Package":"pkg/a","Test":"TestBroken"}
`
	got, err := parseFlakesFromJSONL(strings.NewReader(stream))
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseFlakesFromJSONL_IgnoresPackageLevelEvents(t *testing.T) {
	// Package-level events have Test=="" and reflect rolled-up status.
	// Counting them would inflate the flake count.
	stream := `{"Action":"fail","Package":"pkg/a"}
{"Action":"pass","Package":"pkg/a"}
`
	got, err := parseFlakesFromJSONL(strings.NewReader(stream))
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseFlakesFromJSONL_ToleratesMalformedLines(t *testing.T) {
	stream := `not json
{"Action":"fail","Package":"pkg/a","Test":"TestFlake"}
{ this isn't valid either
{"Action":"pass","Package":"pkg/a","Test":"TestFlake"}
`
	got, err := parseFlakesFromJSONL(strings.NewReader(stream))
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

func TestParseFlakesFromJSONL_Empty(t *testing.T) {
	got, err := parseFlakesFromJSONL(strings.NewReader(""))
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseFlakesFromJSONL_MultipleFlakesAcrossPackages(t *testing.T) {
	stream := `{"Action":"fail","Package":"pkg/a","Test":"T1"}
{"Action":"pass","Package":"pkg/a","Test":"T1"}
{"Action":"fail","Package":"pkg/b","Test":"T2"}
{"Action":"pass","Package":"pkg/b","Test":"T2"}
{"Action":"pass","Package":"pkg/c","Test":"T3"}
`
	got, err := parseFlakesFromJSONL(strings.NewReader(stream))
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestAggregatorDedupesWithinRun(t *testing.T) {
	a := newAggregator()
	run := makeRun(42, "https://example.com/run/42", time.Now())
	k := testKey{pkg: "pkg/a", name: "TestX"}
	a.recordFailure(run, k)
	a.recordFailure(run, k) // same run, should not double-count
	got := a.filter(0)      // threshold 0 → include any test with >0 distinct runs
	require.Len(t, got, 1)
	assert.Equal(t, 1, got[0].Failures, "same run recorded twice should count once")
}

func TestAggregatorThreshold(t *testing.T) {
	a := newAggregator()
	k := testKey{pkg: "p", name: "T"}
	for i := int64(1); i <= 4; i++ {
		a.recordFailure(makeRun(i, "", time.Now()), k)
	}

	got := a.filter(3)
	require.Len(t, got, 1, "threshold>3 with 4 distinct runs should include test")
	assert.Equal(t, 4, got[0].Failures)

	assert.Empty(t, a.filter(4), "threshold>4 with 4 distinct runs should exclude test")
}

func TestAggregatorSortsDescending(t *testing.T) {
	a := newAggregator()
	low := testKey{pkg: "p", name: "Low"}
	high := testKey{pkg: "p", name: "High"}
	for i := int64(1); i <= 5; i++ {
		a.recordFailure(makeRun(i, "", time.Now()), high)
	}
	for i := int64(100); i <= 102; i++ {
		a.recordFailure(makeRun(i, "", time.Now()), low)
	}

	got := a.filter(0)
	require.Len(t, got, 2)
	assert.Equal(t, "High", got[0].Name, "highest-failure entry should sort first")
	assert.Equal(t, 5, got[0].Failures)
	assert.Equal(t, "Low", got[1].Name)
	assert.Equal(t, 3, got[1].Failures)
}

func TestFormatSlackPayload_Empty(t *testing.T) {
	p := formatSlackPayload(nil, 7, 3)
	assert.Contains(t, p.Text, "No flaky tests detected")
}

func TestFormatSlackPayload_NonEmpty(t *testing.T) {
	entries := []flakyEntry{
		{Pkg: "pkg/a", Name: "TestA", Failures: 7, LastSeen: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), LastRunURL: "https://example.com/1"},
		{Pkg: "pkg/b", Name: "TestB", Failures: 4, LastSeen: time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC), LastRunURL: "https://example.com/2"},
	}
	p := formatSlackPayload(entries, 7, 3)

	assert.Contains(t, p.Text, "pkg/a.TestA")
	assert.Contains(t, p.Text, "*7*")
	assert.Contains(t, p.Text, "pkg/b.TestB")
	assert.Contains(t, p.Text, "*4*")
	assert.Less(t, strings.Index(p.Text, "TestA"), strings.Index(p.Text, "TestB"),
		"highest-failure entry should appear first in the rendered payload")
}
